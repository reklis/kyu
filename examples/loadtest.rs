//! End-to-end load test for kyu.
//!
//!   cargo run --release --example loadtest                 # all scenarios, in-process server
//!   cargo run --release --example loadtest -- queue fanout # pick scenarios
//!   cargo run --release --example loadtest -- --addr 127.0.0.1:61613
//!
//! Scenarios: rtt, queue, parallel, ack, fanout, connections
//! Latency is measured from just before SEND to MESSAGE arrival, using a
//! timestamp header (so producers and consumers must share a process).

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use kyu::client::{AckMode, Client, ConnectOptions, OutgoingMessage};
use kyu::server::{Server, ServerConfig};
use tokio::sync::Mutex;
use tokio::task::JoinSet;

static RUN: AtomicU64 = AtomicU64::new(0);

fn unique(prefix: &str) -> String {
    format!("{prefix}-{}", RUN.fetch_add(1, Ordering::Relaxed))
}

fn opts() -> ConnectOptions {
    ConnectOptions::new("localhost")
}

#[derive(Default)]
struct Latencies(Vec<u64>);

impl Latencies {
    fn report(&mut self) -> String {
        if self.0.is_empty() {
            return String::new();
        }
        self.0.sort_unstable();
        let pct = |p: f64| {
            let i = ((self.0.len() as f64 * p) as usize).min(self.0.len() - 1);
            Duration::from_nanos(self.0[i])
        };
        format!("p50 {:>9.2?}  p99 {:>9.2?}  max {:>9.2?}", pct(0.50), pct(0.99), pct(1.0))
    }
}

fn rate(n: u64, elapsed: Duration) -> f64 {
    n as f64 / elapsed.as_secs_f64()
}

fn human(n: f64) -> String {
    if n >= 1e6 { format!("{:.2}M", n / 1e6) } else if n >= 1e3 { format!("{:.1}k", n / 1e3) } else { format!("{n:.0}") }
}

/// P producers -> one queue -> C consumers.
async fn queue(addr: SocketAddr, producers: usize, consumers: usize, size: usize, total: u64, ack: AckMode) {
    let dest = format!("/queue/{}", unique("bench"));
    let epoch = Instant::now();
    let received = Arc::new(AtomicU64::new(0));
    let lat = Arc::new(Mutex::new(Latencies::default()));

    let mut cons = JoinSet::new();
    let mut subs = Vec::new();
    for _ in 0..consumers {
        let c = Client::connect(addr, opts()).await.unwrap();
        let sub = c.subscribe(&dest, ack).await.unwrap();
        subs.push((c, sub));
    }
    for (c, mut sub) in subs {
        let received = received.clone();
        let lat = lat.clone();
        cons.spawn(async move {
            let mut local = Vec::new();
            while received.load(Ordering::Relaxed) < total {
                let Ok(Some(m)) = tokio::time::timeout(Duration::from_millis(500), sub.recv()).await else {
                    continue;
                };
                let sent: u64 = m.header("x-t").unwrap().parse().unwrap();
                local.push(epoch.elapsed().as_nanos() as u64 - sent);
                if ack != AckMode::Auto {
                    c.ack(&m).await.unwrap();
                }
                received.fetch_add(1, Ordering::Relaxed);
            }
            lat.lock().await.0.extend(local);
            c.disconnect().await.ok();
        });
    }

    let body = bytes::Bytes::from(vec![b'x'; size]);
    let per = total / producers as u64;
    let start = Instant::now();
    let mut prods = JoinSet::new();
    for _ in 0..producers {
        let dest = dest.clone();
        let body = body.clone();
        prods.spawn(async move {
            let c = Client::connect(addr, opts()).await.unwrap();
            for _ in 0..per {
                let t = epoch.elapsed().as_nanos() as u64;
                c.send(OutgoingMessage::new(&dest).header("x-t", t.to_string()).body(body.clone()))
                    .await
                    .unwrap();
            }
            c.disconnect().await.ok();
        });
    }
    prods.join_all().await;
    let sent_in = start.elapsed();
    cons.join_all().await;
    let elapsed = start.elapsed();

    let n = per * producers as u64;
    println!(
        "  {producers:>3}P x {consumers:>3}C  {size:>6}B  {:<17}  send {:>7}/s  deliver {:>7}/s  {:>8.1} MB/s   {}",
        format!("{ack:?}"),
        human(rate(n, sent_in)),
        human(rate(n, elapsed)),
        rate(n * size as u64, elapsed) / 1e6,
        lat.lock().await.report()
    );
}

/// N independent producer/consumer pairs, each on its own queue: shows how
/// throughput scales when destinations don't contend with each other.
async fn parallel(addr: SocketAddr, pairs: usize, per_pair: u64) {
    let mut ready = Vec::new();
    for _ in 0..pairs {
        let dest = format!("/queue/{}", unique("par"));
        let c = Client::connect(addr, opts()).await.unwrap();
        let sub = c.subscribe(&dest, AckMode::Auto).await.unwrap();
        let p = Client::connect(addr, opts()).await.unwrap();
        ready.push((dest, c, sub, p));
    }
    let body = bytes::Bytes::from(vec![b'x'; 128]);
    let start = Instant::now();
    let mut set = JoinSet::new();
    for (dest, c, mut sub, p) in ready {
        let body = body.clone();
        set.spawn(async move {
            for _ in 0..per_pair {
                sub.recv().await.unwrap();
            }
            c.disconnect().await.ok();
        });
        set.spawn(async move {
            for _ in 0..per_pair {
                p.send(OutgoingMessage::new(&dest).body(body.clone())).await.unwrap();
            }
            p.disconnect().await.ok();
        });
    }
    set.join_all().await;
    let n = per_pair * pairs as u64;
    println!("  {pairs:>3} pairs x {per_pair} msgs   deliver {:>7}/s", human(rate(n, start.elapsed())));
}

/// One producer -> topic with N subscribers (each on its own connection).
async fn fanout(addr: SocketAddr, subscribers: usize, messages: u64) {
    let dest = format!("/topic/{}", unique("fan"));
    let epoch = Instant::now();
    let lat = Arc::new(Mutex::new(Latencies::default()));
    let mut set = JoinSet::new();
    let mut subs = Vec::new();
    for _ in 0..subscribers {
        let c = Client::connect(addr, opts()).await.unwrap();
        let sub = c.subscribe(&dest, AckMode::Auto).await.unwrap();
        subs.push((c, sub));
    }
    for (c, mut sub) in subs {
        let lat = lat.clone();
        set.spawn(async move {
            let mut local = Vec::with_capacity(messages as usize);
            for _ in 0..messages {
                let m = sub.recv().await.unwrap();
                let sent: u64 = m.header("x-t").unwrap().parse().unwrap();
                local.push(epoch.elapsed().as_nanos() as u64 - sent);
            }
            lat.lock().await.0.extend(local);
            c.disconnect().await.ok();
        });
    }
    let p = Client::connect(addr, opts()).await.unwrap();
    let start = Instant::now();
    for _ in 0..messages {
        let t = epoch.elapsed().as_nanos() as u64;
        p.send(OutgoingMessage::new(&dest).header("x-t", t.to_string()).body("x".repeat(128))).await.unwrap();
    }
    set.join_all().await;
    let elapsed = start.elapsed();
    println!(
        "  1P -> {subscribers:>4} subs  {messages:>6} msgs  deliveries {:>7}/s   {}",
        human(rate(messages * subscribers as u64, elapsed)),
        lat.lock().await.report()
    );
}

/// N clients connect concurrently, then each does K request/response round
/// trips (send_confirmed to its own queue, then receive the message) at once.
async fn connections(addr: SocketAddr, clients: usize, rounds: usize) {
    let start = Instant::now();
    let mut set = JoinSet::new();
    for _ in 0..clients {
        set.spawn(async move { Client::connect(addr, opts()).await });
    }
    let mut cs = Vec::new();
    let mut failed = 0;
    while let Some(r) = set.join_next().await {
        match r.unwrap() {
            Ok(c) => cs.push(c),
            Err(_) => failed += 1,
        }
    }
    let connect_time = start.elapsed();

    let lat = Arc::new(Mutex::new(Latencies::default()));
    let barrier = Arc::new(tokio::sync::Barrier::new(cs.len()));
    let start = Instant::now();
    let mut set = JoinSet::new();
    for c in cs {
        let lat = lat.clone();
        let barrier = barrier.clone();
        set.spawn(async move {
            let dest = format!("/queue/{}", unique("conn"));
            let mut sub = c.subscribe(&dest, AckMode::Auto).await.unwrap();
            barrier.wait().await;
            let mut local = Vec::with_capacity(rounds);
            for _ in 0..rounds {
                let t = Instant::now();
                c.send_confirmed(OutgoingMessage::new(&dest).body("ping")).await.unwrap();
                sub.recv().await.unwrap();
                local.push(t.elapsed().as_nanos() as u64);
            }
            lat.lock().await.0.extend(local);
            c
        });
    }
    let cs: Vec<Client> = set.join_all().await;
    let elapsed = start.elapsed();
    let n = cs.len();
    let mut set = JoinSet::new();
    for c in cs {
        set.spawn(async move { c.disconnect().await.ok() });
    }
    set.join_all().await;
    println!(
        "  {clients:>5} clients  connect {:>9.2?} ({:>6}/s, {failed} failed)  {:>7} rtt/s   {}",
        connect_time,
        human(rate(n as u64, connect_time)),
        human(rate((n * rounds) as u64, elapsed)),
        lat.lock().await.report()
    );
}

/// Sequential round trip latency on a single connection.
async fn rtt(addr: SocketAddr, rounds: usize) {
    let c = Client::connect(addr, opts()).await.unwrap();
    let dest = format!("/queue/{}", unique("rtt"));
    let mut sub = c.subscribe(&dest, AckMode::Auto).await.unwrap();
    let mut lat = Latencies::default();
    let start = Instant::now();
    for _ in 0..rounds {
        let t = Instant::now();
        c.send(OutgoingMessage::new(&dest).body("ping")).await.unwrap();
        sub.recv().await.unwrap();
        lat.0.push(t.elapsed().as_nanos() as u64);
    }
    println!("  {rounds} sequential send->message round trips: {:>7}/s   {}", human(rate(rounds as u64, start.elapsed())), lat.report());
    c.disconnect().await.ok();
}

#[tokio::main]
async fn main() {
    let mut args: Vec<String> = std::env::args().skip(1).collect();
    let addr = match args.iter().position(|a| a == "--addr") {
        Some(i) => {
            let a = args.remove(i + 1).parse().expect("--addr host:port");
            args.remove(i);
            a
        }
        None => {
            let server = Server::bind("127.0.0.1:0", ServerConfig::default()).await.unwrap();
            let addr = server.local_addr().unwrap();
            tokio::spawn(server.run());
            addr
        }
    };
    let all = args.is_empty();
    let want = |s: &str| all || args.iter().any(|a| a == s);
    println!("kyu load test against {addr} ({} worker threads)", std::thread::available_parallelism().unwrap());

    if want("rtt") {
        println!("\n[rtt] single-connection latency");
        rtt(addr, 20_000).await;
    }
    if want("queue") {
        println!("\n[queue] producers -> queue -> consumers");
        for (p, c, size, n) in [
            (1, 1, 128, 200_000),
            (4, 4, 128, 400_000),
            (16, 16, 128, 400_000),
            (64, 64, 128, 400_000),
            (1, 1, 16 * 1024, 50_000),
            (4, 4, 16 * 1024, 100_000),
            (1, 1, 1024 * 1024, 1_000),
        ] {
            queue(addr, p, c, size, n, AckMode::Auto).await;
        }
    }
    if want("parallel") {
        println!("\n[parallel] independent queues, one producer and consumer each");
        for pairs in [1, 2, 4, 8, 16] {
            parallel(addr, pairs, 200_000).await;
        }
    }
    if want("ack") {
        println!("\n[ack] queue with explicit acknowledgement");
        queue(addr, 1, 1, 128, 100_000, AckMode::ClientIndividual).await;
        queue(addr, 4, 4, 128, 200_000, AckMode::ClientIndividual).await;
        queue(addr, 4, 4, 128, 200_000, AckMode::Client).await;
    }
    if want("fanout") {
        println!("\n[fanout] topic broadcast");
        for (subs, msgs) in [(10, 20_000), (100, 5_000), (1000, 500)] {
            fanout(addr, subs, msgs).await;
        }
    }
    if want("connections") {
        println!("\n[connections] concurrent clients, 10 round trips each");
        for n in [100, 1_000, 5_000, 10_000] {
            connections(addr, n, 10).await;
        }
    }
}
