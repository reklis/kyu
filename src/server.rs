//! A STOMP 1.2 broker.
//!
//! Server-specific semantics (which STOMP leaves to implementations):
//! * destinations starting with `/topic/` are publish/subscribe: every current
//!   subscriber gets a copy, and messages with no subscribers are dropped;
//! * every other destination is a queue: each message goes to exactly one
//!   subscriber (round robin) and is held until someone subscribes;
//! * unacknowledged queue messages are redelivered when their subscription
//!   ends (UNSUBSCRIBE, DISCONNECT or connection failure);
//! * NACKed messages are discarded.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::io;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpSocket, TcpStream, ToSocketAddrs};
use tokio::sync::{Notify, mpsc};
use tokio_util::codec::Framed;

use crate::codec::{CodecError, Item, Limits, StompCodec};
use crate::frame::{Command, Frame};
use crate::transport::{Activity, Heartbeats, Tick, negotiate, parse_heartbeat};

pub const SUPPORTED_VERSIONS: &[&str] = &["1.2"];
const SERVER_NAME: &str = concat!("kyu/", env!("CARGO_PKG_VERSION"));

#[derive(Debug, Clone)]
pub struct ServerConfig {
    /// (sx, sy): the smallest interval (ms) at which the server can send
    /// heart-beats, and the interval at which it wants to receive them.
    pub heartbeat: (u32, u32),
    /// Required (login, passcode), if any.
    pub credentials: Option<(String, String)>,
    pub limits: Limits,
    /// TCP listen backlog (the kernel caps it at net.core.somaxconn).
    pub backlog: u32,
    /// Maximum unacknowledged messages per client/client-individual subscription.
    pub prefetch: usize,
    /// Maximum MESSAGE frames queued for one connection's socket writer.
    pub max_in_flight: usize,
    /// Pending messages a queue may hold before producers are throttled.
    pub max_queue_depth: usize,
    /// Per-subscriber buffer for topic messages while the subscriber is busy;
    /// the oldest messages are dropped beyond this.
    pub topic_backlog: usize,
    /// Number of independently locked broker shards (destinations are
    /// spread across them by hash).
    pub shards: usize,
}

impl Default for ServerConfig {
    fn default() -> Self {
        ServerConfig {
            heartbeat: (0, 0),
            credentials: None,
            limits: Limits::default(),
            backlog: 4096,
            prefetch: 1000,
            max_in_flight: 1024,
            max_queue_depth: 100_000,
            topic_backlog: 10_000,
            shards: std::thread::available_parallelism().map_or(8, |n| n.get() * 4),
        }
    }
}

pub struct Server {
    listener: TcpListener,
    config: Arc<ServerConfig>,
    broker: Arc<Broker>,
}

impl Server {
    pub async fn bind(addr: impl ToSocketAddrs, config: ServerConfig) -> io::Result<Server> {
        let mut last_err = None;
        for addr in tokio::net::lookup_host(addr).await? {
            match listen(addr, config.backlog) {
                Ok(listener) => {
                    let broker = Arc::new(Broker::new(&config));
                    return Ok(Server {
                        listener,
                        config: Arc::new(config),
                        broker,
                    });
                }
                Err(e) => last_err = Some(e),
            }
        }
        Err(last_err.unwrap_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "no addresses to bind")))
    }

    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.listener.local_addr()
    }

    /// Accepts connections forever.
    pub async fn run(self) {
        loop {
            let stream = match self.listener.accept().await {
                Ok((stream, _)) => stream,
                Err(_) => {
                    // e.g. out of file descriptors: back off instead of spinning
                    tokio::time::sleep(Duration::from_millis(10)).await;
                    continue;
                }
            };
            let _ = stream.set_nodelay(true);
            let broker = self.broker.clone();
            let config = self.config.clone();
            tokio::spawn(async move {
                Session::run(stream, broker, config).await;
            });
        }
    }
}

fn listen(addr: SocketAddr, backlog: u32) -> io::Result<TcpListener> {
    let socket = if addr.is_ipv4() { TcpSocket::new_v4()? } else { TcpSocket::new_v6()? };
    socket.set_reuseaddr(true)?;
    socket.bind(addr)?;
    socket.listen(backlog)
}

// ---------------------------------------------------------------------------
// Broker state
// ---------------------------------------------------------------------------

type ConnId = u64;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AckMode {
    Auto,
    Client,
    ClientIndividual,
}

#[derive(Debug, Clone)]
struct StoredMessage {
    id: String,
    destination: String,
    /// User-defined headers carried by the message.
    headers: Vec<(String, String)>,
    body: Bytes,
}

struct SubState {
    destination: String,
    ack: AckMode,
    /// Delivered but not yet acknowledged, keyed by delivery sequence.
    unacked: BTreeMap<u64, StoredMessage>,
    /// Topic messages waiting for this subscriber to have room (bounded).
    backlog: VecDeque<StoredMessage>,
}

/// What a session hands the broker so shards can deliver to it.
#[derive(Clone)]
struct ConnHandle {
    id: ConnId,
    tx: mpsc::UnboundedSender<Frame>,
    /// MESSAGE frames handed to the session but not yet written to the socket.
    in_flight: Arc<AtomicUsize>,
}

/// A connection's state within one shard.
struct ConnShard {
    handle: ConnHandle,
    subs: HashMap<String, SubState>,
    /// delivery sequence -> subscription id for every unacked message.
    ack_index: HashMap<u64, String>,
}

#[derive(Default)]
struct Destination {
    subscribers: Vec<(ConnId, String)>,
    next: usize,
    pending: VecDeque<StoredMessage>,
    /// Signalled when a full queue drains enough for producers to continue.
    space: Arc<Notify>,
    throttling: bool,
}

/// Flow-control limits, copied from the server config.
#[derive(Clone, Copy)]
struct FlowLimits {
    prefetch: usize,
    max_in_flight: usize,
    max_queue_depth: usize,
    topic_backlog: usize,
}

/// Broker state is split into shards by destination so that unrelated
/// destinations never contend for the same lock.
struct Broker {
    shards: Box<[Mutex<Shard>]>,
    flow: FlowLimits,
}

#[derive(Default)]
struct Shard {
    index: usize,
    conns: HashMap<ConnId, ConnShard>,
    destinations: HashMap<String, Destination>,
    seq: u64,
}

fn is_topic(destination: &str) -> bool {
    destination.starts_with("/topic/")
}

/// A queue a producer must wait on before sending more.
struct Throttle {
    destination: String,
    space: Arc<Notify>,
}

enum Op {
    Send(StoredMessage),
    /// (shard, delivery sequence) of the message being settled.
    Ack(usize, u64),
    Nack(usize, u64),
}

/// Ack ids are "<shard>-<sequence>", so ACK/NACK go straight to their shard.
fn parse_ack_id(id: &str, shards: usize) -> Option<(usize, u64)> {
    let (shard, seq) = id.split_once('-')?;
    let shard: usize = shard.parse().ok()?;
    (shard < shards).then_some((shard, seq.parse().ok()?))
}

impl Broker {
    fn new(config: &ServerConfig) -> Broker {
        let n = config.shards.max(1);
        Broker {
            shards: (0..n).map(|index| Mutex::new(Shard { index, ..Shard::default() })).collect(),
            flow: FlowLimits {
                prefetch: config.prefetch.max(1),
                max_in_flight: config.max_in_flight.max(1),
                max_queue_depth: config.max_queue_depth.max(1),
                topic_backlog: config.topic_backlog,
            },
        }
    }

    fn shard_of(&self, destination: &str) -> usize {
        use std::hash::{BuildHasher, BuildHasherDefault, DefaultHasher};
        let h = BuildHasherDefault::<DefaultHasher>::default().hash_one(destination);
        (h % self.shards.len() as u64) as usize
    }

    fn lock(&self, shard: usize) -> std::sync::MutexGuard<'_, Shard> {
        self.shards[shard].lock().unwrap()
    }

    fn subscribe(&self, conn: &ConnHandle, shard: usize, id: &str, destination: &str, ack: AckMode) {
        self.lock(shard).subscribe(&self.flow, conn, id, destination, ack);
    }

    fn unsubscribe(&self, conn: ConnId, shard: usize, id: &str) {
        let mut sh = self.lock(shard);
        sh.remove_sub(&self.flow, conn, id);
        if sh.conns.get(&conn).is_some_and(|c| c.subs.is_empty()) {
            sh.conns.remove(&conn);
        }
    }

    fn disconnect(&self, conn: ConnId, shards: impl IntoIterator<Item = usize>) {
        for shard in shards {
            let mut sh = self.lock(shard);
            let ids: Vec<String> = match sh.conns.get(&conn) {
                Some(c) => c.subs.keys().cloned().collect(),
                None => continue,
            };
            for id in ids {
                sh.remove_sub(&self.flow, conn, &id);
            }
            sh.conns.remove(&conn);
        }
    }

    /// Applies a batch of operations atomically, locking every shard involved
    /// (in index order). Returns a throttle if a queue sent to is now full.
    fn apply(&self, conn: ConnId, ops: Vec<Op>) -> Option<Throttle> {
        let shard_of = |op: &Op| match op {
            Op::Send(m) => self.shard_of(&m.destination),
            Op::Ack(s, _) | Op::Nack(s, _) => *s,
        };
        let mut throttle = None;
        let mut run = |sh: &mut Shard, op: Op| match op {
            Op::Send(msg) => {
                if let Some(t) = sh.publish(&self.flow, msg) {
                    throttle = Some(t);
                }
            }
            Op::Ack(_, seq) | Op::Nack(_, seq) => sh.settle(&self.flow, conn, seq),
        };
        if ops.len() == 1 {
            let op = ops.into_iter().next().unwrap();
            let mut sh = self.lock(shard_of(&op));
            run(&mut sh, op);
        } else {
            let mut guards: BTreeMap<usize, std::sync::MutexGuard<'_, Shard>> = BTreeMap::new();
            let shards: std::collections::BTreeSet<usize> = ops.iter().map(shard_of).collect();
            for s in shards {
                guards.insert(s, self.lock(s));
            }
            for op in ops {
                let sh = guards.get_mut(&shard_of(&op)).unwrap();
                run(sh, op);
            }
        }
        throttle
    }

    fn has_space(&self, destination: &str) -> bool {
        let sh = self.lock(self.shard_of(destination));
        sh.destinations.get(destination).is_none_or(|d| d.pending.len() < self.flow.max_queue_depth)
    }

    fn is_outstanding(&self, conn: ConnId, shard: usize, seq: u64) -> bool {
        self.lock(shard).conns.get(&conn).is_some_and(|c| c.ack_index.contains_key(&seq))
    }

    /// Called when a connection that hit its in-flight limit has room again.
    fn resume(&self, conn: ConnId, shards: impl IntoIterator<Item = usize>) {
        for shard in shards {
            let mut sh = self.lock(shard);
            let subs: Vec<String> = match sh.conns.get(&conn) {
                Some(c) => c.subs.keys().cloned().collect(),
                None => continue,
            };
            for sub in subs {
                sh.refill(&self.flow, conn, &sub);
            }
        }
    }
}

/// Whether a subscriber can take another message right now.
fn can_take(flow: &FlowLimits, c: &ConnShard, sub: &SubState) -> bool {
    c.handle.in_flight.load(Ordering::Relaxed) < flow.max_in_flight
        && (sub.ack == AckMode::Auto || sub.unacked.len() < flow.prefetch)
}

impl Shard {
    fn subscribe(&mut self, flow: &FlowLimits, conn: &ConnHandle, id: &str, destination: &str, ack: AckMode) {
        let c = self.conns.entry(conn.id).or_insert_with(|| ConnShard {
            handle: conn.clone(),
            subs: HashMap::new(),
            ack_index: HashMap::new(),
        });
        c.subs.insert(
            id.to_string(),
            SubState {
                destination: destination.to_string(),
                ack,
                unacked: BTreeMap::new(),
                backlog: VecDeque::new(),
            },
        );
        self.destinations
            .entry(destination.to_string())
            .or_default()
            .subscribers
            .push((conn.id, id.to_string()));
        self.dispatch(flow, destination);
    }

    fn remove_sub(&mut self, flow: &FlowLimits, conn: ConnId, id: &str) {
        let Some(c) = self.conns.get_mut(&conn) else { return };
        let Some(sub) = c.subs.remove(id) else { return };
        for seq in sub.unacked.keys() {
            c.ack_index.remove(seq);
        }
        let dest = self.destinations.get_mut(&sub.destination).expect("destination exists");
        dest.subscribers.retain(|(c, s)| !(*c == conn && s == id));
        if !is_topic(&sub.destination) {
            for (_, msg) in sub.unacked.into_iter().rev() {
                dest.pending.push_front(msg);
            }
        }
        self.dispatch(flow, &sub.destination);
    }

    fn publish(&mut self, flow: &FlowLimits, msg: StoredMessage) -> Option<Throttle> {
        let dest_name = msg.destination.clone();
        let dest = self.destinations.entry(dest_name.clone()).or_default();
        if is_topic(&dest_name) {
            let subs = dest.subscribers.clone();
            for (conn, sub) in subs {
                self.offer_topic(flow, conn, &sub, msg.clone());
            }
            None
        } else {
            dest.pending.push_back(msg);
            self.dispatch(flow, &dest_name);
            let dest = self.destinations.get_mut(&dest_name).unwrap();
            if dest.pending.len() >= flow.max_queue_depth {
                dest.throttling = true;
                return Some(Throttle { destination: dest_name, space: dest.space.clone() });
            }
            None
        }
    }

    fn offer_topic(&mut self, flow: &FlowLimits, conn: ConnId, sub_id: &str, msg: StoredMessage) {
        let (index, seq) = (self.index, &mut self.seq);
        let Some(c) = self.conns.get_mut(&conn) else { return };
        let Some(sub) = c.subs.get(sub_id) else { return };
        if sub.backlog.is_empty() && can_take(flow, c, sub) {
            deliver(index, seq, c, sub_id, msg);
        } else if flow.topic_backlog > 0 {
            let sub = c.subs.get_mut(sub_id).unwrap();
            if sub.backlog.len() >= flow.topic_backlog {
                sub.backlog.pop_front();
            }
            sub.backlog.push_back(msg);
        }
    }

    /// Hands pending queue messages to subscribers with room, round robin.
    fn dispatch(&mut self, flow: &FlowLimits, dest_name: &str) {
        let Shard { index, conns, destinations, seq } = self;
        let Some(dest) = destinations.get_mut(dest_name) else { return };
        while !dest.pending.is_empty() && !dest.subscribers.is_empty() {
            let n = dest.subscribers.len();
            let mut chosen = None;
            for i in 0..n {
                let idx = (dest.next + i) % n;
                let (conn, sub_id) = &dest.subscribers[idx];
                if let Some(c) = conns.get(conn)
                    && let Some(sub) = c.subs.get(sub_id)
                    && can_take(flow, c, sub)
                {
                    chosen = Some(idx);
                    break;
                }
            }
            let Some(idx) = chosen else { break };
            dest.next = idx + 1;
            let (conn, sub_id) = &dest.subscribers[idx];
            let msg = dest.pending.pop_front().unwrap();
            deliver(*index, seq, conns.get_mut(conn).unwrap(), sub_id, msg);
        }
        if dest.throttling && dest.pending.len() < flow.max_queue_depth {
            dest.throttling = false;
            dest.space.notify_waiters();
        }
    }

    /// Gives a subscriber that gained room as much as it can now take.
    fn refill(&mut self, flow: &FlowLimits, conn: ConnId, sub_id: &str) {
        let (index, seq) = (self.index, &mut self.seq);
        let Some(c) = self.conns.get_mut(&conn) else { return };
        let Some(sub) = c.subs.get(sub_id) else { return };
        if is_topic(&sub.destination) {
            loop {
                let sub = c.subs.get(sub_id).unwrap();
                if sub.backlog.is_empty() || !can_take(flow, c, sub) {
                    break;
                }
                let msg = c.subs.get_mut(sub_id).unwrap().backlog.pop_front().unwrap();
                deliver(index, seq, c, sub_id, msg);
            }
        } else {
            let dest = sub.destination.clone();
            self.dispatch(flow, &dest);
        }
    }

    /// Removes an acknowledged (or NACKed) message, and for `client` mode
    /// every earlier message on the same subscription; then refills it.
    fn settle(&mut self, flow: &FlowLimits, conn: ConnId, seq: u64) {
        let Some(c) = self.conns.get_mut(&conn) else { return };
        let Some(sub_id) = c.ack_index.remove(&seq) else { return };
        let Some(sub) = c.subs.get_mut(&sub_id) else { return };
        match sub.ack {
            AckMode::Client => {
                let later = sub.unacked.split_off(&(seq + 1));
                let settled = std::mem::replace(&mut sub.unacked, later);
                for earlier in settled.keys() {
                    c.ack_index.remove(earlier);
                }
            }
            _ => {
                sub.unacked.remove(&seq);
            }
        }
        self.refill(flow, conn, &sub_id);
    }
}

fn deliver(shard: usize, seq: &mut u64, c: &mut ConnShard, sub_id: &str, msg: StoredMessage) {
    let Some(sub) = c.subs.get_mut(sub_id) else { return };
    let mut frame = Frame::new(Command::Message)
        .with_header("subscription", sub_id)
        .with_header("message-id", msg.id.clone())
        .with_header("destination", msg.destination.clone());
    if sub.ack != AckMode::Auto {
        *seq += 1;
        frame = frame.with_header("ack", format!("{shard}-{seq}"));
        sub.unacked.insert(*seq, msg.clone());
        c.ack_index.insert(*seq, sub_id.to_string());
    }
    frame.headers.extend(msg.headers);
    frame.body = msg.body;
    c.handle.in_flight.fetch_add(1, Ordering::Relaxed);
    let _ = c.handle.tx.send(frame);
}

// ---------------------------------------------------------------------------
// Sessions
// ---------------------------------------------------------------------------

/// A protocol error to report with an ERROR frame before closing.
struct Fatal {
    message: String,
    detail: Option<String>,
    extra: Vec<(String, String)>,
}

impl Fatal {
    fn new(message: impl Into<String>) -> Fatal {
        Fatal { message: message.into(), detail: None, extra: Vec::new() }
    }
    fn detail(mut self, d: impl Into<String>) -> Fatal {
        self.detail = Some(d.into());
        self
    }
    fn header(mut self, k: &str, v: &str) -> Fatal {
        self.extra.push((k.to_string(), v.to_string()));
        self
    }
}

fn required<'a>(frame: &'a Frame, name: &str) -> Result<&'a str, Fatal> {
    frame
        .header(name)
        .ok_or_else(|| Fatal::new(format!("missing required header '{name}'")).detail(format!(
            "{} frames require a '{name}' header",
            frame.command.as_str()
        )))
}

type Conn = Framed<Activity<TcpStream>, StompCodec>;

static NEXT_CONN: AtomicU64 = AtomicU64::new(0);

struct Session {
    handle: ConnHandle,
    broker: Arc<Broker>,
    /// subscription id -> shard holding it
    subs: HashMap<String, usize>,
    /// per-session counter for message ids
    messages: u64,
    transactions: HashMap<String, Vec<Op>>,
    /// Set while a queue this connection sent to is full.
    throttle: Option<Throttle>,
}

enum Flow {
    Continue,
    Close,
}

impl Session {
    async fn run(stream: TcpStream, broker: Arc<Broker>, config: Arc<ServerConfig>) {
        let mut conn: Conn = Framed::new(
            Activity::new(stream),
            StompCodec::with_limits(config.limits),
        );

        // --- handshake ---
        let connect = loop {
            match conn.next().await {
                Some(Ok(Item::Heartbeat)) => continue,
                Some(Ok(Item::Frame(f))) => break f,
                Some(Err(e)) => return close_with_error(conn, Fatal::new(e.to_string())).await,
                None => return,
            }
        };
        let (hb_out, hb_in) = match handshake(&connect, &config) {
            Ok(v) => v,
            Err(fatal) => return close_with_error(conn, fatal).await,
        };

        let id = NEXT_CONN.fetch_add(1, Ordering::Relaxed);
        let connected = Frame::new(Command::Connected)
            .with_header("version", "1.2")
            .with_header("session", format!("session-{id}"))
            .with_header("server", SERVER_NAME)
            .with_header("heart-beat", format!("{},{}", config.heartbeat.0, config.heartbeat.1));
        if conn.send(connected).await.is_err() {
            return;
        }

        let (tx, mut rx) = mpsc::unbounded_channel();
        let handle = ConnHandle { id, tx, in_flight: Arc::new(AtomicUsize::new(0)) };
        let mut session = Session {
            handle,
            broker: broker.clone(),
            transactions: HashMap::new(),
            throttle: None,
            subs: HashMap::new(),
            messages: 0,
        };

        // --- frame loop ---
        let mut hb = Heartbeats::new(hb_out, hb_in, conn.get_ref().last.clone());
        let outcome: Result<(), Fatal> = loop {
            tokio::select! {
                // While a queue we sent to is full, stop reading (TCP pushes back).
                () = wait_for_space(&broker, session.throttle.as_ref()), if session.throttle.is_some() => {
                    let t = session.throttle.as_ref().unwrap();
                    if broker.has_space(&t.destination) {
                        session.throttle = None;
                    }
                }
                item = conn.next(), if session.throttle.is_none() => match item {
                    Some(Ok(Item::Heartbeat)) => {}
                    Some(Ok(Item::Frame(frame))) => {
                        let receipt = frame.header("receipt").map(str::to_string);
                        match session.handle(frame) {
                            Ok(flow) => {
                                // Deliveries caused by this or earlier frames go out first.
                                match flush_queued(&mut conn, &mut rx).await {
                                    Ok(n) => session.written(n),
                                    Err(_) => break Ok(()),
                                }
                                if let Some(r) = receipt {
                                    let rf = Frame::new(Command::Receipt).with_header("receipt-id", r);
                                    if conn.send(rf).await.is_err() { break Ok(()); }
                                    hb.wrote();
                                }
                                if let Flow::Close = flow {
                                    break Ok(());
                                }
                            }
                            Err(mut fatal) => {
                                if let Some(r) = receipt {
                                    fatal.extra.push(("receipt-id".into(), r));
                                }
                                break Err(fatal);
                            }
                        }
                    }
                    Some(Err(e)) => break Err(Fatal::new(e.to_string())),
                    None => break Ok(()),
                },
                Some(frame) = rx.recv() => {
                    if conn.feed(frame).await.is_err() { break Ok(()); }
                    match flush_queued(&mut conn, &mut rx).await {
                        Ok(n) => session.written(n + 1),
                        Err(_) => break Ok(()),
                    }
                    hb.wrote();
                }
                tick = hb.tick() => match tick {
                    Tick::SendHeartbeat => {
                        if conn.send(Item::Heartbeat).await.is_err() { break Ok(()); }
                        hb.wrote();
                    }
                    Tick::PeerDead => break Ok(()),
                },
            }
        };

        // Any transactions still open are implicitly aborted.
        session.transactions.clear();
        broker.disconnect(id, session.shards());
        let _ = flush_queued(&mut conn, &mut rx).await;
        match outcome {
            Err(fatal) => close_with_error(conn, fatal).await,
            Ok(()) => linger_close(conn).await,
        }
    }

    fn handle(&mut self, frame: Frame) -> Result<Flow, Fatal> {
        if !frame.body.is_empty() && !frame.command.allows_body() {
            return Err(Fatal::new(format!("{} frames must not have a body", frame.command.as_str())));
        }
        match frame.command {
            Command::Send => {
                let destination = required(&frame, "destination")?.to_string();
                let tx = frame.header("transaction").map(str::to_string);
                let headers = frame
                    .headers
                    .iter()
                    .filter(|(k, _)| {
                        !matches!(k.as_str(), "destination" | "transaction" | "receipt" | "content-length")
                    })
                    .cloned()
                    .collect();
                self.messages += 1;
                let msg = StoredMessage {
                    id: format!("{}-{}", self.handle.id, self.messages),
                    destination,
                    headers,
                    body: frame.body,
                };
                self.submit(tx, Op::Send(msg))?;
            }
            Command::Subscribe => {
                let destination = required(&frame, "destination")?;
                let id = required(&frame, "id")?;
                let ack = match frame.header("ack").unwrap_or("auto") {
                    "auto" => AckMode::Auto,
                    "client" => AckMode::Client,
                    "client-individual" => AckMode::ClientIndividual,
                    other => return Err(Fatal::new(format!("invalid ack mode '{other}'"))),
                };
                if self.subs.contains_key(id) {
                    return Err(Fatal::new(format!("subscription id '{id}' already in use")));
                }
                let shard = self.broker.shard_of(destination);
                self.broker.subscribe(&self.handle, shard, id, destination, ack);
                self.subs.insert(id.to_string(), shard);
            }
            Command::Unsubscribe => {
                let id = required(&frame, "id")?;
                let shard = self
                    .subs
                    .remove(id)
                    .ok_or_else(|| Fatal::new(format!("no subscription with id '{id}'")))?;
                self.broker.unsubscribe(self.handle.id, shard, id);
            }
            Command::Ack | Command::Nack => {
                let ack_id = required(&frame, "id")?;
                let outstanding = parse_ack_id(ack_id, self.broker.shards.len())
                    .filter(|&(shard, seq)| self.broker.is_outstanding(self.handle.id, shard, seq));
                let Some((shard, seq)) = outstanding else {
                    return Err(Fatal::new(format!("no outstanding message with ack id '{ack_id}'")));
                };
                let tx = frame.header("transaction").map(str::to_string);
                let op = if frame.command == Command::Ack { Op::Ack(shard, seq) } else { Op::Nack(shard, seq) };
                self.submit(tx, op)?;
            }
            Command::Begin => {
                let tx = required(&frame, "transaction")?;
                if self.transactions.contains_key(tx) {
                    return Err(Fatal::new(format!("transaction '{tx}' already in progress")));
                }
                self.transactions.insert(tx.to_string(), Vec::new());
            }
            Command::Commit => {
                let tx = required(&frame, "transaction")?;
                let ops = self.take_tx(tx)?;
                self.throttle = self.broker.apply(self.handle.id, ops).or(self.throttle.take());
            }
            Command::Abort => {
                let tx = required(&frame, "transaction")?;
                self.take_tx(tx)?;
            }
            Command::Disconnect => return Ok(Flow::Close),
            Command::Connect | Command::Stomp => return Err(Fatal::new("already connected")),
            ref other => {
                return Err(Fatal::new(format!("unsupported command '{}'", other.as_str())));
            }
        }
        Ok(Flow::Continue)
    }

    fn submit(&mut self, tx: Option<String>, op: Op) -> Result<(), Fatal> {
        match tx {
            None => {
                if let Some(t) = self.broker.apply(self.handle.id, vec![op]) {
                    self.throttle = Some(t);
                }
            }
            Some(tx) => self
                .transactions
                .get_mut(&tx)
                .ok_or_else(|| Fatal::new(format!("unknown transaction '{tx}'")))?
                .push(op),
        }
        Ok(())
    }

    /// Shards this connection has subscriptions in.
    fn shards(&self) -> Vec<usize> {
        let mut v: Vec<usize> = self.subs.values().copied().collect();
        v.sort_unstable();
        v.dedup();
        v
    }

    /// Counts frames written to the socket and tells the broker when a
    /// connection that was at its in-flight limit has room again.
    fn written(&self, n: usize) {
        if n > 0 && self.handle.in_flight.fetch_sub(n, Ordering::Relaxed) >= self.broker.flow.max_in_flight {
            self.broker.resume(self.handle.id, self.shards());
        }
    }

    fn take_tx(&mut self, tx: &str) -> Result<Vec<Op>, Fatal> {
        self.transactions.remove(tx).ok_or_else(|| Fatal::new(format!("unknown transaction '{tx}'")))
    }
}

/// Validates CONNECT/STOMP; returns negotiated (outgoing, incoming) heart-beats.
fn handshake(f: &Frame, config: &ServerConfig) -> Result<(Option<Duration>, Option<Duration>), Fatal> {
    if !matches!(f.command, Command::Connect | Command::Stomp) {
        return Err(Fatal::new("expected CONNECT or STOMP frame"));
    }
    if !f.body.is_empty() {
        return Err(Fatal::new("CONNECT frames must not have a body"));
    }

    let offered = f.header("accept-version").unwrap_or("1.0");
    let common = offered.split(',').any(|v| SUPPORTED_VERSIONS.contains(&v));
    if !common {
        let supported = SUPPORTED_VERSIONS.join(",");
        return Err(Fatal::new("unsupported protocol version")
            .header("version", &supported)
            .detail(format!("Supported protocol versions are {}", SUPPORTED_VERSIONS.join(" "))));
    }
    if f.header("host").is_none() {
        return Err(Fatal::new("missing required header 'host'"));
    }
    if let Some((login, passcode)) = &config.credentials
        && (f.header("login") != Some(login) || f.header("passcode") != Some(passcode)) {
            return Err(Fatal::new("authentication failed"));
        }
    let (cx, cy) = parse_heartbeat(f.header("heart-beat"))
        .ok_or_else(|| Fatal::new("invalid heart-beat header"))?;
    let (sx, sy) = config.heartbeat;
    Ok((negotiate(sx, cy), negotiate(cx, sy)))
}

/// Writes the frames already queued for this connection (plus anything
/// previously fed) with a single flush per batch. Returns how many it took
/// from the queue.
async fn flush_queued(conn: &mut Conn, rx: &mut mpsc::UnboundedReceiver<Frame>) -> Result<usize, CodecError> {
    const BATCH: usize = 256;
    let mut total = 0;
    loop {
        let mut n = 0;
        while n < BATCH {
            let Ok(frame) = rx.try_recv() else { break };
            conn.feed(frame).await?;
            n += 1;
        }
        SinkExt::<Frame>::flush(conn).await?;
        total += n;
        if n < BATCH {
            return Ok(total);
        }
    }
}

/// Waits until a full queue may have room (re-checked by the caller).
async fn wait_for_space(broker: &Broker, throttle: Option<&Throttle>) {
    let Some(t) = throttle else { return };
    let notified = t.space.notified();
    tokio::pin!(notified);
    notified.as_mut().enable();
    if broker.has_space(&t.destination) {
        return;
    }
    let _ = tokio::time::timeout(Duration::from_millis(100), notified).await;
}

async fn close_with_error(mut conn: Conn, fatal: Fatal) {
    let mut frame = Frame::new(Command::Error).with_header("message", fatal.message);
    frame.headers.extend(fatal.extra);
    if let Some(detail) = fatal.detail {
        frame = frame.with_header("content-type", "text/plain").with_body(detail);
    }
    let _ = conn.send(frame).await;
    linger_close(conn).await;
}

/// Closes our side, then briefly drains the socket so the peer gets our last
/// frame rather than a reset.
async fn linger_close(conn: Conn) {
    let mut stream = conn.into_inner().inner;
    let _ = stream.shutdown().await;
    let mut buf = [0u8; 4096];
    let _ = tokio::time::timeout(Duration::from_millis(500), async {
        use tokio::io::AsyncReadExt;
        while let Ok(n) = stream.read(&mut buf).await {
            if n == 0 {
                break;
            }
        }
    })
    .await;
}
