//! A STOMP 1.2 broker.
//!
//! Server-specific semantics (which STOMP leaves to implementations):
//! * destinations starting with `/topic/` are publish/subscribe: every current
//!   subscriber gets a copy, and messages with no subscribers are dropped;
//! * every other destination is a queue: each message goes to exactly one
//!   subscriber (round robin) and is held until someone subscribes;
//! * unacknowledged queue messages are redelivered when their subscription
//!   ends (UNSUBSCRIBE, DISCONNECT or connection failure);
//! * messages rejected with NACK are discarded.

use std::collections::{BTreeMap, VecDeque};
use std::io;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::{BufMut, Bytes, BytesMut};
use futures::{FutureExt, SinkExt, StreamExt};
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpSocket, TcpStream, ToSocketAddrs};
use tokio::sync::{Notify, mpsc};
use tokio_util::codec::{Encoder, Framed};

use crate::codec::{CodecError, Item, Limits, StompCodec, escape_into, put_u64, put_usize};
use crate::frame::{Command, Frame, Str};
use crate::transport::{Activity, Heartbeats, Tick, negotiate, parse_heartbeat};

/// Hash maps keyed by client-chosen strings (destinations, subscription
/// ids): foldhash is several times faster than `SipHash` and still randomly
/// seeded per process, so clients cannot precompute colliding keys.
type HashMap<K, V> = std::collections::HashMap<K, V, foldhash::fast::RandomState>;

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
    /// Binds the listening socket (the first address that works).
    ///
    /// # Errors
    /// If no address resolves or none can be bound.
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
        Err(last_err
            .unwrap_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "no addresses to bind")))
    }

    /// The address actually bound (useful after binding port 0).
    ///
    /// # Errors
    /// As for [`TcpListener::local_addr`].
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.listener.local_addr()
    }

    /// Accepts connections forever.
    pub async fn run(self) {
        loop {
            let Ok((stream, _)) = self.listener.accept().await else {
                // e.g. out of file descriptors: back off instead of spinning
                tokio::time::sleep(Duration::from_millis(10)).await;
                continue;
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
    let socket = if addr.is_ipv4() {
        TcpSocket::new_v4()?
    } else {
        TcpSocket::new_v6()?
    };
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

/// A published message. Everything a MESSAGE frame carries apart from the
/// per-subscription `subscription` and `ack` headers is encoded once, at
/// SEND time, and shared by every delivery.
struct StoredMessage {
    destination: Str,
    /// Encoded `message-id`, `destination`, user headers, `content-length`
    /// and the blank line that ends the headers.
    head: Bytes,
    body: Bytes,
}

type Msg = Arc<StoredMessage>;

impl StoredMessage {
    /// Encodes a SEND frame's headers for delivery. Server headers come
    /// first, so a producer cannot override them (the first value wins).
    fn from_send(id: (ConnId, u64), destination: Str, frame: Frame) -> StoredMessage {
        let user = |k: &str| {
            !matches!(
                k,
                "destination" | "transaction" | "receipt" | "content-length"
            )
        };
        let len: usize = frame
            .headers
            .iter()
            .map(|(k, v)| k.len() + v.len() + 2)
            .sum();
        let mut head = BytesMut::with_capacity(len + destination.len() + 64);
        head.put_slice(b"message-id:");
        put_u64(&mut head, id.0);
        head.put_u8(b'-');
        put_u64(&mut head, id.1);
        head.put_slice(b"\ndestination:");
        escape_into(&destination, &mut head);
        head.put_u8(b'\n');
        for (k, v) in frame.headers.iter().filter(|(k, _)| user(k)) {
            escape_into(k, &mut head);
            head.put_u8(b':');
            escape_into(v, &mut head);
            head.put_u8(b'\n');
        }
        if !frame.body.is_empty() {
            head.put_slice(b"content-length:");
            put_usize(&mut head, frame.body.len());
            head.put_u8(b'\n');
        }
        head.put_u8(b'\n');
        StoredMessage {
            destination,
            head: head.freeze(),
            body: frame.body,
        }
    }
}

/// One MESSAGE frame on its way to a connection's writer: a few reference
/// count bumps, no copying.
pub(crate) struct Delivery {
    /// The subscription's encoded "MESSAGE\nsubscription:<id>\n".
    prefix: Bytes,
    /// (shard, sequence) for the `ack` header, if the subscription needs one.
    ack: Option<(usize, u64)>,
    msg: Msg,
}

impl Encoder<Delivery> for StompCodec {
    type Error = CodecError;

    fn encode(&mut self, d: Delivery, dst: &mut BytesMut) -> Result<(), CodecError> {
        dst.reserve(d.prefix.len() + d.msg.head.len() + d.msg.body.len() + 48);
        dst.put_slice(&d.prefix);
        if let Some((shard, seq)) = d.ack {
            dst.put_slice(b"ack:");
            put_usize(dst, shard);
            dst.put_u8(b'-');
            put_u64(dst, seq);
            dst.put_u8(b'\n');
        }
        dst.put_slice(&d.msg.head);
        dst.put_slice(&d.msg.body);
        dst.put_u8(0);
        Ok(())
    }
}

struct SubState {
    destination: String,
    ack: AckMode,
    /// Encoded first lines of every MESSAGE for this subscription.
    prefix: Bytes,
    /// Delivered but not yet acknowledged, keyed by delivery sequence.
    unacked: BTreeMap<u64, Msg>,
    /// Topic messages waiting for this subscriber to have room (bounded).
    backlog: VecDeque<Msg>,
}

fn message_prefix(sub_id: &str) -> Bytes {
    let mut b = BytesMut::with_capacity(sub_id.len() + 24);
    b.put_slice(b"MESSAGE\nsubscription:");
    escape_into(sub_id, &mut b);
    b.put_u8(b'\n');
    b.freeze()
}

/// What a session hands the broker so shards can deliver to it.
#[derive(Clone)]
struct ConnHandle {
    id: ConnId,
    tx: mpsc::UnboundedSender<Delivery>,
    /// MESSAGE frames handed to the session but not yet written to the socket.
    in_flight: Arc<AtomicUsize>,
}

/// A connection's state within one shard.
struct ConnShard {
    handle: ConnHandle,
    subs: HashMap<Str, SubState>,
    /// delivery sequence -> subscription id for every unacked message.
    ack_index: HashMap<u64, Str>,
}

#[derive(Default)]
struct Destination {
    subscribers: Vec<(ConnId, Str)>,
    next: usize,
    pending: VecDeque<Msg>,
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
    Send(Msg),
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
            shards: (0..n)
                .map(|index| {
                    Mutex::new(Shard {
                        index,
                        ..Shard::default()
                    })
                })
                .collect(),
            flow: FlowLimits {
                prefetch: config.prefetch.max(1),
                max_in_flight: config.max_in_flight.max(1),
                max_queue_depth: config.max_queue_depth.max(1),
                topic_backlog: config.topic_backlog,
            },
        }
    }

    fn shard_of(&self, destination: &str) -> usize {
        use std::hash::BuildHasher;
        let h = foldhash::fast::FixedState::default().hash_one(destination);
        // Truncating the hash is fine: any bits pick a shard uniformly.
        #[allow(clippy::cast_possible_truncation)]
        let h = h as usize;
        h % self.shards.len()
    }

    fn lock(&self, shard: usize) -> std::sync::MutexGuard<'_, Shard> {
        self.shards[shard].lock().unwrap()
    }

    fn subscribe(
        &self,
        conn: &ConnHandle,
        shard: usize,
        id: &str,
        destination: &str,
        ack: AckMode,
    ) {
        self.lock(shard)
            .subscribe(&self.flow, conn, id, destination, ack);
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
            let ids: Vec<Str> = match sh.conns.get(&conn) {
                Some(c) => c.subs.keys().cloned().collect(),
                None => continue,
            };
            for id in ids {
                sh.remove_sub(&self.flow, conn, &id);
            }
            sh.conns.remove(&conn);
        }
    }

    /// Applies operations in order, without atomicity: consecutive operations
    /// on the same shard share one lock acquisition, and consecutive ACKs/NACKs
    /// defer refilling until the end of the run.
    fn apply_batch(&self, conn: ConnId, ops: Vec<Op>) -> Result<Option<Throttle>, Fatal> {
        let mut throttle = None;
        let mut ops = ops
            .into_iter()
            .map(|op| (self.op_shard(&op), op))
            .peekable();
        while let Some((shard, first)) = ops.next() {
            let mut sh = self.lock(shard);
            let mut refills = Vec::new();
            let res = sh.run_batched(&self.flow, conn, first, &mut refills, &mut throttle);
            if res.is_ok() {
                while let Some((_, op)) = ops.next_if(|(s, _)| *s == shard) {
                    if let Err(e) =
                        sh.run_batched(&self.flow, conn, op, &mut refills, &mut throttle)
                    {
                        sh.drain_refills(&self.flow, conn, &mut refills);
                        return Err(e);
                    }
                }
            }
            sh.drain_refills(&self.flow, conn, &mut refills);
            res?;
        }
        Ok(throttle)
    }

    fn op_shard(&self, op: &Op) -> usize {
        match op {
            Op::Send(m) => self.shard_of(&m.destination),
            Op::Ack(s, _) | Op::Nack(s, _) => *s,
        }
    }

    /// Applies a batch of operations atomically, locking every shard involved
    /// (in index order). Returns a throttle if a queue sent to is now full.
    fn apply(&self, conn: ConnId, ops: Vec<Op>) -> Option<Throttle> {
        let shard_of = |op: &Op| self.op_shard(op);
        let mut throttle = None;
        let mut run = |sh: &mut Shard, op: Op| {
            if let Some(t) = sh.run(&self.flow, conn, op) {
                throttle = Some(t);
            }
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
        sh.destinations
            .get(destination)
            .is_none_or(|d| d.pending.len() < self.flow.max_queue_depth)
    }

    fn is_outstanding(&self, conn: ConnId, shard: usize, seq: u64) -> bool {
        self.lock(shard)
            .conns
            .get(&conn)
            .is_some_and(|c| c.ack_index.contains_key(&seq))
    }

    /// Called when a connection that hit its in-flight limit has room again.
    fn resume(&self, conn: ConnId, shards: impl IntoIterator<Item = usize>) {
        for shard in shards {
            let mut sh = self.lock(shard);
            let subs: Vec<Str> = match sh.conns.get(&conn) {
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
fn can_take(flow: &FlowLimits, handle: &ConnHandle, sub: &SubState) -> bool {
    handle.in_flight.load(Ordering::Relaxed) < flow.max_in_flight
        && (sub.ack == AckMode::Auto || sub.unacked.len() < flow.prefetch)
}

impl Shard {
    fn run(&mut self, flow: &FlowLimits, conn: ConnId, op: Op) -> Option<Throttle> {
        match op {
            Op::Send(msg) => self.publish(flow, msg),
            Op::Ack(_, seq) | Op::Nack(_, seq) => {
                if let Some(sub_id) = self.settle_only(conn, seq) {
                    self.refill(flow, conn, &sub_id);
                }
                None
            }
        }
    }

    fn run_batched(
        &mut self,
        flow: &FlowLimits,
        conn: ConnId,
        op: Op,
        refills: &mut Vec<Str>,
        throttle: &mut Option<Throttle>,
    ) -> Result<(), Fatal> {
        match op {
            Op::Send(msg) => {
                self.drain_refills(flow, conn, refills);
                if let Some(t) = self.publish(flow, msg) {
                    *throttle = Some(t);
                }
            }
            Op::Ack(shard, seq) | Op::Nack(shard, seq) => {
                let Some(sub_id) = self.settle_only(conn, seq) else {
                    return Err(Fatal::new(format!(
                        "no outstanding message with ack id '{shard}-{seq}'"
                    )));
                };
                if !refills.contains(&sub_id) {
                    refills.push(sub_id);
                }
            }
        }
        Ok(())
    }

    fn drain_refills(&mut self, flow: &FlowLimits, conn: ConnId, refills: &mut Vec<Str>) {
        for sub_id in refills.drain(..) {
            self.refill(flow, conn, &sub_id);
        }
    }

    fn subscribe(
        &mut self,
        flow: &FlowLimits,
        conn: &ConnHandle,
        id: &str,
        destination: &str,
        ack: AckMode,
    ) {
        let sub_id = Str::from(id);
        let c = self.conns.entry(conn.id).or_insert_with(|| ConnShard {
            handle: conn.clone(),
            subs: HashMap::default(),
            ack_index: HashMap::default(),
        });
        c.subs.insert(
            sub_id.clone(),
            SubState {
                destination: destination.to_string(),
                ack,
                prefix: message_prefix(id),
                unacked: BTreeMap::new(),
                backlog: VecDeque::new(),
            },
        );
        self.destinations
            .entry(destination.to_string())
            .or_default()
            .subscribers
            .push((conn.id, sub_id));
        self.dispatch(flow, destination);
    }

    fn remove_sub(&mut self, flow: &FlowLimits, conn: ConnId, id: &str) {
        let Some(c) = self.conns.get_mut(&conn) else {
            return;
        };
        let Some(sub) = c.subs.remove(id) else { return };
        for seq in sub.unacked.keys() {
            c.ack_index.remove(seq);
        }
        let dest = self
            .destinations
            .get_mut(&sub.destination)
            .expect("destination exists");
        dest.subscribers.retain(|(c, s)| !(*c == conn && s == id));
        if !is_topic(&sub.destination) {
            for (_, msg) in sub.unacked.into_iter().rev() {
                dest.pending.push_front(msg);
            }
        }
        self.dispatch(flow, &sub.destination);
    }

    fn publish(&mut self, flow: &FlowLimits, msg: Msg) -> Option<Throttle> {
        if is_topic(&msg.destination) {
            self.broadcast(flow, &msg);
            None
        } else {
            self.enqueue(flow, msg)
        }
    }

    /// Offers a topic message to every current subscriber.
    fn broadcast(&mut self, flow: &FlowLimits, msg: &Msg) {
        let Shard {
            index,
            conns,
            destinations,
            seq,
        } = self;
        let Some(dest) = destinations.get(msg.destination.as_str()) else {
            return;
        };
        for (conn, sub_id) in &dest.subscribers {
            if let Some(c) = conns.get_mut(conn) {
                offer_topic(flow, *index, seq, c, sub_id, msg);
            }
        }
    }

    /// Queues a message and dispatches what it can. Returns a throttle if
    /// the queue is now full.
    fn enqueue(&mut self, flow: &FlowLimits, msg: Msg) -> Option<Throttle> {
        let Shard {
            index,
            conns,
            destinations,
            seq,
        } = self;
        let dest = match destinations.get_mut(msg.destination.as_str()) {
            Some(d) => d,
            None => destinations
                .entry(msg.destination.as_str().to_string())
                .or_default(),
        };
        // Fast path: nothing pending and a subscriber can take it immediately.
        if dest.pending.is_empty() && !dest.subscribers.is_empty() {
            match try_deliver_one(*index, seq, flow, conns, dest, msg) {
                Ok(()) => return None,
                Err(msg) => dest.pending.push_back(msg),
            }
        } else {
            dest.pending.push_back(msg);
            dispatch_dest(*index, seq, flow, conns, dest);
        }
        if dest.pending.len() < flow.max_queue_depth {
            return None;
        }
        dest.throttling = true;
        let dest_name = dest
            .pending
            .back()
            .expect("just pushed")
            .destination
            .as_str()
            .to_string();
        Some(Throttle {
            destination: dest_name,
            space: dest.space.clone(),
        })
    }

    /// Hands pending queue messages to subscribers with room, round robin.
    fn dispatch(&mut self, flow: &FlowLimits, dest_name: &str) {
        let Shard {
            index,
            conns,
            destinations,
            seq,
        } = self;
        if let Some(dest) = destinations.get_mut(dest_name) {
            dispatch_dest(*index, seq, flow, conns, dest);
        }
    }

    /// Gives a subscriber that gained room as much as it can now take.
    fn refill(&mut self, flow: &FlowLimits, conn: ConnId, sub_id: &Str) {
        let (index, seq) = (self.index, &mut self.seq);
        let Some(c) = self.conns.get_mut(&conn) else {
            return;
        };
        let Some(sub) = c.subs.get_mut(sub_id.as_str()) else {
            return;
        };
        if is_topic(&sub.destination) {
            while !sub.backlog.is_empty() && can_take(flow, &c.handle, sub) {
                let msg = sub.backlog.pop_front().unwrap();
                deliver_sub(index, seq, &c.handle, &mut c.ack_index, sub_id, sub, msg);
            }
        } else {
            let dest = sub.destination.clone();
            self.dispatch(flow, &dest);
        }
    }

    /// Removes an acknowledged (or rejected) message, and for `client` mode
    /// every earlier message on the same subscription; returns its subscription
    /// id so the caller can refill it.
    fn settle_only(&mut self, conn: ConnId, seq: u64) -> Option<Str> {
        let c = self.conns.get_mut(&conn)?;
        let sub_id = c.ack_index.remove(&seq)?;
        let sub = c.subs.get_mut(sub_id.as_str())?;
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
        Some(sub_id)
    }
}

/// Tries to hand `msg` to the next round-robin subscriber with room.
/// Returns `Err(msg)` if no subscriber can take a message right now.
fn try_deliver_one(
    index: usize,
    seq: &mut u64,
    flow: &FlowLimits,
    conns: &mut HashMap<ConnId, ConnShard>,
    dest: &mut Destination,
    msg: Msg,
) -> Result<(), Msg> {
    let n = dest.subscribers.len();
    for i in 0..n {
        let idx = (dest.next + i) % n;
        let (conn, sub_id) = &dest.subscribers[idx];
        if let Some(c) = conns.get_mut(conn)
            && let Some(sub) = c.subs.get_mut(sub_id.as_str())
            && can_take(flow, &c.handle, sub)
        {
            dest.next = idx + 1;
            deliver_sub(index, seq, &c.handle, &mut c.ack_index, sub_id, sub, msg);
            return Ok(());
        }
    }
    Err(msg)
}

/// Hands pending messages of `dest` to subscribers with room, round robin.
fn dispatch_dest(
    index: usize,
    seq: &mut u64,
    flow: &FlowLimits,
    conns: &mut HashMap<ConnId, ConnShard>,
    dest: &mut Destination,
) {
    while !dest.pending.is_empty() && !dest.subscribers.is_empty() {
        let msg = dest.pending.pop_front().unwrap();
        if let Err(msg) = try_deliver_one(index, seq, flow, conns, dest, msg) {
            dest.pending.push_front(msg);
            break;
        }
    }
    if dest.throttling && dest.pending.len() < flow.max_queue_depth {
        dest.throttling = false;
        dest.space.notify_waiters();
    }
}

/// Delivers a topic message now, or buffers it if the subscriber is busy.
fn offer_topic(
    flow: &FlowLimits,
    shard: usize,
    seq: &mut u64,
    c: &mut ConnShard,
    sub_id: &Str,
    msg: &Msg,
) {
    let Some(sub) = c.subs.get_mut(sub_id.as_str()) else {
        return;
    };
    if sub.backlog.is_empty() && can_take(flow, &c.handle, sub) {
        deliver_sub(
            shard,
            seq,
            &c.handle,
            &mut c.ack_index,
            sub_id,
            sub,
            msg.clone(),
        );
    } else if flow.topic_backlog > 0 {
        if sub.backlog.len() >= flow.topic_backlog {
            sub.backlog.pop_front();
        }
        sub.backlog.push_back(msg.clone());
    }
}

fn deliver_sub(
    shard: usize,
    seq: &mut u64,
    handle: &ConnHandle,
    ack_index: &mut HashMap<u64, Str>,
    sub_id: &Str,
    sub: &mut SubState,
    msg: Msg,
) {
    let ack = if sub.ack == AckMode::Auto {
        None
    } else {
        *seq += 1;
        sub.unacked.insert(*seq, msg.clone());
        ack_index.insert(*seq, sub_id.clone());
        Some((shard, *seq))
    };
    handle.in_flight.fetch_add(1, Ordering::Relaxed);
    let _ = handle.tx.send(Delivery {
        prefix: sub.prefix.clone(),
        ack,
        msg,
    });
}

// ---------------------------------------------------------------------------
// Sessions
// ---------------------------------------------------------------------------

/// A protocol error to report with an ERROR frame before closing.
struct Fatal {
    message: String,
    detail: Option<String>,
    extra: Vec<(Str, Str)>,
}

impl Fatal {
    fn new(message: impl Into<String>) -> Fatal {
        Fatal {
            message: message.into(),
            detail: None,
            extra: Vec::new(),
        }
    }
    fn detail(mut self, d: impl Into<String>) -> Fatal {
        self.detail = Some(d.into());
        self
    }
    fn header(mut self, k: &str, v: &str) -> Fatal {
        self.extra.push((k.into(), v.into()));
        self
    }
}

fn required<'a>(frame: &'a Frame, name: &str) -> Result<&'a str, Fatal> {
    frame.header(name).ok_or_else(|| {
        Fatal::new(format!("missing required header '{name}'")).detail(format!(
            "{} frames require a '{name}' header",
            frame.command.as_str()
        ))
    })
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
    /// Non-transactional SENDs read but not yet applied (see `burst`).
    batch: Vec<Op>,
}

enum Flow {
    Continue,
    Close,
}

impl Session {
    fn new(id: ConnId, broker: Arc<Broker>) -> (Session, mpsc::UnboundedReceiver<Delivery>) {
        let (tx, rx) = mpsc::unbounded_channel();
        let handle = ConnHandle {
            id,
            tx,
            in_flight: Arc::new(AtomicUsize::new(0)),
        };
        let session = Session {
            handle,
            broker,
            transactions: HashMap::default(),
            throttle: None,
            subs: HashMap::default(),
            messages: 0,
            batch: Vec::new(),
        };
        (session, rx)
    }

    async fn run(stream: TcpStream, broker: Arc<Broker>, config: Arc<ServerConfig>) {
        let mut conn: Conn = Framed::new(
            Activity::new(stream),
            StompCodec::with_limits(config.limits),
        );
        let (hb_out, hb_in) = match accept_connect(&mut conn, &config).await {
            Ok(Some(heartbeats)) => heartbeats,
            Ok(None) => return,
            Err(fatal) => return close_with_error(conn, fatal).await,
        };
        let id = NEXT_CONN.fetch_add(1, Ordering::Relaxed);
        if conn.send(connected_frame(id, &config)).await.is_err() {
            return;
        }
        let (mut session, mut rx) = Session::new(id, broker.clone());

        // --- frame loop ---
        if hb_in.is_some() {
            conn.get_mut().track_reads();
        }
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
                item = conn.next(), if session.throttle.is_none() => {
                    if let Some(outcome) = session.burst(&mut conn, &mut rx, &mut hb, item).await {
                        break outcome;
                    }
                }
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

    /// Handles `first` and then every further frame already readable (up to
    /// `BURST`). Consecutive SENDs are applied together, so a pipelining
    /// producer takes each shard lock once per burst instead of once per
    /// message. Everything else keeps its order: the pending SENDs are applied
    /// before any other command, receipt, error or close.
    ///
    /// Returns `Some(outcome)` when the connection should end.
    async fn burst(
        &mut self,
        conn: &mut Conn,
        rx: &mut mpsc::UnboundedReceiver<Delivery>,
        hb: &mut Heartbeats,
        first: Option<Result<Item, CodecError>>,
    ) -> Option<Result<(), Fatal>> {
        const BURST: usize = 256;
        let mut next = first;
        let mut handled = 0;
        loop {
            let frame = match next {
                Some(Ok(Item::Frame(frame))) => Some(frame),
                Some(Ok(Item::Heartbeat)) => None,
                Some(Err(e)) => return Some(self.end(Err(Fatal::new(e.to_string())))),
                None => return Some(self.end(Ok(()))),
            };
            if let Some(frame) = frame
                && let Some(outcome) = self.on_frame(frame, conn, rx, hb).await
            {
                return Some(outcome);
            }
            handled += 1;
            if handled >= BURST || self.throttle.is_some() {
                break;
            }
            match conn.next().now_or_never() {
                Some(item) => next = item,
                None => break,
            }
        }
        self.write_pending(conn, rx, hb).await
    }

    /// Handles one frame, then writes its RECEIPT if it asked for one.
    /// Returns `Some(outcome)` when the connection should end.
    async fn on_frame(
        &mut self,
        frame: Frame,
        conn: &mut Conn,
        rx: &mut mpsc::UnboundedReceiver<Delivery>,
        hb: &mut Heartbeats,
    ) -> Option<Result<(), Fatal>> {
        let receipt = frame.header_value("receipt").cloned();
        // Flush any earlier batched ops before handling a frame with a receipt,
        // so an error from an earlier frame does not inherit this receipt-id.
        if receipt.is_some()
            && let Err(fatal) = self.flush_batch()
        {
            return Some(Err(fatal));
        }
        let close = match self.handle(frame) {
            Ok(flow) => matches!(flow, Flow::Close),
            Err(fatal) => return Some(self.end(Err(with_receipt(fatal, receipt)))),
        };
        // Deliveries caused by this or earlier frames go out first.
        if (receipt.is_some() || close)
            && let Some(outcome) = self.write_pending(conn, rx, hb).await
        {
            return Some(outcome.map_err(|f| with_receipt(f, receipt)));
        }
        if let Some(r) = receipt {
            let rf = Frame::new(Command::Receipt).with_header(Str::from_static("receipt-id"), r);
            if conn.send(rf).await.is_err() {
                return Some(Ok(()));
            }
            hb.wrote();
        }
        close.then_some(Ok(()))
    }

    /// Ends the connection: ops read before the end still take effect.
    fn end(&mut self, outcome: Result<(), Fatal>) -> Result<(), Fatal> {
        let flushed = self.flush_batch();
        flushed.and(outcome)
    }

    /// Applies batched ops, then writes every delivery queued so far.
    /// Returns `Some(outcome)` if a batched op failed or the socket broke.
    async fn write_pending(
        &mut self,
        conn: &mut Conn,
        rx: &mut mpsc::UnboundedReceiver<Delivery>,
        hb: &mut Heartbeats,
    ) -> Option<Result<(), Fatal>> {
        if let Err(fatal) = self.flush_batch() {
            return Some(Err(fatal));
        }
        match flush_queued(conn, rx).await {
            Ok(0) => None,
            Ok(n) => {
                self.written(n);
                hb.wrote();
                None
            }
            Err(_) => Some(Ok(())),
        }
    }

    fn flush_batch(&mut self) -> Result<(), Fatal> {
        if self.batch.is_empty() {
            return Ok(());
        }
        let ops = std::mem::take(&mut self.batch);
        if let Some(t) = self.broker.apply_batch(self.handle.id, ops)? {
            self.throttle = Some(t);
        }
        Ok(())
    }

    fn handle(&mut self, frame: Frame) -> Result<Flow, Fatal> {
        if !matches!(frame.command, Command::Send | Command::Ack | Command::Nack) {
            // Keep order: earlier SENDs/ACKs take effect before anything else.
            self.flush_batch()?;
        }
        if !frame.body.is_empty() && !frame.command.allows_body() {
            let command = frame.command.as_str();
            return Err(Fatal::new(format!("{command} frames must not have a body")));
        }
        let handled = match frame.command {
            Command::Send => self.on_send(frame),
            Command::Subscribe => self.on_subscribe(&frame),
            Command::Unsubscribe => self.on_unsubscribe(&frame),
            Command::Ack | Command::Nack => self.on_ack(&frame),
            Command::Begin => self.on_begin(&frame),
            Command::Commit => self.on_commit(&frame),
            Command::Abort => self.on_abort(&frame),
            Command::Disconnect => return Ok(Flow::Close),
            Command::Connect | Command::Stomp => Err(Fatal::new("already connected")),
            ref other => {
                let command = other.as_str();
                Err(Fatal::new(format!("unsupported command '{command}'")))
            }
        };
        handled.map(|()| Flow::Continue)
    }

    fn on_commit(&mut self, frame: &Frame) -> Result<(), Fatal> {
        let ops = self.take_tx(required(frame, "transaction")?)?;
        let throttle = self.broker.apply(self.handle.id, ops);
        self.throttle = throttle.or(self.throttle.take());
        Ok(())
    }

    fn on_abort(&mut self, frame: &Frame) -> Result<(), Fatal> {
        self.take_tx(required(frame, "transaction")?).map(drop)
    }

    fn on_send(&mut self, frame: Frame) -> Result<(), Fatal> {
        required(&frame, "destination")?;
        let destination = frame.header_value("destination").unwrap().clone();
        let tx = frame.header("transaction").map(str::to_string);
        self.messages += 1;
        let msg = StoredMessage::from_send((self.handle.id, self.messages), destination, frame);
        self.submit(tx, Op::Send(Arc::new(msg)))
    }

    fn on_subscribe(&mut self, frame: &Frame) -> Result<(), Fatal> {
        let destination = required(frame, "destination")?;
        let id = required(frame, "id")?;
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
        self.broker
            .subscribe(&self.handle, shard, id, destination, ack);
        self.subs.insert(id.to_string(), shard);
        Ok(())
    }

    fn on_unsubscribe(&mut self, frame: &Frame) -> Result<(), Fatal> {
        let id = required(frame, "id")?;
        let shard = self
            .subs
            .remove(id)
            .ok_or_else(|| Fatal::new(format!("no subscription with id '{id}'")))?;
        self.broker.unsubscribe(self.handle.id, shard, id);
        Ok(())
    }

    /// ACK or NACK.
    fn on_ack(&mut self, frame: &Frame) -> Result<(), Fatal> {
        let ack_id = required(frame, "id")?;
        let Some((shard, seq)) = parse_ack_id(ack_id, self.broker.shards.len()) else {
            return Err(Fatal::new(format!(
                "no outstanding message with ack id '{ack_id}'"
            )));
        };
        let tx = frame.header("transaction").map(str::to_string);
        if tx.is_some() && !self.broker.is_outstanding(self.handle.id, shard, seq) {
            return Err(Fatal::new(format!(
                "no outstanding message with ack id '{ack_id}'"
            )));
        }
        let op = if frame.command == Command::Ack {
            Op::Ack(shard, seq)
        } else {
            Op::Nack(shard, seq)
        };
        self.submit(tx, op)
    }

    fn on_begin(&mut self, frame: &Frame) -> Result<(), Fatal> {
        let tx = required(frame, "transaction")?;
        if self.transactions.contains_key(tx) {
            return Err(Fatal::new(format!(
                "transaction '{tx}' already in progress"
            )));
        }
        self.transactions.insert(tx.to_string(), Vec::new());
        Ok(())
    }

    fn submit(&mut self, tx: Option<String>, op: Op) -> Result<(), Fatal> {
        match tx {
            None => self.batch.push(op),
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
        if n > 0
            && self.handle.in_flight.fetch_sub(n, Ordering::Relaxed)
                >= self.broker.flow.max_in_flight
        {
            self.broker.resume(self.handle.id, self.shards());
        }
    }

    fn take_tx(&mut self, tx: &str) -> Result<Vec<Op>, Fatal> {
        self.transactions
            .remove(tx)
            .ok_or_else(|| Fatal::new(format!("unknown transaction '{tx}'")))
    }
}

type Heartbeating = (Option<Duration>, Option<Duration>);

/// Reads and validates the CONNECT frame. Returns the negotiated
/// (outgoing, incoming) heart-beats, or `None` if the peer went away.
async fn accept_connect(
    conn: &mut Conn,
    config: &ServerConfig,
) -> Result<Option<Heartbeating>, Fatal> {
    loop {
        match conn.next().await {
            None => return Ok(None),
            Some(Ok(Item::Heartbeat)) => {}
            Some(Ok(Item::Frame(f))) => return handshake(&f, config).map(Some),
            Some(Err(e)) => return Err(Fatal::new(e.to_string())),
        }
    }
}

fn with_receipt(mut fatal: Fatal, receipt: Option<Str>) -> Fatal {
    if let Some(r) = receipt {
        fatal.extra.push((Str::from_static("receipt-id"), r));
    }
    fatal
}

fn connected_frame(id: ConnId, config: &ServerConfig) -> Frame {
    let (sx, sy) = config.heartbeat;
    Frame::new(Command::Connected)
        .with_header(Str::from_static("version"), Str::from_static("1.2"))
        .with_header(Str::from_static("session"), format!("session-{id}"))
        .with_header(Str::from_static("server"), Str::from_static(SERVER_NAME))
        .with_header(Str::from_static("heart-beat"), format!("{sx},{sy}"))
}

/// Validates CONNECT/STOMP; returns negotiated (outgoing, incoming) heart-beats.
fn handshake(f: &Frame, config: &ServerConfig) -> Result<Heartbeating, Fatal> {
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
            .detail(format!(
                "Supported protocol versions are {}",
                SUPPORTED_VERSIONS.join(" ")
            )));
    }
    if f.header("host").is_none() {
        return Err(Fatal::new("missing required header 'host'"));
    }
    if let Some((login, passcode)) = &config.credentials
        && (f.header("login") != Some(login) || f.header("passcode") != Some(passcode))
    {
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
async fn flush_queued(
    conn: &mut Conn,
    rx: &mut mpsc::UnboundedReceiver<Delivery>,
) -> Result<usize, CodecError> {
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
        frame = frame
            .with_header("content-type", "text/plain")
            .with_body(detail);
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
