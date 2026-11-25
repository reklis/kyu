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
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpSocket, TcpStream, ToSocketAddrs};
use tokio::sync::mpsc;
use tokio::time::{Instant, sleep_until};
use tokio_util::codec::Framed;

use crate::codec::{CodecError, Item, Limits, StompCodec};
use crate::frame::{Command, Frame};
use crate::transport::{Activity, negotiate, parse_heartbeat, read_deadline};

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
}

impl Default for ServerConfig {
    fn default() -> Self {
        ServerConfig { heartbeat: (0, 0), credentials: None, limits: Limits::default(), backlog: 4096 }
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
                    return Ok(Server {
                        listener,
                        config: Arc::new(config),
                        broker: Arc::new(Broker::default()),
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
}

struct ConnState {
    tx: mpsc::UnboundedSender<Frame>,
    subs: HashMap<String, SubState>,
    /// ack id -> (subscription id, delivery sequence) for every unacked message.
    ack_index: HashMap<String, (String, u64)>,
}

fn make_ack_id(conn: ConnId, seq: u64) -> String {
    format!("{conn}-{seq}")
}

#[derive(Default)]
struct Destination {
    subscribers: Vec<(ConnId, String)>,
    next: usize,
    pending: VecDeque<StoredMessage>,
}

#[derive(Default)]
struct Broker {
    state: Mutex<BrokerState>,
    ids: AtomicU64,
}

#[derive(Default)]
struct BrokerState {
    conns: HashMap<ConnId, ConnState>,
    destinations: HashMap<String, Destination>,
}

fn is_topic(destination: &str) -> bool {
    destination.starts_with("/topic/")
}

impl Broker {
    fn next_id(&self) -> u64 {
        self.ids.fetch_add(1, Ordering::Relaxed)
    }

    fn register(&self, conn: ConnId, tx: mpsc::UnboundedSender<Frame>) {
        self.state.lock().unwrap().conns.insert(conn, ConnState { tx, subs: HashMap::new(), ack_index: HashMap::new() });
    }

    fn subscribe(&self, conn: ConnId, id: &str, destination: &str, ack: AckMode) -> Result<(), String> {
        let mut st = self.state.lock().unwrap();
        let c = st.conns.get_mut(&conn).expect("registered");
        if c.subs.contains_key(id) {
            return Err(format!("subscription id '{id}' already in use"));
        }
        c.subs.insert(
            id.to_string(),
            SubState { destination: destination.to_string(), ack, unacked: BTreeMap::new() },
        );
        st.destinations
            .entry(destination.to_string())
            .or_default()
            .subscribers
            .push((conn, id.to_string()));
        self.dispatch(&mut st, destination);
        Ok(())
    }

    fn unsubscribe(&self, conn: ConnId, id: &str) -> Result<(), String> {
        let mut st = self.state.lock().unwrap();
        if !st.conns[&conn].subs.contains_key(id) {
            return Err(format!("no subscription with id '{id}'"));
        }
        self.remove_sub(&mut st, conn, id);
        Ok(())
    }

    fn remove_sub(&self, st: &mut BrokerState, conn: ConnId, id: &str) {
        let Some(c) = st.conns.get_mut(&conn) else { return };
        let Some(sub) = c.subs.remove(id) else { return };
        for seq in sub.unacked.keys() {
            c.ack_index.remove(&make_ack_id(conn, *seq));
        }
        let dest = st.destinations.get_mut(&sub.destination).expect("destination exists");
        dest.subscribers.retain(|(c, s)| !(*c == conn && s == id));
        if !is_topic(&sub.destination) {
            for (_, msg) in sub.unacked.into_iter().rev() {
                dest.pending.push_front(msg);
            }
        }
        self.dispatch(st, &sub.destination);
    }

    fn disconnect(&self, conn: ConnId) {
        let mut st = self.state.lock().unwrap();
        let ids: Vec<String> = match st.conns.get(&conn) {
            Some(c) => c.subs.keys().cloned().collect(),
            None => return,
        };
        for id in ids {
            self.remove_sub(&mut st, conn, &id);
        }
        st.conns.remove(&conn);
    }

    /// Applies a batch of operations atomically.
    fn apply(&self, conn: ConnId, ops: Vec<Op>) {
        let mut st = self.state.lock().unwrap();
        for op in ops {
            match op {
                Op::Send(msg) => self.publish(&mut st, msg),
                Op::Ack(id) => Self::settle(&mut st, conn, &id),
                Op::Nack(id) => Self::settle(&mut st, conn, &id),
            }
        }
    }

    fn publish(&self, st: &mut BrokerState, msg: StoredMessage) {
        let dest_name = msg.destination.clone();
        let dest = st.destinations.entry(dest_name.clone()).or_default();
        if is_topic(&dest_name) {
            let subs = dest.subscribers.clone();
            for (conn, sub) in subs {
                Self::deliver(st, conn, &sub, msg.clone(), self.next_id());
            }
        } else {
            dest.pending.push_back(msg);
            self.dispatch(st, &dest_name);
        }
    }

    /// Hands pending queue messages to subscribers, round robin.
    fn dispatch(&self, st: &mut BrokerState, dest_name: &str) {
        loop {
            let Some(dest) = st.destinations.get_mut(dest_name) else { return };
            if dest.subscribers.is_empty() || dest.pending.is_empty() {
                return;
            }
            let msg = dest.pending.pop_front().unwrap();
            dest.next %= dest.subscribers.len();
            let (conn, sub) = dest.subscribers[dest.next].clone();
            dest.next += 1;
            Self::deliver(st, conn, &sub, msg, self.next_id());
        }
    }

    fn deliver(st: &mut BrokerState, conn: ConnId, sub_id: &str, msg: StoredMessage, ack_seq: u64) {
        let Some(c) = st.conns.get_mut(&conn) else { return };
        let Some(sub) = c.subs.get_mut(sub_id) else { return };
        let mut frame = Frame::new(Command::Message)
            .with_header("subscription", sub_id)
            .with_header("message-id", msg.id.clone())
            .with_header("destination", msg.destination.clone());
        if sub.ack != AckMode::Auto {
            let id = make_ack_id(conn, ack_seq);
            frame = frame.with_header("ack", id.clone());
            sub.unacked.insert(ack_seq, msg.clone());
            c.ack_index.insert(id, (sub_id.to_string(), ack_seq));
        }
        frame.headers.extend(msg.headers);
        frame.body = msg.body;
        let _ = c.tx.send(frame);
    }

    fn is_outstanding(&self, conn: ConnId, ack_id: &str) -> bool {
        let st = self.state.lock().unwrap();
        st.conns[&conn].ack_index.contains_key(ack_id)
    }

    /// Removes an acknowledged (or NACKed) message, and for `client` mode
    /// every earlier message on the same subscription.
    fn settle(st: &mut BrokerState, conn: ConnId, ack_id: &str) {
        let Some(c) = st.conns.get_mut(&conn) else { return };
        let Some((sub_id, seq)) = c.ack_index.remove(ack_id) else { return };
        let Some(sub) = c.subs.get_mut(&sub_id) else { return };
        match sub.ack {
            AckMode::Client => {
                let later = sub.unacked.split_off(&(seq + 1));
                let settled = std::mem::replace(&mut sub.unacked, later);
                for earlier in settled.keys() {
                    c.ack_index.remove(&make_ack_id(conn, *earlier));
                }
            }
            _ => {
                sub.unacked.remove(&seq);
            }
        }
    }
}

enum Op {
    Send(StoredMessage),
    Ack(String),
    Nack(String),
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

struct Session {
    id: ConnId,
    broker: Arc<Broker>,
    transactions: HashMap<String, Vec<Op>>,
}

enum Flow {
    Continue,
    Close,
}

impl Session {
    async fn run(stream: TcpStream, broker: Arc<Broker>, config: Arc<ServerConfig>) {
        let last_read = Arc::new(Mutex::new(Instant::now()));
        let mut conn: Conn = Framed::new(
            Activity { inner: stream, last: last_read.clone() },
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

        let id = broker.next_id();
        let connected = Frame::new(Command::Connected)
            .with_header("version", "1.2")
            .with_header("session", format!("session-{id}"))
            .with_header("server", SERVER_NAME)
            .with_header("heart-beat", format!("{},{}", config.heartbeat.0, config.heartbeat.1));
        if conn.send(connected).await.is_err() {
            return;
        }

        let (tx, mut rx) = mpsc::unbounded_channel();
        broker.register(id, tx);
        let mut session = Session { id, broker: broker.clone(), transactions: HashMap::new() };

        // --- frame loop ---
        let far_future = Duration::from_secs(86400 * 365);
        let mut last_write = Instant::now();
        let outcome: Result<(), Fatal> = loop {
            let write_due = last_write + hb_out.unwrap_or(far_future);
            let read_due = *last_read.lock().unwrap() + hb_in.map(read_deadline).unwrap_or(far_future);
            tokio::select! {
                item = conn.next() => match item {
                    Some(Ok(Item::Heartbeat)) => {}
                    Some(Ok(Item::Frame(frame))) => {
                        let receipt = frame.header("receipt").map(str::to_string);
                        match session.handle(frame) {
                            Ok(flow) => {
                                // Deliveries caused by this or earlier frames go out first.
                                if flush_queued(&mut conn, &mut rx).await.is_err() { break Ok(()); }
                                if let Some(r) = receipt {
                                    let rf = Frame::new(Command::Receipt).with_header("receipt-id", r);
                                    if conn.send(rf).await.is_err() { break Ok(()); }
                                    last_write = Instant::now();
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
                    if flush_queued(&mut conn, &mut rx).await.is_err() { break Ok(()); }
                    last_write = Instant::now();
                }
                _ = sleep_until(write_due), if hb_out.is_some() => {
                    if conn.send(Item::Heartbeat).await.is_err() { break Ok(()); }
                    last_write = Instant::now();
                }
                _ = sleep_until(read_due), if hb_in.is_some() => {
                    // Re-check: bytes of a partial frame may have arrived.
                    if last_read.lock().unwrap().elapsed() >= read_deadline(hb_in.unwrap()) {
                        break Ok(());
                    }
                }
            }
        };

        // Any transactions still open are implicitly aborted.
        session.transactions.clear();
        broker.disconnect(id);
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
                let msg = StoredMessage {
                    id: format!("msg-{}", self.broker.next_id()),
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
                self.broker.subscribe(self.id, id, destination, ack).map_err(Fatal::new)?;
            }
            Command::Unsubscribe => {
                let id = required(&frame, "id")?;
                self.broker.unsubscribe(self.id, id).map_err(Fatal::new)?;
            }
            Command::Ack | Command::Nack => {
                let ack_id = required(&frame, "id")?.to_string();
                if !self.broker.is_outstanding(self.id, &ack_id) {
                    return Err(Fatal::new(format!("no outstanding message with ack id '{ack_id}'")));
                }
                let tx = frame.header("transaction").map(str::to_string);
                let op = if frame.command == Command::Ack { Op::Ack(ack_id) } else { Op::Nack(ack_id) };
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
                self.broker.apply(self.id, ops);
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
            None => self.broker.apply(self.id, vec![op]),
            Some(tx) => self
                .transactions
                .get_mut(&tx)
                .ok_or_else(|| Fatal::new(format!("unknown transaction '{tx}'")))?
                .push(op),
        }
        Ok(())
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
/// previously fed) with a single flush per batch.
async fn flush_queued(conn: &mut Conn, rx: &mut mpsc::UnboundedReceiver<Frame>) -> Result<(), CodecError> {
    const BATCH: usize = 256;
    loop {
        let mut n = 0;
        while n < BATCH {
            let Ok(frame) = rx.try_recv() else { break };
            conn.feed(frame).await?;
            n += 1;
        }
        SinkExt::<Frame>::flush(conn).await?;
        if n < BATCH {
            return Ok(());
        }
    }
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
