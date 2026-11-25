//! A STOMP 1.2 client.
//!
//! ```no_run
//! # async fn demo() -> Result<(), kyu::client::Error> {
//! use kyu::client::{AckMode, Client, ConnectOptions, OutgoingMessage};
//!
//! let client = Client::connect("127.0.0.1:61613", ConnectOptions::new("localhost")).await?;
//! let mut sub = client.subscribe("/queue/a", AckMode::ClientIndividual).await?;
//! client.send_confirmed(OutgoingMessage::new("/queue/a").content_type("text/plain").body("hi")).await?;
//! if let Some(msg) = sub.recv().await {
//!     client.ack(&msg).await?;
//! }
//! client.disconnect().await?;
//! # Ok(()) }
//! ```

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpStream, ToSocketAddrs};
use tokio::sync::{mpsc, oneshot, watch};
use tokio_util::codec::Framed;

use crate::codec::{CodecError, Item, StompCodec};
use crate::frame::{Command, Frame, Str};
use crate::transport::{Activity, Heartbeats, Tick, negotiate, parse_heartbeat};

#[derive(Debug, Clone, thiserror::Error)]
pub enum Error {
    #[error("i/o error: {0}")]
    Io(String),
    #[error("protocol error: {0}")]
    Protocol(String),
    #[error("server error: {message:?}")]
    Server {
        message: Option<String>,
        frame: Frame,
    },
    #[error("server negotiated unsupported version {0}")]
    UnsupportedVersion(String),
    #[error("connection closed")]
    ConnectionClosed,
    #[error("server stopped heart-beating")]
    HeartbeatTimeout,
    #[error("message has no ack header")]
    NoAckHeader,
}

impl From<CodecError> for Error {
    fn from(e: CodecError) -> Error {
        match e {
            CodecError::Io(e) => Error::Io(e.to_string()),
            other => Error::Protocol(other.to_string()),
        }
    }
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Error {
        Error::Io(e.to_string())
    }
}

#[derive(Debug, Clone)]
pub struct ConnectOptions {
    host: String,
    login: Option<String>,
    passcode: Option<String>,
    heartbeat: (u32, u32),
    use_stomp_command: bool,
    subscription_buffer: usize,
}

impl ConnectOptions {
    /// `host` is the virtual host to connect to (the CONNECT `host` header).
    pub fn new(host: impl Into<String>) -> ConnectOptions {
        ConnectOptions {
            host: host.into(),
            login: None,
            passcode: None,
            heartbeat: (0, 0),
            use_stomp_command: false,
            subscription_buffer: 1024,
        }
    }

    #[must_use]
    pub fn credentials(mut self, login: impl Into<String>, passcode: impl Into<String>) -> Self {
        self.login = Some(login.into());
        self.passcode = Some(passcode.into());
        self
    }

    /// (cx, cy): the interval (ms) at which this client can send heart-beats,
    /// and the interval at which it wants to receive them. 0 disables.
    #[must_use]
    pub fn heartbeat(mut self, cx: u32, cy: u32) -> Self {
        self.heartbeat = (cx, cy);
        self
    }

    /// How many received messages each subscription buffers before the client
    /// stops reading from the socket (pushing back on the server).
    #[must_use]
    pub fn subscription_buffer(mut self, messages: usize) -> Self {
        self.subscription_buffer = messages.max(1);
        self
    }

    /// Send STOMP instead of CONNECT (only 1.2 servers understand it).
    #[must_use]
    pub fn use_stomp_command(mut self, yes: bool) -> Self {
        self.use_stomp_command = yes;
        self
    }
}

/// Details from the server's CONNECTED frame.
#[derive(Debug, Clone)]
pub struct Connected {
    pub version: String,
    pub session: Option<String>,
    pub server: Option<String>,
    /// The server's (sx, sy) heart-beat settings.
    pub heartbeat: (u32, u32),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AckMode {
    Auto,
    Client,
    ClientIndividual,
}

impl AckMode {
    #[must_use]
    pub fn as_str(&self) -> &'static str {
        match self {
            AckMode::Auto => "auto",
            AckMode::Client => "client",
            AckMode::ClientIndividual => "client-individual",
        }
    }
}

/// A message to SEND.
#[derive(Debug, Clone)]
pub struct OutgoingMessage {
    frame: Frame,
}

impl OutgoingMessage {
    pub fn new(destination: impl Into<Str>) -> Self {
        OutgoingMessage {
            frame: Frame::new(Command::Send).with_header("destination", destination),
        }
    }

    /// Adds a user-defined header.
    #[must_use]
    pub fn header(mut self, name: impl Into<Str>, value: impl Into<Str>) -> Self {
        self.frame = self.frame.with_header(name, value);
        self
    }

    #[must_use]
    pub fn content_type(self, ct: impl Into<Str>) -> Self {
        self.header("content-type", ct)
    }

    #[must_use]
    pub fn body(mut self, body: impl Into<Bytes>) -> Self {
        self.frame.body = body.into();
        self
    }
}

/// A MESSAGE received on a subscription.
#[derive(Debug, Clone)]
pub struct Message {
    frame: Frame,
}

impl Message {
    pub fn header(&self, name: &str) -> Option<&str> {
        self.frame.header(name)
    }
    pub fn body(&self) -> &Bytes {
        &self.frame.body
    }
    pub fn destination(&self) -> Option<&str> {
        self.header("destination")
    }
    pub fn message_id(&self) -> Option<&str> {
        self.header("message-id")
    }
    pub fn subscription(&self) -> Option<&str> {
        self.header("subscription")
    }
    /// The `ack` header, present for client and client-individual subscriptions.
    pub fn ack_id(&self) -> Option<&str> {
        self.header("ack")
    }
    pub fn frame(&self) -> &Frame {
        &self.frame
    }
}

pub struct Subscription {
    id: String,
    rx: mpsc::Receiver<Message>,
}

impl Subscription {
    #[must_use]
    pub fn id(&self) -> &str {
        &self.id
    }

    /// The next message, or None once the connection has closed.
    pub async fn recv(&mut self) -> Option<Message> {
        self.rx.recv().await
    }
}

#[derive(Default)]
struct Shared {
    receipts: HashMap<String, oneshot::Sender<Result<(), Error>>>,
    subs: HashMap<String, mpsc::Sender<Message>>,
    closed: Option<Error>,
}

enum Outgoing {
    Frame(Frame, oneshot::Sender<Result<(), Error>>),
    Close,
}

pub struct Client {
    out: mpsc::UnboundedSender<Outgoing>,
    shared: Arc<Mutex<Shared>>,
    closed: watch::Receiver<Option<Error>>,
    connected: Connected,
    ids: AtomicU64,
    subscription_buffer: usize,
}

type Conn = Framed<Activity<TcpStream>, StompCodec>;

/// Locks the state shared between a client and its I/O task. The state stays
/// consistent even if a holder panicked, so a poisoned lock is taken over.
fn lock(shared: &Mutex<Shared>) -> std::sync::MutexGuard<'_, Shared> {
    shared
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

impl ConnectOptions {
    fn connect_frame(&self) -> Frame {
        let command = if self.use_stomp_command {
            Command::Stomp
        } else {
            Command::Connect
        };
        let mut connect = Frame::new(command)
            .with_header("accept-version", "1.2")
            .with_header("host", &self.host);
        if let Some(login) = &self.login {
            connect = connect.with_header("login", login);
        }
        if let Some(passcode) = &self.passcode {
            connect = connect.with_header("passcode", passcode);
        }
        let (cx, cy) = self.heartbeat;
        if (cx, cy) != (0, 0) {
            connect = connect.with_header("heart-beat", format!("{cx},{cy}"));
        }
        connect
    }
}

/// The next frame, skipping heart-beats.
async fn next_frame(conn: &mut Conn) -> Result<Frame, Error> {
    loop {
        match conn.next().await {
            Some(Ok(Item::Heartbeat)) => {}
            Some(Ok(Item::Frame(f))) => return Ok(f),
            Some(Err(e)) => return Err(e.into()),
            None => return Err(Error::ConnectionClosed),
        }
    }
}

/// Checks the server's reply to CONNECT.
fn parse_connected(reply: Frame) -> Result<Connected, Error> {
    match reply.command {
        Command::Connected => {}
        Command::Error => return Err(server_error(reply)),
        other => {
            let got = other.as_str();
            return Err(Error::Protocol(format!("expected CONNECTED, got {got}")));
        }
    }
    // A CONNECTED frame without a version header comes from a 1.0 server.
    let version = reply.header("version").unwrap_or("1.0").to_string();
    if version != "1.2" {
        return Err(Error::UnsupportedVersion(version));
    }
    let heartbeat = parse_heartbeat(reply.header("heart-beat"))
        .ok_or_else(|| Error::Protocol("invalid heart-beat header".into()))?;
    Ok(Connected {
        version,
        session: reply.header("session").map(str::to_string),
        server: reply.header("server").map(str::to_string),
        heartbeat,
    })
}

impl Client {
    /// Connects and performs the STOMP 1.2 handshake.
    ///
    /// # Errors
    /// I/O errors, an ERROR reply (e.g. bad credentials), a server that does
    /// not speak STOMP 1.2, or a malformed CONNECTED frame.
    pub async fn connect(
        addr: impl ToSocketAddrs,
        options: ConnectOptions,
    ) -> Result<Client, Error> {
        let stream = TcpStream::connect(addr).await?;
        let _ = stream.set_nodelay(true);
        let mut conn: Conn = Framed::new(Activity::new(stream), StompCodec::new());

        conn.send(options.connect_frame()).await?;
        let connected = parse_connected(next_frame(&mut conn).await?)?;
        let (cx, cy) = options.heartbeat;
        let (sx, sy) = connected.heartbeat;

        let shared = Arc::new(Mutex::new(Shared::default()));
        let (out_tx, out_rx) = mpsc::unbounded_channel();
        let (closed_tx, closed_rx) = watch::channel(None);
        let io = IoTask {
            conn,
            shared: shared.clone(),
            out: out_rx,
            hb_out: negotiate(cx, sy),
            hb_in: negotiate(sx, cy),
        };
        tokio::spawn(io.run(closed_tx));

        Ok(Client {
            out: out_tx,
            shared,
            closed: closed_rx,
            connected,
            ids: AtomicU64::new(0),
            subscription_buffer: options.subscription_buffer,
        })
    }

    pub fn connected(&self) -> &Connected {
        &self.connected
    }

    fn next_id(&self, prefix: &str) -> String {
        format!("{prefix}-{}", self.ids.fetch_add(1, Ordering::Relaxed))
    }

    /// Writes a raw frame to the server.
    ///
    /// # Errors
    /// [`Error::ConnectionClosed`] (or the reason it closed) if the
    /// connection is gone, or the encoding error if the frame is invalid.
    pub async fn send_frame(&self, frame: Frame) -> Result<(), Error> {
        if let Some(e) = lock(&self.shared).closed.clone() {
            return Err(e);
        }
        let (tx, rx) = oneshot::channel();
        if self.out.send(Outgoing::Frame(frame, tx)).is_err() {
            return Err(self.closed().await);
        }
        rx.await.unwrap_or(Err(Error::ConnectionClosed))
    }

    /// Writes a frame with a fresh receipt header and waits for the RECEIPT.
    async fn send_with_receipt(&self, frame: Frame) -> Result<(), Error> {
        let receipt = self.next_id("rcpt");
        let (tx, rx) = oneshot::channel();
        {
            let mut shared = lock(&self.shared);
            if let Some(e) = shared.closed.clone() {
                return Err(e);
            }
            shared.receipts.insert(receipt.clone(), tx);
        }
        self.send_frame(frame.with_header("receipt", receipt))
            .await?;
        rx.await.unwrap_or(Err(Error::ConnectionClosed))
    }

    /// Sends a message without waiting for the server.
    ///
    /// # Errors
    /// As for [`Client::send_frame`].
    pub async fn send(&self, msg: OutgoingMessage) -> Result<(), Error> {
        self.send_frame(msg.frame).await
    }

    /// Sends and waits for the server's RECEIPT.
    ///
    /// # Errors
    /// As for [`Client::send_frame`], or [`Error::Server`] if the server
    /// rejects the message.
    pub async fn send_confirmed(&self, msg: OutgoingMessage) -> Result<(), Error> {
        self.send_with_receipt(msg.frame).await
    }

    /// Subscribes and waits until the server confirms the subscription.
    ///
    /// # Errors
    /// As for [`Client::send_confirmed`].
    pub async fn subscribe(&self, destination: &str, ack: AckMode) -> Result<Subscription, Error> {
        let id = self.next_id("sub");
        let (tx, rx) = mpsc::channel(self.subscription_buffer);
        lock(&self.shared).subs.insert(id.clone(), tx);
        let frame = Frame::new(Command::Subscribe)
            .with_header("id", id.clone())
            .with_header("destination", destination)
            .with_header("ack", ack.as_str());
        if let Err(e) = self.send_with_receipt(frame).await {
            lock(&self.shared).subs.remove(&id);
            return Err(e);
        }
        Ok(Subscription { id, rx })
    }

    /// Ends a subscription and waits for the server to confirm it.
    ///
    /// # Errors
    /// As for [`Client::send_confirmed`].
    pub async fn unsubscribe(&self, sub: Subscription) -> Result<(), Error> {
        let res = self
            .send_with_receipt(Frame::new(Command::Unsubscribe).with_header("id", sub.id.clone()))
            .await;
        lock(&self.shared).subs.remove(&sub.id);
        res
    }

    fn ack_frame(command: Command, msg: &Message, tx: Option<&str>) -> Result<Frame, Error> {
        let id = msg.ack_id().ok_or(Error::NoAckHeader)?;
        let mut f = Frame::new(command).with_header("id", id);
        if let Some(tx) = tx {
            f = f.with_header("transaction", tx);
        }
        Ok(f)
    }

    /// Acknowledges a message from a `client` or `client-individual`
    /// subscription.
    ///
    /// # Errors
    /// [`Error::NoAckHeader`] for messages from `auto` subscriptions, or as
    /// for [`Client::send_frame`].
    pub async fn ack(&self, msg: &Message) -> Result<(), Error> {
        self.send_frame(Self::ack_frame(Command::Ack, msg, None)?)
            .await
    }

    /// Tells the server a message was not consumed.
    ///
    /// # Errors
    /// As for [`Client::ack`].
    pub async fn nack(&self, msg: &Message) -> Result<(), Error> {
        self.send_frame(Self::ack_frame(Command::Nack, msg, None)?)
            .await
    }

    /// Starts a transaction.
    ///
    /// # Errors
    /// As for [`Client::send_frame`].
    pub async fn begin(&self) -> Result<Transaction<'_>, Error> {
        let id = self.next_id("tx");
        self.send_frame(Frame::new(Command::Begin).with_header("transaction", id.clone()))
            .await?;
        Ok(Transaction { client: self, id })
    }

    /// Gracefully disconnects: sends DISCONNECT with a receipt, waits for the
    /// RECEIPT (all previous frames have then been received), and closes.
    ///
    /// # Errors
    /// As for [`Client::send_confirmed`].
    pub async fn disconnect(self) -> Result<(), Error> {
        let res = self
            .send_with_receipt(Frame::new(Command::Disconnect))
            .await;
        let _ = self.out.send(Outgoing::Close);
        let _ = self.closed().await;
        res
    }

    /// Resolves when the connection ends, with the reason.
    pub async fn closed(&self) -> Error {
        let mut rx = self.closed.clone();
        match rx.wait_for(Option::is_some).await {
            Ok(v) => v.clone().unwrap_or(Error::ConnectionClosed),
            Err(_) => Error::ConnectionClosed,
        }
    }
}

/// A transaction; frames sent through it are bound to it. Dropping it without
/// committing leaves it open until the connection ends, when the server aborts it.
pub struct Transaction<'a> {
    client: &'a Client,
    id: String,
}

impl Transaction<'_> {
    #[must_use]
    pub fn id(&self) -> &str {
        &self.id
    }

    /// Sends a message as part of the transaction; it is delivered on commit.
    ///
    /// # Errors
    /// As for [`Client::send_frame`].
    pub async fn send(&self, msg: OutgoingMessage) -> Result<(), Error> {
        self.client
            .send_frame(msg.frame.with_header("transaction", self.id.clone()))
            .await
    }

    /// Acknowledges a message as part of the transaction.
    ///
    /// # Errors
    /// As for [`Client::ack`].
    pub async fn ack(&self, msg: &Message) -> Result<(), Error> {
        self.client
            .send_frame(Client::ack_frame(Command::Ack, msg, Some(&self.id))?)
            .await
    }

    /// Rejects a message as part of the transaction.
    ///
    /// # Errors
    /// As for [`Client::ack`].
    pub async fn nack(&self, msg: &Message) -> Result<(), Error> {
        self.client
            .send_frame(Client::ack_frame(Command::Nack, msg, Some(&self.id))?)
            .await
    }

    /// Commits and waits for the server to confirm.
    ///
    /// # Errors
    /// As for [`Client::send_confirmed`].
    pub async fn commit(self) -> Result<(), Error> {
        self.client
            .send_with_receipt(Frame::new(Command::Commit).with_header("transaction", self.id))
            .await
    }

    /// Aborts and waits for the server to confirm.
    ///
    /// # Errors
    /// As for [`Client::send_confirmed`].
    pub async fn abort(self) -> Result<(), Error> {
        self.client
            .send_with_receipt(Frame::new(Command::Abort).with_header("transaction", self.id))
            .await
    }
}

fn server_error(frame: Frame) -> Error {
    Error::Server {
        message: frame.header("message").map(str::to_string),
        frame,
    }
}

/// Owns the socket: writes outgoing frames, routes incoming ones, and keeps
/// heart-beats flowing in both directions.
struct IoTask {
    conn: Conn,
    shared: Arc<Mutex<Shared>>,
    out: mpsc::UnboundedReceiver<Outgoing>,
    hb_out: Option<Duration>,
    hb_in: Option<Duration>,
}

impl IoTask {
    async fn run(mut self, closed_tx: watch::Sender<Option<Error>>) {
        let reason = self.event_loop().await;
        {
            let mut shared = lock(&self.shared);
            shared.closed = Some(reason.clone());
            for (_, waiter) in shared.receipts.drain() {
                let _ = waiter.send(Err(reason.clone()));
            }
            shared.subs.clear();
        }
        // Fail anything still queued for writing.
        self.out.close();
        while let Ok(cmd) = self.out.try_recv() {
            if let Outgoing::Frame(_, done) = cmd {
                let _ = done.send(Err(reason.clone()));
            }
        }
        let _ = closed_tx.send(Some(reason));
        let _ = self.conn.get_mut().inner.shutdown().await;
    }

    async fn event_loop(&mut self) -> Error {
        let mut hb = Heartbeats::new(self.hb_out, self.hb_in, self.conn.get_ref().last.clone());
        let mut disconnecting = false;
        // A message waiting for room in its subscription's buffer. While set,
        // the socket is not read, so TCP pushes back on the server.
        let mut stalled: Option<(mpsc::Sender<Message>, Message)> = None;
        loop {
            tokio::select! {
                permit = async { stalled.as_ref().unwrap().0.clone().reserve_owned().await },
                    if stalled.is_some() =>
                {
                    let (_, msg) = stalled.take().unwrap();
                    if let Ok(permit) = permit {
                        permit.send(msg);
                    }
                }
                item = self.conn.next(), if stalled.is_none() => match item {
                    Some(Ok(Item::Heartbeat)) => {}
                    Some(Ok(Item::Frame(f))) => match self.incoming(f) {
                        Incoming::Handled => {}
                        Incoming::Stall(tx, msg) => stalled = Some((tx, msg)),
                        Incoming::End(reason) => return reason,
                    },
                    Some(Err(e)) => return e.into(),
                    None => return Error::ConnectionClosed,
                },
                cmd = self.out.recv() => {
                    // Write everything already queued, then flush once.
                    let mut next = cmd;
                    let mut waiters = Vec::new();
                    let mut close = false;
                    let mut failed = None;
                    loop {
                        match next {
                            Some(Outgoing::Frame(frame, done)) => {
                                // Clients MUST NOT send any more frames after DISCONNECT.
                                if disconnecting || failed.is_some() {
                                    let _ = done.send(Err(Error::ConnectionClosed));
                                } else {
                                    disconnecting = frame.command == Command::Disconnect;
                                    match self.conn.feed(frame).await {
                                        Ok(()) => waiters.push(done),
                                        Err(e) => {
                                            let e = Error::from(e);
                                            let _ = done.send(Err(e.clone()));
                                            failed = Some(e);
                                        }
                                    }
                                }
                            }
                            Some(Outgoing::Close) | None => {
                                close = true;
                                break;
                            }
                        }
                        if waiters.len() >= 256 {
                            break;
                        }
                        match self.out.try_recv() {
                            Ok(cmd) => next = Some(cmd),
                            Err(_) => break,
                        }
                    }
                    if failed.is_none()
                        && let Err(e) = SinkExt::<Frame>::flush(&mut self.conn).await {
                            failed = Some(e.into());
                        }
                    let res = failed.clone().map_or(Ok(()), Err);
                    for done in waiters {
                        let _ = done.send(res.clone());
                    }
                    if let Some(e) = failed {
                        return e;
                    }
                    if close {
                        return Error::ConnectionClosed;
                    }
                    hb.wrote();
                }
                tick = hb.tick() => match tick {
                    Tick::SendHeartbeat => {
                        if let Err(e) = self.conn.send(Item::Heartbeat).await {
                            return e.into();
                        }
                        hb.wrote();
                    }
                    // Unread data may be waiting while we push back; the peer is alive.
                    Tick::PeerDead if stalled.is_some() => hb.read_alive(),
                    Tick::PeerDead => return Error::HeartbeatTimeout,
                },
            }
        }
    }

    /// Handles a server frame.
    fn incoming(&mut self, f: Frame) -> Incoming {
        match f.command {
            Command::Message => {
                let shared = lock(&self.shared);
                if let Some(tx) = f.header("subscription").and_then(|s| shared.subs.get(s))
                    && let Err(mpsc::error::TrySendError::Full(msg)) =
                        tx.try_send(Message { frame: f })
                {
                    return Incoming::Stall(tx.clone(), msg);
                }
                Incoming::Handled
            }
            Command::Receipt => {
                if let Some(id) = f.header("receipt-id")
                    && let Some(waiter) = lock(&self.shared).receipts.remove(id)
                {
                    let _ = waiter.send(Ok(()));
                }
                Incoming::Handled
            }
            // The server closes the connection after an ERROR.
            Command::Error => Incoming::End(server_error(f)),
            other => Incoming::End(Error::Protocol(format!(
                "unexpected {} frame from server",
                other.as_str()
            ))),
        }
    }
}

enum Incoming {
    Handled,
    /// The subscription's buffer is full: hold this message and stop reading.
    Stall(mpsc::Sender<Message>, Message),
    /// The connection must end.
    End(Error),
}
