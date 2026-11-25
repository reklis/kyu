#![allow(dead_code)]

use std::collections::VecDeque;
use std::net::SocketAddr;
use std::time::Duration;

use futures::{SinkExt, StreamExt};
use kyu::codec::{Item, StompCodec};
use kyu::frame::{Command, Frame};
use kyu::server::{Server, ServerConfig};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::time::timeout;
use tokio_util::codec::Framed;

pub const WAIT: Duration = Duration::from_secs(2);
pub const QUIET: Duration = Duration::from_millis(200);

pub async fn start_server(config: ServerConfig) -> SocketAddr {
    let server = Server::bind("127.0.0.1:0", config).await.expect("bind");
    let addr = server.local_addr().unwrap();
    tokio::spawn(server.run());
    addr
}

pub async fn start_default_server() -> SocketAddr {
    start_server(ServerConfig::default()).await
}

/// A raw STOMP connection used to drive the server frame by frame.
pub struct Conn {
    pub framed: Framed<TcpStream, StompCodec>,
    /// Frames that arrived while waiting for a specific RECEIPT.
    pub held: VecDeque<Frame>,
}

impl Conn {
    pub async fn open(addr: SocketAddr) -> Conn {
        Conn::from_stream(TcpStream::connect(addr).await.expect("connect"))
    }

    pub fn from_stream(stream: TcpStream) -> Conn {
        Conn {
            framed: Framed::new(stream, StompCodec::new()),
            held: VecDeque::new(),
        }
    }

    /// Opens a connection and completes a standard 1.2 CONNECT handshake.
    pub async fn connect(addr: SocketAddr) -> Conn {
        let mut c = Conn::open(addr).await;
        c.send(connect_frame()).await;
        let f = c.recv().await;
        assert_eq!(f.command, Command::Connected, "handshake failed: {f:?}");
        c
    }

    pub async fn send(&mut self, frame: Frame) {
        self.framed.send(frame).await.expect("send frame");
    }

    pub async fn send_heartbeat(&mut self) {
        self.framed
            .send(Item::Heartbeat)
            .await
            .expect("send heartbeat");
    }

    pub async fn send_raw(&mut self, bytes: &[u8]) {
        self.framed
            .get_mut()
            .write_all(bytes)
            .await
            .expect("raw write");
    }

    /// Next item (frame or heartbeat), or None on close/timeout.
    pub async fn next_item(&mut self, wait: Duration) -> Option<Item> {
        match timeout(wait, self.framed.next()).await {
            Ok(Some(Ok(item))) => Some(item),
            _ => None,
        }
    }

    /// Next frame, skipping heart-beats. Panics on timeout/close.
    pub async fn recv(&mut self) -> Frame {
        self.try_recv(WAIT).await.expect("expected a frame")
    }

    pub async fn try_recv(&mut self, wait: Duration) -> Option<Frame> {
        if let Some(f) = self.held.pop_front() {
            return Some(f);
        }
        self.try_recv_wire(wait).await
    }

    async fn try_recv_wire(&mut self, wait: Duration) -> Option<Frame> {
        let deadline = tokio::time::Instant::now() + wait;
        loop {
            let left = deadline.saturating_duration_since(tokio::time::Instant::now());
            match timeout(left, self.framed.next()).await {
                Ok(Some(Ok(Item::Frame(f)))) => return Some(f),
                Ok(Some(Ok(Item::Heartbeat))) => {}
                _ => return None,
            }
        }
    }

    pub async fn expect_command(&mut self, cmd: Command) -> Frame {
        let f = self.recv().await;
        assert_eq!(f.command, cmd, "unexpected frame: {f:?}");
        f
    }

    /// Asserts an ERROR frame arrives and the server then closes the connection.
    pub async fn expect_error_and_close(&mut self) -> Frame {
        let f = self.expect_command(Command::Error).await;
        self.expect_closed().await;
        f
    }

    pub async fn expect_closed(&mut self) {
        let deadline = tokio::time::Instant::now() + WAIT;
        loop {
            let left = deadline.saturating_duration_since(tokio::time::Instant::now());
            match timeout(left, self.framed.next()).await {
                Ok(None | Some(Err(_))) => return,
                Ok(Some(Ok(Item::Heartbeat))) => {}
                Ok(Some(Ok(Item::Frame(f)))) => panic!("expected close, got frame {f:?}"),
                Err(elapsed) => panic!("connection was not closed: {elapsed}"),
            }
        }
    }

    /// Waits (unbounded) until the peer closes the connection.
    pub async fn wait_closed(&mut self) {
        while let Some(Ok(_)) = self.framed.next().await {}
    }

    pub async fn expect_nothing(&mut self) {
        if let Some(f) = self.try_recv(QUIET).await {
            panic!("expected no frame, got {f:?}");
        }
    }

    /// Sends a frame with a receipt header and waits for the matching RECEIPT.
    pub async fn send_with_receipt(&mut self, frame: Frame, receipt: &str) {
        self.send(frame.with_header("receipt", receipt)).await;
        loop {
            let f = self.try_recv_wire(WAIT).await.expect("expected RECEIPT");
            if f.command == Command::Receipt && f.header("receipt-id") == Some(receipt) {
                return;
            }
            assert_ne!(
                f.command,
                Command::Error,
                "got ERROR waiting for receipt: {f:?}"
            );
            self.held.push_back(f);
        }
    }

    pub async fn subscribe(&mut self, id: &str, dest: &str, ack: &str) {
        self.send_with_receipt(
            Frame::new(Command::Subscribe)
                .with_header("id", id)
                .with_header("destination", dest)
                .with_header("ack", ack),
            &format!("sub-{id}"),
        )
        .await;
    }

    pub async fn publish(&mut self, dest: &str, body: &str) {
        self.send_with_receipt(send_frame(dest, body), &format!("pub-{body}"))
            .await;
    }

    pub async fn disconnect(mut self) {
        self.send_with_receipt(Frame::new(Command::Disconnect), "bye")
            .await;
        self.expect_closed().await;
    }
}

pub fn connect_frame() -> Frame {
    Frame::new(Command::Connect)
        .with_header("accept-version", "1.2")
        .with_header("host", "localhost")
}

pub fn send_frame(dest: &str, body: &str) -> Frame {
    Frame::new(Command::Send)
        .with_header("destination", dest)
        .with_header("content-type", "text/plain")
        .with_body(body.to_string())
}

pub fn body(f: &Frame) -> String {
    String::from_utf8(f.body.to_vec()).unwrap()
}
