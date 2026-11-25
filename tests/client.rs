//! Spec compliance tests for the STOMP 1.2 client.
//! The client is driven against a scripted fake server so that the exact
//! frames it puts on the wire can be inspected.

mod common;

use std::net::SocketAddr;
use std::time::Duration;

use common::{start_default_server, Conn, WAIT};
use kyu::client::{AckMode, Client, ConnectOptions, Error, OutgoingMessage};
use kyu::codec::Item;
use kyu::frame::{Command, Frame};
use tokio::net::TcpListener;
use tokio::time::timeout;

/// A fake server: accepts one connection and lets the test script it.
struct FakeServer {
    listener: TcpListener,
}

impl FakeServer {
    async fn new() -> (FakeServer, SocketAddr) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        (FakeServer { listener }, addr)
    }

    async fn accept(&self) -> Conn {
        let (stream, _) = self.listener.accept().await.unwrap();
        Conn::from_stream(stream)
    }

    /// Accepts, reads the CONNECT frame and answers with the given CONNECTED headers.
    async fn handshake(&self, extra: &[(&str, &str)]) -> (Conn, Frame) {
        let mut c = self.accept().await;
        let connect = c.recv().await;
        let mut connected = Frame::new(Command::Connected).with_header("version", "1.2");
        for (k, v) in extra {
            connected = connected.with_header(*k, *v);
        }
        c.send(connected).await;
        (c, connect)
    }
}

fn opts() -> ConnectOptions {
    ConnectOptions::new("example.org")
}

/// Runs a client connect concurrently with a fake server handshake.
async fn connected_pair(extra: &[(&str, &str)], options: ConnectOptions) -> (Client, Conn, Frame) {
    let (server, addr) = FakeServer::new().await;
    let (client, (conn, connect)) =
        tokio::join!(Client::connect(addr, options), server.handshake(extra));
    (client.expect("client connect"), conn, connect)
}

fn reply_receipt(f: &Frame) -> Frame {
    Frame::new(Command::Receipt).with_header("receipt-id", f.header("receipt").expect("receipt header"))
}

// ---------------------------------------------------------------------------
// Connecting
// ---------------------------------------------------------------------------

#[tokio::test]
async fn connect_sends_required_headers() {
    let (_client, _conn, connect) = connected_pair(&[], opts()).await;
    assert_eq!(connect.command, Command::Connect, "1.2 clients SHOULD use CONNECT");
    assert_eq!(connect.header("accept-version"), Some("1.2"));
    assert_eq!(connect.header("host"), Some("example.org"));
    assert_eq!(connect.header("login"), None);
    assert_eq!(connect.header("passcode"), None);
    assert!(connect.body.is_empty());
}

#[tokio::test]
async fn connect_sends_optional_headers() {
    let (_client, _conn, connect) = connected_pair(
        &[],
        opts().credentials("guest", "pa:ss").heartbeat(1000, 2000),
    )
    .await;
    assert_eq!(connect.header("login"), Some("guest"));
    assert_eq!(connect.header("passcode"), Some("pa:ss"));
    assert_eq!(connect.header("heart-beat"), Some("1000,2000"));
}

#[tokio::test]
async fn connect_can_use_stomp_command() {
    let (_client, _conn, connect) = connected_pair(&[], opts().use_stomp_command(true)).await;
    assert_eq!(connect.command, Command::Stomp);
}

#[tokio::test]
async fn connected_details_are_exposed() {
    let (client, _conn, _) = connected_pair(
        &[("session", "s-1"), ("server", "fake/1.0 test"), ("heart-beat", "0,0")],
        opts(),
    )
    .await;
    let info = client.connected();
    assert_eq!(info.version, "1.2");
    assert_eq!(info.session.as_deref(), Some("s-1"));
    assert_eq!(info.server.as_deref(), Some("fake/1.0 test"));
    assert_eq!(info.heartbeat, (0, 0));
}

#[tokio::test]
async fn connect_fails_with_server_error() {
    let (server, addr) = FakeServer::new().await;
    let fake = async {
        let mut c = server.accept().await;
        c.recv().await;
        c.send(Frame::new(Command::Error).with_header("message", "go away").with_body("details")).await;
    };
    let (res, _) = tokio::join!(Client::connect(addr, opts()), fake);
    match res {
        Err(Error::Server { message, .. }) => assert_eq!(message.as_deref(), Some("go away")),
        other => panic!("expected server error, got {:?}", other.map(|_| ())),
    }
}

#[tokio::test]
async fn connect_fails_on_unsupported_version() {
    let (server, addr) = FakeServer::new().await;
    let fake = async {
        let mut c = server.accept().await;
        c.recv().await;
        c.send(Frame::new(Command::Connected).with_header("version", "1.1")).await;
    };
    let (res, _) = tokio::join!(Client::connect(addr, opts()), fake);
    assert!(matches!(res, Err(Error::UnsupportedVersion(v)) if v == "1.1"));
}

#[tokio::test]
async fn connect_fails_when_server_closes() {
    let (server, addr) = FakeServer::new().await;
    let fake = async {
        let mut c = server.accept().await;
        c.recv().await;
    };
    let (res, _) = tokio::join!(Client::connect(addr, opts()), fake);
    assert!(matches!(res, Err(Error::ConnectionClosed)));
}

// ---------------------------------------------------------------------------
// SEND
// ---------------------------------------------------------------------------

#[tokio::test]
async fn send_puts_message_on_the_wire() {
    let (client, mut conn, _) = connected_pair(&[], opts()).await;
    client
        .send(
            OutgoingMessage::new("/queue/a")
                .content_type("text/plain")
                .header("x-user", "a:b")
                .body("hello queue a"),
        )
        .await
        .unwrap();
    let f = conn.expect_command(Command::Send).await;
    assert_eq!(f.header("destination"), Some("/queue/a"));
    assert_eq!(f.header("content-type"), Some("text/plain"));
    assert_eq!(f.header("content-length"), Some("13"));
    assert_eq!(f.header("x-user"), Some("a:b"));
    assert_eq!(f.header("receipt"), None);
    assert_eq!(&f.body[..], b"hello queue a");
}

#[tokio::test]
async fn send_confirmed_waits_for_receipt() {
    let (client, mut conn, _) = connected_pair(&[], opts()).await;
    let fake = async {
        let f = conn.expect_command(Command::Send).await;
        tokio::time::sleep(Duration::from_millis(100)).await;
        conn.send(reply_receipt(&f)).await;
        conn
    };
    let start = std::time::Instant::now();
    let (res, _conn) = tokio::join!(client.send_confirmed(OutgoingMessage::new("/q").body("x")), fake);
    res.unwrap();
    assert!(start.elapsed() >= Duration::from_millis(100));
}

#[tokio::test]
async fn receipts_are_matched_by_id() {
    let (client, mut conn, _) = connected_pair(&[], opts()).await;
    let fake = async {
        let a = conn.expect_command(Command::Send).await;
        let b = conn.expect_command(Command::Send).await;
        assert_ne!(a.header("receipt"), b.header("receipt"));
        conn.send(reply_receipt(&b)).await;
        conn.send(reply_receipt(&a)).await;
        conn
    };
    let (ra, rb, _conn) = tokio::join!(
        client.send_confirmed(OutgoingMessage::new("/q").body("a")),
        client.send_confirmed(OutgoingMessage::new("/q").body("b")),
        fake
    );
    ra.unwrap();
    rb.unwrap();
}

// ---------------------------------------------------------------------------
// SUBSCRIBE / MESSAGE / UNSUBSCRIBE
// ---------------------------------------------------------------------------

#[tokio::test]
async fn subscribe_sends_id_destination_and_ack() {
    let (client, mut conn, _) = connected_pair(&[], opts()).await;
    let fake = async {
        let f = conn.expect_command(Command::Subscribe).await;
        conn.send(reply_receipt(&f)).await;
        let g = conn.expect_command(Command::Subscribe).await;
        conn.send(reply_receipt(&g)).await;
        (f, g)
    };
    let (s1, s2, (f, g)) = tokio::join!(
        client.subscribe("/queue/foo", AckMode::Client),
        async {
            tokio::time::sleep(Duration::from_millis(20)).await;
            client.subscribe("/queue/bar", AckMode::Auto).await
        },
        fake
    );
    let (s1, s2) = (s1.unwrap(), s2.unwrap());
    assert_eq!(f.header("destination"), Some("/queue/foo"));
    assert_eq!(f.header("ack"), Some("client"));
    assert_eq!(f.header("id"), Some(s1.id()));
    assert_eq!(g.header("ack"), Some("auto"));
    assert_eq!(g.header("id"), Some(s2.id()));
    assert_ne!(s1.id(), s2.id(), "subscription ids must be unique per connection");
}

#[tokio::test]
async fn ack_mode_header_values() {
    assert_eq!(AckMode::Auto.as_str(), "auto");
    assert_eq!(AckMode::Client.as_str(), "client");
    assert_eq!(AckMode::ClientIndividual.as_str(), "client-individual");
}

async fn subscribed(
    ack: AckMode,
) -> (Client, Conn, kyu::client::Subscription) {
    let (client, mut conn, _) = connected_pair(&[], opts()).await;
    let fake = async {
        let f = conn.expect_command(Command::Subscribe).await;
        conn.send(reply_receipt(&f)).await;
        conn
    };
    let (sub, conn) = tokio::join!(client.subscribe("/queue/foo", ack), fake);
    (client, conn, sub.unwrap())
}

fn message(sub: &str, id: &str, body: &str) -> Frame {
    Frame::new(Command::Message)
        .with_header("subscription", sub)
        .with_header("message-id", id)
        .with_header("destination", "/queue/foo")
        .with_body(body.to_string())
}

#[tokio::test]
async fn messages_are_routed_by_subscription_header() {
    let (client, mut conn, _) = connected_pair(&[], opts()).await;
    let fake = async {
        for _ in 0..2 {
            let f = conn.expect_command(Command::Subscribe).await;
            conn.send(reply_receipt(&f)).await;
        }
        conn
    };
    let (a, b, mut conn) = tokio::join!(
        client.subscribe("/queue/a", AckMode::Auto),
        async {
            tokio::time::sleep(Duration::from_millis(20)).await;
            client.subscribe("/queue/b", AckMode::Auto).await
        },
        fake
    );
    let (mut a, mut b) = (a.unwrap(), b.unwrap());
    conn.send(message(b.id(), "1", "for b")).await;
    conn.send(message(a.id(), "2", "for a")).await;
    let ma = timeout(WAIT, a.recv()).await.unwrap().unwrap();
    let mb = timeout(WAIT, b.recv()).await.unwrap().unwrap();
    assert_eq!(&ma.body()[..], b"for a");
    assert_eq!(ma.message_id(), Some("2"));
    assert_eq!(ma.destination(), Some("/queue/foo"));
    assert_eq!(&mb.body()[..], b"for b");
}

#[tokio::test]
async fn unsubscribe_sends_matching_id() {
    let (client, mut conn, sub) = subscribed(AckMode::Auto).await;
    let id = sub.id().to_string();
    let fake = async {
        let f = conn.expect_command(Command::Unsubscribe).await;
        conn.send(reply_receipt(&f)).await;
        f
    };
    let (res, f) = tokio::join!(client.unsubscribe(sub), fake);
    res.unwrap();
    assert_eq!(f.header("id"), Some(id.as_str()));
}

// ---------------------------------------------------------------------------
// ACK / NACK
// ---------------------------------------------------------------------------

#[tokio::test]
async fn ack_uses_message_ack_header_as_id() {
    let (client, mut conn, mut sub) = subscribed(AckMode::ClientIndividual).await;
    conn.send(message(sub.id(), "m1", "x").with_header("ack", "ack-77")).await;
    let m = timeout(WAIT, sub.recv()).await.unwrap().unwrap();
    client.ack(&m).await.unwrap();
    let f = conn.expect_command(Command::Ack).await;
    assert_eq!(f.header("id"), Some("ack-77"));
    assert_eq!(f.header("transaction"), None);
    client.nack(&m).await.unwrap();
    let f = conn.expect_command(Command::Nack).await;
    assert_eq!(f.header("id"), Some("ack-77"));
}

#[tokio::test]
async fn acking_a_message_without_ack_header_is_an_error() {
    let (client, conn, mut sub) = subscribed(AckMode::Auto).await;
    let mut conn = conn;
    conn.send(message(sub.id(), "m1", "x")).await;
    let m = timeout(WAIT, sub.recv()).await.unwrap().unwrap();
    assert!(matches!(client.ack(&m).await, Err(Error::NoAckHeader)));
    assert!(matches!(client.nack(&m).await, Err(Error::NoAckHeader)));
}

// ---------------------------------------------------------------------------
// Transactions
// ---------------------------------------------------------------------------

#[tokio::test]
async fn transaction_binds_frames_and_commits() {
    let (client, mut conn, mut sub) = subscribed(AckMode::Client).await;
    conn.send(message(sub.id(), "m1", "x").with_header("ack", "a1")).await;
    let m = timeout(WAIT, sub.recv()).await.unwrap().unwrap();

    let fake = async {
        let b = conn.expect_command(Command::Begin).await;
        let tx = b.header("transaction").expect("transaction header").to_string();
        let s = conn.expect_command(Command::Send).await;
        assert_eq!(s.header("transaction"), Some(tx.as_str()));
        let a = conn.expect_command(Command::Ack).await;
        assert_eq!(a.header("transaction"), Some(tx.as_str()));
        let n = conn.expect_command(Command::Nack).await;
        assert_eq!(n.header("transaction"), Some(tx.as_str()));
        let c = conn.expect_command(Command::Commit).await;
        assert_eq!(c.header("transaction"), Some(tx.as_str()));
        conn.send(reply_receipt(&c)).await;
        tx
    };
    let work = async {
        let tx = client.begin().await.unwrap();
        tx.send(OutgoingMessage::new("/q").body("in tx")).await.unwrap();
        tx.ack(&m).await.unwrap();
        tx.nack(&m).await.unwrap();
        tx.commit().await.unwrap();
    };
    tokio::join!(work, fake);
}

#[tokio::test]
async fn transaction_abort_and_unique_ids() {
    let (client, mut conn, _) = connected_pair(&[], opts()).await;
    let fake = async {
        let b1 = conn.expect_command(Command::Begin).await;
        let a = conn.expect_command(Command::Abort).await;
        assert_eq!(a.header("transaction"), b1.header("transaction"));
        conn.send(reply_receipt(&a)).await;
        let b2 = conn.expect_command(Command::Begin).await;
        assert_ne!(b1.header("transaction"), b2.header("transaction"));
    };
    let work = async {
        client.begin().await.unwrap().abort().await.unwrap();
        let _tx = client.begin().await.unwrap();
    };
    tokio::join!(work, fake);
}

// ---------------------------------------------------------------------------
// DISCONNECT
// ---------------------------------------------------------------------------

#[tokio::test]
async fn disconnect_sends_receipt_waits_and_closes() {
    let (client, mut conn, _) = connected_pair(&[], opts()).await;
    let fake = async {
        let f = conn.expect_command(Command::Disconnect).await;
        assert!(f.header("receipt").is_some(), "graceful DISCONNECT SHOULD request a receipt");
        tokio::time::sleep(Duration::from_millis(50)).await;
        conn.send(reply_receipt(&f)).await;
        conn.expect_closed().await;
    };
    let (res, _) = tokio::join!(client.disconnect(), fake);
    res.unwrap();
}

#[tokio::test]
async fn disconnect_does_not_hang_if_server_closes_without_receipt() {
    let (client, mut conn, _) = connected_pair(&[], opts()).await;
    let fake = async {
        conn.expect_command(Command::Disconnect).await;
        drop(conn);
    };
    let (res, _) = tokio::join!(timeout(WAIT, client.disconnect()), fake);
    assert!(res.is_ok(), "disconnect hung");
}

// ---------------------------------------------------------------------------
// Heart-beating
// ---------------------------------------------------------------------------

#[tokio::test]
async fn client_sends_heartbeats_when_negotiated() {
    // client can send every 50ms, server wants every 100ms => MAX = 100ms
    let (_client, mut conn, _) =
        connected_pair(&[("heart-beat", "0,100")], opts().heartbeat(50, 0)).await;
    let start = tokio::time::Instant::now();
    let mut beats = 0;
    while start.elapsed() < Duration::from_millis(550) {
        match conn.next_item(Duration::from_millis(300)).await {
            Some(Item::Heartbeat) => beats += 1,
            other => panic!("expected heart-beat, got {other:?}"),
        }
    }
    assert!((3..=7).contains(&beats), "got {beats} heart-beats");
}

#[tokio::test]
async fn client_sends_no_heartbeats_when_server_does_not_want_them() {
    let (_client, mut conn, _) =
        connected_pair(&[("heart-beat", "0,0")], opts().heartbeat(20, 0)).await;
    assert_eq!(conn.next_item(Duration::from_millis(200)).await, None);
}

#[tokio::test]
async fn client_detects_silent_server() {
    // server promises heart-beats every 50ms but sends nothing
    let (client, _conn, _) =
        connected_pair(&[("heart-beat", "50,0")], opts().heartbeat(0, 50)).await;
    let reason = timeout(Duration::from_secs(1), client.closed()).await.expect("not detected");
    assert!(matches!(reason, Error::HeartbeatTimeout));
}

#[tokio::test]
async fn server_heartbeats_keep_client_alive() {
    let (client, mut conn, _) =
        connected_pair(&[("heart-beat", "50,0")], opts().heartbeat(0, 50)).await;
    let beats = async {
        for _ in 0..10 {
            tokio::time::sleep(Duration::from_millis(40)).await;
            conn.send_heartbeat().await;
        }
    };
    let res = tokio::select! {
        _ = beats => None,
        r = client.closed() => Some(r),
    };
    assert!(res.is_none(), "client gave up despite heart-beats: {res:?}");
}

// ---------------------------------------------------------------------------
// ERROR from server mid-session
// ---------------------------------------------------------------------------

#[tokio::test]
async fn server_error_fails_pending_receipts_and_closes() {
    let (client, mut conn, _) = connected_pair(&[], opts()).await;
    let fake = async {
        let f = conn.expect_command(Command::Send).await;
        conn.send(
            Frame::new(Command::Error)
                .with_header("message", "bad send")
                .with_header("receipt-id", f.header("receipt").unwrap()),
        )
        .await;
    };
    let (res, _) = tokio::join!(client.send_confirmed(OutgoingMessage::new("/q")), fake);
    assert!(matches!(res, Err(Error::Server { message: Some(m), .. }) if m == "bad send"));
    assert!(matches!(client.closed().await, Error::Server { .. }));
    assert!(client.send(OutgoingMessage::new("/q")).await.is_err());
}

#[tokio::test]
async fn subscription_ends_when_connection_closes() {
    let (_client, conn, mut sub) = subscribed(AckMode::Auto).await;
    drop(conn);
    assert!(timeout(WAIT, sub.recv()).await.unwrap().is_none());
}

// ---------------------------------------------------------------------------
// Client <-> real server
// ---------------------------------------------------------------------------

#[tokio::test]
async fn end_to_end_publish_subscribe() {
    let addr = start_default_server().await;
    let consumer = Client::connect(addr, opts()).await.unwrap();
    let producer = Client::connect(addr, opts()).await.unwrap();
    let mut sub = consumer.subscribe("/topic/e2e", AckMode::Auto).await.unwrap();
    producer
        .send_confirmed(OutgoingMessage::new("/topic/e2e").content_type("text/plain").header("k", "v").body("hi"))
        .await
        .unwrap();
    let m = timeout(WAIT, sub.recv()).await.unwrap().unwrap();
    assert_eq!(&m.body()[..], b"hi");
    assert_eq!(m.header("k"), Some("v"));
    assert_eq!(m.subscription(), Some(sub.id()));
    producer.disconnect().await.unwrap();
    consumer.disconnect().await.unwrap();
}

#[tokio::test]
async fn end_to_end_transactions_and_acks() {
    let addr = start_default_server().await;
    let c = Client::connect(addr, opts()).await.unwrap();
    let mut sub = c.subscribe("/queue/e2e-tx", AckMode::ClientIndividual).await.unwrap();

    let tx = c.begin().await.unwrap();
    tx.send(OutgoingMessage::new("/queue/e2e-tx").body("one")).await.unwrap();
    assert!(timeout(Duration::from_millis(150), sub.recv()).await.is_err());
    tx.commit().await.unwrap();

    let m = timeout(WAIT, sub.recv()).await.unwrap().unwrap();
    assert_eq!(&m.body()[..], b"one");
    assert!(m.ack_id().is_some());
    c.ack(&m).await.unwrap();
    c.disconnect().await.unwrap();

    // acked, so nothing is redelivered
    let c = Client::connect(addr, opts()).await.unwrap();
    let mut sub = c.subscribe("/queue/e2e-tx", AckMode::Auto).await.unwrap();
    assert!(timeout(Duration::from_millis(150), sub.recv()).await.is_err());
}

#[tokio::test]
async fn end_to_end_heartbeats() {
    let addr = common::start_server(kyu::server::ServerConfig {
        heartbeat: (30, 30),
        ..Default::default()
    })
    .await;
    let c = Client::connect(addr, opts().heartbeat(30, 30)).await.unwrap();
    assert_eq!(c.connected().heartbeat, (30, 30));
    let res = timeout(Duration::from_millis(400), c.closed()).await;
    assert!(res.is_err(), "connection died despite heart-beating: {res:?}");
    c.send_confirmed(OutgoingMessage::new("/queue/hb").body("x")).await.unwrap();
}

#[tokio::test]
async fn end_to_end_server_error_surfaces() {
    let addr = start_default_server().await;
    let c = Client::connect(addr, opts()).await.unwrap();
    // ACK of an unknown id is an error per this server
    let res = c.send_frame(Frame::new(Command::Ack).with_header("id", "nope")).await;
    assert!(res.is_ok());
    assert!(matches!(timeout(WAIT, c.closed()).await.unwrap(), Error::Server { .. }));
}
