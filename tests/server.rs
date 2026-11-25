//! Spec compliance tests for the STOMP 1.2 server, driven over real TCP.

mod common;

use std::collections::HashSet;
use std::time::Duration;

use common::*;
use kyu::codec::Item;
use kyu::frame::{Command, Frame};
use kyu::server::ServerConfig;

// ---------------------------------------------------------------------------
// Connecting
// ---------------------------------------------------------------------------

#[tokio::test]
async fn connect_is_answered_with_connected_version_1_2() {
    let addr = start_default_server().await;
    let mut c = Conn::open(addr).await;
    c.send(connect_frame()).await;
    let f = c.expect_command(Command::Connected).await;
    assert_eq!(f.header("version"), Some("1.2"));
}

#[tokio::test]
async fn stomp_frame_is_handled_like_connect() {
    let addr = start_default_server().await;
    let mut c = Conn::open(addr).await;
    c.send(
        Frame::new(Command::Stomp)
            .with_header("accept-version", "1.2")
            .with_header("host", "localhost"),
    )
    .await;
    let f = c.expect_command(Command::Connected).await;
    assert_eq!(f.header("version"), Some("1.2"));
}

#[tokio::test]
async fn connected_carries_session_and_server_headers() {
    let addr = start_default_server().await;
    let mut a = Conn::open(addr).await;
    a.send(connect_frame()).await;
    let fa = a.expect_command(Command::Connected).await;
    let mut b = Conn::open(addr).await;
    b.send(connect_frame()).await;
    let fb = b.expect_command(Command::Connected).await;

    let sa = fa.header("session").expect("session header");
    let sb = fb.header("session").expect("session header");
    assert_ne!(sa, sb, "session ids must be unique");

    // server = name ["/" version] *(comment)
    let server = fa.header("server").expect("server header");
    let name = server.split(' ').next().unwrap();
    assert!(name.starts_with("kyu/"), "{server}");
}

#[tokio::test]
async fn negotiates_highest_common_version() {
    let addr = start_default_server().await;
    let mut c = Conn::open(addr).await;
    c.send(
        Frame::new(Command::Connect)
            .with_header("accept-version", "1.0,1.1,1.2,2.0")
            .with_header("host", "localhost"),
    )
    .await;
    let f = c.expect_command(Command::Connected).await;
    assert_eq!(f.header("version"), Some("1.2"));
}

#[tokio::test]
async fn no_common_version_is_an_error_listing_supported_versions() {
    let addr = start_default_server().await;
    let mut c = Conn::open(addr).await;
    c.send(
        Frame::new(Command::Connect)
            .with_header("accept-version", "1.0,1.1,2.1")
            .with_header("host", "localhost"),
    )
    .await;
    let f = c.expect_error_and_close().await;
    assert_eq!(f.header("version"), Some("1.2"));
    assert_eq!(f.header("content-type"), Some("text/plain"));
    assert!(body(&f).contains("1.2"));
}

#[tokio::test]
async fn missing_accept_version_means_1_0_only() {
    let addr = start_default_server().await;
    let mut c = Conn::open(addr).await;
    c.send(Frame::new(Command::Connect).with_header("host", "localhost")).await;
    let f = c.expect_error_and_close().await;
    assert_eq!(f.header("version"), Some("1.2"));
}

#[tokio::test]
async fn missing_host_is_rejected() {
    let addr = start_default_server().await;
    let mut c = Conn::open(addr).await;
    c.send(Frame::new(Command::Connect).with_header("accept-version", "1.2")).await;
    let f = c.expect_error_and_close().await;
    assert!(f.header("message").is_some());
}

#[tokio::test]
async fn first_frame_must_be_connect() {
    let addr = start_default_server().await;
    let mut c = Conn::open(addr).await;
    c.send(send_frame("/queue/a", "hi")).await;
    c.expect_error_and_close().await;
}

#[tokio::test]
async fn connect_with_body_is_rejected() {
    let addr = start_default_server().await;
    let mut c = Conn::open(addr).await;
    c.send_raw(b"CONNECT\naccept-version:1.2\nhost:localhost\n\nbody\0").await;
    c.expect_error_and_close().await;
}

#[tokio::test]
async fn authentication_rejects_bad_credentials() {
    let addr = start_server(ServerConfig {
        credentials: Some(("guest".into(), "s3cret".into())),
        ..ServerConfig::default()
    })
    .await;

    let mut c = Conn::open(addr).await;
    c.send(connect_frame()).await;
    c.expect_error_and_close().await;

    let mut c = Conn::open(addr).await;
    c.send(connect_frame().with_header("login", "guest").with_header("passcode", "nope")).await;
    c.expect_error_and_close().await;

    let mut c = Conn::open(addr).await;
    c.send(connect_frame().with_header("login", "guest").with_header("passcode", "s3cret")).await;
    c.expect_command(Command::Connected).await;
}

#[tokio::test]
async fn connect_headers_are_not_unescaped() {
    // CONNECT does not use escaping, so a literal "\c" is a backslash and a 'c'.
    let addr = start_server(ServerConfig {
        credentials: Some(("a\\cb".into(), "p:w".into())),
        ..ServerConfig::default()
    })
    .await;
    let mut c = Conn::open(addr).await;
    c.send_raw(b"CONNECT\naccept-version:1.2\nhost:h\nlogin:a\\cb\npasscode:p:w\n\n\0").await;
    c.expect_command(Command::Connected).await;
}

// ---------------------------------------------------------------------------
// Heart-beating
// ---------------------------------------------------------------------------

fn hb_server(sx: u32, sy: u32) -> ServerConfig {
    ServerConfig { heartbeat: (sx, sy), ..ServerConfig::default() }
}

#[tokio::test]
async fn connected_advertises_server_heartbeat() {
    let addr = start_server(hb_server(100, 200)).await;
    let mut c = Conn::open(addr).await;
    c.send(connect_frame()).await;
    let f = c.expect_command(Command::Connected).await;
    assert_eq!(f.header("heart-beat"), Some("100,200"));
}

#[tokio::test]
async fn server_sends_heartbeats_at_negotiated_rate() {
    let addr = start_server(hb_server(50, 0)).await;
    let mut c = Conn::open(addr).await;
    // client cannot send, wants one every 100ms => MAX(50, 100) = 100ms
    c.send(connect_frame().with_header("heart-beat", "0,100")).await;
    c.expect_command(Command::Connected).await;

    let start = tokio::time::Instant::now();
    let mut beats = 0;
    while start.elapsed() < Duration::from_millis(550) {
        match c.next_item(Duration::from_millis(300)).await {
            Some(Item::Heartbeat) => beats += 1,
            other => panic!("expected heart-beat, got {other:?}"),
        }
    }
    assert!((3..=7).contains(&beats), "got {beats} heart-beats in ~550ms");
}

#[tokio::test]
async fn no_heartbeat_header_means_no_heartbeats() {
    let addr = start_server(hb_server(20, 20)).await;
    let mut c = Conn::connect(addr).await;
    assert_eq!(c.next_item(Duration::from_millis(200)).await, None);
}

#[tokio::test]
async fn zero_client_desire_means_no_heartbeats() {
    let addr = start_server(hb_server(20, 0)).await;
    let mut c = Conn::open(addr).await;
    c.send(connect_frame().with_header("heart-beat", "0,0")).await;
    c.expect_command(Command::Connected).await;
    assert_eq!(c.next_item(Duration::from_millis(200)).await, None);
}

#[tokio::test]
async fn server_drops_client_that_stops_heartbeating() {
    let addr = start_server(hb_server(0, 50)).await;
    let mut c = Conn::open(addr).await;
    // client promises a heart-beat every 50ms, then goes silent
    c.send(connect_frame().with_header("heart-beat", "50,0")).await;
    c.expect_command(Command::Connected).await;
    let closed = tokio::time::timeout(Duration::from_secs(1), c.wait_closed()).await;
    assert!(closed.is_ok(), "server kept a silent connection open");
}

#[tokio::test]
async fn client_heartbeats_keep_connection_alive() {
    let addr = start_server(hb_server(0, 50)).await;
    let mut c = Conn::open(addr).await;
    c.send(connect_frame().with_header("heart-beat", "50,0")).await;
    c.expect_command(Command::Connected).await;
    for _ in 0..10 {
        tokio::time::sleep(Duration::from_millis(40)).await;
        c.send_heartbeat().await;
    }
    c.send_with_receipt(send_frame("/queue/hb", "still here"), "r1").await;
}

#[tokio::test]
async fn any_frame_counts_as_heartbeat_activity() {
    let addr = start_server(hb_server(0, 50)).await;
    let mut c = Conn::open(addr).await;
    c.send(connect_frame().with_header("heart-beat", "50,0")).await;
    c.expect_command(Command::Connected).await;
    for i in 0..8 {
        tokio::time::sleep(Duration::from_millis(40)).await;
        c.send_with_receipt(send_frame("/queue/hb2", "x"), &format!("r{i}")).await;
    }
}

#[tokio::test]
async fn invalid_heartbeat_header_is_rejected() {
    let addr = start_default_server().await;
    let mut c = Conn::open(addr).await;
    c.send(connect_frame().with_header("heart-beat", "fast")).await;
    c.expect_error_and_close().await;
}

// ---------------------------------------------------------------------------
// SEND / SUBSCRIBE / MESSAGE
// ---------------------------------------------------------------------------

#[tokio::test]
async fn message_is_delivered_with_required_headers() {
    let addr = start_default_server().await;
    let mut sub = Conn::connect(addr).await;
    sub.subscribe("0", "/queue/a", "auto").await;

    let mut publ = Conn::connect(addr).await;
    publ.send_with_receipt(
        send_frame("/queue/a", "hello queue a").with_header("x-custom", "v:1"),
        "r",
    )
    .await;

    let m = sub.expect_command(Command::Message).await;
    assert_eq!(m.header("destination"), Some("/queue/a"));
    assert_eq!(m.header("subscription"), Some("0"));
    assert!(m.header("message-id").is_some());
    assert_eq!(m.header("content-type"), Some("text/plain"));
    assert_eq!(m.header("content-length"), Some("13"));
    assert_eq!(m.header("x-custom"), Some("v:1"), "user headers must pass through");
    assert_eq!(m.header("ack"), None, "auto subscriptions carry no ack header");
    assert_eq!(m.header("receipt"), None);
    assert_eq!(body(&m), "hello queue a");
}

#[tokio::test]
async fn message_ids_are_unique() {
    let addr = start_default_server().await;
    let mut c = Conn::connect(addr).await;
    c.subscribe("s", "/topic/ids", "auto").await;
    for i in 0..5 {
        c.send(send_frame("/topic/ids", &i.to_string())).await;
    }
    let mut ids = HashSet::new();
    for _ in 0..5 {
        let m = c.expect_command(Command::Message).await;
        ids.insert(m.header("message-id").unwrap().to_string());
    }
    assert_eq!(ids.len(), 5);
}

#[tokio::test]
async fn binary_body_with_nulls_is_delivered_intact() {
    let addr = start_default_server().await;
    let mut c = Conn::connect(addr).await;
    c.subscribe("s", "/queue/bin", "auto").await;
    c.send(
        Frame::new(Command::Send)
            .with_header("destination", "/queue/bin")
            .with_body(vec![0u8, 1, 0, 255]),
    )
    .await;
    let m = c.expect_command(Command::Message).await;
    assert_eq!(&m.body[..], &[0u8, 1, 0, 255]);
    assert_eq!(m.header("content-length"), Some("4"));
}

#[tokio::test]
async fn crlf_frames_are_accepted() {
    let addr = start_default_server().await;
    let mut c = Conn::open(addr).await;
    c.send_raw(b"CONNECT\r\naccept-version:1.2\r\nhost:localhost\r\n\r\n\0").await;
    c.expect_command(Command::Connected).await;
    c.send_raw(b"SEND\r\ndestination:/queue/crlf\r\nreceipt:1\r\n\r\nhi\0\r\n").await;
    c.expect_command(Command::Receipt).await;
}

#[tokio::test]
async fn escaped_headers_survive_the_round_trip() {
    let addr = start_default_server().await;
    let mut c = Conn::connect(addr).await;
    c.subscribe("a:b", "/queue/esc:ape\n", "auto").await;
    c.send(send_frame("/queue/esc:ape\n", "x").with_header("we\\ird", "li\rne")).await;
    let m = c.expect_command(Command::Message).await;
    assert_eq!(m.header("subscription"), Some("a:b"));
    assert_eq!(m.header("destination"), Some("/queue/esc:ape\n"));
    assert_eq!(m.header("we\\ird"), Some("li\rne"));
}

#[tokio::test]
async fn repeated_destination_uses_first_value() {
    let addr = start_default_server().await;
    let mut c = Conn::connect(addr).await;
    c.subscribe("1", "/queue/first", "auto").await;
    c.subscribe("2", "/queue/second", "auto").await;
    c.send_raw(b"SEND\ndestination:/queue/first\ndestination:/queue/second\n\nx\0").await;
    let m = c.expect_command(Command::Message).await;
    assert_eq!(m.header("subscription"), Some("1"));
    c.expect_nothing().await;
}

#[tokio::test]
async fn send_without_destination_is_an_error() {
    let addr = start_default_server().await;
    let mut c = Conn::connect(addr).await;
    c.send(Frame::new(Command::Send).with_header("receipt", "r9").with_body("x")).await;
    let e = c.expect_error_and_close().await;
    assert_eq!(e.header("receipt-id"), Some("r9"));
    assert!(e.header("message").is_some());
}

#[tokio::test]
async fn subscribe_requires_destination() {
    let addr = start_default_server().await;
    let mut c = Conn::connect(addr).await;
    c.send(Frame::new(Command::Subscribe).with_header("id", "0")).await;
    c.expect_error_and_close().await;
}

#[tokio::test]
async fn subscribe_requires_id() {
    let addr = start_default_server().await;
    let mut c = Conn::connect(addr).await;
    c.send(Frame::new(Command::Subscribe).with_header("destination", "/queue/a")).await;
    c.expect_error_and_close().await;
}

#[tokio::test]
async fn duplicate_subscription_id_is_an_error() {
    let addr = start_default_server().await;
    let mut c = Conn::connect(addr).await;
    c.subscribe("0", "/queue/a", "auto").await;
    c.send(
        Frame::new(Command::Subscribe)
            .with_header("id", "0")
            .with_header("destination", "/queue/b"),
    )
    .await;
    c.expect_error_and_close().await;
}

#[tokio::test]
async fn same_subscription_id_on_different_connections_is_fine() {
    let addr = start_default_server().await;
    let mut a = Conn::connect(addr).await;
    let mut b = Conn::connect(addr).await;
    a.subscribe("0", "/topic/same", "auto").await;
    b.subscribe("0", "/topic/same", "auto").await;
    a.publish("/topic/same", "x").await;
    assert_eq!(a.expect_command(Command::Message).await.header("subscription"), Some("0"));
    assert_eq!(b.expect_command(Command::Message).await.header("subscription"), Some("0"));
}

#[tokio::test]
async fn invalid_ack_mode_is_an_error() {
    let addr = start_default_server().await;
    let mut c = Conn::connect(addr).await;
    c.send(
        Frame::new(Command::Subscribe)
            .with_header("id", "0")
            .with_header("destination", "/queue/a")
            .with_header("ack", "sometimes"),
    )
    .await;
    c.expect_error_and_close().await;
}

#[tokio::test]
async fn ack_defaults_to_auto() {
    let addr = start_default_server().await;
    let mut c = Conn::connect(addr).await;
    c.send_with_receipt(
        Frame::new(Command::Subscribe).with_header("id", "0").with_header("destination", "/queue/d"),
        "s",
    )
    .await;
    c.publish("/queue/d", "x").await;
    let m = c.expect_command(Command::Message).await;
    assert_eq!(m.header("ack"), None);
}

#[tokio::test]
async fn client_ack_modes_add_ack_header() {
    let addr = start_default_server().await;
    let mut c = Conn::connect(addr).await;
    c.subscribe("c", "/queue/c1", "client").await;
    c.subscribe("ci", "/queue/c2", "client-individual").await;
    c.publish("/queue/c1", "a").await;
    c.publish("/queue/c2", "b").await;
    let m1 = c.expect_command(Command::Message).await;
    let m2 = c.expect_command(Command::Message).await;
    let a1 = m1.header("ack").expect("ack header");
    let a2 = m2.header("ack").expect("ack header");
    assert_ne!(a1, a2);
}

#[tokio::test]
async fn topic_broadcasts_to_every_subscriber() {
    let addr = start_default_server().await;
    let mut a = Conn::connect(addr).await;
    let mut b = Conn::connect(addr).await;
    a.subscribe("1", "/topic/news", "auto").await;
    b.subscribe("1", "/topic/news", "auto").await;
    let mut p = Conn::connect(addr).await;
    p.publish("/topic/news", "extra").await;
    assert_eq!(body(&a.expect_command(Command::Message).await), "extra");
    assert_eq!(body(&b.expect_command(Command::Message).await), "extra");
}

#[tokio::test]
async fn topic_messages_without_subscribers_are_dropped() {
    let addr = start_default_server().await;
    let mut c = Conn::connect(addr).await;
    c.publish("/topic/void", "lost").await;
    c.subscribe("1", "/topic/void", "auto").await;
    c.expect_nothing().await;
}

#[tokio::test]
async fn queue_delivers_each_message_to_one_subscriber() {
    let addr = start_default_server().await;
    let mut a = Conn::connect(addr).await;
    let mut b = Conn::connect(addr).await;
    a.subscribe("1", "/queue/work", "auto").await;
    b.subscribe("1", "/queue/work", "auto").await;
    let mut p = Conn::connect(addr).await;
    for i in 0..4 {
        p.publish("/queue/work", &i.to_string()).await;
    }
    let mut got = Vec::new();
    while let Some(m) = a.try_recv(QUIET).await {
        got.push(body(&m));
    }
    while let Some(m) = b.try_recv(QUIET).await {
        got.push(body(&m));
    }
    got.sort();
    assert_eq!(got, vec!["0", "1", "2", "3"]);
}

#[tokio::test]
async fn queue_retains_messages_until_subscribed() {
    let addr = start_default_server().await;
    let mut p = Conn::connect(addr).await;
    p.publish("/queue/later", "one").await;
    p.publish("/queue/later", "two").await;
    let mut c = Conn::connect(addr).await;
    c.subscribe("1", "/queue/later", "auto").await;
    assert_eq!(body(&c.expect_command(Command::Message).await), "one");
    assert_eq!(body(&c.expect_command(Command::Message).await), "two");
}

// ---------------------------------------------------------------------------
// UNSUBSCRIBE
// ---------------------------------------------------------------------------

#[tokio::test]
async fn unsubscribe_stops_delivery() {
    let addr = start_default_server().await;
    let mut c = Conn::connect(addr).await;
    c.subscribe("0", "/topic/u", "auto").await;
    c.send_with_receipt(Frame::new(Command::Unsubscribe).with_header("id", "0"), "u").await;
    c.publish("/topic/u", "x").await;
    c.expect_nothing().await;
}

#[tokio::test]
async fn unsubscribe_requires_id() {
    let addr = start_default_server().await;
    let mut c = Conn::connect(addr).await;
    c.send(Frame::new(Command::Unsubscribe)).await;
    c.expect_error_and_close().await;
}

#[tokio::test]
async fn unsubscribe_unknown_id_is_an_error() {
    let addr = start_default_server().await;
    let mut c = Conn::connect(addr).await;
    c.send(Frame::new(Command::Unsubscribe).with_header("id", "nope")).await;
    c.expect_error_and_close().await;
}

#[tokio::test]
async fn subscription_id_can_be_reused_after_unsubscribe() {
    let addr = start_default_server().await;
    let mut c = Conn::connect(addr).await;
    c.subscribe("0", "/topic/r1", "auto").await;
    c.send_with_receipt(Frame::new(Command::Unsubscribe).with_header("id", "0"), "u").await;
    c.subscribe("0", "/topic/r2", "auto").await;
    c.publish("/topic/r2", "x").await;
    c.expect_command(Command::Message).await;
}

// ---------------------------------------------------------------------------
// ACK / NACK
// ---------------------------------------------------------------------------

async fn receive_bodies(c: &mut Conn, n: usize) -> Vec<Frame> {
    let mut out = Vec::new();
    for _ in 0..n {
        out.push(c.expect_command(Command::Message).await);
    }
    out
}

async fn remaining_bodies(addr: std::net::SocketAddr, dest: &str) -> Vec<String> {
    let mut c = Conn::connect(addr).await;
    c.subscribe("check", dest, "auto").await;
    let mut out = Vec::new();
    while let Some(m) = c.try_recv(QUIET).await {
        out.push(body(&m));
    }
    out
}

fn ack(m: &Frame) -> Frame {
    Frame::new(Command::Ack).with_header("id", m.header("ack").unwrap())
}

fn nack(m: &Frame) -> Frame {
    Frame::new(Command::Nack).with_header("id", m.header("ack").unwrap())
}

#[tokio::test]
async fn unacked_messages_are_redelivered_after_disconnect() {
    let addr = start_default_server().await;
    let mut c = Conn::connect(addr).await;
    c.subscribe("0", "/queue/redeliver", "client").await;
    c.publish("/queue/redeliver", "m1").await;
    receive_bodies(&mut c, 1).await;
    c.disconnect().await;
    assert_eq!(remaining_bodies(addr, "/queue/redeliver").await, vec!["m1"]);
}

#[tokio::test]
async fn unacked_messages_are_redelivered_after_connection_failure() {
    let addr = start_default_server().await;
    let mut c = Conn::connect(addr).await;
    c.subscribe("0", "/queue/drop", "client-individual").await;
    c.publish("/queue/drop", "m1").await;
    receive_bodies(&mut c, 1).await;
    drop(c);
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(remaining_bodies(addr, "/queue/drop").await, vec!["m1"]);
}

#[tokio::test]
async fn auto_ack_messages_are_not_redelivered() {
    let addr = start_default_server().await;
    let mut c = Conn::connect(addr).await;
    c.subscribe("0", "/queue/auto", "auto").await;
    c.publish("/queue/auto", "m1").await;
    receive_bodies(&mut c, 1).await;
    c.disconnect().await;
    assert!(remaining_bodies(addr, "/queue/auto").await.is_empty());
}

#[tokio::test]
async fn client_ack_is_cumulative() {
    let addr = start_default_server().await;
    let mut c = Conn::connect(addr).await;
    c.subscribe("0", "/queue/cum", "client").await;
    for b in ["m1", "m2", "m3"] {
        c.publish("/queue/cum", b).await;
    }
    let ms = receive_bodies(&mut c, 3).await;
    c.send(ack(&ms[1])).await;
    c.disconnect().await;
    assert_eq!(remaining_bodies(addr, "/queue/cum").await, vec!["m3"]);
}

#[tokio::test]
async fn client_individual_ack_is_not_cumulative() {
    let addr = start_default_server().await;
    let mut c = Conn::connect(addr).await;
    c.subscribe("0", "/queue/ind", "client-individual").await;
    for b in ["m1", "m2", "m3"] {
        c.publish("/queue/ind", b).await;
    }
    let ms = receive_bodies(&mut c, 3).await;
    c.send(ack(&ms[1])).await;
    c.disconnect().await;
    assert_eq!(remaining_bodies(addr, "/queue/ind").await, vec!["m1", "m3"]);
}

#[tokio::test]
async fn client_nack_is_cumulative() {
    let addr = start_default_server().await;
    let mut c = Conn::connect(addr).await;
    c.subscribe("0", "/queue/ncum", "client").await;
    for b in ["m1", "m2", "m3"] {
        c.publish("/queue/ncum", b).await;
    }
    let ms = receive_bodies(&mut c, 3).await;
    c.send(nack(&ms[1])).await;
    c.disconnect().await;
    // m1 and m2 were NACKed (this server discards NACKed messages); m3 is redelivered
    assert_eq!(remaining_bodies(addr, "/queue/ncum").await, vec!["m3"]);
}

#[tokio::test]
async fn client_individual_nack_is_not_cumulative() {
    let addr = start_default_server().await;
    let mut c = Conn::connect(addr).await;
    c.subscribe("0", "/queue/nind", "client-individual").await;
    for b in ["m1", "m2", "m3"] {
        c.publish("/queue/nind", b).await;
    }
    let ms = receive_bodies(&mut c, 3).await;
    c.send(nack(&ms[1])).await;
    c.disconnect().await;
    assert_eq!(remaining_bodies(addr, "/queue/nind").await, vec!["m1", "m3"]);
}

#[tokio::test]
async fn ack_requires_id() {
    let addr = start_default_server().await;
    let mut c = Conn::connect(addr).await;
    c.send(Frame::new(Command::Ack)).await;
    c.expect_error_and_close().await;
}

#[tokio::test]
async fn nack_requires_id() {
    let addr = start_default_server().await;
    let mut c = Conn::connect(addr).await;
    c.send(Frame::new(Command::Nack)).await;
    c.expect_error_and_close().await;
}

#[tokio::test]
async fn ack_with_unknown_id_is_an_error() {
    let addr = start_default_server().await;
    let mut c = Conn::connect(addr).await;
    c.send(Frame::new(Command::Ack).with_header("id", "bogus")).await;
    c.expect_error_and_close().await;
}

#[tokio::test]
async fn unsubscribe_requeues_unacked_queue_messages() {
    let addr = start_default_server().await;
    let mut c = Conn::connect(addr).await;
    c.subscribe("0", "/queue/unsub", "client").await;
    c.publish("/queue/unsub", "m1").await;
    receive_bodies(&mut c, 1).await;
    c.send_with_receipt(Frame::new(Command::Unsubscribe).with_header("id", "0"), "u").await;
    assert_eq!(remaining_bodies(addr, "/queue/unsub").await, vec!["m1"]);
}

// ---------------------------------------------------------------------------
// Transactions
// ---------------------------------------------------------------------------

fn begin(tx: &str) -> Frame {
    Frame::new(Command::Begin).with_header("transaction", tx)
}
fn commit(tx: &str) -> Frame {
    Frame::new(Command::Commit).with_header("transaction", tx)
}
fn abort(tx: &str) -> Frame {
    Frame::new(Command::Abort).with_header("transaction", tx)
}

#[tokio::test]
async fn transactional_sends_are_delivered_on_commit() {
    let addr = start_default_server().await;
    let mut sub = Conn::connect(addr).await;
    sub.subscribe("0", "/queue/tx", "auto").await;

    let mut c = Conn::connect(addr).await;
    c.send(begin("tx1")).await;
    c.send_with_receipt(send_frame("/queue/tx", "a").with_header("transaction", "tx1"), "s1").await;
    c.send_with_receipt(send_frame("/queue/tx", "b").with_header("transaction", "tx1"), "s2").await;
    sub.expect_nothing().await;

    c.send_with_receipt(commit("tx1"), "c").await;
    let ms = receive_bodies(&mut sub, 2).await;
    assert_eq!(body(&ms[0]), "a");
    assert_eq!(body(&ms[1]), "b");
    assert_eq!(ms[0].header("transaction"), None);
}

#[tokio::test]
async fn aborted_sends_are_discarded() {
    let addr = start_default_server().await;
    let mut sub = Conn::connect(addr).await;
    sub.subscribe("0", "/queue/txa", "auto").await;
    let mut c = Conn::connect(addr).await;
    c.send(begin("tx1")).await;
    c.send(send_frame("/queue/txa", "a").with_header("transaction", "tx1")).await;
    c.send_with_receipt(abort("tx1"), "a").await;
    sub.expect_nothing().await;
}

#[tokio::test]
async fn transactional_ack_applies_on_commit_only() {
    let addr = start_default_server().await;
    let mut c = Conn::connect(addr).await;
    c.subscribe("0", "/queue/txack", "client-individual").await;
    c.publish("/queue/txack", "m1").await;
    c.publish("/queue/txack", "m2").await;
    let ms = receive_bodies(&mut c, 2).await;
    c.send(begin("t1")).await;
    c.send(ack(&ms[0]).with_header("transaction", "t1")).await;
    c.send(commit("t1")).await;
    c.send(begin("t2")).await;
    c.send(ack(&ms[1]).with_header("transaction", "t2")).await;
    c.send(abort("t2")).await;
    c.disconnect().await;
    assert_eq!(remaining_bodies(addr, "/queue/txack").await, vec!["m2"]);
}

#[tokio::test]
async fn transactional_nack_applies_on_commit_only() {
    let addr = start_default_server().await;
    let mut c = Conn::connect(addr).await;
    c.subscribe("0", "/queue/txnack", "client-individual").await;
    c.publish("/queue/txnack", "m1").await;
    let ms = receive_bodies(&mut c, 1).await;
    c.send(begin("t1")).await;
    c.send(nack(&ms[0]).with_header("transaction", "t1")).await;
    c.send(abort("t1")).await;
    c.disconnect().await;
    assert_eq!(remaining_bodies(addr, "/queue/txnack").await, vec!["m1"]);
}

#[tokio::test]
async fn begin_requires_transaction() {
    let addr = start_default_server().await;
    let mut c = Conn::connect(addr).await;
    c.send(Frame::new(Command::Begin)).await;
    c.expect_error_and_close().await;
}

#[tokio::test]
async fn commit_requires_transaction() {
    let addr = start_default_server().await;
    let mut c = Conn::connect(addr).await;
    c.send(Frame::new(Command::Commit)).await;
    c.expect_error_and_close().await;
}

#[tokio::test]
async fn abort_requires_transaction() {
    let addr = start_default_server().await;
    let mut c = Conn::connect(addr).await;
    c.send(Frame::new(Command::Abort)).await;
    c.expect_error_and_close().await;
}

#[tokio::test]
async fn duplicate_transaction_id_is_an_error() {
    let addr = start_default_server().await;
    let mut c = Conn::connect(addr).await;
    c.send(begin("t")).await;
    c.send(begin("t")).await;
    c.expect_error_and_close().await;
}

#[tokio::test]
async fn commit_of_unknown_transaction_is_an_error() {
    let addr = start_default_server().await;
    let mut c = Conn::connect(addr).await;
    c.send(commit("nope")).await;
    c.expect_error_and_close().await;
}

#[tokio::test]
async fn abort_of_unknown_transaction_is_an_error() {
    let addr = start_default_server().await;
    let mut c = Conn::connect(addr).await;
    c.send(abort("nope")).await;
    c.expect_error_and_close().await;
}

#[tokio::test]
async fn send_in_unknown_transaction_is_an_error() {
    let addr = start_default_server().await;
    let mut c = Conn::connect(addr).await;
    c.send(send_frame("/queue/a", "x").with_header("transaction", "nope")).await;
    c.expect_error_and_close().await;
}

#[tokio::test]
async fn transaction_id_cannot_be_used_after_commit() {
    let addr = start_default_server().await;
    let mut c = Conn::connect(addr).await;
    c.send(begin("t")).await;
    c.send(commit("t")).await;
    c.send(commit("t")).await;
    c.expect_error_and_close().await;
}

#[tokio::test]
async fn disconnect_aborts_open_transactions() {
    let addr = start_default_server().await;
    let mut sub = Conn::connect(addr).await;
    sub.subscribe("0", "/queue/txd", "auto").await;
    let mut c = Conn::connect(addr).await;
    c.send(begin("t")).await;
    c.send(send_frame("/queue/txd", "x").with_header("transaction", "t")).await;
    c.disconnect().await;
    sub.expect_nothing().await;
}

#[tokio::test]
async fn connection_failure_aborts_open_transactions() {
    let addr = start_default_server().await;
    let mut sub = Conn::connect(addr).await;
    sub.subscribe("0", "/queue/txf", "auto").await;
    let mut c = Conn::connect(addr).await;
    c.send(begin("t")).await;
    c.send_with_receipt(send_frame("/queue/txf", "x").with_header("transaction", "t"), "r").await;
    drop(c);
    sub.expect_nothing().await;
}

// ---------------------------------------------------------------------------
// RECEIPT / DISCONNECT
// ---------------------------------------------------------------------------

#[tokio::test]
async fn every_client_frame_can_request_a_receipt() {
    let addr = start_default_server().await;
    let mut c = Conn::connect(addr).await;
    c.send_with_receipt(send_frame("/queue/rc", "x"), "1").await;
    c.send_with_receipt(
        Frame::new(Command::Subscribe).with_header("id", "s").with_header("destination", "/queue/rc").with_header("ack", "client"),
        "2",
    )
    .await;
    let m = c.expect_command(Command::Message).await;
    c.send_with_receipt(ack(&m), "3").await;
    c.send_with_receipt(begin("t"), "4").await;
    c.send_with_receipt(commit("t"), "5").await;
    c.send_with_receipt(begin("t2"), "6").await;
    c.send_with_receipt(abort("t2"), "7").await;
    c.publish("/queue/rc", "y").await;
    let m = c.expect_command(Command::Message).await;
    c.send_with_receipt(nack(&m), "8").await;
    c.send_with_receipt(Frame::new(Command::Unsubscribe).with_header("id", "s"), "9").await;
}

#[tokio::test]
async fn receipt_ids_are_escaped_correctly() {
    let addr = start_default_server().await;
    let mut c = Conn::connect(addr).await;
    c.send_with_receipt(send_frame("/queue/a", "x"), "message:12345\n").await;
}

#[tokio::test]
async fn disconnect_with_receipt_gets_receipt_then_close() {
    let addr = start_default_server().await;
    let c = Conn::connect(addr).await;
    c.disconnect().await;
}

#[tokio::test]
async fn disconnect_without_receipt_closes() {
    let addr = start_default_server().await;
    let mut c = Conn::connect(addr).await;
    c.send(Frame::new(Command::Disconnect)).await;
    c.expect_closed().await;
}

#[tokio::test]
async fn frames_before_disconnect_are_processed() {
    let addr = start_default_server().await;
    let mut sub = Conn::connect(addr).await;
    sub.subscribe("0", "/queue/before", "auto").await;
    let mut c = Conn::connect(addr).await;
    for i in 0..20 {
        c.send(send_frame("/queue/before", &i.to_string())).await;
    }
    c.disconnect().await;
    assert_eq!(receive_bodies(&mut sub, 20).await.len(), 20);
}

// ---------------------------------------------------------------------------
// ERROR handling
// ---------------------------------------------------------------------------

#[tokio::test]
async fn unknown_command_is_an_error() {
    let addr = start_default_server().await;
    let mut c = Conn::connect(addr).await;
    c.send(Frame::new(Command::Other("FROB".into())).with_header("receipt", "f")).await;
    let e = c.expect_error_and_close().await;
    assert_eq!(e.header("receipt-id"), Some("f"));
}

#[tokio::test]
async fn server_frames_from_client_are_errors() {
    let addr = start_default_server().await;
    let mut c = Conn::connect(addr).await;
    c.send(Frame::new(Command::Receipt).with_header("receipt-id", "x")).await;
    c.expect_error_and_close().await;
}

#[tokio::test]
async fn body_on_frame_that_forbids_it_is_an_error() {
    let addr = start_default_server().await;
    let mut c = Conn::connect(addr).await;
    c.send_raw(b"SUBSCRIBE\nid:0\ndestination:/queue/a\n\nbody\0").await;
    c.expect_error_and_close().await;
}

#[tokio::test]
async fn invalid_escape_is_a_fatal_error() {
    let addr = start_default_server().await;
    let mut c = Conn::connect(addr).await;
    c.send_raw(b"SEND\ndestination:/queue/\\t\n\nx\0").await;
    let e = c.expect_error_and_close().await;
    assert!(e.header("message").is_some());
}

#[tokio::test]
async fn oversized_frame_is_an_error() {
    let addr = start_server(ServerConfig {
        limits: kyu::codec::Limits { max_frame_size: 1024, ..Default::default() },
        ..ServerConfig::default()
    })
    .await;
    let mut c = Conn::connect(addr).await;
    c.send(send_frame("/queue/big", &"x".repeat(4096))).await;
    c.expect_error_and_close().await;
}

#[tokio::test]
async fn error_frame_has_content_type_and_length_when_it_has_a_body() {
    let addr = start_default_server().await;
    let mut c = Conn::connect(addr).await;
    c.send(Frame::new(Command::Ack)).await;
    let e = c.expect_error_and_close().await;
    if !e.body.is_empty() {
        assert!(e.header("content-type").is_some());
        assert_eq!(e.header("content-length"), Some(e.body.len().to_string().as_str()));
    }
}

#[tokio::test]
async fn error_on_one_connection_does_not_affect_others() {
    let addr = start_default_server().await;
    let mut good = Conn::connect(addr).await;
    let mut bad = Conn::connect(addr).await;
    bad.send(Frame::new(Command::Ack)).await;
    bad.expect_error_and_close().await;
    good.publish("/queue/ok", "x").await;
}

#[tokio::test]
async fn messages_queued_for_a_client_are_flushed_before_its_receipt_and_close() {
    let addr = start_default_server().await;
    let mut c = Conn::connect(addr).await;
    // all in one write, so the server reads every frame before replying
    c.send_raw(
        b"SUBSCRIBE\nid:0\ndestination:/queue/flush\n\n\0\
          SEND\ndestination:/queue/flush\n\nhello\0\
          DISCONNECT\nreceipt:77\n\n\0",
    )
    .await;
    let m = c.expect_command(Command::Message).await;
    assert_eq!(body(&m), "hello");
    let r = c.expect_command(Command::Receipt).await;
    assert_eq!(r.header("receipt-id"), Some("77"));
    c.expect_closed().await;
}
