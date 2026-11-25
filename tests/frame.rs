//! `Str`, the header string type: cheap to clone and to slice from a frame's
//! read buffer.

use std::collections::HashMap;

use bytes::{Bytes, BytesMut};
use kyu::codec::{Item, StompCodec};
use kyu::frame::{Command, Frame, Str};
use tokio_util::codec::Decoder;

#[test]
fn str_behaves_like_a_string() {
    let owned = Str::from(String::from("abc"));
    let borrowed = Str::from("abc");
    let fixed = Str::from_static("abc");
    assert_eq!(owned, borrowed);
    assert_eq!(borrowed, fixed);
    assert_eq!(fixed, "abc");
    assert_eq!(&*fixed, "abc");
    assert_eq!(fixed.len(), 3);
    assert_eq!(fixed.to_string(), "abc");
    assert_eq!(format!("{fixed:?}"), "\"abc\"");
}

#[test]
fn str_can_be_looked_up_by_str() {
    let mut map = HashMap::new();
    map.insert(Str::from("key"), 1);
    assert_eq!(map.get("key"), Some(&1));
}

#[test]
fn str_rejects_invalid_utf8() {
    assert!(Str::from_utf8(Bytes::from_static(b"ok")).is_ok());
    assert!(Str::from_utf8(Bytes::from_static(&[0xff, 0xfe])).is_err());
}

#[test]
fn decoded_headers_share_the_read_buffer() {
    let mut buf = BytesMut::from(&b"SEND\ndestination:/queue/a\nx-plain:value\n\nbody\0"[..]);
    let range = buf.as_ptr() as usize..buf.as_ptr() as usize + buf.len();
    let Some(Item::Frame(frame)) = StompCodec::new().decode(&mut buf).unwrap() else {
        panic!("expected a frame")
    };
    for (k, v) in &frame.headers {
        for s in [k, v] {
            let p = s.as_bytes().as_ptr() as usize;
            assert!(range.contains(&p), "{s:?} was copied out of the read buffer");
        }
    }
    assert_eq!(frame.command, Command::Send);
    assert_eq!(frame.header("x-plain"), Some("value"));
}

#[test]
fn frames_can_be_built_from_any_string_kind() {
    let id = String::from("7");
    let f = Frame::new(Command::Ack)
        .with_header("id", id.as_str())
        .with_header(String::from("transaction"), id.clone())
        .with_header(Str::from_static("receipt"), Str::from("r"));
    assert_eq!(f.header("id"), Some("7"));
    assert_eq!(f.header("transaction"), Some("7"));
    assert_eq!(f.header("receipt"), Some("r"));
}
