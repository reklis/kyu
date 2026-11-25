//! Spec compliance tests for the STOMP 1.2 wire format.
//! Section references are to <https://stomp.github.io/stomp-specification-1.2.html>

use bytes::{Bytes, BytesMut};
use kyu::codec::{CodecError, Item, Limits, StompCodec};
use kyu::frame::{Command, Frame, Str};
use tokio_util::codec::{Decoder, Encoder};

fn decode_all(input: &[u8]) -> Result<Vec<Item>, CodecError> {
    decode_all_with(StompCodec::new(), input)
}

fn decode_all_with(mut codec: StompCodec, input: &[u8]) -> Result<Vec<Item>, CodecError> {
    let mut buf = BytesMut::from(input);
    let mut out = Vec::new();
    while let Some(item) = codec.decode(&mut buf)? {
        out.push(item);
    }
    assert!(buf.is_empty(), "undecoded bytes left over: {buf:?}");
    Ok(out)
}

fn frames(items: Vec<Item>) -> Vec<Frame> {
    items
        .into_iter()
        .filter_map(|i| match i {
            Item::Frame(f) => Some(f),
            Item::Heartbeat => None,
        })
        .collect()
}

fn decode_one(input: &[u8]) -> Frame {
    let mut fs = frames(decode_all(input).expect("decode failed"));
    assert_eq!(fs.len(), 1, "expected exactly one frame");
    fs.remove(0)
}

fn encode(frame: Frame) -> Result<Vec<u8>, CodecError> {
    let mut buf = BytesMut::new();
    StompCodec::new().encode(frame, &mut buf)?;
    Ok(buf.to_vec())
}

// ---------------------------------------------------------------------------
// STOMP Frames
// ---------------------------------------------------------------------------

#[test]
fn decodes_command_headers_and_body() {
    let f = decode_one(b"SEND\ndestination:/queue/a\nreceipt:message-12345\n\nhello queue a\0");
    assert_eq!(f.command, Command::Send);
    assert_eq!(f.header("destination"), Some("/queue/a"));
    assert_eq!(f.header("receipt"), Some("message-12345"));
    assert_eq!(&f.body[..], b"hello queue a");
}

#[test]
fn eol_may_be_crlf() {
    let f = decode_one(b"SEND\r\ndestination:/queue/a\r\n\r\nhi\0");
    assert_eq!(f.command, Command::Send);
    assert_eq!(f.header("destination"), Some("/queue/a"));
    assert_eq!(&f.body[..], b"hi");
}

#[test]
fn frame_without_headers_or_body() {
    let f = decode_one(b"DISCONNECT\n\n\0");
    assert_eq!(f.command, Command::Disconnect);
    assert!(f.headers.is_empty());
    assert!(f.body.is_empty());
}

#[test]
fn null_may_be_followed_by_multiple_eols() {
    let fs = frames(
        decode_all(b"BEGIN\ntransaction:a\n\n\0\n\r\n\nBEGIN\ntransaction:b\n\n\0\n").unwrap(),
    );
    assert_eq!(fs.len(), 2);
    assert_eq!(fs[1].header("transaction"), Some("b"));
}

#[test]
fn commands_are_case_sensitive() {
    let f = decode_one(b"send\ndestination:/queue/a\n\n\0");
    assert_eq!(f.command, Command::Other("send".into()));
}

#[test]
fn header_names_are_case_sensitive() {
    let f = decode_one(b"SEND\nDestination:/queue/a\n\n\0");
    assert_eq!(f.header("destination"), None);
    assert_eq!(f.header("Destination"), Some("/queue/a"));
}

#[test]
fn all_spec_commands_are_recognised() {
    for (name, cmd) in [
        ("CONNECT", Command::Connect),
        ("STOMP", Command::Stomp),
        ("CONNECTED", Command::Connected),
        ("SEND", Command::Send),
        ("SUBSCRIBE", Command::Subscribe),
        ("UNSUBSCRIBE", Command::Unsubscribe),
        ("ACK", Command::Ack),
        ("NACK", Command::Nack),
        ("BEGIN", Command::Begin),
        ("COMMIT", Command::Commit),
        ("ABORT", Command::Abort),
        ("DISCONNECT", Command::Disconnect),
        ("MESSAGE", Command::Message),
        ("RECEIPT", Command::Receipt),
        ("ERROR", Command::Error),
    ] {
        let f = decode_one(format!("{name}\n\n\0").as_bytes());
        assert_eq!(f.command, cmd);
        assert_eq!(cmd.as_str(), name);
    }
}

#[test]
fn incomplete_frame_waits_for_more_data() {
    let input = b"SEND\ndestination:/queue/a\n\nhello\0";
    let mut codec = StompCodec::new();
    let mut buf = BytesMut::new();
    for (i, byte) in input.iter().enumerate() {
        buf.extend_from_slice(&[*byte]);
        let res = codec.decode(&mut buf).unwrap();
        if i < input.len() - 1 {
            assert!(res.is_none(), "frame decoded early at byte {i}");
        } else {
            match res {
                Some(Item::Frame(f)) => assert_eq!(&f.body[..], b"hello"),
                other => panic!("expected frame, got {other:?}"),
            }
        }
    }
}

#[test]
fn line_without_colon_is_malformed() {
    assert!(matches!(
        decode_all(b"SEND\ndestination\n\n\0"),
        Err(CodecError::MalformedHeader(_))
    ));
}

#[test]
fn empty_header_name_is_malformed() {
    assert!(matches!(
        decode_all(b"SEND\n:value\n\n\0"),
        Err(CodecError::MalformedHeader(_))
    ));
}

#[test]
fn bare_carriage_return_in_header_is_malformed() {
    assert!(matches!(
        decode_all(b"SEND\nfoo:a\rb\n\n\0"),
        Err(CodecError::MalformedHeader(_))
    ));
}

#[test]
fn headers_must_be_utf8() {
    assert!(matches!(
        decode_all(b"SEND\nfoo:\xff\xfe\n\n\0"),
        Err(CodecError::InvalidUtf8)
    ));
}

#[test]
fn utf8_headers_are_supported() {
    let f = decode_one("SEND\ndestination:/queue/日本\n\n\0".as_bytes());
    assert_eq!(f.header("destination"), Some("/queue/日本"));
}

// ---------------------------------------------------------------------------
// Value Encoding
// ---------------------------------------------------------------------------

#[test]
fn decodes_escape_sequences_in_names_and_values() {
    let f = decode_one(b"MESSAGE\na\\cb\\\\c:x\\ry\\nz\\cw\\\\\n\n\0");
    assert_eq!(f.header("a:b\\c"), Some("x\ry\nz:w\\"));
}

#[test]
fn undefined_escape_is_fatal() {
    assert!(matches!(
        decode_all(b"SEND\nfoo:a\\tb\n\n\0"),
        Err(CodecError::InvalidEscape)
    ));
}

#[test]
fn dangling_backslash_is_fatal() {
    assert!(matches!(
        decode_all(b"SEND\nfoo:ab\\\n\n\0"),
        Err(CodecError::InvalidEscape)
    ));
}

#[test]
fn connect_and_connected_are_not_unescaped() {
    let f = decode_one(b"CONNECT\nlogin:a\\cb\npasscode:x:y\n\n\0");
    assert_eq!(f.header("login"), Some("a\\cb"));
    assert_eq!(f.header("passcode"), Some("x:y"));
    let f = decode_one(b"CONNECTED\nserver:s\\t\n\n\0");
    assert_eq!(f.header("server"), Some("s\\t"));
}

#[test]
fn header_values_are_never_trimmed() {
    let f = decode_one(b"SEND\n foo : bar \n\n\0");
    assert_eq!(f.header(" foo "), Some(" bar "));
}

#[test]
fn empty_header_value_is_allowed() {
    let f = decode_one(b"SEND\nfoo:\n\n\0");
    assert_eq!(f.header("foo"), Some(""));
}

#[test]
fn encodes_escape_sequences_in_names_and_values() {
    let frame = Frame::new(Command::Message).with_header("a:b\\c", "x\ry\nz:w\\");
    assert_eq!(
        encode(frame).unwrap(),
        b"MESSAGE\na\\cb\\\\c:x\\ry\\nz\\cw\\\\\n\n\0"
    );
}

#[test]
fn connect_headers_are_not_escaped_on_encode() {
    let frame = Frame::new(Command::Connect).with_header("passcode", "a:b\\c");
    assert_eq!(encode(frame).unwrap(), b"CONNECT\npasscode:a:b\\c\n\n\0");
}

#[test]
fn connect_header_with_newline_cannot_be_encoded() {
    let frame = Frame::new(Command::Connect).with_header("login", "a\nb");
    assert!(matches!(
        encode(frame),
        Err(CodecError::UnencodableHeader(_))
    ));
    let frame = Frame::new(Command::Connected).with_header("a:b", "c");
    assert!(matches!(
        encode(frame),
        Err(CodecError::UnencodableHeader(_))
    ));
}

#[test]
fn encoder_never_pads_headers() {
    let frame = Frame::new(Command::Send).with_header(" k ", " v ");
    assert_eq!(encode(frame).unwrap(), b"SEND\n k : v \n\n\0");
}

#[test]
fn escaped_headers_round_trip() {
    let original = Frame::new(Command::Send)
        .with_header("destination", "/queue/a")
        .with_header("weird:\r\n\\", "va:l\r\nue\\");
    let decoded = decode_one(&encode(original.clone()).unwrap());
    assert_eq!(decoded, original);
}

// ---------------------------------------------------------------------------
// Body / content-length
// ---------------------------------------------------------------------------

#[test]
fn content_length_reads_exact_octets_including_nulls() {
    let f = decode_one(b"SEND\ndestination:/queue/a\ncontent-length:5\n\na\0b\0c\0");
    assert_eq!(&f.body[..], b"a\0b\0c");
}

#[test]
fn content_length_still_requires_terminating_null() {
    assert!(matches!(
        decode_all(b"SEND\ncontent-length:2\n\nabc\0"),
        Err(CodecError::MissingNull)
    ));
}

#[test]
fn invalid_content_length_is_rejected() {
    assert!(matches!(
        decode_all(b"SEND\ncontent-length:abc\n\nabc\0"),
        Err(CodecError::InvalidContentLength)
    ));
    assert!(matches!(
        decode_all(b"SEND\ncontent-length:-1\n\nabc\0"),
        Err(CodecError::InvalidContentLength)
    ));
}

#[test]
fn zero_content_length() {
    let f = decode_one(b"SEND\ncontent-length:0\n\n\0");
    assert!(f.body.is_empty());
}

#[test]
fn encoder_adds_content_length_for_bodies() {
    let frame = Frame::new(Command::Send)
        .with_header("destination", "/queue/a")
        .with_body("a\0b");
    let bytes = encode(frame).unwrap();
    assert_eq!(
        bytes,
        b"SEND\ndestination:/queue/a\ncontent-length:3\n\na\0b\0"
    );
    assert_eq!(&decode_one(&bytes).body[..], b"a\0b");
}

#[test]
fn encoder_replaces_wrong_content_length() {
    let frame = Frame::new(Command::Send)
        .with_header("content-length", "99")
        .with_body("abc");
    assert_eq!(encode(frame).unwrap(), b"SEND\ncontent-length:3\n\nabc\0");
}

#[test]
fn encoder_omits_content_length_without_body() {
    let frame = Frame::new(Command::Send).with_header("destination", "/q");
    assert_eq!(encode(frame).unwrap(), b"SEND\ndestination:/q\n\n\0");
}

#[test]
fn only_send_message_and_error_may_have_a_body() {
    for cmd in [Command::Send, Command::Message, Command::Error] {
        assert!(cmd.allows_body());
        assert!(encode(Frame::new(cmd).with_body("x")).is_ok());
    }
    for cmd in [
        Command::Connect,
        Command::Stomp,
        Command::Connected,
        Command::Subscribe,
        Command::Unsubscribe,
        Command::Ack,
        Command::Nack,
        Command::Begin,
        Command::Commit,
        Command::Abort,
        Command::Disconnect,
        Command::Receipt,
    ] {
        assert!(!cmd.allows_body(), "{cmd:?}");
        assert!(matches!(
            encode(Frame::new(cmd).with_body("x")),
            Err(CodecError::BodyNotAllowed(_))
        ));
    }
}

// ---------------------------------------------------------------------------
// Repeated Header Entries
// ---------------------------------------------------------------------------

#[test]
fn first_repeated_header_wins_and_history_is_kept() {
    let f = decode_one(b"MESSAGE\nfoo:World\nfoo:Hello\n\n\0");
    assert_eq!(f.header("foo"), Some("World"));
    assert_eq!(
        f.headers,
        vec![
            ("foo".into(), "World".into()),
            ("foo".into(), "Hello".into())
        ] as Vec<(Str, Str)>
    );
}

#[test]
fn first_content_length_wins() {
    let f = decode_one(b"SEND\ncontent-length:1\ncontent-length:3\n\na\0");
    assert_eq!(&f.body[..], b"a");
}

// ---------------------------------------------------------------------------
// Heart-beating: an EOL on its own is a heart-beat
// ---------------------------------------------------------------------------

#[test]
fn lone_eols_are_heartbeats() {
    let items = decode_all(b"\n\r\n\n").unwrap();
    assert_eq!(
        items,
        vec![Item::Heartbeat, Item::Heartbeat, Item::Heartbeat]
    );
}

#[test]
fn heartbeats_between_frames() {
    let items = decode_all(b"\nDISCONNECT\n\n\0\n").unwrap();
    assert_eq!(items.len(), 3);
    assert_eq!(items[0], Item::Heartbeat);
    assert!(matches!(items[1], Item::Frame(_)));
    assert_eq!(items[2], Item::Heartbeat);
}

#[test]
fn lone_carriage_return_waits_for_line_feed() {
    let mut codec = StompCodec::new();
    let mut buf = BytesMut::from(&b"\r"[..]);
    assert_eq!(codec.decode(&mut buf).unwrap(), None);
    buf.extend_from_slice(b"\n");
    assert_eq!(codec.decode(&mut buf).unwrap(), Some(Item::Heartbeat));
}

#[test]
fn heartbeat_encodes_as_eol() {
    let mut buf = BytesMut::new();
    StompCodec::new().encode(Item::Heartbeat, &mut buf).unwrap();
    assert_eq!(&buf[..], b"\n");
}

// ---------------------------------------------------------------------------
// Size Limits
// ---------------------------------------------------------------------------

fn limited(limits: Limits) -> StompCodec {
    StompCodec::with_limits(limits)
}

#[test]
fn rejects_frames_over_max_size() {
    let codec = limited(Limits {
        max_frame_size: 32,
        ..Limits::default()
    });
    let body = vec![b'x'; 64];
    let mut input = b"SEND\n\n".to_vec();
    input.extend_from_slice(&body);
    // no NUL yet: the decoder must not buffer forever
    assert!(matches!(
        decode_all_with(codec, &input),
        Err(CodecError::FrameTooLarge)
    ));
}

#[test]
fn rejects_content_length_over_max_size() {
    let codec = limited(Limits {
        max_frame_size: 32,
        ..Limits::default()
    });
    assert!(matches!(
        decode_all_with(codec, b"SEND\ncontent-length:1000\n\n"),
        Err(CodecError::FrameTooLarge)
    ));
}

#[test]
fn rejects_too_many_headers() {
    let codec = limited(Limits {
        max_headers: 2,
        ..Limits::default()
    });
    assert!(matches!(
        decode_all_with(codec, b"SEND\na:1\nb:2\nc:3\n\n\0"),
        Err(CodecError::TooManyHeaders)
    ));
}

#[test]
fn rejects_header_lines_that_are_too_long() {
    let codec = limited(Limits {
        max_header_length: 8,
        ..Limits::default()
    });
    assert!(matches!(
        decode_all_with(codec, b"SEND\nabc:0123456789\n\n\0"),
        Err(CodecError::HeaderTooLong)
    ));
}

#[test]
fn frame_body_bytes_type() {
    // Bodies are arbitrary octets
    let f = Frame::new(Command::Send).with_body(Bytes::from_static(&[0xff, 0x00, 0x01]));
    assert_eq!(
        &decode_one(&encode(f).unwrap()).body[..],
        &[0xff, 0x00, 0x01]
    );
}
