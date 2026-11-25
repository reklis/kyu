//! tokio-util codec for the STOMP 1.2 wire format.

use bytes::{Buf, BufMut, BytesMut};
use tokio_util::codec::{Decoder, Encoder};

use crate::frame::{Command, Frame};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Item {
    Frame(Frame),
    Heartbeat,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Limits {
    pub max_frame_size: usize,
    pub max_headers: usize,
    pub max_header_length: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Limits { max_frame_size: 16 * 1024 * 1024, max_headers: 1000, max_header_length: 64 * 1024 }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum CodecError {
    #[error("i/o error: {0}")]
    Io(#[from] std::io::Error),
    #[error("malformed header: {0}")]
    MalformedHeader(String),
    #[error("invalid escape sequence in header")]
    InvalidEscape,
    #[error("headers are not valid UTF-8")]
    InvalidUtf8,
    #[error("invalid content-length header")]
    InvalidContentLength,
    #[error("frame body not terminated by NULL")]
    MissingNull,
    #[error("frame too large")]
    FrameTooLarge,
    #[error("too many headers")]
    TooManyHeaders,
    #[error("header line too long")]
    HeaderTooLong,
    #[error("{0:?} frames must not have a body")]
    BodyNotAllowed(Command),
    #[error("header cannot be encoded: {0}")]
    UnencodableHeader(String),
}

#[derive(Debug, Clone, Default)]
pub struct StompCodec {
    limits: Limits,
}

impl StompCodec {
    pub fn new() -> StompCodec {
        StompCodec::default()
    }

    pub fn with_limits(limits: Limits) -> StompCodec {
        StompCodec { limits }
    }
}

impl Decoder for StompCodec {
    type Item = Item;
    type Error = CodecError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Item>, CodecError> {
        // An EOL outside a frame is a heart-beat (this also covers the
        // optional EOLs that may follow a frame's NULL octet).
        match src.first() {
            None => return Ok(None),
            Some(b'\n') => {
                src.advance(1);
                return Ok(Some(Item::Heartbeat));
            }
            Some(b'\r') => {
                return match src.get(1) {
                    None => Ok(None),
                    Some(b'\n') => {
                        src.advance(2);
                        Ok(Some(Item::Heartbeat))
                    }
                    Some(_) => Err(CodecError::MalformedHeader("bare carriage return".into())),
                };
            }
            Some(_) => {}
        }

        let limits = self.limits;

        // Pass 1: find the blank line ending the headers, enforcing limits,
        // without allocating.
        let mut pos = 0;
        let mut header_lines = 0usize;
        let body_start = loop {
            let Some(nl) = src[pos..].iter().position(|&b| b == b'\n') else {
                let partial = src.len() - pos;
                if pos > 0 && partial > limits.max_header_length {
                    return Err(CodecError::HeaderTooLong);
                }
                if src.len() > limits.max_frame_size {
                    return Err(CodecError::FrameTooLarge);
                }
                return Ok(None);
            };
            let line = trim_cr(&src[pos..pos + nl]);
            let first = pos == 0;
            pos += nl + 1;
            if line.is_empty() {
                break pos;
            }
            if !first {
                header_lines += 1;
                if line.len() > limits.max_header_length {
                    return Err(CodecError::HeaderTooLong);
                }
                if header_lines > limits.max_headers {
                    return Err(CodecError::TooManyHeaders);
                }
            }
            if pos > limits.max_frame_size {
                return Err(CodecError::FrameTooLarge);
            }
        };

        // Pass 2: parse the command and headers.
        let head = &src[..body_start];
        let mut lines = head.split(|&b| b == b'\n').map(trim_cr);
        let command = Command::parse(utf8(lines.next().unwrap_or_default())?);
        let escaped = command.escapes_headers();
        let mut headers = Vec::with_capacity(header_lines);
        for line in lines.take(header_lines) {
            if line.contains(&b'\r') {
                return Err(CodecError::MalformedHeader("bare carriage return".into()));
            }
            let colon = line
                .iter()
                .position(|&b| b == b':')
                .ok_or_else(|| CodecError::MalformedHeader(String::from_utf8_lossy(line).into()))?;
            let (name, value) = (&line[..colon], &line[colon + 1..]);
            if name.is_empty() {
                return Err(CodecError::MalformedHeader("empty header name".into()));
            }
            let (name, value) = if escaped {
                (unescape(name)?, unescape(value)?)
            } else {
                (utf8(name)?.to_string(), utf8(value)?.to_string())
            };
            headers.push((name, value));
        }

        let content_length = match headers.iter().find(|(k, _)| k == "content-length") {
            None => None,
            Some((_, v)) => {
                if v.is_empty() || !v.bytes().all(|b| b.is_ascii_digit()) {
                    return Err(CodecError::InvalidContentLength);
                }
                Some(v.parse::<usize>().map_err(|_| CodecError::InvalidContentLength)?)
            }
        };

        let body_end = match content_length {
            Some(len) => {
                let end = body_start.checked_add(len).ok_or(CodecError::FrameTooLarge)?;
                if end + 1 > limits.max_frame_size {
                    return Err(CodecError::FrameTooLarge);
                }
                if src.len() < end + 1 {
                    return Ok(None);
                }
                if src[end] != 0 {
                    return Err(CodecError::MissingNull);
                }
                end
            }
            None => match src[body_start..].iter().position(|&b| b == 0) {
                Some(i) => body_start + i,
                None => {
                    if src.len() > limits.max_frame_size {
                        return Err(CodecError::FrameTooLarge);
                    }
                    return Ok(None);
                }
            },
        };
        if body_end + 1 > limits.max_frame_size {
            return Err(CodecError::FrameTooLarge);
        }

        let mut frame_bytes = src.split_to(body_end + 1);
        frame_bytes.advance(body_start);
        frame_bytes.truncate(body_end - body_start);
        Ok(Some(Item::Frame(Frame { command, headers, body: frame_bytes.freeze() })))
    }
}

fn trim_cr(line: &[u8]) -> &[u8] {
    line.strip_suffix(b"\r").unwrap_or(line)
}

fn utf8(bytes: &[u8]) -> Result<&str, CodecError> {
    std::str::from_utf8(bytes).map_err(|_| CodecError::InvalidUtf8)
}

fn unescape(bytes: &[u8]) -> Result<String, CodecError> {
    if !bytes.contains(&b'\\') {
        return utf8(bytes).map(str::to_owned);
    }
    let mut out = Vec::with_capacity(bytes.len());
    let mut iter = bytes.iter();
    while let Some(&b) = iter.next() {
        if b != b'\\' {
            out.push(b);
            continue;
        }
        out.push(match iter.next() {
            Some(b'r') => b'\r',
            Some(b'n') => b'\n',
            Some(b'c') => b':',
            Some(b'\\') => b'\\',
            _ => return Err(CodecError::InvalidEscape),
        });
    }
    String::from_utf8(out).map_err(|_| CodecError::InvalidUtf8)
}

fn needs_escape(b: u8) -> bool {
    matches!(b, b'\\' | b'\r' | b'\n' | b':')
}

fn escape_into(s: &str, dst: &mut BytesMut) {
    if !s.bytes().any(needs_escape) {
        dst.put_slice(s.as_bytes());
        return;
    }
    for b in s.bytes() {
        match b {
            b'\\' => dst.put_slice(b"\\\\"),
            b'\r' => dst.put_slice(b"\\r"),
            b'\n' => dst.put_slice(b"\\n"),
            b':' => dst.put_slice(b"\\c"),
            _ => dst.put_u8(b),
        }
    }
}

impl Encoder<Frame> for StompCodec {
    type Error = CodecError;

    fn encode(&mut self, frame: Frame, dst: &mut BytesMut) -> Result<(), CodecError> {
        if !frame.body.is_empty() && !frame.command.allows_body() {
            return Err(CodecError::BodyNotAllowed(frame.command));
        }
        let escaped = frame.command.escapes_headers();
        if !escaped {
            for (k, v) in &frame.headers {
                if k.contains(['\r', '\n', ':']) || v.contains(['\r', '\n']) {
                    return Err(CodecError::UnencodableHeader(k.clone()));
                }
            }
        }

        let headers_len: usize = frame.headers.iter().map(|(k, v)| k.len() + v.len() + 2).sum();
        dst.reserve(frame.command.as_str().len() + headers_len + frame.body.len() + 40);
        dst.put_slice(frame.command.as_str().as_bytes());
        dst.put_u8(b'\n');
        for (k, v) in frame.headers.iter().filter(|(k, _)| k != "content-length") {
            if escaped {
                escape_into(k, dst);
                dst.put_u8(b':');
                escape_into(v, dst);
            } else {
                dst.put_slice(k.as_bytes());
                dst.put_u8(b':');
                dst.put_slice(v.as_bytes());
            }
            dst.put_u8(b'\n');
        }
        if !frame.body.is_empty() {
            dst.put_slice(format!("content-length:{}\n", frame.body.len()).as_bytes());
        }
        dst.put_u8(b'\n');
        dst.put_slice(&frame.body);
        dst.put_u8(0);
        Ok(())
    }
}

impl Encoder<Item> for StompCodec {
    type Error = CodecError;

    fn encode(&mut self, item: Item, dst: &mut BytesMut) -> Result<(), CodecError> {
        match item {
            Item::Frame(frame) => self.encode(frame, dst),
            Item::Heartbeat => {
                dst.put_u8(b'\n');
                Ok(())
            }
        }
    }
}
