//! tokio-util codec for the STOMP 1.2 wire format.

use bytes::{Buf, BufMut, BytesMut};
use tokio_util::codec::{Decoder, Encoder};

use crate::frame::{Command, Frame, Str};

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

        // Pass 1, without allocating: find the blank line ending the headers,
        // enforce limits, check header syntax and note content-length.
        let mut pos = 0;
        let mut header_lines = 0usize;
        let mut content_length = None;
        let body_start = loop {
            let Some(nl) = memchr(b'\n', &src[pos..]) else {
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
            if pos > limits.max_frame_size {
                return Err(CodecError::FrameTooLarge);
            }
            if first {
                continue;
            }
            header_lines += 1;
            if line.len() > limits.max_header_length {
                return Err(CodecError::HeaderTooLong);
            }
            if header_lines > limits.max_headers {
                return Err(CodecError::TooManyHeaders);
            }
            if line.contains(&b'\r') {
                return Err(CodecError::MalformedHeader("bare carriage return".into()));
            }
            let Some(colon) = memchr(b':', line) else {
                return Err(CodecError::MalformedHeader(String::from_utf8_lossy(line).into()));
            };
            if colon == 0 {
                return Err(CodecError::MalformedHeader("empty header name".into()));
            }
            // The first content-length wins, like every repeated header.
            if content_length.is_none() && &line[..colon] == b"content-length" {
                content_length = Some(parse_content_length(&line[colon + 1..])?);
            }
        };

        // Validating the whole header block once also covers every header:
        // slicing UTF-8 at ASCII bytes (EOL, ':') leaves valid UTF-8.
        let head = std::str::from_utf8(&src[..body_start]).map_err(|_| CodecError::InvalidUtf8)?;
        let command_line = head.split('\n').next().unwrap_or_default();
        let command = Command::parse(command_line.strip_suffix('\r').unwrap_or(command_line));

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
            None => match memchr(0, &src[body_start..]) {
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

        // Pass 2: the frame is complete. Take it out of the buffer as one
        // shared allocation and hand out headers as slices of it.
        let raw = src.split_to(body_end + 1).freeze();
        let escaped = command.escapes_headers();
        let mut headers = Vec::with_capacity(header_lines);
        let mut pos = memchr(b'\n', &raw).unwrap() + 1;
        for _ in 0..header_lines {
            let nl = pos + memchr(b'\n', &raw[pos..body_start]).unwrap();
            let end = if raw[nl - 1] == b'\r' { nl - 1 } else { nl };
            let colon = pos + memchr(b':', &raw[pos..end]).unwrap();
            let field = |from: usize, to: usize| -> Result<Str, CodecError> {
                let bytes = &raw[from..to];
                if escaped && bytes.contains(&b'\\') {
                    unescape(bytes).map(Str::from)
                } else {
                    // SAFETY: inside the UTF-8 checked header block, cut at ASCII.
                    Ok(unsafe { Str::from_utf8_unchecked(raw.slice(from..to)) })
                }
            };
            headers.push((field(pos, colon)?, field(colon + 1, end)?));
            pos = nl + 1;
        }
        let body = raw.slice(body_start..body_end);
        Ok(Some(Item::Frame(Frame { command, headers, body })))
    }
}

use memchr::memchr;

fn parse_content_length(v: &[u8]) -> Result<usize, CodecError> {
    if v.is_empty() || !v.iter().all(u8::is_ascii_digit) {
        return Err(CodecError::InvalidContentLength);
    }
    std::str::from_utf8(v)
        .ok()
        .and_then(|s| s.parse().ok())
        .ok_or(CodecError::InvalidContentLength)
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

pub(crate) fn escape_into(s: &str, dst: &mut BytesMut) {
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
                    return Err(CodecError::UnencodableHeader(k.to_string()));
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
