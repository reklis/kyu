//! tokio-util codec for the STOMP 1.2 wire format.

use bytes::{Buf, BufMut, Bytes, BytesMut};
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
        Limits {
            max_frame_size: 16 * 1024 * 1024,
            max_headers: 1000,
            max_header_length: 64 * 1024,
        }
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
    pending: Option<ParsedHead>,
}

impl StompCodec {
    #[must_use]
    pub fn new() -> StompCodec {
        StompCodec::default()
    }

    #[must_use]
    pub fn with_limits(limits: Limits) -> StompCodec {
        StompCodec {
            limits,
            pending: None,
        }
    }
}

#[derive(Debug, Clone)]
struct ParsedHead {
    command: Command,
    head: Head,
    /// Offset up to which we have already scanned for the NULL terminator.
    scanned_to: usize,
}

impl Decoder for StompCodec {
    type Item = Item;
    type Error = CodecError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Item>, CodecError> {
        let mut parsed = if let Some(p) = self.pending.take() {
            p
        } else {
            if let Some(eol) = decode_eol(src) {
                return eol;
            }
            let Some(head) = scan_head(src, &self.limits)? else {
                return Ok(None);
            };
            let command = parse_command(&src[..head.body_start])?;
            let scanned_to = head.body_start;
            ParsedHead {
                command,
                head,
                scanned_to,
            }
        };
        let Some(body_end) = find_body_end(src, &mut parsed, &self.limits)? else {
            self.pending = Some(parsed);
            return Ok(None);
        };
        // The frame is complete: take it out of the buffer as one shared
        // allocation and hand out headers and body as slices of it.
        let raw = src.split_to(body_end + 1).freeze();
        let headers = slice_headers(&raw, &parsed.head, parsed.command.escapes_headers())?;
        let body = raw.slice(parsed.head.body_start..body_end);
        Ok(Some(Item::Frame(Frame {
            command: parsed.command,
            headers,
            body,
        })))
    }
}

/// An EOL outside a frame is a heart-beat (this also covers the optional
/// EOLs that may follow a frame's NULL octet). Returns `None` if `src`
/// starts with a frame instead.
fn decode_eol(src: &mut BytesMut) -> Option<Result<Option<Item>, CodecError>> {
    let len = match (src.first(), src.get(1)) {
        (None, _) | (Some(b'\r'), None) => return Some(Ok(None)),
        (Some(b'\n'), _) => 1,
        (Some(b'\r'), Some(b'\n')) => 2,
        (Some(b'\r'), Some(_)) => {
            return Some(Err(CodecError::MalformedHeader(
                "bare carriage return".into(),
            )));
        }
        _ => return None,
    };
    src.advance(len);
    Some(Ok(Some(Item::Heartbeat)))
}

/// What a scan of the command and header lines found.
#[derive(Debug, Clone, Default)]
struct Head {
    /// Offset just past the blank line that ends the headers.
    body_start: usize,
    header_lines: usize,
    content_length: Option<usize>,
}

/// Finds the end of the headers without allocating, enforcing limits and
/// header syntax on the way. `None` if the headers are not complete yet.
fn scan_head(src: &[u8], limits: &Limits) -> Result<Option<Head>, CodecError> {
    let mut head = Head::default();
    let mut pos = 0;
    loop {
        let Some(nl) = memchr(b'\n', &src[pos..]) else {
            return incomplete_head(src, pos, limits);
        };
        let line = trim_cr(&src[pos..pos + nl]);
        let first = pos == 0;
        pos += nl + 1;
        if line.is_empty() {
            head.body_start = pos;
            return Ok(Some(head));
        }
        if pos > limits.max_frame_size {
            return Err(CodecError::FrameTooLarge);
        }
        if !first {
            head.header_line(line, limits)?;
        }
    }
}

/// Headers still arriving: fine unless a limit is already exceeded.
fn incomplete_head(
    src: &[u8],
    line_start: usize,
    limits: &Limits,
) -> Result<Option<Head>, CodecError> {
    if line_start > 0 && src.len() - line_start > limits.max_header_length {
        Err(CodecError::HeaderTooLong)
    } else if src.len() > limits.max_frame_size {
        Err(CodecError::FrameTooLarge)
    } else {
        Ok(None)
    }
}

impl Head {
    /// Checks one header line (EOL already removed).
    fn header_line(&mut self, line: &[u8], limits: &Limits) -> Result<(), CodecError> {
        self.header_lines += 1;
        if line.len() > limits.max_header_length {
            return Err(CodecError::HeaderTooLong);
        }
        if self.header_lines > limits.max_headers {
            return Err(CodecError::TooManyHeaders);
        }
        if memchr(b'\r', line).is_some() {
            return Err(CodecError::MalformedHeader("bare carriage return".into()));
        }
        let colon = match memchr(b':', line) {
            Some(0) => return Err(CodecError::MalformedHeader("empty header name".into())),
            Some(colon) => colon,
            None => {
                let line = String::from_utf8_lossy(line).into();
                return Err(CodecError::MalformedHeader(line));
            }
        };
        // The first content-length wins, like every repeated header.
        if self.content_length.is_none() && &line[..colon] == b"content-length" {
            self.content_length = Some(parse_content_length(&line[colon + 1..])?);
        }
        Ok(())
    }
}

/// Checks the header block is UTF-8 and parses the command line. Checking
/// the whole block once also covers every header: slicing UTF-8 at ASCII
/// bytes (EOL, ':') leaves valid UTF-8.
fn parse_command(head: &[u8]) -> Result<Command, CodecError> {
    let head = utf8(head)?;
    let line = head.split('\n').next().unwrap_or_default();
    Ok(Command::parse(line.strip_suffix('\r').unwrap_or(line)))
}

/// Locates the body's terminating NULL. `None` if the body is incomplete.
/// Reserves space in `src` when `content-length` is known to avoid repeated
/// buffer reallocations while reading large frames.
fn find_body_end(
    src: &mut BytesMut,
    parsed: &mut ParsedHead,
    limits: &Limits,
) -> Result<Option<usize>, CodecError> {
    let end = match parsed.head.content_length {
        Some(len) => {
            let end = parsed
                .head
                .body_start
                .checked_add(len)
                .ok_or(CodecError::FrameTooLarge)?;
            if end + 1 > limits.max_frame_size {
                return Err(CodecError::FrameTooLarge);
            }
            match src.get(end) {
                None => {
                    src.reserve(end + 1 - src.len());
                    return Ok(None);
                }
                Some(0) => end,
                Some(_) => return Err(CodecError::MissingNull),
            }
        }
        None => match memchr(0, &src[parsed.scanned_to..]) {
            Some(i) => parsed.scanned_to + i,
            None if src.len() > limits.max_frame_size => return Err(CodecError::FrameTooLarge),
            None => {
                parsed.scanned_to = src.len();
                return Ok(None);
            }
        },
    };
    if end + 1 > limits.max_frame_size {
        return Err(CodecError::FrameTooLarge);
    }
    Ok(Some(end))
}

/// Builds the header list from a complete frame already checked by
/// `scan_head` and `parse_command`.
fn slice_headers(raw: &Bytes, head: &Head, escaped: bool) -> Result<Vec<(Str, Str)>, CodecError> {
    let mut headers = Vec::with_capacity(head.header_lines);
    let mut pos = memchr(b'\n', raw).expect("command line") + 1;
    for _ in 0..head.header_lines {
        let nl = pos + memchr(b'\n', &raw[pos..head.body_start]).expect("header line");
        let end = if raw[nl - 1] == b'\r' { nl - 1 } else { nl };
        let colon = pos + memchr(b':', &raw[pos..end]).expect("checked by scan_head");
        headers.push((
            header_field(raw, pos, colon, escaped)?,
            header_field(raw, colon + 1, end, escaped)?,
        ));
        pos = nl + 1;
    }
    Ok(headers)
}

/// A header name or value: a slice of the frame unless it must be unescaped.
fn header_field(raw: &Bytes, from: usize, to: usize, escaped: bool) -> Result<Str, CodecError> {
    let bytes = &raw[from..to];
    if escaped && bytes.contains(&b'\\') {
        unescape(bytes).map(Str::from)
    } else {
        // SAFETY: inside the UTF-8 checked header block, cut at ASCII bytes.
        Ok(unsafe { Str::from_utf8_unchecked(raw.slice(from..to)) })
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

/// Writes an unsigned integer in decimal ASCII without going through `core::fmt`.
pub(crate) fn put_u64(dst: &mut BytesMut, mut n: u64) {
    let mut buf = [0u8; 20];
    let mut i = buf.len();
    loop {
        i -= 1;
        buf[i] = b'0' + u8::try_from(n % 10).unwrap();
        n /= 10;
        if n == 0 {
            break;
        }
    }
    dst.put_slice(&buf[i..]);
}

pub(crate) fn put_usize(dst: &mut BytesMut, n: usize) {
    put_u64(dst, u64::try_from(n).unwrap_or(u64::MAX));
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

/// CONNECT/CONNECTED headers cannot be escaped, so they must not contain
/// characters that would need it.
fn check_unescaped(headers: &[(Str, Str)]) -> Result<(), CodecError> {
    match headers
        .iter()
        .find(|(k, v)| k.contains(['\r', '\n', ':']) || v.contains(['\r', '\n']))
    {
        Some((k, _)) => Err(CodecError::UnencodableHeader(k.to_string())),
        None => Ok(()),
    }
}

fn put_header(dst: &mut BytesMut, name: &str, value: &str, escaped: bool) {
    if escaped {
        escape_into(name, dst);
        dst.put_u8(b':');
        escape_into(value, dst);
    } else {
        dst.put_slice(name.as_bytes());
        dst.put_u8(b':');
        dst.put_slice(value.as_bytes());
    }
    dst.put_u8(b'\n');
}

impl Encoder<Frame> for StompCodec {
    type Error = CodecError;

    fn encode(&mut self, frame: Frame, dst: &mut BytesMut) -> Result<(), CodecError> {
        if !frame.body.is_empty() && !frame.command.allows_body() {
            return Err(CodecError::BodyNotAllowed(frame.command));
        }
        let escaped = frame.command.escapes_headers();
        if !escaped {
            check_unescaped(&frame.headers)?;
        }
        let headers_len: usize = frame
            .headers
            .iter()
            .map(|(k, v)| k.len() + v.len() + 2)
            .sum();
        dst.reserve(frame.command.as_str().len() + headers_len + frame.body.len() + 40);
        dst.put_slice(frame.command.as_str().as_bytes());
        dst.put_u8(b'\n');
        // We always write the correct content-length ourselves.
        for (k, v) in frame.headers.iter().filter(|(k, _)| k != "content-length") {
            put_header(dst, k, v, escaped);
        }
        if !frame.body.is_empty() {
            dst.put_slice(b"content-length:");
            put_usize(dst, frame.body.len());
            dst.put_u8(b'\n');
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
