//! STOMP frames and commands.

use std::borrow::Borrow;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::ops::Deref;

use bytes::Bytes;

/// An immutable UTF-8 string backed by [`Bytes`].
///
/// Cloning is a reference-count bump, and the decoder hands out header names
/// and values as slices of the frame's read buffer instead of copying each
/// into its own allocation.
#[derive(Clone, Default, PartialEq, Eq, PartialOrd, Ord)]
pub struct Str(Bytes);

impl Str {
    /// Wraps bytes after checking they are UTF-8.
    ///
    /// # Errors
    /// If `bytes` is not valid UTF-8.
    pub fn from_utf8(bytes: Bytes) -> Result<Str, std::str::Utf8Error> {
        std::str::from_utf8(&bytes)?;
        Ok(Str(bytes))
    }

    /// Wraps bytes already known to be UTF-8.
    ///
    /// # Safety
    /// `bytes` must be valid UTF-8.
    pub(crate) unsafe fn from_utf8_unchecked(bytes: Bytes) -> Str {
        debug_assert!(std::str::from_utf8(&bytes).is_ok());
        Str(bytes)
    }

    /// A string with static lifetime: no allocation.
    #[must_use]
    pub const fn from_static(s: &'static str) -> Str {
        Str(Bytes::from_static(s.as_bytes()))
    }

    pub fn as_str(&self) -> &str {
        // SAFETY: every constructor guarantees UTF-8.
        unsafe { std::str::from_utf8_unchecked(&self.0) }
    }

    /// The underlying bytes (shared, not copied).
    pub fn as_bytes(&self) -> &Bytes {
        &self.0
    }
}

impl Deref for Str {
    type Target = str;
    fn deref(&self) -> &str {
        self.as_str()
    }
}

impl AsRef<str> for Str {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl Borrow<str> for Str {
    fn borrow(&self) -> &str {
        self.as_str()
    }
}

// Must agree with `str`'s Hash so that `Borrow<str>` lookups work.
impl Hash for Str {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.as_str().hash(state);
    }
}

impl fmt::Debug for Str {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self.as_str(), f)
    }
}

impl fmt::Display for Str {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self.as_str(), f)
    }
}

impl From<&str> for Str {
    fn from(s: &str) -> Str {
        Str(Bytes::copy_from_slice(s.as_bytes()))
    }
}

impl From<String> for Str {
    fn from(s: String) -> Str {
        Str(Bytes::from(s.into_bytes()))
    }
}

impl From<&String> for Str {
    fn from(s: &String) -> Str {
        Str::from(s.as_str())
    }
}

impl From<Str> for String {
    fn from(s: Str) -> String {
        s.as_str().to_owned()
    }
}

impl PartialEq<str> for Str {
    fn eq(&self, other: &str) -> bool {
        self.as_str() == other
    }
}

impl PartialEq<&str> for Str {
    fn eq(&self, other: &&str) -> bool {
        self.as_str() == *other
    }
}

impl PartialEq<String> for Str {
    fn eq(&self, other: &String) -> bool {
        self.as_str() == other
    }
}

impl PartialEq<Str> for str {
    fn eq(&self, other: &Str) -> bool {
        self == other.as_str()
    }
}

impl PartialEq<Str> for &str {
    fn eq(&self, other: &Str) -> bool {
        *self == other.as_str()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Command {
    Connect,
    Stomp,
    Connected,
    Send,
    Subscribe,
    Unsubscribe,
    Ack,
    Nack,
    Begin,
    Commit,
    Abort,
    Disconnect,
    Message,
    Receipt,
    Error,
    /// A command not defined by the specification.
    Other(String),
}

impl Command {
    /// Parses a command line. Commands are case sensitive.
    #[must_use]
    pub fn parse(s: &str) -> Command {
        match s {
            "CONNECT" => Command::Connect,
            "STOMP" => Command::Stomp,
            "CONNECTED" => Command::Connected,
            "SEND" => Command::Send,
            "SUBSCRIBE" => Command::Subscribe,
            "UNSUBSCRIBE" => Command::Unsubscribe,
            "ACK" => Command::Ack,
            "NACK" => Command::Nack,
            "BEGIN" => Command::Begin,
            "COMMIT" => Command::Commit,
            "ABORT" => Command::Abort,
            "DISCONNECT" => Command::Disconnect,
            "MESSAGE" => Command::Message,
            "RECEIPT" => Command::Receipt,
            "ERROR" => Command::Error,
            other => Command::Other(other.to_string()),
        }
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        match self {
            Command::Connect => "CONNECT",
            Command::Stomp => "STOMP",
            Command::Connected => "CONNECTED",
            Command::Send => "SEND",
            Command::Subscribe => "SUBSCRIBE",
            Command::Unsubscribe => "UNSUBSCRIBE",
            Command::Ack => "ACK",
            Command::Nack => "NACK",
            Command::Begin => "BEGIN",
            Command::Commit => "COMMIT",
            Command::Abort => "ABORT",
            Command::Disconnect => "DISCONNECT",
            Command::Message => "MESSAGE",
            Command::Receipt => "RECEIPT",
            Command::Error => "ERROR",
            Command::Other(s) => s,
        }
    }

    /// Only SEND, MESSAGE and ERROR frames may carry a body.
    #[must_use]
    pub fn allows_body(&self) -> bool {
        matches!(self, Command::Send | Command::Message | Command::Error)
    }

    /// CONNECT and CONNECTED headers are not escaped (STOMP 1.0 compatibility).
    #[must_use]
    pub fn escapes_headers(&self) -> bool {
        !matches!(self, Command::Connect | Command::Connected)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Frame {
    pub command: Command,
    pub headers: Vec<(Str, Str)>,
    pub body: Bytes,
}

impl Frame {
    #[must_use]
    pub fn new(command: Command) -> Frame {
        Frame {
            command,
            headers: Vec::new(),
            body: Bytes::new(),
        }
    }

    #[must_use]
    pub fn with_header(mut self, name: impl Into<Str>, value: impl Into<Str>) -> Frame {
        self.headers.push((name.into(), value.into()));
        self
    }

    #[must_use]
    pub fn with_body(mut self, body: impl Into<Bytes>) -> Frame {
        self.body = body.into();
        self
    }

    /// The value of a header. When a header is repeated, the first entry wins.
    pub fn header(&self, name: &str) -> Option<&str> {
        self.header_value(name).map(Str::as_str)
    }

    /// Like [`Frame::header`], but returns the shared [`Str`] so the value can
    /// be kept without copying.
    pub fn header_value(&self, name: &str) -> Option<&Str> {
        self.headers.iter().find(|(k, _)| k == name).map(|(_, v)| v)
    }
}
