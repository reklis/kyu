//! STOMP frames and commands.

use bytes::Bytes;

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
    pub fn allows_body(&self) -> bool {
        matches!(self, Command::Send | Command::Message | Command::Error)
    }

    /// CONNECT and CONNECTED headers are not escaped (STOMP 1.0 compatibility).
    pub fn escapes_headers(&self) -> bool {
        !matches!(self, Command::Connect | Command::Connected)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Frame {
    pub command: Command,
    pub headers: Vec<(String, String)>,
    pub body: Bytes,
}

impl Frame {
    pub fn new(command: Command) -> Frame {
        Frame { command, headers: Vec::new(), body: Bytes::new() }
    }

    pub fn with_header(mut self, name: impl Into<String>, value: impl Into<String>) -> Frame {
        self.headers.push((name.into(), value.into()));
        self
    }

    pub fn with_body(mut self, body: impl Into<Bytes>) -> Frame {
        self.body = body.into();
        self
    }

    /// The value of a header. When a header is repeated, the first entry wins.
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers.iter().find(|(k, _)| k == name).map(|(_, v)| v.as_str())
    }
}
