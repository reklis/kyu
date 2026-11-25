//! Pieces shared by the client and server: heart-beat negotiation and
//! read-activity tracking.

use std::io;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::time::Instant;

/// Wraps the socket's read half to record when any bytes last arrived, so
/// partially received frames also count as heart-beat activity.
pub struct Activity<S> {
    pub inner: S,
    pub last: Arc<Mutex<Instant>>,
}

impl<S: AsyncRead + Unpin> AsyncRead for Activity<S> {
    fn poll_read(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        let before = buf.filled().len();
        let res = Pin::new(&mut this.inner).poll_read(cx, buf);
        if buf.filled().len() > before {
            *this.last.lock().unwrap() = Instant::now();
        }
        res
    }
}

impl<S: AsyncWrite + Unpin> AsyncWrite for Activity<S> {
    fn poll_write(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.get_mut().inner).poll_write(cx, buf)
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_flush(cx)
    }
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_shutdown(cx)
    }
}

/// Parses a `heart-beat` header value: two non-negative integers.
pub fn parse_heartbeat(v: Option<&str>) -> Option<(u32, u32)> {
    let Some(v) = v else { return Some((0, 0)) };
    let (a, b) = v.split_once(',')?;
    let num = |s: &str| {
        if s.is_empty() || !s.bytes().all(|c| c.is_ascii_digit()) {
            None
        } else {
            s.parse::<u32>().ok()
        }
    };
    Some((num(a)?, num(b)?))
}

/// Heart-beat interval in one direction: 0 if either side opts out.
pub fn negotiate(can_send: u32, wants: u32) -> Option<Duration> {
    if can_send == 0 || wants == 0 {
        None
    } else {
        Some(Duration::from_millis(can_send.max(wants) as u64))
    }
}

/// Allowance on top of the agreed interval before declaring a peer dead.
pub fn read_deadline(interval: Duration) -> Duration {
    interval * 2 + Duration::from_millis(20)
}


impl<S> Activity<S> {
    pub fn new(inner: S) -> Activity<S> {
        Activity { inner, last: Arc::new(Mutex::new(Instant::now())) }
    }
}
