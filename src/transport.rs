//! Pieces shared by the client and server: heart-beat negotiation and
//! read-activity tracking.

use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Context, Poll};
use std::time::Duration;

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::time::{Instant, Sleep, sleep};

/// When bytes were last read from a connection; lock-free.
#[derive(Clone)]
pub struct LastRead {
    base: Instant,
    nanos: Arc<AtomicU64>,
}

impl LastRead {
    fn new() -> LastRead {
        LastRead { base: Instant::now(), nanos: Arc::new(AtomicU64::new(0)) }
    }

    fn touch(&self) {
        self.nanos.store(self.base.elapsed().as_nanos() as u64, Ordering::Relaxed);
    }

    pub fn get(&self) -> Instant {
        self.base + Duration::from_nanos(self.nanos.load(Ordering::Relaxed))
    }
}

/// Wraps the socket to record when any bytes last arrived, so partially
/// received frames also count as heart-beat activity.
pub struct Activity<S> {
    pub inner: S,
    pub last: LastRead,
}

impl<S: AsyncRead + Unpin> AsyncRead for Activity<S> {
    fn poll_read(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        let before = buf.filled().len();
        let res = Pin::new(&mut this.inner).poll_read(cx, buf);
        if buf.filled().len() > before {
            this.last.touch();
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
        Activity { inner, last: LastRead::new() }
    }
}

pub enum Tick {
    /// Nothing was written for the agreed interval: send an EOL.
    SendHeartbeat,
    /// Nothing was read within the allowed window.
    PeerDead,
}

/// Heart-beat bookkeeping for one connection. The timers are created once and
/// only re-armed when they fire, so the per-frame cost is a timestamp store.
pub struct Heartbeats {
    out: Option<Duration>,
    inn: Option<Duration>,
    last_write: Instant,
    last_read: LastRead,
    write_timer: Pin<Box<Sleep>>,
    read_timer: Pin<Box<Sleep>>,
}

impl Heartbeats {
    pub fn new(out: Option<Duration>, inn: Option<Duration>, last_read: LastRead) -> Heartbeats {
        let far = Duration::from_secs(86400 * 365);
        Heartbeats {
            out,
            inn,
            last_write: Instant::now(),
            last_read,
            write_timer: Box::pin(sleep(out.unwrap_or(far))),
            read_timer: Box::pin(sleep(inn.map(read_deadline).unwrap_or(far))),
        }
    }

    /// Treats the peer as alive now (used while deliberately not reading).
    pub fn read_alive(&mut self) {
        self.last_read.touch();
    }

    /// Records that something was written.
    pub fn wrote(&mut self) {
        if self.out.is_some() {
            self.last_write = Instant::now();
        }
    }

    /// Resolves when a heart-beat must be sent or the peer is considered dead;
    /// never resolves if heart-beating is off in both directions.
    pub async fn tick(&mut self) -> Tick {
        loop {
            tokio::select! {
                () = self.write_timer.as_mut(), if self.out.is_some() => {
                    let due = self.last_write + self.out.unwrap();
                    if Instant::now() >= due {
                        return Tick::SendHeartbeat;
                    }
                    self.write_timer.as_mut().reset(due);
                }
                () = self.read_timer.as_mut(), if self.inn.is_some() => {
                    let due = self.last_read.get() + read_deadline(self.inn.unwrap());
                    if Instant::now() >= due {
                        return Tick::PeerDead;
                    }
                    self.read_timer.as_mut().reset(due);
                }
                else => std::future::pending::<()>().await,
            }
        }
    }
}
