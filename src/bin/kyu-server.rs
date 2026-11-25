//! Runs a kyu STOMP 1.2 broker.
//!
//! Usage: kyu-server [ADDR]           run the broker until SIGTERM / Ctrl-C
//!        kyu-server --check [ADDR]   exit 0 if a broker at ADDR accepts a
//!                                    STOMP CONNECT (for container health checks)
//!
//! ADDR defaults to `KYU_ADDR`, else 127.0.0.1:61613.
//! Env:   `KYU_LOGIN` / `KYU_PASSCODE`  require these credentials (`--check`
//!                                    connects with them)
//!        `KYU_HEARTBEAT=sx,sy`       server heart-beat settings in ms

use std::process::ExitCode;
use std::time::Duration;

use kyu::client::{Client, ConnectOptions};
use kyu::server::{Server, ServerConfig};

#[cfg(feature = "mimalloc")]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

const DEFAULT_ADDR: &str = "127.0.0.1:61613";

#[tokio::main]
async fn main() -> ExitCode {
    let mut args: Vec<String> = std::env::args().skip(1).collect();
    let checking = args.first().is_some_and(|a| a == "--check");
    if checking {
        args.remove(0);
    }
    let addr = args
        .first()
        .cloned()
        .or_else(|| std::env::var("KYU_ADDR").ok())
        .unwrap_or_else(|| DEFAULT_ADDR.into());
    let result = if checking {
        check(&addr).await
    } else {
        serve(&addr).await
    };
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            log(&format!("kyu-server: {e}"));
            ExitCode::FAILURE
        }
    }
}

/// Writes a line to stderr. Unlike `eprintln!`, never panics if stderr has
/// been closed: a server must not die because nobody reads its logs.
fn log(line: &str) {
    use std::io::Write;
    let _ = writeln!(std::io::stderr(), "{line}");
}

fn credentials() -> Option<(String, String)> {
    match (std::env::var("KYU_LOGIN"), std::env::var("KYU_PASSCODE")) {
        (Ok(login), Ok(passcode)) => Some((login, passcode)),
        _ => None,
    }
}

fn config() -> Result<ServerConfig, String> {
    let mut config = ServerConfig {
        credentials: credentials(),
        ..ServerConfig::default()
    };
    if let Ok(hb) = std::env::var("KYU_HEARTBEAT") {
        let parsed = hb
            .split_once(',')
            .and_then(|(sx, sy)| Some((sx.parse().ok()?, sy.parse().ok()?)));
        config.heartbeat = parsed.ok_or(format!("KYU_HEARTBEAT must be sx,sy (got {hb:?})"))?;
    }
    Ok(config)
}

async fn serve(addr: &str) -> Result<(), String> {
    let server = Server::bind(addr, config()?)
        .await
        .map_err(|e| format!("cannot listen on {addr}: {e}"))?;
    let local = server.local_addr().map_err(|e| e.to_string())?;
    log(&format!("kyu listening on {local}"));
    tokio::select! {
        () = server.run() => {}
        signal = shutdown_signal() => log(&format!("kyu received {signal}, stopping")),
    }
    Ok(())
}

/// Resolves on SIGTERM (what `docker stop` sends) or SIGINT (Ctrl-C).
async fn shutdown_signal() -> &'static str {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        let mut term = signal(SignalKind::terminate()).expect("install SIGTERM handler");
        tokio::select! {
            _ = term.recv() => "SIGTERM",
            _ = tokio::signal::ctrl_c() => "SIGINT",
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
        "Ctrl-C"
    }
}

/// Connects, completes the STOMP handshake and disconnects.
async fn check(addr: &str) -> Result<(), String> {
    let mut options = ConnectOptions::new("localhost");
    if let Some((login, passcode)) = credentials() {
        options = options.credentials(login, passcode);
    }
    let attempt = async {
        let client = Client::connect(addr, options).await?;
        client.disconnect().await
    };
    match tokio::time::timeout(Duration::from_secs(5), attempt).await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(e)) => Err(format!("{addr}: {e}")),
        Err(_) => Err(format!("{addr}: no answer within 5s")),
    }
}
