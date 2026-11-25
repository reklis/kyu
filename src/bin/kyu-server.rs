//! Runs a kyu STOMP 1.2 broker.
//!
//! Usage: kyu-server [ADDR]   (default 127.0.0.1:61613)
//! Env:   KYU_LOGIN / KYU_PASSCODE  require these credentials
//!        KYU_HEARTBEAT=sx,sy       server heart-beat settings in ms

use kyu::server::{Server, ServerConfig};

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let addr = std::env::args().nth(1).unwrap_or_else(|| "127.0.0.1:61613".into());
    let mut config = ServerConfig::default();
    if let (Ok(l), Ok(p)) = (std::env::var("KYU_LOGIN"), std::env::var("KYU_PASSCODE")) {
        config.credentials = Some((l, p));
    }
    if let Ok(hb) = std::env::var("KYU_HEARTBEAT") {
        let (sx, sy) = hb.split_once(',').expect("KYU_HEARTBEAT must be sx,sy");
        config.heartbeat = (sx.parse().expect("sx"), sy.parse().expect("sy"));
    }
    let server = Server::bind(&addr, config).await?;
    eprintln!("kyu listening on {}", server.local_addr()?);
    server.run().await;
    Ok(())
}
