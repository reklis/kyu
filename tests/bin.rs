//! The `kyu-server` binary as a container runs it: configured through the
//! environment, stopped with SIGTERM, health-checked with `--check`.

use std::io::{BufRead, BufReader};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

const BIN: &str = env!("CARGO_BIN_EXE_kyu-server");

/// A running server, killed when dropped so failing tests leave nothing behind.
struct Running(Child);

impl Drop for Running {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// Starts the server with `envs` and returns it with the address it bound,
/// read from its "kyu listening on ADDR" line.
fn spawn(envs: &[(&str, &str)]) -> (Running, String) {
    let mut child = Command::new(BIN)
        .env_remove("KYU_ADDR")
        .env_remove("KYU_LOGIN")
        .env_remove("KYU_PASSCODE")
        .envs(envs.iter().copied())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .expect("start kyu-server");
    let stderr = child.stderr.take().unwrap();
    let child = Running(child);
    let mut line = String::new();
    BufReader::new(stderr).read_line(&mut line).unwrap();
    let addr = line
        .trim()
        .strip_prefix("kyu listening on ")
        .unwrap_or_else(|| panic!("unexpected first line: {line:?}"))
        .to_string();
    (child, addr)
}

fn check(envs: &[(&str, &str)]) -> bool {
    Command::new(BIN)
        .arg("--check")
        .env_remove("KYU_LOGIN")
        .env_remove("KYU_PASSCODE")
        .envs(envs.iter().copied())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .unwrap()
        .success()
}

fn wait_exit(child: &mut Child, within: Duration) -> Option<std::process::ExitStatus> {
    let deadline = Instant::now() + within;
    while Instant::now() < deadline {
        if let Some(status) = child.try_wait().unwrap() {
            return Some(status);
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    None
}

#[test]
fn listen_address_comes_from_kyu_addr() {
    let (_server, addr) = spawn(&[("KYU_ADDR", "127.0.0.1:0")]);
    assert!(addr.starts_with("127.0.0.1:"), "{addr}");
    assert!(
        !addr.ends_with(":0"),
        "should report the actual port: {addr}"
    );
}

#[test]
fn sigterm_stops_the_server_cleanly() {
    let (mut server, _) = spawn(&[("KYU_ADDR", "127.0.0.1:0")]);
    let term = Command::new("kill")
        .args(["-TERM", &server.0.id().to_string()])
        .status()
        .unwrap();
    assert!(term.success());
    let status = wait_exit(&mut server.0, Duration::from_secs(5)).expect("server ignored SIGTERM");
    assert!(status.success(), "exit status {status}");
}

#[test]
fn check_succeeds_against_a_running_server() {
    let (_server, addr) = spawn(&[("KYU_ADDR", "127.0.0.1:0")]);
    assert!(check(&[("KYU_ADDR", &addr)]));
}

#[test]
fn check_fails_when_nothing_is_listening() {
    // Bind and release a port so that nothing is listening on it.
    let addr = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .to_string();
    assert!(!check(&[("KYU_ADDR", &addr)]));
}

#[test]
fn check_uses_the_configured_credentials() {
    let creds = [("KYU_LOGIN", "admin"), ("KYU_PASSCODE", "secret")];
    let (_server, addr) = spawn(&[("KYU_ADDR", "127.0.0.1:0"), creds[0], creds[1]]);
    assert!(check(&[("KYU_ADDR", &addr), creds[0], creds[1]]));
    assert!(
        !check(&[
            ("KYU_ADDR", &addr),
            ("KYU_LOGIN", "admin"),
            ("KYU_PASSCODE", "wrong")
        ]),
        "a rejected CONNECT must fail the check"
    );
}
