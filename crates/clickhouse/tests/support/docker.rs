//! Isolated synchronous ClickHouse container fixture with deterministic cleanup.
use std::{
    io::{Read, Write},
    net::TcpStream,
    process::Command,
    time::{Duration, Instant},
};

use clickhouse::{Error, Result};
/// A live server exclusively owned by one integration test.
pub struct Server {
    id: String,
    pub address: String,
    pub tcp_address: String,
}
impl Server {
    /// Start the exact supported server image on ephemeral Native and HTTP ports.
    /// # Panics
    /// Panics if Docker fails or the server does not become ready within sixty seconds.
    pub fn start() -> Self {
        let output = Command::new("docker")
            .args([
                "run",
                "--detach",
                "--rm",
                "-p",
                "0:9000",
                "-p",
                "0:8123",
                "-e",
                "CLICKHOUSE_SKIP_USER_SETUP=1",
                "clickhouse/clickhouse-server:26.8.2.7",
            ])
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "docker run failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let id = String::from_utf8(output.stdout).unwrap().trim().to_string();
        let mut server = Self {
            id,
            address: String::new(),
            tcp_address: String::new(),
        };
        let port = Command::new("docker")
            .args(["port", &server.id, "8123/tcp"])
            .output()
            .unwrap();
        assert!(port.status.success(), "docker port failed");
        let binding = String::from_utf8(port.stdout).unwrap();
        let port = binding
            .lines()
            .next()
            .and_then(|s| s.rsplit(':').next())
            .expect("mapped HTTP port");
        server.address = format!("127.0.0.1:{port}");
        let tcp = Command::new("docker")
            .args(["port", &server.id, "9000/tcp"])
            .output()
            .unwrap();
        assert!(tcp.status.success(), "docker port failed for Native TCP");
        let tcp_binding = String::from_utf8(tcp.stdout).unwrap();
        let tcp_port = tcp_binding
            .lines()
            .next()
            .and_then(|s| s.rsplit(':').next())
            .expect("mapped TCP port");
        server.tcp_address = format!("127.0.0.1:{tcp_port}");
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            if request(&server.address, b"SELECT 1").is_ok_and(|body| body == b"1\n") {
                break;
            }
            assert!(Instant::now() < deadline, "ClickHouse readiness timed out");
            std::thread::sleep(Duration::from_millis(50));
        }
        server
    }
}
impl Server {
    // Shared fixture: not every test inspects the container id.
    #[allow(dead_code)]
    pub fn id(&self) -> &str {
        &self.id
    }
}
impl Drop for Server {
    fn drop(&mut self) {
        if std::thread::panicking() {
            match Command::new("docker")
                .args(["logs", "--tail", "100", &self.id])
                .output()
            {
                Ok(output) => eprintln!(
                    "owned ClickHouse container diagnostics:\n{}\n{}",
                    String::from_utf8_lossy(&output.stdout),
                    String::from_utf8_lossy(&output.stderr)
                ),
                Err(error) => eprintln!("owned ClickHouse diagnostics failed: {error}"),
            }
        }
        match Command::new("docker")
            .args(["rm", "--force", &self.id])
            .output()
        {
            Ok(output) if output.status.success() => {}
            Ok(output) => eprintln!(
                "container cleanup rejected: {}",
                String::from_utf8_lossy(&output.stderr)
            ),
            Err(error) => eprintln!("container cleanup failed: {error}"),
        }
    }
}
fn error(reason: impl Into<std::borrow::Cow<'static, str>>) -> Error {
    Error::Config {
        reason: reason.into(),
    }
}
/// Send a complete synchronous HTTP request and decode its response framing.
/// # Errors
/// Network errors, malformed framing, and server rejections return an error.
pub fn request(address: &str, body: &[u8]) -> Result<Vec<u8>> {
    let mut socket = TcpStream::connect(address).map_err(|e| error(format!("connect: {e}")))?;
    socket
        .set_read_timeout(Some(Duration::from_secs(15)))
        .map_err(|e| error(format!("read timeout: {e}")))?;
    socket
        .set_write_timeout(Some(Duration::from_secs(15)))
        .map_err(|e| error(format!("write timeout: {e}")))?;
    write!(
        socket,
        "POST / HTTP/1.0\r\nHost: localhost\r\nConnection: close\r\nContent-Length: {}\r\n\r\n",
        body.len()
    )
    .map_err(|e| error(format!("header write: {e}")))?;
    socket
        .write_all(body)
        .map_err(|e| error(format!("body write: {e}")))?;
    let mut response = Vec::new();
    socket
        .read_to_end(&mut response)
        .map_err(|e| error(format!("response read: {e}")))?;
    let split = response
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .ok_or_else(|| error("missing HTTP response header"))?;
    let header =
        std::str::from_utf8(&response[..split]).map_err(|_| error("invalid HTTP header"))?;
    if !header.starts_with("HTTP/1.1 200 ") && !header.starts_with("HTTP/1.0 200 ") {
        return Err(error(format!(
            "ClickHouse HTTP rejected: {}",
            String::from_utf8_lossy(&response)
        )));
    }
    let body = &response[split + 4..];
    if header
        .to_ascii_lowercase()
        .contains("transfer-encoding: chunked")
    {
        let mut out = Vec::new();
        let mut remaining = body;
        loop {
            let end = remaining
                .windows(2)
                .position(|w| w == b"\r\n")
                .ok_or_else(|| error("invalid chunk size"))?;
            let size = std::str::from_utf8(&remaining[..end])
                .map_err(|_| error("invalid chunk header"))?;
            let n = usize::from_str_radix(size.split(';').next().unwrap_or(""), 16)
                .map_err(|_| error("invalid chunk length"))?;
            if n == 0 {
                break;
            }
            remaining = &remaining[end + 2..];
            out.extend_from_slice(
                remaining
                    .get(..n)
                    .ok_or_else(|| error("truncated HTTP chunk"))?,
            );
            remaining = remaining
                .get(n + 2..)
                .ok_or_else(|| error("truncated HTTP chunk delimiter"))?;
        }
        Ok(out)
    } else {
        Ok(body.to_vec())
    }
}
