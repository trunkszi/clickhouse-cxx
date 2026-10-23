//! An owned cleartext HTTP/2 proxy forwarding to an isolated ClickHouse server.
use std::{
    fs,
    path::PathBuf,
    process::Command,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

/// A controlled HTTP/2 edge; ClickHouse receives the same request over HTTP/1.1.
pub struct Http2Proxy {
    id: Option<String>,
    directory: PathBuf,
    pub address: String,
}
impl Http2Proxy {
    /// Start an isolated nginx container with an ephemeral frontend port.
    /// # Panics
    /// Panics if Docker, temporary configuration, or readiness fails.
    pub fn start(upstream: &str) -> Self {
        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let directory =
            std::env::temp_dir().join(format!("clickhouse-http2-{}-{suffix}", std::process::id()));
        fs::create_dir(&directory).unwrap();
        let mut proxy = Self {
            id: None,
            directory,
            address: String::new(),
        };
        let port = upstream.rsplit(':').next().unwrap();
        let config = format!(
            "error_log /dev/stderr info;\nevents {{}}\nhttp {{ server {{ listen 8080; http2 on; location / {{ proxy_pass http://host.docker.internal:{port}; proxy_http_version 1.1; }} }} }}\n"
        );
        let path = proxy.directory.join("nginx.conf");
        fs::write(&path, config).unwrap();
        let output = Command::new("docker")
            .args([
                "run",
                "--detach",
                "--rm",
                "--add-host",
                "host.docker.internal:host-gateway",
                "-p",
                "0:8080",
                "-v",
                &format!("{}:/etc/nginx/nginx.conf:ro", path.display()),
                "nginx:1.28.0-alpine",
            ])
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "nginx start failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        proxy.id = Some(String::from_utf8(output.stdout).unwrap().trim().to_string());
        let output = Command::new("docker")
            .args(["port", proxy.id.as_deref().unwrap(), "8080/tcp"])
            .output()
            .unwrap();
        assert!(output.status.success());
        let binding = String::from_utf8(output.stdout).unwrap();
        let port = binding
            .lines()
            .next()
            .unwrap()
            .rsplit(':')
            .next()
            .unwrap()
            .parse::<u16>()
            .unwrap();
        proxy.address = format!("127.0.0.1:{port}");
        let deadline = Instant::now() + Duration::from_secs(30);
        while !super::docker::request(&proxy.address, b"SELECT 1").is_ok_and(|body| body == b"1\n")
        {
            assert!(
                Instant::now() < deadline,
                "HTTP/2 proxy readiness timed out"
            );
            std::thread::sleep(Duration::from_millis(50));
        }
        proxy
    }
}
impl Drop for Http2Proxy {
    fn drop(&mut self) {
        if let Some(id) = &self.id {
            if std::thread::panicking() {
                match Command::new("docker")
                    .args(["logs", "--tail", "100", id])
                    .output()
                {
                    Ok(output) => eprintln!(
                        "owned HTTP/2 proxy diagnostics:\n{}\n{}",
                        String::from_utf8_lossy(&output.stdout),
                        String::from_utf8_lossy(&output.stderr)
                    ),
                    Err(error) => eprintln!("owned HTTP/2 proxy diagnostics failed: {error}"),
                }
            }
            match Command::new("docker").args(["rm", "--force", id]).output() {
                Ok(output) if output.status.success() => {}
                Ok(output) => eprintln!(
                    "proxy cleanup rejected: {}",
                    String::from_utf8_lossy(&output.stderr)
                ),
                Err(error) => eprintln!("proxy cleanup failed: {error}"),
            }
        }
        if let Err(error) = fs::remove_dir_all(&self.directory) {
            eprintln!("proxy configuration cleanup failed: {error}");
        }
    }
}
