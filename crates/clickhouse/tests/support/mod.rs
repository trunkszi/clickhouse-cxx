//! Isolated live-server fixture; all resources belong to the current test.

use std::{
    process::Command,
    thread,
    time::{Duration, Instant},
};

use clickhouse::{Client, ClientConfig, CompressionMethod};

pub struct TestClickHouseContainer {
    id: String,
    endpoint: String,
    live: bool,
}

impl TestClickHouseContainer {
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
                "CLICKHOUSE_USER=default",
                "-e",
                "CLICKHOUSE_PASSWORD=streaming-test",
                "clickhouse/clickhouse-server:26.8.2.7",
            ])
            .output()
            .expect("Docker must be available for live server tests");
        assert!(
            output.status.success(),
            "cannot start ClickHouse container: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        // Establish the cleanup guard before any further fallible setup.
        let mut fixture = Self {
            id: String::from_utf8(output.stdout)
                .expect("Docker returns a UTF-8 container ID")
                .trim()
                .into(),
            endpoint: String::new(),
            live: true,
        };
        let output = Command::new("docker")
            .args(["port", &fixture.id, "9000/tcp"])
            .output()
            .expect("cannot inspect container port");
        assert!(output.status.success(), "Docker port inspection failed");
        let binding = String::from_utf8(output.stdout).expect("Docker port is UTF-8");
        let port = binding
            .lines()
            .next()
            .and_then(|line| line.rsplit_once(':'))
            .expect("Docker published the Native port")
            .1
            .parse::<u16>()
            .expect("valid port");
        fixture.endpoint = format!("tcp://127.0.0.1:{port}");
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            let config = ClientConfig::parse(&fixture.endpoint)
                .expect("live endpoint")
                .with_password("streaming-test")
                .with_timeout(Duration::from_millis(500));
            if let Ok(mut client) = Client::new(config)
                && client.ping().is_ok()
            {
                break;
            }
            assert!(Instant::now() < deadline, "ClickHouse did not become ready");
            thread::sleep(Duration::from_millis(100));
        }
        fixture
    }

    pub fn client(&self, compression: CompressionMethod) -> Client {
        Client::new(
            ClientConfig::parse(&self.endpoint)
                .expect("live endpoint")
                .with_password("streaming-test")
                .with_compression(compression),
        )
        .expect("live ClickHouse connection")
    }

    pub fn terminate(&mut self) {
        let status = Command::new("docker")
            .args(["rm", "--force", &self.id])
            .status()
            .expect("Docker termination is available");
        assert!(status.success(), "cannot terminate test server");
        self.live = false;
    }
}

impl Drop for TestClickHouseContainer {
    fn drop(&mut self) {
        if !self.live {
            return;
        }
        match Command::new("docker")
            .args(["rm", "--force", &self.id])
            .output()
        {
            Ok(output) if output.status.success() => {}
            Ok(output) => eprintln!(
                "cannot remove test container: {}",
                String::from_utf8_lossy(&output.stderr)
            ),
            Err(error) => eprintln!("cannot invoke Docker cleanup: {error}"),
        }
    }
}
