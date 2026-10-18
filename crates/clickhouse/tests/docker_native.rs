//! Live Native TCP tracer bullet against an isolated ClickHouse container.

use std::{
    process::Command,
    thread,
    time::{Duration, Instant},
};

use clickhouse::{Block, Client, Endpoint, NativeBlock};

const IMAGE: &str = "clickhouse/clickhouse-server:26.8.2.7";

/// RAII fixture that removes its container on drop.
struct TestClickHouseContainer {
    id: String,
    tcp_port: u16,
}

impl TestClickHouseContainer {
    /// Start the pinned image on ephemeral host ports.
    fn start() -> Option<Self> {
        let probe = Command::new("docker").arg("info").output().ok()?;
        if !probe.status.success() {
            return None;
        }
        let output = Command::new("docker")
            .args([
                "run",
                "-d",
                "--rm",
                "-e",
                "CLICKHOUSE_USER=default",
                "-e",
                "CLICKHOUSE_PASSWORD=chcxx",
                "-p",
                "0:9000",
                "-p",
                "0:8123",
                IMAGE,
            ])
            .output()
            .ok()?;
        if !output.status.success() {
            return None;
        }
        let id = String::from_utf8_lossy(&output.stdout).trim().to_string();
        if id.is_empty() {
            return None;
        }
        let mut container = Self { id, tcp_port: 0 };
        container.tcp_port = container.wait_for_port("9000/tcp")?;
        Some(container)
    }

    /// TCP port exposed on the host.
    fn tcp_port(&self) -> u16 {
        self.tcp_port
    }

    fn wait_for_port(&self, target: &str) -> Option<u16> {
        let deadline = Instant::now() + Duration::from_secs(120);
        while Instant::now() < deadline {
            if let Ok(output) = Command::new("docker")
                .args(["port", &self.id, target])
                .output()
                && output.status.success()
            {
                let text = String::from_utf8_lossy(&output.stdout);
                if let Some(port) = parse_port(&text) {
                    return Some(port);
                }
            }
            thread::sleep(Duration::from_millis(250));
        }
        None
    }
}

impl Drop for TestClickHouseContainer {
    fn drop(&mut self) {
        drop(Command::new("docker").args(["rm", "-f", &self.id]).output());
    }
}

fn parse_port(text: &str) -> Option<u16> {
    text.lines()
        .find_map(|line| line.rsplit(':').next()?.trim().parse().ok())
}

fn connect_ready(port: u16) -> Client {
    let deadline = Instant::now() + Duration::from_secs(120);
    loop {
        let endpoint = Endpoint::parse(format!("tcp://127.0.0.1:{port}")).expect("endpoint");
        if let Ok(mut client) = Client::connect(endpoint, "default", "chcxx", "default")
            && client.ping().is_ok()
        {
            return client;
        }
        assert!(
            Instant::now() < deadline,
            "ClickHouse did not become ready on port {port}"
        );
        thread::sleep(Duration::from_millis(250));
    }
}

#[test]
fn native_tcp_tracer_bullet() {
    let Some(container) = TestClickHouseContainer::start() else {
        eprintln!("docker is unavailable; skipping the live Native TCP test");
        return;
    };
    let mut client = connect_ready(container.tcp_port());
    client
        .execute(
            "CREATE TABLE IF NOT EXISTS chcxx_t01 (id UInt64, delta Int64, price Float64, symbol \
             String) ENGINE = Memory",
        )
        .expect("create table");

    let mut block = NativeBlock::new();
    block.push_u64("id", &[1, 2, 3]).expect("push column");
    block.push_i64("delta", &[-5, 0, 5]).expect("push column");
    block
        .push_f64("price", &[10.5, 11.25, 12.75])
        .expect("push column");
    let symbols = vec!["alpha".to_string(), "beta".to_string(), "gamma".to_string()];
    block.push_string("symbol", &symbols).expect("push column");

    let capacity = block.column_capacity("id").expect("capacity");
    block.clear();
    assert_eq!(block.rows(), 0);
    assert_eq!(block.column_capacity("id").expect("capacity"), capacity);
    block.push_u64("id", &[1, 2, 3]).expect("push column");
    block.push_i64("delta", &[-5, 0, 5]).expect("push column");
    block
        .push_f64("price", &[10.5, 11.25, 12.75])
        .expect("push column");
    block.push_string("symbol", &symbols).expect("push column");

    client
        .insert_block("chcxx_t01", &Block::from_native(&block).unwrap())
        .expect("insert block");

    let blocks = client
        .query_blocks("SELECT id, delta, price, symbol FROM chcxx_t01 ORDER BY id")
        .expect("select blocks");
    let mut ids = Vec::new();
    let mut deltas = Vec::new();
    let mut prices = Vec::new();
    let mut names = Vec::new();
    for returned in &blocks {
        if returned.row_count() == 0 {
            continue;
        }
        ids.extend_from_slice(returned.column_slice_by_name::<u64>("id").expect("id"));
        deltas.extend_from_slice(
            returned
                .column_slice_by_name::<i64>("delta")
                .expect("delta"),
        );
        prices.extend_from_slice(
            returned
                .column_slice_by_name::<f64>("price")
                .expect("price"),
        );
        let symbol = returned.column_index("symbol").expect("symbol column");
        for row in 0..returned.row_count() {
            names.push(returned.column_str(symbol, row).expect("symbol").to_owned());
        }
    }
    assert_eq!(ids, [1, 2, 3]);
    assert_eq!(deltas, [-5, 0, 5]);
    assert_eq!(prices, [10.5, 11.25, 12.75]);
    assert_eq!(names, symbols);

    client.execute("DROP TABLE chcxx_t01").expect("drop table");
}
