//! Fixed-iteration wall-clock measurements without a statistics dependency.
use std::{hint::black_box, time::Instant};

pub(super) fn measure<T>(name: &str, elements: usize, mut operation: impl FnMut() -> T) {
    let smoke = std::env::args().any(|arg| arg == "--test");
    let iterations = if smoke {
        1
    } else {
        std::env::var("CLICKHOUSE_BENCH_ITERATIONS").map_or(100, |value| {
            value.parse::<u32>().expect("positive iteration count")
        })
    };
    assert!(iterations > 0, "iteration count must be positive");
    black_box(operation());
    let start = Instant::now();
    for _ in 0..iterations {
        black_box(operation());
    }
    let seconds = start.elapsed().as_secs_f64();
    let result = sonic_rs::json!({
        "benchmark": name,
        "iterations": iterations,
        "ns_per_iteration": seconds * 1e9 / f64::from(iterations),
        "elements_per_second": (seconds > 0.0).then(|| elements as f64 * f64::from(iterations) / seconds),
    });
    println!(
        "{}",
        sonic_rs::to_string(&result).expect("benchmark metrics are serializable")
    );
}
