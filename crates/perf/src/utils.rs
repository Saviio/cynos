//! Utility functions for benchmarking

use std::time::{Duration, Instant};

/// Number of iterations for each benchmark
pub const ITERATIONS: usize = 100;

/// Warmup iterations before measurement
pub const WARMUP_ITERATIONS: usize = 10;

/// Data sizes to test
pub const SIZES: [usize; 4] = [100, 1_000, 10_000, 100_000];

/// Smaller sizes for expensive operations
pub const SMALL_SIZES: [usize; 3] = [100, 1_000, 10_000];

/// Measure execution time with multiple iterations (includes warmup)
pub fn measure<F, R>(iterations: usize, mut f: F) -> BenchResult
where
    F: FnMut() -> R,
{
    // Warmup phase - exclude from measurements
    for _ in 0..WARMUP_ITERATIONS {
        std::hint::black_box(f());
    }

    let mut times = Vec::with_capacity(iterations);

    for _ in 0..iterations {
        let start = Instant::now();
        std::hint::black_box(f());
        times.push(start.elapsed());
    }

    BenchResult::from_times(&times)
}

/// Measure with setup function (setup time excluded, includes warmup)
pub fn measure_with_setup<S, F, T, R>(iterations: usize, mut setup: S, mut f: F) -> BenchResult
where
    S: FnMut() -> T,
    F: FnMut(T) -> R,
{
    // Warmup phase
    for _ in 0..WARMUP_ITERATIONS {
        let data = setup();
        std::hint::black_box(f(data));
    }

    let mut times = Vec::with_capacity(iterations);

    for _ in 0..iterations {
        let data = setup();
        let start = Instant::now();
        std::hint::black_box(f(data));
        times.push(start.elapsed());
    }

    BenchResult::from_times(&times)
}

#[derive(Clone)]
#[allow(dead_code)]
pub struct BenchResult {
    pub min: Duration,
    pub max: Duration,
    pub mean: Duration,
    pub median: Duration,
    pub std_dev: Duration,
    pub iterations: usize,
}

impl BenchResult {
    pub fn from_times(times: &[Duration]) -> Self {
        let mut sorted: Vec<_> = times.to_vec();
        sorted.sort();

        let min = *sorted.first().unwrap();
        let max = *sorted.last().unwrap();
        let sum: Duration = sorted.iter().sum();
        let mean = sum / sorted.len() as u32;
        let median = sorted[sorted.len() / 2];

        // Calculate standard deviation
        let mean_nanos = mean.as_nanos() as f64;
        let variance: f64 = sorted
            .iter()
            .map(|t| {
                let diff = t.as_nanos() as f64 - mean_nanos;
                diff * diff
            })
            .sum::<f64>()
            / sorted.len() as f64;
        let std_dev = Duration::from_nanos(variance.sqrt() as u64);

        Self {
            min,
            max,
            mean,
            median,
            std_dev,
            iterations: times.len(),
        }
    }

    #[allow(dead_code)]
    pub fn mean_ms(&self) -> f64 {
        self.mean.as_secs_f64() * 1000.0
    }

    pub fn mean_us(&self) -> f64 {
        self.mean.as_secs_f64() * 1_000_000.0
    }

    pub fn throughput(&self, count: usize) -> f64 {
        count as f64 / self.mean.as_secs_f64()
    }
}

/// Format duration for display
pub fn format_duration(d: Duration) -> String {
    let nanos = d.as_nanos();
    if nanos < 1_000 {
        format!("{} ns", nanos)
    } else if nanos < 1_000_000 {
        format!("{:.2} μs", nanos as f64 / 1_000.0)
    } else if nanos < 1_000_000_000 {
        format!("{:.2} ms", nanos as f64 / 1_000_000.0)
    } else {
        format!("{:.2} s", nanos as f64 / 1_000_000_000.0)
    }
}

/// Format throughput for display
pub fn format_throughput(ops_per_sec: f64) -> String {
    if ops_per_sec >= 1_000_000.0 {
        format!("{:.2}M ops/s", ops_per_sec / 1_000_000.0)
    } else if ops_per_sec >= 1_000.0 {
        format!("{:.2}K ops/s", ops_per_sec / 1_000.0)
    } else {
        format!("{:.2} ops/s", ops_per_sec)
    }
}

/// Simple LCG for reproducible pseudo-random shuffling
pub fn shuffle_indices(count: usize, seed: u64) -> Vec<usize> {
    let mut indices: Vec<usize> = (0..count).collect();
    let mut s = seed;
    for i in (1..count).rev() {
        s = s.wrapping_mul(6364136223846793005).wrapping_add(1);
        let j = (s as usize) % (i + 1);
        indices.swap(i, j);
    }
    indices
}
