//! Benchmarks for GIN index multi-term lookups.

use alloc::collections::BTreeSet;
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use cynos_index::GinIndex;

extern crate alloc;

fn build_skewed_gin_index(row_count: u64) -> GinIndex {
    let mut gin = GinIndex::new();

    for row_id in 1..=row_count {
        gin.add_key_value("scope".into(), "all".into(), row_id);

        if row_id % 2 == 0 {
            gin.add_key_value("status".into(), "active".into(), row_id);
        }

        if row_id % 100 == 0 {
            gin.add_key_value("tenant".into(), "small".into(), row_id);
        }

        if row_id % 250 == 0 {
            gin.add_key_value("region".into(), "cn".into(), row_id);
        }
    }

    gin
}

fn legacy_get_by_key_values_all(gin: &GinIndex, pairs: &[(&str, &str)]) -> Vec<u64> {
    if pairs.is_empty() {
        return Vec::new();
    }

    let mut iter = pairs.iter();
    let Some(&(first_key, first_value)) = iter.next() else {
        return Vec::new();
    };

    let mut result: BTreeSet<u64> = gin
        .get_by_key_value(first_key, first_value)
        .into_iter()
        .collect();

    for &(key, value) in iter {
        if result.is_empty() {
            break;
        }

        let matches: BTreeSet<u64> = gin.get_by_key_value(key, value).into_iter().collect();
        result = result.intersection(&matches).copied().collect();
    }

    result.into_iter().collect()
}

fn gin_multi_lookup_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("gin_multi_lookup");

    for row_count in [10_000u64, 100_000u64].iter() {
        let gin = build_skewed_gin_index(*row_count);
        let worst_case_pairs = [
            ("scope", "all"),
            ("status", "active"),
            ("tenant", "small"),
            ("region", "cn"),
        ];

        group.bench_with_input(BenchmarkId::new("legacy", row_count), row_count, |b, _| {
            b.iter(|| black_box(legacy_get_by_key_values_all(&gin, &worst_case_pairs)));
        });

        group.bench_with_input(
            BenchmarkId::new("optimized", row_count),
            row_count,
            |b, _| {
                b.iter(|| black_box(gin.get_by_key_values_all(&worst_case_pairs)));
            },
        );
    }

    group.finish();
}

criterion_group!(benches, gin_multi_lookup_benchmark);
criterion_main!(benches);
