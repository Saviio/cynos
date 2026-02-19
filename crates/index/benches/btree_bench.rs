//! Benchmarks for cynos-index using criterion.

use cynos_index::{BTreeIndex, HashIndex, Index, KeyRange, RangeIndex};
use criterion::{black_box, criterion_group, criterion_main, Criterion, BenchmarkId};

fn btree_insert_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("btree_insert");

    for size in [100, 1000, 10000].iter() {
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            b.iter(|| {
                let mut tree = BTreeIndex::new(64, false);
                for i in 0..size {
                    tree.add(i, i as u64).unwrap();
                }
                black_box(tree)
            });
        });
    }

    group.finish();
}

fn btree_get_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("btree_get");

    for size in [100, 1000, 10000].iter() {
        // Pre-populate the tree
        let mut tree = BTreeIndex::new(64, true);
        for i in 0..*size {
            tree.add(i, i as u64).unwrap();
        }

        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            b.iter(|| {
                // Query random keys
                for i in (0..100).map(|x| x * size / 100) {
                    black_box(tree.get(&i));
                }
            });
        });
    }

    group.finish();
}

fn btree_range_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("btree_range");

    // Pre-populate a large tree
    let mut tree = BTreeIndex::new(64, true);
    for i in 0..100000i64 {
        tree.add(i, i as u64).unwrap();
    }

    for range_size in [100, 1000, 10000].iter() {
        group.bench_with_input(
            BenchmarkId::from_parameter(range_size),
            range_size,
            |b, &range_size| {
                let range = KeyRange::bound(1000i64, 1000 + range_size as i64, false, false);
                b.iter(|| {
                    let results = tree.get_range(Some(&range), false, None, 0);
                    black_box(results)
                });
            },
        );
    }

    group.finish();
}

fn hash_insert_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("hash_insert");

    for size in [100, 1000, 10000].iter() {
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            b.iter(|| {
                let mut index = HashIndex::new(false);
                for i in 0..size {
                    index.add(i, i as u64).unwrap();
                }
                black_box(index)
            });
        });
    }

    group.finish();
}

fn hash_get_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("hash_get");

    for size in [100, 1000, 10000].iter() {
        // Pre-populate the index
        let mut index = HashIndex::new(true);
        for i in 0..*size {
            index.add(i, i as u64).unwrap();
        }

        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            b.iter(|| {
                // Query random keys
                for i in (0..100).map(|x| x * size / 100) {
                    black_box(index.get(&i));
                }
            });
        });
    }

    group.finish();
}

fn btree_vs_hash_point_query(c: &mut Criterion) {
    let mut group = c.benchmark_group("point_query_comparison");

    let size = 10000i64;

    // Pre-populate both indexes
    let mut btree = BTreeIndex::new(64, true);
    let mut hash = HashIndex::new(true);
    for i in 0..size {
        btree.add(i, i as u64).unwrap();
        hash.add(i, i as u64).unwrap();
    }

    group.bench_function("btree", |b| {
        b.iter(|| {
            for i in (0..100).map(|x| x * size / 100) {
                black_box(btree.get(&i));
            }
        });
    });

    group.bench_function("hash", |b| {
        b.iter(|| {
            for i in (0..100).map(|x| x * size / 100) {
                black_box(hash.get(&i));
            }
        });
    });

    group.finish();
}

/// Benchmark: individual remove() calls vs remove_batch()
fn btree_remove_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("btree_remove");

    for delete_count in [100, 1000, 10000].iter() {
        let size = 100000i64;

        // Benchmark individual removes
        group.bench_with_input(
            BenchmarkId::new("individual", delete_count),
            delete_count,
            |b, &delete_count| {
                b.iter_batched(
                    || {
                        // Setup: create and populate tree
                        let mut tree = BTreeIndex::new(64, true);
                        for i in 0..size {
                            tree.add(i, i as u64).unwrap();
                        }
                        tree
                    },
                    |mut tree| {
                        // Benchmark: remove entries one by one
                        for i in 0..delete_count as i64 {
                            tree.remove(&i, Some(i as u64));
                        }
                        black_box(tree)
                    },
                    criterion::BatchSize::SmallInput,
                );
            },
        );

        // Benchmark batch remove
        group.bench_with_input(
            BenchmarkId::new("batch", delete_count),
            delete_count,
            |b, &delete_count| {
                b.iter_batched(
                    || {
                        // Setup: create and populate tree
                        let mut tree = BTreeIndex::new(64, true);
                        for i in 0..size {
                            tree.add(i, i as u64).unwrap();
                        }
                        // Prepare entries to remove
                        let entries: Vec<(i64, u64)> = (0..delete_count as i64)
                            .map(|i| (i, i as u64))
                            .collect();
                        (tree, entries)
                    },
                    |(mut tree, entries)| {
                        // Benchmark: remove entries in batch
                        tree.remove_batch(&entries);
                        black_box(tree)
                    },
                    criterion::BatchSize::SmallInput,
                );
            },
        );
    }

    group.finish();
}

/// Benchmark: remove_batch with non-unique index (multiple values per key)
fn btree_remove_batch_non_unique(c: &mut Criterion) {
    let mut group = c.benchmark_group("btree_remove_batch_non_unique");

    let key_count = 10000i64;
    let values_per_key = 10;

    // Benchmark individual removes
    group.bench_function("individual", |b| {
        b.iter_batched(
            || {
                let mut tree = BTreeIndex::new(64, false);
                for k in 0..key_count {
                    for v in 0..values_per_key {
                        tree.add(k, (k * values_per_key + v) as u64).unwrap();
                    }
                }
                tree
            },
            |mut tree| {
                // Remove half of the values
                for k in 0..key_count {
                    for v in 0..(values_per_key / 2) {
                        tree.remove(&k, Some((k * values_per_key + v) as u64));
                    }
                }
                black_box(tree)
            },
            criterion::BatchSize::SmallInput,
        );
    });

    // Benchmark batch remove
    group.bench_function("batch", |b| {
        b.iter_batched(
            || {
                let mut tree = BTreeIndex::new(64, false);
                for k in 0..key_count {
                    for v in 0..values_per_key {
                        tree.add(k, (k * values_per_key + v) as u64).unwrap();
                    }
                }
                let entries: Vec<(i64, u64)> = (0..key_count)
                    .flat_map(|k| {
                        (0..(values_per_key / 2)).map(move |v| (k, (k * values_per_key + v) as u64))
                    })
                    .collect();
                (tree, entries)
            },
            |(mut tree, entries)| {
                tree.remove_batch(&entries);
                black_box(tree)
            },
            criterion::BatchSize::SmallInput,
        );
    });

    group.finish();
}

criterion_group!(
    benches,
    btree_insert_benchmark,
    btree_get_benchmark,
    btree_range_benchmark,
    hash_insert_benchmark,
    hash_get_benchmark,
    btree_vs_hash_point_query,
    btree_remove_benchmark,
    btree_remove_batch_non_unique,
);

criterion_main!(benches);
