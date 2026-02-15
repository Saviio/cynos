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

criterion_group!(
    benches,
    btree_insert_benchmark,
    btree_get_benchmark,
    btree_range_benchmark,
    hash_insert_benchmark,
    hash_get_benchmark,
    btree_vs_hash_point_query,
);

criterion_main!(benches);
