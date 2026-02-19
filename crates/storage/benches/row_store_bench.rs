//! Benchmarks for RowStore delete operations.

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use cynos_core::schema::TableBuilder;
use cynos_core::{DataType, Row, Value};
use cynos_storage::RowStore;

fn create_test_schema_with_indices() -> cynos_core::schema::Table {
    TableBuilder::new("test")
        .unwrap()
        .add_column("id", DataType::Int64)
        .unwrap()
        .add_column("price", DataType::Float64)
        .unwrap()
        .add_column("symbol", DataType::String)
        .unwrap()
        .add_column("sector", DataType::String)
        .unwrap()
        .add_primary_key(&["id"], false)
        .unwrap()
        .add_index("idx_price", &["price"], false)
        .unwrap()
        .add_index("idx_symbol", &["symbol"], false)
        .unwrap()
        .add_index("idx_sector", &["sector"], false)
        .unwrap()
        .build()
        .unwrap()
}

fn populate_store(store: &mut RowStore, count: u64) {
    let sectors = ["Tech", "Finance", "Health", "Energy", "Consumer"];
    for i in 1..=count {
        let row = Row::new(
            i,
            vec![
                Value::Int64(i as i64),
                Value::Float64(100.0 + (i as f64) * 0.1),
                Value::String(format!("SYM{}", i)),
                Value::String(sectors[(i as usize) % sectors.len()].into()),
            ],
        );
        store.insert(row).unwrap();
    }
}

/// Benchmark: individual delete() calls vs delete_batch()
fn row_store_delete_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("row_store_delete");

    // Test with different delete counts
    for delete_count in [100u64, 1000, 10000].iter() {
        let total_rows = 100000u64;

        // Benchmark individual deletes
        group.bench_with_input(
            BenchmarkId::new("individual", delete_count),
            delete_count,
            |b, &delete_count| {
                b.iter_batched(
                    || {
                        let mut store = RowStore::new(create_test_schema_with_indices());
                        populate_store(&mut store, total_rows);
                        store
                    },
                    |mut store| {
                        // Delete rows one by one
                        for i in 1..=delete_count {
                            let _ = store.delete(i);
                        }
                        black_box(store)
                    },
                    criterion::BatchSize::SmallInput,
                );
            },
        );

        // Benchmark batch delete
        group.bench_with_input(
            BenchmarkId::new("batch", delete_count),
            delete_count,
            |b, &delete_count| {
                b.iter_batched(
                    || {
                        let mut store = RowStore::new(create_test_schema_with_indices());
                        populate_store(&mut store, total_rows);
                        let row_ids: Vec<u64> = (1..=delete_count).collect();
                        (store, row_ids)
                    },
                    |(mut store, row_ids)| {
                        // Delete rows in batch
                        store.delete_batch(&row_ids);
                        black_box(store)
                    },
                    criterion::BatchSize::SmallInput,
                );
            },
        );
    }

    group.finish();
}

/// Benchmark: delete all rows (simulating DELETE without WHERE)
fn row_store_delete_all_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("row_store_delete_all");

    for total_rows in [1000u64, 10000, 50000].iter() {
        // Benchmark individual deletes (old approach)
        group.bench_with_input(
            BenchmarkId::new("individual", total_rows),
            total_rows,
            |b, &total_rows| {
                b.iter_batched(
                    || {
                        let mut store = RowStore::new(create_test_schema_with_indices());
                        populate_store(&mut store, total_rows);
                        let row_ids: Vec<u64> = (1..=total_rows).collect();
                        (store, row_ids)
                    },
                    |(mut store, row_ids)| {
                        for id in row_ids {
                            let _ = store.delete(id);
                        }
                        black_box(store)
                    },
                    criterion::BatchSize::SmallInput,
                );
            },
        );

        // Benchmark clear() (new optimized approach for DELETE without WHERE)
        group.bench_with_input(
            BenchmarkId::new("clear", total_rows),
            total_rows,
            |b, &total_rows| {
                b.iter_batched(
                    || {
                        let mut store = RowStore::new(create_test_schema_with_indices());
                        populate_store(&mut store, total_rows);
                        store
                    },
                    |mut store| {
                        store.clear();
                        black_box(store)
                    },
                    criterion::BatchSize::SmallInput,
                );
            },
        );

        // Benchmark batch delete
        group.bench_with_input(
            BenchmarkId::new("batch", total_rows),
            total_rows,
            |b, &total_rows| {
                b.iter_batched(
                    || {
                        let mut store = RowStore::new(create_test_schema_with_indices());
                        populate_store(&mut store, total_rows);
                        let row_ids: Vec<u64> = (1..=total_rows).collect();
                        (store, row_ids)
                    },
                    |(mut store, row_ids)| {
                        store.delete_batch(&row_ids);
                        black_box(store)
                    },
                    criterion::BatchSize::SmallInput,
                );
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    row_store_delete_benchmark,
    row_store_delete_all_benchmark,
);

criterion_main!(benches);
