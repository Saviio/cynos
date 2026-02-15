//! Benchmarks for cynos-incremental module.
//!
//! Target: single row incremental update < 100μs

use cynos_core::{Row, Value};
use cynos_incremental::{
    filter_incremental, map_incremental, Delta, IncrementalAvg, IncrementalCount,
    IncrementalHashJoin, IncrementalSum, MaterializedView, DataflowNode,
};
use criterion::{black_box, criterion_group, criterion_main, Criterion, BenchmarkId};

fn make_row(id: u64, age: i64) -> Row {
    Row::new(id, vec![Value::Int64(id as i64), Value::Int64(age)])
}

fn bench_delta_operations(c: &mut Criterion) {
    let mut group = c.benchmark_group("delta");

    // Single insert delta creation
    group.bench_function("create_insert", |b| {
        b.iter(|| Delta::insert(black_box(42i64)))
    });

    // Single delete delta creation
    group.bench_function("create_delete", |b| {
        b.iter(|| Delta::delete(black_box(42i64)))
    });

    group.finish();
}

fn bench_filter_incremental(c: &mut Criterion) {
    let mut group = c.benchmark_group("filter");

    for size in [1, 10, 100, 1000] {
        let deltas: Vec<Delta<i64>> = (0..size).map(|i| Delta::insert(i)).collect();

        group.bench_with_input(
            BenchmarkId::new("filter_gt_50", size),
            &deltas,
            |b, deltas| {
                b.iter(|| filter_incremental(black_box(deltas), |&x| x > 50))
            },
        );
    }

    group.finish();
}

fn bench_map_incremental(c: &mut Criterion) {
    let mut group = c.benchmark_group("map");

    for size in [1, 10, 100, 1000] {
        let deltas: Vec<Delta<i64>> = (0..size).map(|i| Delta::insert(i)).collect();

        group.bench_with_input(
            BenchmarkId::new("map_double", size),
            &deltas,
            |b, deltas| {
                b.iter(|| map_incremental(black_box(deltas), |&x| x * 2))
            },
        );
    }

    group.finish();
}

fn bench_incremental_count(c: &mut Criterion) {
    let mut group = c.benchmark_group("aggregate/count");

    // Single row insert
    group.bench_function("single_insert", |b| {
        let mut count = IncrementalCount::new();
        let delta = [Delta::insert(1i64)];
        b.iter(|| {
            count.apply(black_box(&delta));
        })
    });

    // Batch insert
    for size in [10, 100, 1000] {
        let deltas: Vec<Delta<i64>> = (0..size).map(|i| Delta::insert(i)).collect();
        group.bench_with_input(
            BenchmarkId::new("batch_insert", size),
            &deltas,
            |b, deltas| {
                let mut count = IncrementalCount::new();
                b.iter(|| count.apply(black_box(deltas)))
            },
        );
    }

    group.finish();
}

fn bench_incremental_sum(c: &mut Criterion) {
    let mut group = c.benchmark_group("aggregate/sum");

    // Single row insert
    group.bench_function("single_insert", |b| {
        let mut sum = IncrementalSum::new(0);
        let delta = [Delta::insert(make_row(1, 100))];
        b.iter(|| {
            sum.apply(black_box(&delta));
        })
    });

    // Batch insert
    for size in [10, 100, 1000] {
        let deltas: Vec<Delta<Row>> = (0..size).map(|i| Delta::insert(make_row(i, i as i64 * 10))).collect();
        group.bench_with_input(
            BenchmarkId::new("batch_insert", size),
            &deltas,
            |b, deltas| {
                let mut sum = IncrementalSum::new(0);
                b.iter(|| sum.apply(black_box(deltas)))
            },
        );
    }

    group.finish();
}

fn bench_incremental_avg(c: &mut Criterion) {
    let mut group = c.benchmark_group("aggregate/avg");

    // Single row insert
    group.bench_function("single_insert", |b| {
        let mut avg = IncrementalAvg::new(0);
        let delta = [Delta::insert(make_row(1, 100))];
        b.iter(|| {
            avg.apply(black_box(&delta));
        })
    });

    group.finish();
}

fn bench_incremental_join(c: &mut Criterion) {
    let mut group = c.benchmark_group("join");

    // Setup: pre-populate right side with departments
    fn setup_join() -> IncrementalHashJoin<i64, Row, Row> {
        let mut join = IncrementalHashJoin::new(
            |r: &Row| r.get(1).and_then(|v| v.as_i64()).unwrap_or(0), // dept_id
            |r: &Row| r.get(0).and_then(|v| v.as_i64()).unwrap_or(0), // id
        );
        // Add 10 departments
        for i in 0..10 {
            let dept = Row::new(i as u64, vec![Value::Int64(i), Value::String(format!("Dept{}", i).into())]);
            join.on_right_insert(dept);
        }
        join
    }

    // Single left insert (employee joining department)
    group.bench_function("single_left_insert", |b| {
        let mut join = setup_join();
        let emp = Row::new(100, vec![Value::Int64(100), Value::Int64(5)]); // emp with dept_id=5
        b.iter(|| {
            join.on_left_insert(black_box(emp.clone()))
        })
    });

    // Batch left inserts
    for size in [10, 100] {
        let employees: Vec<Row> = (0..size)
            .map(|i| Row::new(i as u64, vec![Value::Int64(i as i64), Value::Int64(i as i64 % 10)]))
            .collect();

        group.bench_with_input(
            BenchmarkId::new("batch_left_insert", size),
            &employees,
            |b, employees| {
                let mut join = setup_join();
                b.iter(|| {
                    for emp in employees {
                        black_box(join.on_left_insert(emp.clone()));
                    }
                })
            },
        );
    }

    group.finish();
}

fn bench_materialized_view(c: &mut Criterion) {
    let mut group = c.benchmark_group("materialized_view");

    // Simple source view - single insert propagation
    group.bench_function("source_single_insert", |b| {
        let dataflow = DataflowNode::source(1);
        let mut view = MaterializedView::new(dataflow);
        let deltas = vec![Delta::insert(make_row(1, 25))];

        b.iter(|| {
            view.on_table_change(1, black_box(deltas.clone()))
        })
    });

    // Filter view - single insert propagation
    group.bench_function("filter_single_insert", |b| {
        let dataflow = DataflowNode::filter(
            DataflowNode::source(1),
            |row| row.get(1).and_then(|v| v.as_i64()).map(|age| age > 18).unwrap_or(false)
        );
        let mut view = MaterializedView::new(dataflow);
        let deltas = vec![Delta::insert(make_row(1, 25))];

        b.iter(|| {
            view.on_table_change(1, black_box(deltas.clone()))
        })
    });

    // Batch propagation through filter
    for size in [10, 100, 1000] {
        let deltas: Vec<Delta<Row>> = (0..size)
            .map(|i| Delta::insert(make_row(i, (i % 50) as i64)))
            .collect();

        group.bench_with_input(
            BenchmarkId::new("filter_batch", size),
            &deltas,
            |b, deltas| {
                let dataflow = DataflowNode::filter(
                    DataflowNode::source(1),
                    |row| row.get(1).and_then(|v| v.as_i64()).map(|age| age > 25).unwrap_or(false)
                );
                let mut view = MaterializedView::new(dataflow);
                b.iter(|| view.on_table_change(1, black_box(deltas.clone())))
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_delta_operations,
    bench_filter_incremental,
    bench_map_incremental,
    bench_incremental_count,
    bench_incremental_sum,
    bench_incremental_avg,
    bench_incremental_join,
    bench_materialized_view,
);

criterion_main!(benches);
