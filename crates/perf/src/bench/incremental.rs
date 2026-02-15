//! Incremental computation performance benchmarks

use crate::report::Report;
use crate::utils::*;
use cynos_core::{Row, Value};
use cynos_incremental::{
    filter_incremental, map_incremental, DataflowNode, Delta, IncrementalAvg, IncrementalCount,
    IncrementalHashJoin, IncrementalSum, MaterializedView,
};

pub fn run(report: &mut Report) {
    delta_operations(report);
    filter_incremental_bench(report);
    map_incremental_bench(report);
    aggregates(report);
    incremental_join(report);
    materialized_view(report);
}

fn make_row(id: u64, age: i64) -> Row {
    Row::new(id, vec![Value::Int64(id as i64), Value::Int64(age)])
}

fn delta_operations(report: &mut Report) {
    println!("  Delta Operations:");

    // Single insert
    let result = measure(ITERATIONS * 100, || Delta::insert(42i64));
    println!(
        "    create_insert:  {:>10}",
        format_duration(result.mean)
    );
    report.add_result("Incremental/Delta", "create_insert", None, result, None);

    // Single delete
    let result = measure(ITERATIONS * 100, || Delta::delete(42i64));
    println!(
        "    create_delete:  {:>10}",
        format_duration(result.mean)
    );
    report.add_result("Incremental/Delta", "create_delete", None, result, None);
}

fn filter_incremental_bench(report: &mut Report) {
    println!("  Filter Incremental:");
    for &size in &[1usize, 10, 100, 1000] {
        let deltas: Vec<Delta<i64>> = (0..size as i64).map(|i| Delta::insert(i)).collect();

        let result = measure(ITERATIONS, || {
            filter_incremental(&deltas, |&x| x > 50)
        });

        let throughput = result.throughput(size);
        println!(
            "    {:>7} deltas: {:>10} ({:>12})",
            size,
            format_duration(result.mean),
            format_throughput(throughput)
        );
        report.add_result("Incremental", "filter", Some(size), result, Some(throughput));
    }
}

fn map_incremental_bench(report: &mut Report) {
    println!("  Map Incremental:");
    for &size in &[1usize, 10, 100, 1000] {
        let deltas: Vec<Delta<i64>> = (0..size as i64).map(|i| Delta::insert(i)).collect();

        let result = measure(ITERATIONS, || {
            map_incremental(&deltas, |&x| x * 2)
        });

        let throughput = result.throughput(size);
        println!(
            "    {:>7} deltas: {:>10} ({:>12})",
            size,
            format_duration(result.mean),
            format_throughput(throughput)
        );
        report.add_result("Incremental", "map", Some(size), result, Some(throughput));
    }
}

fn aggregates(report: &mut Report) {
    println!("  Incremental Aggregates:");

    // Count - single insert
    let result = measure(ITERATIONS * 100, || {
        let mut count = IncrementalCount::new();
        let delta = [Delta::insert(1i64)];
        count.apply(&delta);
        count.get()
    });
    // Target: < 100μs for single row update
    let passed = result.mean_us() < 100.0;
    println!(
        "    count (single):  {:>10} [target: <100μs] {}",
        format_duration(result.mean),
        if passed { "✓" } else { "✗" }
    );
    report.add_with_target(
        "Incremental/Aggregate",
        "count_single",
        None,
        result,
        None,
        "<100μs",
        passed,
    );

    // Sum - single insert
    let result = measure(ITERATIONS * 100, || {
        let mut sum = IncrementalSum::new(0);
        let delta = [Delta::insert(make_row(1, 100))];
        sum.apply(&delta);
        sum.get()
    });
    let passed = result.mean_us() < 100.0;
    println!(
        "    sum (single):    {:>10} [target: <100μs] {}",
        format_duration(result.mean),
        if passed { "✓" } else { "✗" }
    );
    report.add_with_target(
        "Incremental/Aggregate",
        "sum_single",
        None,
        result,
        None,
        "<100μs",
        passed,
    );

    // Avg - single insert
    let result = measure(ITERATIONS * 100, || {
        let mut avg = IncrementalAvg::new(0);
        let delta = [Delta::insert(make_row(1, 100))];
        avg.apply(&delta);
        avg.get()
    });
    let passed = result.mean_us() < 100.0;
    println!(
        "    avg (single):    {:>10} [target: <100μs] {}",
        format_duration(result.mean),
        if passed { "✓" } else { "✗" }
    );
    report.add_with_target(
        "Incremental/Aggregate",
        "avg_single",
        None,
        result,
        None,
        "<100μs",
        passed,
    );

    // Batch aggregates
    println!("  Incremental Aggregates (batch):");
    for &size in &[10, 100, 1000] {
        let deltas: Vec<Delta<Row>> = (0..size)
            .map(|i| Delta::insert(make_row(i as u64, i as i64 * 10)))
            .collect();

        let result = measure(ITERATIONS, || {
            let mut sum = IncrementalSum::new(0);
            sum.apply(&deltas);
            sum.get()
        });

        let throughput = result.throughput(size);
        println!(
            "    {:>7} deltas: {:>10} ({:>12})",
            size,
            format_duration(result.mean),
            format_throughput(throughput)
        );
        report.add_result("Incremental/Aggregate", "sum_batch", Some(size), result, Some(throughput));
    }
}

fn incremental_join(report: &mut Report) {
    println!("  Incremental Hash Join:");

    fn setup_join() -> IncrementalHashJoin<i64, Row, Row> {
        let mut join = IncrementalHashJoin::new(
            |r: &Row| r.get(1).and_then(|v| v.as_i64()).unwrap_or(0),
            |r: &Row| r.get(0).and_then(|v| v.as_i64()).unwrap_or(0),
        );
        // Add 10 departments
        for i in 0..10 {
            let dept = Row::new(
                i as u64,
                vec![Value::Int64(i), Value::String(format!("Dept{}", i).into())],
            );
            join.on_right_insert(dept);
        }
        join
    }

    // Single left insert
    let result = measure(ITERATIONS * 100, || {
        let mut join = setup_join();
        let emp = Row::new(100, vec![Value::Int64(100), Value::Int64(5)]);
        join.on_left_insert(emp)
    });
    let passed = result.mean_us() < 100.0;
    println!(
        "    single insert:   {:>10} [target: <100μs] {}",
        format_duration(result.mean),
        if passed { "✓" } else { "✗" }
    );
    report.add_with_target(
        "Incremental/Join",
        "single_insert",
        None,
        result,
        None,
        "<100μs",
        passed,
    );

    // Batch inserts
    for &size in &[10, 100] {
        let employees: Vec<Row> = (0..size)
            .map(|i| {
                Row::new(
                    i as u64,
                    vec![Value::Int64(i as i64), Value::Int64(i as i64 % 10)],
                )
            })
            .collect();

        let result = measure(ITERATIONS, || {
            let mut join = setup_join();
            for emp in &employees {
                join.on_left_insert(emp.clone());
            }
        });

        let throughput = result.throughput(size);
        println!(
            "    {:>7} inserts: {:>10} ({:>12})",
            size,
            format_duration(result.mean),
            format_throughput(throughput)
        );
        report.add_result("Incremental/Join", "batch_insert", Some(size), result, Some(throughput));
    }
}

fn materialized_view(report: &mut Report) {
    println!("  Materialized View:");

    // Source view - single insert
    let result = measure(ITERATIONS * 100, || {
        let dataflow = DataflowNode::source(1);
        let mut view = MaterializedView::new(dataflow);
        let deltas = vec![Delta::insert(make_row(1, 25))];
        view.on_table_change(1, deltas)
    });
    let passed = result.mean_us() < 100.0;
    println!(
        "    source (single): {:>10} [target: <100μs] {}",
        format_duration(result.mean),
        if passed { "✓" } else { "✗" }
    );
    report.add_with_target(
        "Incremental/View",
        "source_single",
        None,
        result,
        None,
        "<100μs",
        passed,
    );

    // Filter view - single insert
    let result = measure(ITERATIONS * 100, || {
        let dataflow = DataflowNode::filter(DataflowNode::source(1), |row| {
            row.get(1)
                .and_then(|v| v.as_i64())
                .map(|age| age > 18)
                .unwrap_or(false)
        });
        let mut view = MaterializedView::new(dataflow);
        let deltas = vec![Delta::insert(make_row(1, 25))];
        view.on_table_change(1, deltas)
    });
    let passed = result.mean_us() < 100.0;
    println!(
        "    filter (single): {:>10} [target: <100μs] {}",
        format_duration(result.mean),
        if passed { "✓" } else { "✗" }
    );
    report.add_with_target(
        "Incremental/View",
        "filter_single",
        None,
        result,
        None,
        "<100μs",
        passed,
    );

    // Batch propagation
    println!("  Materialized View (batch):");
    for &size in &[10, 100, 1000] {
        let deltas: Vec<Delta<Row>> = (0..size)
            .map(|i| Delta::insert(make_row(i as u64, (i % 50) as i64)))
            .collect();

        let result = measure(ITERATIONS, || {
            let dataflow = DataflowNode::filter(DataflowNode::source(1), |row| {
                row.get(1)
                    .and_then(|v| v.as_i64())
                    .map(|age| age > 25)
                    .unwrap_or(false)
            });
            let mut view = MaterializedView::new(dataflow);
            view.on_table_change(1, deltas.clone())
        });

        let throughput = result.throughput(size);
        println!(
            "    {:>7} deltas: {:>10} ({:>12})",
            size,
            format_duration(result.mean),
            format_throughput(throughput)
        );
        report.add_result("Incremental/View", "filter_batch", Some(size), result, Some(throughput));
    }
}
