//! Reactive query performance benchmarks

use crate::report::Report;
use crate::utils::*;
use cynos_core::{Row, Value};
use cynos_incremental::{DataflowNode, Delta};
use cynos_reactive::ObservableQuery;
use std::cell::RefCell;
use std::rc::Rc;

pub fn run(report: &mut Report) {
    observable_query_create(report);
    observable_query_subscribe(report);
    change_propagation(report);
}

fn make_row(id: u64, age: i64) -> Row {
    Row::new(id, vec![Value::Int64(id as i64), Value::Int64(age)])
}

fn observable_query_create(report: &mut Report) {
    println!("  Observable Query Creation:");

    for &size in &SMALL_SIZES {
        let initial_rows: Vec<Row> = (0..size).map(|i| make_row(i as u64, (i % 50) as i64)).collect();

        let result = measure(ITERATIONS, || {
            let dataflow = DataflowNode::source(1);
            ObservableQuery::with_initial(dataflow, initial_rows.clone())
        });

        let throughput = result.throughput(size);
        println!(
            "    {:>7} rows: {:>10} ({:>12})",
            size,
            format_duration(result.mean),
            format_throughput(throughput)
        );
        report.add_result("Reactive", "create", Some(size), result, Some(throughput));
    }
}

fn observable_query_subscribe(report: &mut Report) {
    println!("  Observable Query with Subscription:");

    for &size in &SMALL_SIZES {
        let initial_rows: Vec<Row> = (0..size).map(|i| make_row(i as u64, (i % 50) as i64)).collect();

        let result = measure(ITERATIONS, || {
            let dataflow = DataflowNode::filter(DataflowNode::source(1), |row| {
                row.get(1)
                    .and_then(|v| v.as_i64())
                    .map(|age| age > 25)
                    .unwrap_or(false)
            });
            let mut query = ObservableQuery::with_initial(dataflow, initial_rows.clone());
            query.subscribe(|_changes| {});
            query
        });

        let throughput = result.throughput(size);
        println!(
            "    {:>7} rows: {:>10} ({:>12})",
            size,
            format_duration(result.mean),
            format_throughput(throughput)
        );
        report.add_result("Reactive", "subscribe", Some(size), result, Some(throughput));
    }
}

fn change_propagation(report: &mut Report) {
    println!("  Change Propagation:");

    for &size in &SMALL_SIZES {
        let initial_rows: Vec<Row> = (0..size).map(|i| make_row(i as u64, (i % 50) as i64)).collect();

        // Setup query with subscription
        let dataflow = DataflowNode::source(1);
        let query = Rc::new(RefCell::new(ObservableQuery::with_initial(
            dataflow,
            initial_rows,
        )));

        let update_count = Rc::new(RefCell::new(0));
        let update_count_clone = update_count.clone();
        query.borrow_mut().subscribe(move |_changes| {
            *update_count_clone.borrow_mut() += 1;
        });

        // Measure single update propagation
        let result = measure(ITERATIONS * 10, || {
            let new_row = make_row(size as u64 + 1, 35);
            let deltas = vec![Delta::insert(new_row)];
            query.borrow_mut().on_table_change(1, deltas)
        });

        let passed = result.mean_us() < 100.0;
        println!(
            "    {:>7} rows: {:>10} [target: <100μs] {}",
            size,
            format_duration(result.mean),
            if passed { "✓" } else { "✗" }
        );
        report.add_with_target(
            "Reactive",
            "propagation",
            Some(size),
            result,
            None,
            "<100μs",
            passed,
        );
    }

    // Batch change propagation
    println!("  Batch Change Propagation:");
    for &batch_size in &[1, 10, 100] {
        let initial_rows: Vec<Row> = (0..1000).map(|i| make_row(i as u64, (i % 50) as i64)).collect();

        let dataflow = DataflowNode::source(1);
        let query = Rc::new(RefCell::new(ObservableQuery::with_initial(
            dataflow,
            initial_rows,
        )));

        query.borrow_mut().subscribe(|_changes| {});

        let deltas: Vec<Delta<Row>> = (0..batch_size)
            .map(|i| Delta::insert(make_row(10000 + i as u64, 35)))
            .collect();

        let result = measure(ITERATIONS, || {
            query.borrow_mut().on_table_change(1, deltas.clone())
        });

        let throughput = result.throughput(batch_size);
        println!(
            "    {:>7} changes: {:>10} ({:>12})",
            batch_size,
            format_duration(result.mean),
            format_throughput(throughput)
        );
        report.add_result("Reactive", "batch_propagation", Some(batch_size), result, Some(throughput));
    }
}
