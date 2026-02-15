//! Join performance benchmarks

use crate::report::Report;
use crate::utils::*;
use cynos_core::{Row, Value};
use cynos_query::executor::join::{HashJoin, NestedLoopJoin, SortMergeJoin};
use cynos_query::executor::Relation;

pub fn run(report: &mut Report) {
    hash_join(report);
    sort_merge_join(report);
    nested_loop_join(report);
    join_comparison(report);
}

fn create_join_rows(count: usize, key_range: usize, seed: u64) -> Vec<Row> {
    shuffle_indices(count, seed)
        .into_iter()
        .map(|i| {
            Row::new(
                i as u64,
                vec![
                    Value::Int64((i % key_range) as i64),
                    Value::String(format!("value_{}", i).into()),
                ],
            )
        })
        .collect()
}

fn hash_join(report: &mut Report) {
    println!("  Hash Join:");
    for &size in &SIZES {
        let key_range = size / 10;
        let left_rows = create_join_rows(size, key_range, 12345);
        let right_rows = create_join_rows(size, key_range, 67890);

        let result = measure_with_setup(
            ITERATIONS,
            || {
                (
                    Relation::from_rows_owned(left_rows.clone(), vec!["left".into()]),
                    Relation::from_rows_owned(right_rows.clone(), vec!["right".into()]),
                )
            },
            |(left, right)| {
                let join = HashJoin::inner(0, 0);
                join.execute(left, right)
            },
        );

        let throughput = (size * 2) as f64 / result.mean.as_secs_f64();
        println!(
            "    {:>7} x {:>7}: {:>10} ({:>12})",
            size,
            size,
            format_duration(result.mean),
            format_throughput(throughput)
        );
        report.add_result("Join", "hash_join", Some(size), result, Some(throughput));
    }
}

fn sort_merge_join(report: &mut Report) {
    println!("  Sort-Merge Join:");
    for &size in &SIZES {
        let key_range = size / 10;
        let left_rows = create_join_rows(size, key_range, 12345);
        let right_rows = create_join_rows(size, key_range, 67890);

        let result = measure_with_setup(
            ITERATIONS,
            || {
                (
                    Relation::from_rows_owned(left_rows.clone(), vec!["left".into()]),
                    Relation::from_rows_owned(right_rows.clone(), vec!["right".into()]),
                )
            },
            |(left, right)| {
                let join = SortMergeJoin::inner(0, 0);
                join.execute_with_sort(left, right)
            },
        );

        let throughput = (size * 2) as f64 / result.mean.as_secs_f64();
        println!(
            "    {:>7} x {:>7}: {:>10} ({:>12})",
            size,
            size,
            format_duration(result.mean),
            format_throughput(throughput)
        );
        report.add_result("Join", "sort_merge_join", Some(size), result, Some(throughput));
    }
}

fn nested_loop_join(report: &mut Report) {
    println!("  Nested Loop Join (smaller sizes):");
    // Smaller sizes for O(n*m) algorithm
    for &size in &[100, 500, 1000] {
        let key_range = size / 10;
        let left_rows = create_join_rows(size, key_range, 12345);
        let right_rows = create_join_rows(size, key_range, 67890);

        let result = measure_with_setup(
            ITERATIONS,
            || {
                (
                    Relation::from_rows_owned(left_rows.clone(), vec!["left".into()]),
                    Relation::from_rows_owned(right_rows.clone(), vec!["right".into()]),
                )
            },
            |(left, right)| {
                let join = NestedLoopJoin::inner(0, 0);
                join.execute(left, right)
            },
        );

        let throughput = (size * 2) as f64 / result.mean.as_secs_f64();
        println!(
            "    {:>7} x {:>7}: {:>10} ({:>12})",
            size,
            size,
            format_duration(result.mean),
            format_throughput(throughput)
        );
        report.add_result("Join", "nested_loop_join", Some(size), result, Some(throughput));
    }
}

fn join_comparison(report: &mut Report) {
    println!("  Join Algorithm Comparison (1K x 1K):");
    let size = 1000;
    let key_range = size / 10;
    let left_rows = create_join_rows(size, key_range, 12345);
    let right_rows = create_join_rows(size, key_range, 67890);

    // Hash Join
    let hash_result = measure_with_setup(
        ITERATIONS,
        || {
            (
                Relation::from_rows_owned(left_rows.clone(), vec!["left".into()]),
                Relation::from_rows_owned(right_rows.clone(), vec!["right".into()]),
            )
        },
        |(left, right)| {
            let join = HashJoin::inner(0, 0);
            join.execute(left, right)
        },
    );

    // Sort-Merge Join
    let merge_result = measure_with_setup(
        ITERATIONS,
        || {
            (
                Relation::from_rows_owned(left_rows.clone(), vec!["left".into()]),
                Relation::from_rows_owned(right_rows.clone(), vec!["right".into()]),
            )
        },
        |(left, right)| {
            let join = SortMergeJoin::inner(0, 0);
            join.execute_with_sort(left, right)
        },
    );

    // Nested Loop Join
    let nested_result = measure_with_setup(
        ITERATIONS,
        || {
            (
                Relation::from_rows_owned(left_rows.clone(), vec!["left".into()]),
                Relation::from_rows_owned(right_rows.clone(), vec!["right".into()]),
            )
        },
        |(left, right)| {
            let join = NestedLoopJoin::inner(0, 0);
            join.execute(left, right)
        },
    );

    println!(
        "    Hash:        {:>10}",
        format_duration(hash_result.mean)
    );
    println!(
        "    Sort-Merge:  {:>10}",
        format_duration(merge_result.mean)
    );
    println!(
        "    Nested Loop: {:>10}",
        format_duration(nested_result.mean)
    );

    report.add_result("Join/Comparison", "hash", Some(1000), hash_result, None);
    report.add_result("Join/Comparison", "sort_merge", Some(1000), merge_result, None);
    report.add_result("Join/Comparison", "nested_loop", Some(1000), nested_result, None);
}
