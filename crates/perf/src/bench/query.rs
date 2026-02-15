//! Query execution performance benchmarks

use crate::report::Report;
use crate::utils::*;
use cynos_core::{Row, Value};
use cynos_query::ast::{ColumnRef, SortOrder, ValuePredicate};
use cynos_query::executor::{
    FilterExecutor, LimitExecutor, ProjectExecutor, Relation, SortExecutor,
};

pub fn run(report: &mut Report) {
    filter(report);
    sort(report);
    project(report);
    limit(report);
    combined_query(report);
}

fn create_rows(count: usize) -> Vec<Row> {
    shuffle_indices(count, 12345)
        .into_iter()
        .map(|i| {
            Row::new(
                i as u64,
                vec![
                    Value::Int64(i as i64),
                    Value::String(format!("name_{}", i).into()),
                    Value::Int64((i % 100) as i64),
                ],
            )
        })
        .collect()
}

fn filter(report: &mut Report) {
    println!("  Filter (id > N/2):");
    for &size in &SIZES {
        let rows = create_rows(size);
        let threshold = (size / 2) as i64;

        let result = measure_with_setup(
            ITERATIONS,
            || Relation::from_rows_owned(rows.clone(), vec!["table".into()]),
            |relation| {
                let col = ColumnRef::new("table", "id", 0);
                let pred = ValuePredicate::gt(col, Value::Int64(threshold));
                let executor = FilterExecutor::new(pred);
                executor.execute(relation)
            },
        );

        let throughput = result.throughput(size);
        println!(
            "    {:>7} rows: {:>10} ({:>12})",
            size,
            format_duration(result.mean),
            format_throughput(throughput)
        );
        report.add_result("Query/Executor", "filter", Some(size), result, Some(throughput));
    }
}

fn sort(report: &mut Report) {
    println!("  Sort (ORDER BY id):");
    for &size in &SIZES {
        let rows = create_rows(size);

        let result = measure_with_setup(
            ITERATIONS,
            || Relation::from_rows_owned(rows.clone(), vec!["table".into()]),
            |relation| {
                let executor = SortExecutor::new(vec![(0, SortOrder::Asc)]);
                executor.execute(relation)
            },
        );

        let throughput = result.throughput(size);
        println!(
            "    {:>7} rows: {:>10} ({:>12})",
            size,
            format_duration(result.mean),
            format_throughput(throughput)
        );
        report.add_result("Query/Executor", "sort", Some(size), result, Some(throughput));
    }
}

fn project(report: &mut Report) {
    println!("  Project (SELECT id, category):");
    for &size in &SIZES {
        let rows = create_rows(size);

        let result = measure_with_setup(
            ITERATIONS,
            || Relation::from_rows_owned(rows.clone(), vec!["table".into()]),
            |relation| {
                let executor = ProjectExecutor::new(vec![0, 2]);
                executor.execute(relation)
            },
        );

        let throughput = result.throughput(size);
        println!(
            "    {:>7} rows: {:>10} ({:>12})",
            size,
            format_duration(result.mean),
            format_throughput(throughput)
        );
        report.add_result("Query/Executor", "project", Some(size), result, Some(throughput));
    }
}

fn limit(report: &mut Report) {
    println!("  Limit (LIMIT 10 OFFSET 5):");
    for &size in &SIZES {
        let rows = create_rows(size);

        let result = measure_with_setup(
            ITERATIONS,
            || Relation::from_rows_owned(rows.clone(), vec!["table".into()]),
            |relation| {
                let executor = LimitExecutor::new(10, 5);
                executor.execute(relation)
            },
        );

        let throughput = result.throughput(size);
        println!(
            "    {:>7} rows: {:>10} ({:>12})",
            size,
            format_duration(result.mean),
            format_throughput(throughput)
        );
        report.add_result("Query/Executor", "limit", Some(size), result, Some(throughput));
    }
}

fn combined_query(report: &mut Report) {
    println!("  Combined (Filter + Sort + Limit + Project):");
    for &size in &SIZES {
        let rows = create_rows(size);
        let threshold = (size / 2) as i64;

        let result = measure_with_setup(
            ITERATIONS,
            || Relation::from_rows_owned(rows.clone(), vec!["table".into()]),
            |relation| {
                // Filter
                let col = ColumnRef::new("table", "id", 0);
                let pred = ValuePredicate::gt(col, Value::Int64(threshold));
                let filter = FilterExecutor::new(pred);
                let filtered = filter.execute(relation);

                // Sort
                let sort = SortExecutor::new(vec![(0, SortOrder::Asc)]);
                let sorted = sort.execute(filtered);

                // Limit
                let limit = LimitExecutor::new(10, 0);
                let limited = limit.execute(sorted);

                // Project
                let project = ProjectExecutor::new(vec![0, 1]);
                project.execute(limited)
            },
        );

        let throughput = result.throughput(size);
        println!(
            "    {:>7} rows: {:>10} ({:>12})",
            size,
            format_duration(result.mean),
            format_throughput(throughput)
        );
        report.add_result("Query/Executor", "combined", Some(size), result, Some(throughput));
    }
}
