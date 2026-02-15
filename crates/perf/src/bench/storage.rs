//! Storage performance benchmarks

use crate::report::Report;
use crate::utils::*;
use cynos_core::schema::TableBuilder;
use cynos_core::{next_row_id, DataType, Row, Value};
use cynos_storage::RowStore;
use std::rc::Rc;

pub fn run(report: &mut Report) {
    insert(report);
    scan(report);
    filter(report);
    update(report);
    delete(report);
}

fn create_schema() -> cynos_core::schema::Table {
    TableBuilder::new("users")
        .unwrap()
        .add_column("id", DataType::Int64)
        .unwrap()
        .add_column("name", DataType::String)
        .unwrap()
        .add_column("age", DataType::Int32)
        .unwrap()
        .add_column("department", DataType::String)
        .unwrap()
        .add_column("salary", DataType::Int64)
        .unwrap()
        .add_primary_key(&["id"], false)
        .unwrap()
        .build()
        .unwrap()
}

fn generate_rows(count: usize) -> Vec<Row> {
    let departments = ["Engineering", "Sales", "Marketing", "HR", "Finance"];
    (0..count)
        .map(|i| {
            let id = next_row_id();
            Row::new(
                id,
                vec![
                    Value::Int64((i + 1) as i64),
                    Value::String(format!("User {}", i + 1).into()),
                    Value::Int32((20 + (i % 50)) as i32),
                    Value::String(departments[i % departments.len()].into()),
                    Value::Int64((50000 + (i % 100) * 1000) as i64),
                ],
            )
        })
        .collect()
}

fn insert(report: &mut Report) {
    println!("  Insert:");
    for &size in &SIZES {
        let result = measure_with_setup(
            ITERATIONS,
            || (create_schema(), generate_rows(size)),
            |(schema, rows)| {
                let mut store = RowStore::new(schema);
                for row in rows {
                    store.insert(row).unwrap();
                }
                store
            },
        );

        let throughput = result.throughput(size);
        println!(
            "    {:>7} rows: {:>10} ({:>12})",
            size,
            format_duration(result.mean),
            format_throughput(throughput)
        );
        report.add_result("Storage", "insert", Some(size), result, Some(throughput));
    }
}

fn scan(report: &mut Report) {
    println!("  Full Scan (with data access):");
    for &size in &SIZES {
        // Setup
        let schema = create_schema();
        let mut store = RowStore::new(schema);
        for row in generate_rows(size) {
            store.insert(row).unwrap();
        }

        // Measure scan with actual data access (not just Rc clone)
        let result = measure(ITERATIONS, || {
            let mut sum: i64 = 0;
            for row in store.scan() {
                // Access actual data to prevent optimization
                if let Some(Value::Int64(salary)) = row.get(4) {
                    sum += salary;
                }
            }
            sum
        });

        let throughput = result.throughput(size);
        println!(
            "    {:>7} rows: {:>10} ({:>12})",
            size,
            format_duration(result.mean),
            format_throughput(throughput)
        );
        report.add_result("Storage", "scan", Some(size), result, Some(throughput));
    }
}

fn filter(report: &mut Report) {
    println!("  Filter (age > 30):");
    for &size in &SIZES {
        // Setup
        let schema = create_schema();
        let mut store = RowStore::new(schema);
        for row in generate_rows(size) {
            store.insert(row).unwrap();
        }

        let result = measure(ITERATIONS, || {
            let results: Vec<Rc<Row>> = store
                .scan()
                .filter(|row| {
                    if let Some(Value::Int32(age)) = row.get(2) {
                        *age > 30
                    } else {
                        false
                    }
                })
                .collect();
            results
        });

        let throughput = result.throughput(size);
        println!(
            "    {:>7} rows: {:>10} ({:>12})",
            size,
            format_duration(result.mean),
            format_throughput(throughput)
        );
        report.add_result("Storage", "filter", Some(size), result, Some(throughput));
    }
}

fn update(report: &mut Report) {
    println!("  Update (Engineering dept):");
    for &size in &SMALL_SIZES {
        let result = measure(ITERATIONS, || {
            // Setup fresh store each iteration
            let schema = create_schema();
            let mut store = RowStore::new(schema);
            for row in generate_rows(size) {
                store.insert(row).unwrap();
            }

            // Find rows to update
            let to_update: Vec<(u64, Row)> = store
                .scan()
                .filter(|row| {
                    row.get(3)
                        .map(|v| matches!(v, Value::String(s) if s.as_str() == "Engineering"))
                        .unwrap_or(false)
                })
                .map(|row| {
                    let mut new_values = row.values().to_vec();
                    new_values[4] = Value::Int64(100000);
                    (row.id(), Row::new(row.id(), new_values))
                })
                .collect();

            let update_count = to_update.len();
            for (id, new_row) in to_update {
                store.update(id, new_row).unwrap();
            }
            update_count
        });

        let throughput = result.throughput(size);
        println!(
            "    {:>7} rows: {:>10} ({:>12})",
            size,
            format_duration(result.mean),
            format_throughput(throughput)
        );
        report.add_result("Storage", "update", Some(size), result, Some(throughput));
    }
}

fn delete(report: &mut Report) {
    println!("  Delete (age < 30):");
    for &size in &SMALL_SIZES {
        let result = measure(ITERATIONS, || {
            // Setup fresh store each iteration
            let schema = create_schema();
            let mut store = RowStore::new(schema);
            for row in generate_rows(size) {
                store.insert(row).unwrap();
            }

            let to_delete: Vec<u64> = store
                .scan()
                .filter(|row| {
                    row.get(2)
                        .map(|v| matches!(v, Value::Int32(a) if *a < 30))
                        .unwrap_or(false)
                })
                .map(|row| row.id())
                .collect();

            let delete_count = to_delete.len();
            for id in to_delete {
                store.delete(id).unwrap();
            }
            delete_count
        });

        let throughput = result.throughput(size);
        println!(
            "    {:>7} rows: {:>10} ({:>12})",
            size,
            format_duration(result.mean),
            format_throughput(throughput)
        );
        report.add_result("Storage", "delete", Some(size), result, Some(throughput));
    }
}
