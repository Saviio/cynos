//! Performance benchmarks for Cynos API
//!
//! Run with: cargo bench -p cynos-database

use cynos_core::schema::TableBuilder;
use cynos_core::{next_row_id, DataType, Row, Value};
use cynos_incremental::Delta;
use cynos_reactive::{DataflowNode, ObservableQuery};
use cynos_storage::RowStore;
use std::cell::RefCell;
use std::rc::Rc;
use std::time::Instant;

fn create_test_schema() -> cynos_core::schema::Table {
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

fn generate_users(count: usize) -> Vec<Row> {
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

fn benchmark_insert(size: usize) -> f64 {
    let schema = create_test_schema();
    let mut store = RowStore::new(schema);
    let users = generate_users(size);

    let start = Instant::now();
    for user in users {
        store.insert(user).unwrap();
    }
    start.elapsed().as_secs_f64() * 1000.0
}

fn benchmark_scan(size: usize) -> f64 {
    let schema = create_test_schema();
    let mut store = RowStore::new(schema);
    let users = generate_users(size);
    for user in users {
        store.insert(user).unwrap();
    }

    let start = Instant::now();
    let _results: Vec<Rc<Row>> = store.scan().collect();
    start.elapsed().as_secs_f64() * 1000.0
}

fn benchmark_filter(size: usize) -> f64 {
    let schema = create_test_schema();
    let mut store = RowStore::new(schema);
    let users = generate_users(size);
    for user in users {
        store.insert(user).unwrap();
    }

    let start = Instant::now();
    let _results: Vec<Rc<Row>> = store
        .scan()
        .filter(|row| {
            if let Some(Value::Int32(age)) = row.get(2) {
                *age > 30
            } else {
                false
            }
        })
        .collect();
    start.elapsed().as_secs_f64() * 1000.0
}

fn benchmark_sort(size: usize) -> f64 {
    let schema = create_test_schema();
    let mut store = RowStore::new(schema);
    let users = generate_users(size);
    for user in users {
        store.insert(user).unwrap();
    }

    let start = Instant::now();
    let mut results: Vec<Rc<Row>> = store.scan().collect();
    results.sort_by(|a, b| {
        let a_salary = a.get(4).and_then(|v| {
            if let Value::Int64(s) = v {
                Some(*s)
            } else {
                None
            }
        });
        let b_salary = b.get(4).and_then(|v| {
            if let Value::Int64(s) = v {
                Some(*s)
            } else {
                None
            }
        });
        b_salary.cmp(&a_salary) // DESC
    });
    start.elapsed().as_secs_f64() * 1000.0
}

fn benchmark_complex_query(size: usize) -> f64 {
    let schema = create_test_schema();
    let mut store = RowStore::new(schema);
    let users = generate_users(size);
    for user in users {
        store.insert(user).unwrap();
    }

    let start = Instant::now();
    // Filter: age > 25 AND department = 'Engineering'
    let mut results: Vec<Rc<Row>> = store
        .scan()
        .filter(|row| {
            let age_ok = row
                .get(2)
                .map(|v| matches!(v, Value::Int32(a) if *a > 25))
                .unwrap_or(false);
            let dept_ok = row
                .get(3)
                .map(|v| matches!(v, Value::String(s) if s.as_str() == "Engineering"))
                .unwrap_or(false);
            age_ok && dept_ok
        })
        .collect();

    // Sort by salary DESC
    results.sort_by(|a, b| {
        let a_salary = a.get(4).and_then(|v| {
            if let Value::Int64(s) = v {
                Some(*s)
            } else {
                None
            }
        });
        let b_salary = b.get(4).and_then(|v| {
            if let Value::Int64(s) = v {
                Some(*s)
            } else {
                None
            }
        });
        b_salary.cmp(&a_salary)
    });

    // Limit 50
    results.truncate(50);

    start.elapsed().as_secs_f64() * 1000.0
}

fn benchmark_update(size: usize) -> f64 {
    let schema = create_test_schema();
    let mut store = RowStore::new(schema);
    let users = generate_users(size);
    for user in users {
        store.insert(user).unwrap();
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
            new_values[4] = Value::Int64(100000); // Update salary
            (row.id(), Row::new(row.id(), new_values))
        })
        .collect();

    let start = Instant::now();
    for (id, new_row) in to_update {
        store.update(id, new_row).unwrap();
    }
    start.elapsed().as_secs_f64() * 1000.0
}

fn benchmark_delete(size: usize) -> f64 {
    let schema = create_test_schema();
    let mut store = RowStore::new(schema);
    let users = generate_users(size);
    for user in users {
        store.insert(user).unwrap();
    }

    // Find rows to delete
    let to_delete: Vec<u64> = store
        .scan()
        .filter(|row| {
            row.get(2)
                .map(|v| matches!(v, Value::Int32(a) if *a < 30))
                .unwrap_or(false)
        })
        .map(|row| row.id())
        .collect();

    let start = Instant::now();
    for id in to_delete {
        store.delete(id).unwrap();
    }
    start.elapsed().as_secs_f64() * 1000.0
}

/// Benchmark: Live query creation with dataflow setup
/// Measures: DataflowNode construction + ObservableQuery initialization
fn benchmark_live_query_create(size: usize) -> f64 {
    let schema = create_test_schema();
    let mut store = RowStore::new(schema);
    let users = generate_users(size);
    for user in users {
        store.insert(user).unwrap();
    }

    // Collect initial data (not measured)
    let initial_rows: Vec<Row> = store.scan().map(|r| (*r).clone()).collect();

    // Measure: dataflow construction + query initialization with initial data
    let start = Instant::now();
    let dataflow = DataflowNode::filter(
        DataflowNode::source(1),
        |row| row.get(2).map(|v| matches!(v, Value::Int32(a) if *a > 30)).unwrap_or(false)
    );
    let mut query = ObservableQuery::with_initial(dataflow, initial_rows);

    // Subscribe to ensure the query is fully set up
    query.subscribe(|_changes| {});

    start.elapsed().as_secs_f64() * 1000.0
}

/// Benchmark: Live query with filter - incremental propagation
/// Measures: Single row insert propagation through filter dataflow
fn benchmark_live_query_with_filter(size: usize) -> f64 {
    let schema = create_test_schema();
    let mut store = RowStore::new(schema);
    let users = generate_users(size);
    for user in users {
        store.insert(user).unwrap();
    }

    // Create dataflow with filter: age > 30
    let dataflow = DataflowNode::filter(
        DataflowNode::source(1),
        |row| row.get(2).map(|v| matches!(v, Value::Int32(a) if *a > 30)).unwrap_or(false)
    );

    // Initialize with filtered data
    let initial_rows: Vec<Row> = store
        .scan()
        .filter(|row| {
            row.get(2)
                .map(|v| matches!(v, Value::Int32(a) if *a > 30))
                .unwrap_or(false)
        })
        .map(|r| (*r).clone())
        .collect();

    let query = Rc::new(RefCell::new(ObservableQuery::with_initial(
        dataflow,
        initial_rows,
    )));

    // Subscribe to changes
    let update_count = Rc::new(RefCell::new(0));
    let update_count_clone = update_count.clone();
    query.borrow_mut().subscribe(move |_changes| {
        *update_count_clone.borrow_mut() += 1;
    });

    // Create a new row that passes the filter (age=35 > 30)
    let new_id = next_row_id();
    let new_row = Row::new(
        new_id,
        vec![
            Value::Int64((size + 1) as i64),
            Value::String("New User".into()),
            Value::Int32(35), // passes filter
            Value::String("Engineering".into()),
            Value::Int64(90000),
        ],
    );

    // Measure: incremental propagation through filter
    let start = Instant::now();
    let deltas = vec![Delta::insert(new_row)];
    query.borrow_mut().on_table_change(1, deltas);
    let duration = start.elapsed().as_secs_f64() * 1000.0;

    // Verify update was received
    assert_eq!(*update_count.borrow(), 1);

    duration
}

/// Benchmark: Live query update propagation through source dataflow
/// Measures: Single row insert propagation (baseline without operators)
fn benchmark_live_query_update_propagation(size: usize) -> f64 {
    let schema = create_test_schema();
    let mut store = RowStore::new(schema);
    let users = generate_users(size);
    for user in users {
        store.insert(user).unwrap();
    }

    // Create observable query with source dataflow
    let initial_rows: Vec<Row> = store.scan().map(|r| (*r).clone()).collect();
    let dataflow = DataflowNode::source(1);
    let query = Rc::new(RefCell::new(ObservableQuery::with_initial(
        dataflow,
        initial_rows,
    )));

    // Subscribe to changes
    let update_count = Rc::new(RefCell::new(0));
    let update_count_clone = update_count.clone();
    query.borrow_mut().subscribe(move |_changes| {
        *update_count_clone.borrow_mut() += 1;
    });

    // Create a new row to insert
    let new_id = next_row_id();
    let new_row = Row::new(
        new_id,
        vec![
            Value::Int64((size + 1) as i64),
            Value::String("New User".into()),
            Value::Int32(35),
            Value::String("Engineering".into()),
            Value::Int64(90000),
        ],
    );

    // Measure: single row propagation through source dataflow
    let start = Instant::now();
    let deltas = vec![Delta::insert(new_row)];
    query.borrow_mut().on_table_change(1, deltas);
    let duration = start.elapsed().as_secs_f64() * 1000.0;

    // Verify update was received
    assert_eq!(*update_count.borrow(), 1);

    duration
}

/// Benchmark: Live query delete propagation
/// Measures: Single row delete propagation through filter dataflow
fn benchmark_live_query_delete_propagation(size: usize) -> f64 {
    let schema = create_test_schema();
    let mut store = RowStore::new(schema);
    let users = generate_users(size);
    for user in users {
        store.insert(user).unwrap();
    }

    // Create dataflow with filter: age > 30
    let dataflow = DataflowNode::filter(
        DataflowNode::source(1),
        |row| row.get(2).map(|v| matches!(v, Value::Int32(a) if *a > 30)).unwrap_or(false)
    );

    // Initialize with filtered data
    let initial_rows: Vec<Row> = store
        .scan()
        .filter(|row| {
            row.get(2)
                .map(|v| matches!(v, Value::Int32(a) if *a > 30))
                .unwrap_or(false)
        })
        .map(|r| (*r).clone())
        .collect();

    let query = Rc::new(RefCell::new(ObservableQuery::with_initial(
        dataflow,
        initial_rows.clone(),
    )));

    // Subscribe to changes
    let update_count = Rc::new(RefCell::new(0));
    let update_count_clone = update_count.clone();
    query.borrow_mut().subscribe(move |_changes| {
        *update_count_clone.borrow_mut() += 1;
    });

    // Get a row to delete (one that passes the filter)
    let row_to_delete = initial_rows.first().cloned().unwrap();

    // Measure: single row delete propagation
    let start = Instant::now();
    let deltas = vec![Delta::delete(row_to_delete)];
    query.borrow_mut().on_table_change(1, deltas);
    let duration = start.elapsed().as_secs_f64() * 1000.0;

    // Verify update was received
    assert_eq!(*update_count.borrow(), 1);

    duration
}

/// Benchmark: Live query batch update propagation
/// Measures: Multiple rows insert propagation through filter dataflow
fn benchmark_live_query_batch_propagation(size: usize, batch_size: usize) -> f64 {
    let schema = create_test_schema();
    let mut store = RowStore::new(schema);
    let users = generate_users(size);
    for user in users {
        store.insert(user).unwrap();
    }

    // Create dataflow with filter: age > 30
    let dataflow = DataflowNode::filter(
        DataflowNode::source(1),
        |row| row.get(2).map(|v| matches!(v, Value::Int32(a) if *a > 30)).unwrap_or(false)
    );

    // Initialize with filtered data
    let initial_rows: Vec<Row> = store
        .scan()
        .filter(|row| {
            row.get(2)
                .map(|v| matches!(v, Value::Int32(a) if *a > 30))
                .unwrap_or(false)
        })
        .map(|r| (*r).clone())
        .collect();

    let query = Rc::new(RefCell::new(ObservableQuery::with_initial(
        dataflow,
        initial_rows,
    )));

    // Subscribe to changes
    let update_count = Rc::new(RefCell::new(0));
    let update_count_clone = update_count.clone();
    query.borrow_mut().subscribe(move |_changes| {
        *update_count_clone.borrow_mut() += 1;
    });

    // Create batch of new rows (all pass filter with age=35)
    let deltas: Vec<Delta<Row>> = (0..batch_size)
        .map(|i| {
            let new_id = next_row_id();
            Delta::insert(Row::new(
                new_id,
                vec![
                    Value::Int64((size + i + 1) as i64),
                    Value::String(format!("Batch User {}", i).into()),
                    Value::Int32(35), // passes filter
                    Value::String("Engineering".into()),
                    Value::Int64(90000),
                ],
            ))
        })
        .collect();

    // Measure: batch propagation through filter
    let start = Instant::now();
    query.borrow_mut().on_table_change(1, deltas);
    let duration = start.elapsed().as_secs_f64() * 1000.0;

    // Verify update was received
    assert_eq!(*update_count.borrow(), 1);

    duration
}

/// Benchmark: Live query with chained operators (filter -> project)
/// Measures: Single row propagation through multi-stage dataflow
fn benchmark_live_query_chained_operators(size: usize) -> f64 {
    let schema = create_test_schema();
    let mut store = RowStore::new(schema);
    let users = generate_users(size);
    for user in users {
        store.insert(user).unwrap();
    }

    // Create dataflow: filter(age > 30) -> project(id, name, salary)
    let dataflow = DataflowNode::project(
        DataflowNode::filter(
            DataflowNode::source(1),
            |row| row.get(2).map(|v| matches!(v, Value::Int32(a) if *a > 30)).unwrap_or(false)
        ),
        vec![0, 1, 4], // id, name, salary
    );

    // Initialize with filtered and projected data
    let initial_rows: Vec<Row> = store
        .scan()
        .filter(|row| {
            row.get(2)
                .map(|v| matches!(v, Value::Int32(a) if *a > 30))
                .unwrap_or(false)
        })
        .map(|row| {
            Row::new(row.id(), vec![
                row.get(0).cloned().unwrap_or(Value::Null),
                row.get(1).cloned().unwrap_or(Value::Null),
                row.get(4).cloned().unwrap_or(Value::Null),
            ])
        })
        .collect();

    let query = Rc::new(RefCell::new(ObservableQuery::with_initial(
        dataflow,
        initial_rows,
    )));

    // Subscribe to changes
    let update_count = Rc::new(RefCell::new(0));
    let update_count_clone = update_count.clone();
    query.borrow_mut().subscribe(move |_changes| {
        *update_count_clone.borrow_mut() += 1;
    });

    // Create a new row that passes the filter
    let new_id = next_row_id();
    let new_row = Row::new(
        new_id,
        vec![
            Value::Int64((size + 1) as i64),
            Value::String("New User".into()),
            Value::Int32(35), // passes filter
            Value::String("Engineering".into()),
            Value::Int64(90000),
        ],
    );

    // Measure: propagation through filter -> project
    let start = Instant::now();
    let deltas = vec![Delta::insert(new_row)];
    query.borrow_mut().on_table_change(1, deltas);
    let duration = start.elapsed().as_secs_f64() * 1000.0;

    // Verify update was received
    assert_eq!(*update_count.borrow(), 1);

    duration
}

/// Benchmark: Live query filtered out (no propagation to subscriber)
/// Measures: Overhead when row doesn't pass filter
fn benchmark_live_query_filtered_out(size: usize) -> f64 {
    let schema = create_test_schema();
    let mut store = RowStore::new(schema);
    let users = generate_users(size);
    for user in users {
        store.insert(user).unwrap();
    }

    // Create dataflow with filter: age > 30
    let dataflow = DataflowNode::filter(
        DataflowNode::source(1),
        |row| row.get(2).map(|v| matches!(v, Value::Int32(a) if *a > 30)).unwrap_or(false)
    );

    // Initialize with filtered data
    let initial_rows: Vec<Row> = store
        .scan()
        .filter(|row| {
            row.get(2)
                .map(|v| matches!(v, Value::Int32(a) if *a > 30))
                .unwrap_or(false)
        })
        .map(|r| (*r).clone())
        .collect();

    let query = Rc::new(RefCell::new(ObservableQuery::with_initial(
        dataflow,
        initial_rows,
    )));

    // Subscribe to changes
    let update_count = Rc::new(RefCell::new(0));
    let update_count_clone = update_count.clone();
    query.borrow_mut().subscribe(move |_changes| {
        *update_count_clone.borrow_mut() += 1;
    });

    // Create a new row that does NOT pass the filter (age=25 <= 30)
    let new_id = next_row_id();
    let new_row = Row::new(
        new_id,
        vec![
            Value::Int64((size + 1) as i64),
            Value::String("Young User".into()),
            Value::Int32(25), // does NOT pass filter
            Value::String("Engineering".into()),
            Value::Int64(50000),
        ],
    );

    // Measure: propagation that gets filtered out
    let start = Instant::now();
    let deltas = vec![Delta::insert(new_row)];
    query.borrow_mut().on_table_change(1, deltas);
    let duration = start.elapsed().as_secs_f64() * 1000.0;

    // Verify NO update was received (filtered out)
    assert_eq!(*update_count.borrow(), 0);

    duration
}

fn main() {
    println!("========================================");
    println!("CYNOS DATABASE PERFORMANCE BENCHMARK");
    println!("========================================\n");

    let sizes = [100, 1000, 10000];

    // Warm up
    let _ = benchmark_insert(100);
    let _ = benchmark_scan(100);

    println!("Insert Performance:");
    for &size in &sizes {
        let duration = benchmark_insert(size);
        let throughput = size as f64 / (duration / 1000.0);
        println!(
            "  {:>5} rows: {:>8.2}ms ({:>10.0} rows/sec)",
            size, duration, throughput
        );
    }
    println!();

    println!("Simple Query (Full Scan):");
    for &size in &sizes {
        let duration = benchmark_scan(size);
        let throughput = size as f64 / (duration / 1000.0);
        println!(
            "  {:>5} rows: {:>8.2}ms ({:>10.0} rows/sec)",
            size, duration, throughput
        );
    }
    println!();

    println!("Filter Query (age > 30):");
    for &size in &sizes {
        let duration = benchmark_filter(size);
        let throughput = size as f64 / (duration / 1000.0);
        println!(
            "  {:>5} rows: {:>8.2}ms ({:>10.0} rows/sec)",
            size, duration, throughput
        );
    }
    println!();

    println!("Sort Query (ORDER BY salary DESC):");
    for &size in &sizes {
        let duration = benchmark_sort(size);
        let throughput = size as f64 / (duration / 1000.0);
        println!(
            "  {:>5} rows: {:>8.2}ms ({:>10.0} rows/sec)",
            size, duration, throughput
        );
    }
    println!();

    println!("Complex Query (Filter + Sort + Limit):");
    for &size in &sizes {
        let duration = benchmark_complex_query(size);
        let throughput = size as f64 / (duration / 1000.0);
        println!(
            "  {:>5} rows: {:>8.2}ms ({:>10.0} rows/sec)",
            size, duration, throughput
        );
    }
    println!();

    println!("Update Performance (Engineering dept):");
    for &size in &sizes {
        let duration = benchmark_update(size);
        let throughput = size as f64 / (duration / 1000.0);
        println!(
            "  {:>5} rows: {:>8.2}ms ({:>10.0} rows/sec)",
            size, duration, throughput
        );
    }
    println!();

    println!("Delete Performance (age < 30):");
    for &size in &sizes {
        let duration = benchmark_delete(size);
        let throughput = size as f64 / (duration / 1000.0);
        println!(
            "  {:>5} rows: {:>8.2}ms ({:>10.0} rows/sec)",
            size, duration, throughput
        );
    }
    println!();

    println!("Live Query (create with filter dataflow):");
    for &size in &sizes {
        let duration = benchmark_live_query_create(size);
        println!(
            "  {:>5} rows: {:>8.3}ms",
            size, duration
        );
    }
    println!();

    println!("Live Query (single insert through filter):");
    for &size in &sizes {
        let duration = benchmark_live_query_with_filter(size);
        println!(
            "  {:>5} rows: {:>8.3}ms (single insert, passes filter)",
            size, duration
        );
    }
    println!();

    println!("Live Query (single insert, source only):");
    for &size in &sizes {
        let duration = benchmark_live_query_update_propagation(size);
        println!(
            "  {:>5} rows: {:>8.3}ms (baseline, no operators)",
            size, duration
        );
    }
    println!();

    println!("Live Query (single delete through filter):");
    for &size in &sizes {
        let duration = benchmark_live_query_delete_propagation(size);
        println!(
            "  {:>5} rows: {:>8.3}ms (single delete)",
            size, duration
        );
    }
    println!();

    println!("Live Query (batch insert through filter):");
    for &size in &sizes {
        for batch_size in [10, 100] {
            let duration = benchmark_live_query_batch_propagation(size, batch_size);
            println!(
                "  {:>5} rows, batch {:>3}: {:>8.3}ms ({:>8.1}μs/row)",
                size, batch_size, duration, duration * 1000.0 / batch_size as f64
            );
        }
    }
    println!();

    println!("Live Query (chained operators: filter -> project):");
    for &size in &sizes {
        let duration = benchmark_live_query_chained_operators(size);
        println!(
            "  {:>5} rows: {:>8.3}ms (single insert)",
            size, duration
        );
    }
    println!();

    println!("Live Query (filtered out, no notification):");
    for &size in &sizes {
        let duration = benchmark_live_query_filtered_out(size);
        println!(
            "  {:>5} rows: {:>8.3}ms (row rejected by filter)",
            size, duration
        );
    }
    println!();

    println!("========================================");
    println!("BENCHMARK COMPLETE");
    println!("========================================");
}
