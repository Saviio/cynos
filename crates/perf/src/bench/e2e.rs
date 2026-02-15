//! End-to-end scenario benchmarks

use crate::report::Report;
use crate::utils::*;
use cynos_core::schema::TableBuilder;
use cynos_core::{next_row_id, DataType, Row, Value};
use cynos_incremental::{DataflowNode, Delta};
use cynos_query::ast::{Expr, SortOrder};
use cynos_query::executor::{InMemoryDataSource, PhysicalPlanRunner};
use cynos_query::optimizer::Optimizer;
use cynos_query::planner::{LogicalPlan, PhysicalPlan};
use cynos_reactive::ObservableQuery;
use cynos_storage::RowStore;
use std::cell::RefCell;
use std::rc::Rc;

pub fn run(report: &mut Report) {
    crud_workflow(report);
    query_workflow(report);
    live_query_workflow(report);
    optimized_query(report);
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

fn crud_workflow(report: &mut Report) {
    println!("  CRUD Workflow (Insert -> Query -> Update -> Delete):");

    for &size in &SMALL_SIZES {
        let result = measure(ITERATIONS, || {
            let schema = create_schema();
            let mut store = RowStore::new(schema);

            // Insert
            for row in generate_rows(size) {
                store.insert(row).unwrap();
            }

            // Query (filter + sort)
            let mut results: Vec<Rc<Row>> = store
                .scan()
                .filter(|row| {
                    row.get(2)
                        .map(|v| matches!(v, Value::Int32(a) if *a > 30))
                        .unwrap_or(false)
                })
                .collect();
            results.sort_by(|a, b| {
                let a_salary = a.get(4).and_then(|v| v.as_i64()).unwrap_or(0);
                let b_salary = b.get(4).and_then(|v| v.as_i64()).unwrap_or(0);
                b_salary.cmp(&a_salary)
            });

            // Update (first 10%)
            let to_update: Vec<(u64, Row)> = store
                .scan()
                .take(size / 10)
                .map(|row| {
                    let mut new_values = row.values().to_vec();
                    new_values[4] = Value::Int64(100000);
                    (row.id(), Row::new(row.id(), new_values))
                })
                .collect();
            for (id, new_row) in to_update {
                store.update(id, new_row).unwrap();
            }

            // Delete (first 5%)
            let to_delete: Vec<u64> = store.scan().take(size / 20).map(|row| row.id()).collect();
            for id in to_delete {
                store.delete(id).unwrap();
            }

            store.len()
        });

        let throughput = result.throughput(size);
        println!(
            "    {:>7} rows: {:>10} ({:>12})",
            size,
            format_duration(result.mean),
            format_throughput(throughput)
        );
        report.add_result("E2E", "crud_workflow", Some(size), result, Some(throughput));
    }
}

fn query_workflow(report: &mut Report) {
    println!("  Query Workflow (PhysicalPlanRunner):");

    for &size in &SMALL_SIZES {
        // Setup data source
        let mut ds = InMemoryDataSource::new();
        let rows: Vec<Row> = shuffle_indices(size, 12345)
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
            .collect();
        ds.add_table("users", rows, 3);
        ds.create_index("users", "idx_id", 0).unwrap();

        let threshold = (size / 2) as i64;

        // Complex query: Filter + Sort + Limit + Project
        let plan = PhysicalPlan::limit(
            PhysicalPlan::sort(
                PhysicalPlan::project(
                    PhysicalPlan::filter(
                        PhysicalPlan::table_scan("users"),
                        Expr::gt(
                            Expr::column("users", "id", 0),
                            Expr::literal(Value::Int64(threshold)),
                        ),
                    ),
                    vec![
                        Expr::column("users", "id", 0),
                        Expr::column("users", "name", 1),
                    ],
                ),
                vec![(Expr::column("users", "id", 0), SortOrder::Asc)],
            ),
            10,
            0,
        );

        let runner = PhysicalPlanRunner::new(&ds);
        let result = measure(ITERATIONS, || runner.execute(&plan).unwrap());

        let throughput = result.throughput(size);
        println!(
            "    {:>7} rows: {:>10} ({:>12})",
            size,
            format_duration(result.mean),
            format_throughput(throughput)
        );
        report.add_result("E2E", "query_workflow", Some(size), result, Some(throughput));
    }
}

fn live_query_workflow(report: &mut Report) {
    println!("  Live Query Workflow (Create + Subscribe + Update):");

    for &size in &SMALL_SIZES {
        let initial_rows: Vec<Row> = (0..size)
            .map(|i| {
                Row::new(
                    i as u64,
                    vec![Value::Int64(i as i64), Value::Int64((i % 50) as i64)],
                )
            })
            .collect();

        let result = measure(ITERATIONS, || {
            // Create observable query with filter
            let dataflow = DataflowNode::filter(DataflowNode::source(1), |row| {
                row.get(1)
                    .and_then(|v| v.as_i64())
                    .map(|age| age > 25)
                    .unwrap_or(false)
            });
            let query = Rc::new(RefCell::new(ObservableQuery::with_initial(
                dataflow,
                initial_rows.clone(),
            )));

            // Subscribe
            let count = Rc::new(RefCell::new(0));
            let count_clone = count.clone();
            query.borrow_mut().subscribe(move |_| {
                *count_clone.borrow_mut() += 1;
            });

            // Simulate 10 updates
            for i in 0..10 {
                let new_row = Row::new(
                    (size + i) as u64,
                    vec![Value::Int64((size + i) as i64), Value::Int64(35)],
                );
                let deltas = vec![Delta::insert(new_row)];
                query.borrow_mut().on_table_change(1, deltas);
            }

            let result = *count.borrow();
            result
        });

        let throughput = result.throughput(size);
        println!(
            "    {:>7} rows: {:>10} ({:>12})",
            size,
            format_duration(result.mean),
            format_throughput(throughput)
        );
        report.add_result("E2E", "live_query_workflow", Some(size), result, Some(throughput));
    }
}

fn optimized_query(report: &mut Report) {
    println!("  Optimized Query (with Optimizer):");

    for &size in &SMALL_SIZES {
        // Setup data source
        let mut ds = InMemoryDataSource::new();
        let rows: Vec<Row> = shuffle_indices(size, 12345)
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
            .collect();
        ds.add_table("users", rows.clone(), 3);
        ds.create_index("users", "idx_id", 0).unwrap();

        // Add orders table for join
        let orders: Vec<Row> = shuffle_indices(size, 67890)
            .into_iter()
            .map(|i| {
                Row::new(
                    i as u64,
                    vec![
                        Value::Int64((i % (size / 10).max(1)) as i64),
                        Value::String(format!("order_{}", i).into()),
                    ],
                )
            })
            .collect();
        ds.add_table("orders", orders, 2);
        ds.create_index("orders", "idx_user_id", 0).unwrap();

        let threshold = (size / 2) as i64;

        // Complex query with join
        let logical = LogicalPlan::limit(
            LogicalPlan::sort(
                LogicalPlan::project(
                    LogicalPlan::filter(
                        LogicalPlan::inner_join(
                            LogicalPlan::scan("users"),
                            LogicalPlan::scan("orders"),
                            Expr::eq(
                                Expr::column("users", "id", 0),
                                Expr::column("orders", "user_id", 0),
                            ),
                        ),
                        Expr::gt(
                            Expr::column("users", "id", 0),
                            Expr::literal(Value::Int64(threshold)),
                        ),
                    ),
                    vec![
                        Expr::column("users", "id", 0),
                        Expr::column("users", "name", 1),
                    ],
                ),
                vec![(Expr::column("users", "id", 0), SortOrder::Asc)],
            ),
            100,
            0,
        );

        let optimizer = Optimizer::new();
        let runner = PhysicalPlanRunner::new(&ds);

        let result = measure(ITERATIONS, || {
            let optimized = optimizer.optimize(logical.clone());
            let physical = optimizer.to_physical(optimized);
            runner.execute(&physical).unwrap()
        });

        let throughput = result.throughput(size);
        println!(
            "    {:>7} rows: {:>10} ({:>12})",
            size,
            format_duration(result.mean),
            format_throughput(throughput)
        );
        report.add_result("E2E", "optimized_query", Some(size), result, Some(throughput));
    }
}
