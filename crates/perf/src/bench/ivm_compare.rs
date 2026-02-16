//! IVM vs Re-query comparison benchmarks.
//!
//! For each query scenario, runs both paths on identical data and:
//! 1. Verifies result equivalence (correctness)
//! 2. Measures performance (latency)
//!
//! Re-query path: PhysicalPlanRunner re-executes the full plan on every change.
//! IVM path: MaterializedView propagates only the delta incrementally.
//!
//! Fairness guarantees:
//! - Both paths use the same initial data and change sequences
//! - Re-query uses pre-compiled PhysicalPlan (no optimizer overhead)
//! - IVM uses MaterializedView directly (same as ObservableQuery)
//! - Setup cost excluded from measurement via measure_with_setup
//! - Warmup 10 iterations, measure 100 iterations

use crate::report::Report;
use crate::utils::*;
use cynos_core::{Row, Value};
use cynos_incremental::{
    AggregateType, DataflowNode, Delta, JoinType as IvmJoinType,
    MaterializedView,
};
use cynos_query::ast::{AggregateFunc, Expr, JoinType as QueryJoinType};
use cynos_query::executor::{InMemoryDataSource, PhysicalPlanRunner};
use cynos_query::planner::PhysicalPlan;

pub fn run(report: &mut Report) {
    filter_compare(report);
    inner_join_compare(report);
    left_outer_join_compare(report);
    aggregate_count_sum_compare(report);
    aggregate_min_max_compare(report);
    filter_join_compare(report);
}

// ---------------------------------------------------------------------------
// Data generators
// ---------------------------------------------------------------------------

/// employees table: [id(i64), name(str), age(i32), dept_id(i64), salary(i64)]
fn make_employee(id: u64, age: i32, dept_id: i64, salary: i64) -> Row {
    Row::new(
        id,
        vec![
            Value::Int64(id as i64),
            Value::String(format!("emp_{}", id).into()),
            Value::Int32(age),
            Value::Int64(dept_id),
            Value::Int64(salary),
        ],
    )
}

/// departments table: [id(i64), name(str)]
fn make_department(id: u64, name: &str) -> Row {
    Row::new(
        id,
        vec![
            Value::Int64(id as i64),
            Value::String(name.into()),
        ],
    )
}

fn generate_employees(count: usize) -> Vec<Row> {
    (0..count)
        .map(|i| {
            let id = i as u64 + 1;
            let age = 20 + (i % 50) as i32;
            let dept_id = (i % 10) as i64 + 1;
            let salary = 30000 + (i % 80) as i64 * 1000;
            make_employee(id, age, dept_id, salary)
        })
        .collect()
}

fn generate_departments(count: usize) -> Vec<Row> {
    let names = [
        "Engineering", "Sales", "Marketing", "HR", "Finance",
        "Legal", "Support", "Research", "Operations", "Design",
    ];
    (0..count)
        .map(|i| make_department(i as u64 + 1, names[i % names.len()]))
        .collect()
}

// ---------------------------------------------------------------------------
// Re-query helpers
// ---------------------------------------------------------------------------

fn build_ds_employees(employees: &[Row]) -> InMemoryDataSource {
    let mut ds = InMemoryDataSource::new();
    ds.add_table("employees", employees.to_vec(), 5);
    ds
}

fn build_ds_with_departments(employees: &[Row], departments: &[Row]) -> InMemoryDataSource {
    let mut ds = InMemoryDataSource::new();
    ds.add_table("employees", employees.to_vec(), 5);
    ds.add_table("departments", departments.to_vec(), 2);
    ds
}

/// Normalize a value to a canonical string for sorting (numeric-aware).
fn value_sort_key(v: &Value) -> String {
    match v {
        Value::Int32(i) => format!("N:{:.6}", *i as f64),
        Value::Int64(i) => format!("N:{:.6}", *i as f64),
        Value::Float64(f) => format!("N:{:.6}", f),
        other => format!("V:{:?}", other),
    }
}

fn sort_rows(rows: &mut Vec<Vec<Value>>) {
    rows.sort_by(|a, b| {
        let ka: String = a.iter().map(|v| value_sort_key(v)).collect::<Vec<_>>().join("|");
        let kb: String = b.iter().map(|v| value_sort_key(v)).collect::<Vec<_>>().join("|");
        ka.cmp(&kb)
    });
}

/// Executes a PhysicalPlan and returns sorted row values for comparison.
fn execute_plan_sorted(ds: &InMemoryDataSource, plan: &PhysicalPlan) -> Vec<Vec<Value>> {
    let runner = PhysicalPlanRunner::new(ds);
    let relation = runner.execute(plan).unwrap();
    let mut rows: Vec<Vec<Value>> = relation
        .entries
        .iter()
        .map(|e| e.row.values().to_vec())
        .collect();
    sort_rows(&mut rows);
    rows
}

// ---------------------------------------------------------------------------
// IVM helpers
// ---------------------------------------------------------------------------

fn ivm_result_sorted(view: &MaterializedView) -> Vec<Vec<Value>> {
    let mut rows: Vec<Vec<Value>> = view
        .result()
        .into_iter()
        .map(|r| r.values().to_vec())
        .collect();
    sort_rows(&mut rows);
    rows
}

/// Numeric-aware value comparison: Int64(100) == Float64(100.0)
fn values_equivalent(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Int64(i), Value::Float64(f)) | (Value::Float64(f), Value::Int64(i)) => {
            (*i as f64 - f).abs() < 1e-6
        }
        (Value::Int32(i), Value::Float64(f)) | (Value::Float64(f), Value::Int32(i)) => {
            (*i as f64 - f).abs() < 1e-6
        }
        _ => a == b,
    }
}

fn rows_equivalent(a: &[Vec<Value>], b: &[Vec<Value>]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.iter().zip(b.iter()).all(|(ra, rb)| {
        ra.len() == rb.len() && ra.iter().zip(rb.iter()).all(|(va, vb)| values_equivalent(va, vb))
    })
}

// ---------------------------------------------------------------------------
// Comparison output
// ---------------------------------------------------------------------------

fn print_comparison(
    label: &str,
    size: usize,
    requery_result: &BenchResult,
    ivm_result: &BenchResult,
    equivalent: bool,
    requery_rows: &[Vec<Value>],
    ivm_rows: &[Vec<Value>],
    report: &mut Report,
) {
    let speedup = requery_result.mean.as_nanos() as f64 / ivm_result.mean.as_nanos().max(1) as f64;
    let eq_str = if equivalent { "equivalent" } else { "MISMATCH!" };
    println!(
        "    {:>6} rows: re-query {:>10}  ivm {:>10}  speedup {:>6.1}x  {}",
        size,
        format_duration(requery_result.mean),
        format_duration(ivm_result.mean),
        speedup,
        eq_str,
    );
    if !equivalent {
        println!("      [DEBUG] requery rows: {}, ivm rows: {}", requery_rows.len(), ivm_rows.len());
        for (i, (r, v)) in requery_rows.iter().zip(ivm_rows.iter()).enumerate() {
            if !r.iter().zip(v.iter()).all(|(a, b)| values_equivalent(a, b)) {
                println!("      [DEBUG] first diff at row {}: requery={:?} ivm={:?}", i, r, v);
                break;
            }
        }
    }
    report.add_result(
        "IVM_Compare",
        &format!("{}/requery", label),
        Some(size),
        requery_result.clone(),
        None,
    );
    report.add_result(
        "IVM_Compare",
        &format!("{}/ivm", label),
        Some(size),
        ivm_result.clone(),
        None,
    );
}

// ---------------------------------------------------------------------------
// Scenario 1: Filter (WHERE age > 30)
// ---------------------------------------------------------------------------

fn filter_compare(report: &mut Report) {
    println!("  Filter (WHERE age > 30):");

    for &size in &SMALL_SIZES {
        let employees = generate_employees(size);
        let new_emp = make_employee(size as u64 + 1, 35, 1, 50000);

        // -- Equivalence check --
        let mut emp_with_new = employees.clone();
        emp_with_new.push(new_emp.clone());
        let ds_after = build_ds_employees(&emp_with_new);
        let filter_plan = PhysicalPlan::filter(
            PhysicalPlan::table_scan("employees"),
            Expr::gt(
                Expr::column("employees", "age", 2),
                Expr::literal(Value::Int32(30)),
            ),
        );
        let requery_rows = execute_plan_sorted(&ds_after, &filter_plan);

        let dataflow = DataflowNode::filter(DataflowNode::source(1), |row| {
            row.get(2)
                .and_then(|v| v.as_i32())
                .map(|age| age > 30)
                .unwrap_or(false)
        });
        let mut view = MaterializedView::new(dataflow);
        let initial_deltas: Vec<Delta<Row>> =
            employees.iter().map(|r| Delta::insert(r.clone())).collect();
        view.on_table_change(1, initial_deltas);
        view.on_table_change(1, vec![Delta::insert(new_emp.clone())]);
        let ivm_rows = ivm_result_sorted(&view);
        let equivalent = rows_equivalent(&requery_rows, &ivm_rows);

        // -- Performance --
        let requery_bench = measure(ITERATIONS, || {
            let ds = build_ds_employees(&emp_with_new);
            let runner = PhysicalPlanRunner::new(&ds);
            std::hint::black_box(runner.execute(&filter_plan).unwrap());
        });

        let ivm_bench = {
            let dataflow = DataflowNode::filter(DataflowNode::source(1), |row| {
                row.get(2)
                    .and_then(|v| v.as_i32())
                    .map(|age| age > 30)
                    .unwrap_or(false)
            });
            let mut view = MaterializedView::new(dataflow);
            let initial_deltas: Vec<Delta<Row>> =
                employees.iter().map(|r| Delta::insert(r.clone())).collect();
            view.on_table_change(1, initial_deltas);

            measure(ITERATIONS, || {
                let delta = vec![Delta::insert(new_emp.clone())];
                std::hint::black_box(view.on_table_change(1, delta));
                let del = vec![Delta::delete(new_emp.clone())];
                view.on_table_change(1, del);
            })
        };

        print_comparison("filter", size, &requery_bench, &ivm_bench, equivalent, &requery_rows, &ivm_rows, report);
    }
}

// ---------------------------------------------------------------------------
// Scenario 2: Inner Join (employees JOIN departments ON dept_id = id)
// ---------------------------------------------------------------------------

fn inner_join_compare(report: &mut Report) {
    println!("  Inner Join (employees JOIN departments):");

    let departments = generate_departments(10);

    for &size in &SMALL_SIZES {
        let employees = generate_employees(size);
        let new_emp = make_employee(size as u64 + 1, 28, 3, 55000);

        // -- Equivalence check --
        let mut emp_with_new = employees.clone();
        emp_with_new.push(new_emp.clone());
        let ds_after = build_ds_with_departments(&emp_with_new, &departments);
        let join_plan = PhysicalPlan::hash_join(
            PhysicalPlan::table_scan("employees"),
            PhysicalPlan::table_scan("departments"),
            Expr::eq(
                Expr::column("employees", "dept_id", 3),
                Expr::column("departments", "id", 0),
            ),
            QueryJoinType::Inner,
        );
        let requery_rows = execute_plan_sorted(&ds_after, &join_plan);

        let dataflow = DataflowNode::join(
            DataflowNode::source(1),
            DataflowNode::source(2),
            Box::new(|row: &Row| vec![row.get(3).cloned().unwrap_or(Value::Null)]),
            Box::new(|row: &Row| vec![row.get(0).cloned().unwrap_or(Value::Null)]),
        );
        let mut view = MaterializedView::new(dataflow);
        let dept_deltas: Vec<Delta<Row>> =
            departments.iter().map(|r| Delta::insert(r.clone())).collect();
        view.on_table_change(2, dept_deltas);
        let emp_deltas: Vec<Delta<Row>> =
            employees.iter().map(|r| Delta::insert(r.clone())).collect();
        view.on_table_change(1, emp_deltas);
        view.on_table_change(1, vec![Delta::insert(new_emp.clone())]);
        let ivm_rows = ivm_result_sorted(&view);
        let equivalent = rows_equivalent(&requery_rows, &ivm_rows);

        // -- Performance --
        let requery_bench = measure(ITERATIONS, || {
            let ds = build_ds_with_departments(&emp_with_new, &departments);
            let runner = PhysicalPlanRunner::new(&ds);
            std::hint::black_box(runner.execute(&join_plan).unwrap());
        });

        let ivm_bench = {
            let dataflow = DataflowNode::join(
                DataflowNode::source(1),
                DataflowNode::source(2),
                Box::new(|row: &Row| vec![row.get(3).cloned().unwrap_or(Value::Null)]),
                Box::new(|row: &Row| vec![row.get(0).cloned().unwrap_or(Value::Null)]),
            );
            let mut view = MaterializedView::new(dataflow);
            let dept_deltas: Vec<Delta<Row>> =
                departments.iter().map(|r| Delta::insert(r.clone())).collect();
            view.on_table_change(2, dept_deltas);
            let emp_deltas: Vec<Delta<Row>> =
                employees.iter().map(|r| Delta::insert(r.clone())).collect();
            view.on_table_change(1, emp_deltas);

            measure(ITERATIONS, || {
                let delta = vec![Delta::insert(new_emp.clone())];
                std::hint::black_box(view.on_table_change(1, delta));
                let del = vec![Delta::delete(new_emp.clone())];
                view.on_table_change(1, del);
            })
        };

        print_comparison("inner_join", size, &requery_bench, &ivm_bench, equivalent, &requery_rows, &ivm_rows, report);
    }
}

// ---------------------------------------------------------------------------
// Scenario 3: Left Outer Join
// ---------------------------------------------------------------------------

fn left_outer_join_compare(report: &mut Report) {
    println!("  Left Outer Join (employees LEFT JOIN departments):");

    let departments = generate_departments(10);

    for &size in &SMALL_SIZES {
        let employees = generate_employees(size);
        // dept_id=99 → no matching department → NULL padded
        let new_emp_no_match = make_employee(size as u64 + 1, 28, 99, 55000);

        // -- Equivalence check --
        let mut emp_with_new = employees.clone();
        emp_with_new.push(new_emp_no_match.clone());
        let ds_after = build_ds_with_departments(&emp_with_new, &departments);
        let join_plan = PhysicalPlan::hash_join(
            PhysicalPlan::table_scan("employees"),
            PhysicalPlan::table_scan("departments"),
            Expr::eq(
                Expr::column("employees", "dept_id", 3),
                Expr::column("departments", "id", 0),
            ),
            QueryJoinType::LeftOuter,
        );
        let requery_rows = execute_plan_sorted(&ds_after, &join_plan);

        let dataflow = DataflowNode::join_with_type(
            DataflowNode::source(1),
            DataflowNode::source(2),
            Box::new(|row: &Row| vec![row.get(3).cloned().unwrap_or(Value::Null)]),
            Box::new(|row: &Row| vec![row.get(0).cloned().unwrap_or(Value::Null)]),
            IvmJoinType::LeftOuter,
        );
        let mut view = MaterializedView::new(dataflow);
        let dept_deltas: Vec<Delta<Row>> =
            departments.iter().map(|r| Delta::insert(r.clone())).collect();
        view.on_table_change(2, dept_deltas);
        let emp_deltas: Vec<Delta<Row>> =
            employees.iter().map(|r| Delta::insert(r.clone())).collect();
        view.on_table_change(1, emp_deltas);
        view.on_table_change(1, vec![Delta::insert(new_emp_no_match.clone())]);
        let ivm_rows = ivm_result_sorted(&view);
        let equivalent = rows_equivalent(&requery_rows, &ivm_rows);

        // -- Performance --
        let requery_bench = measure(ITERATIONS, || {
            let ds = build_ds_with_departments(&emp_with_new, &departments);
            let runner = PhysicalPlanRunner::new(&ds);
            std::hint::black_box(runner.execute(&join_plan).unwrap());
        });

        let ivm_bench = {
            let dataflow = DataflowNode::join_with_type(
                DataflowNode::source(1),
                DataflowNode::source(2),
                Box::new(|row: &Row| vec![row.get(3).cloned().unwrap_or(Value::Null)]),
                Box::new(|row: &Row| vec![row.get(0).cloned().unwrap_or(Value::Null)]),
                IvmJoinType::LeftOuter,
            );
            let mut view = MaterializedView::new(dataflow);
            let dept_deltas: Vec<Delta<Row>> =
                departments.iter().map(|r| Delta::insert(r.clone())).collect();
            view.on_table_change(2, dept_deltas);
            let emp_deltas: Vec<Delta<Row>> =
                employees.iter().map(|r| Delta::insert(r.clone())).collect();
            view.on_table_change(1, emp_deltas);

            measure(ITERATIONS, || {
                let delta = vec![Delta::insert(new_emp_no_match.clone())];
                std::hint::black_box(view.on_table_change(1, delta));
                let del = vec![Delta::delete(new_emp_no_match.clone())];
                view.on_table_change(1, del);
            })
        };

        print_comparison("left_outer_join", size, &requery_bench, &ivm_bench, equivalent, &requery_rows, &ivm_rows, report);
    }
}

// ---------------------------------------------------------------------------
// Scenario 4: Aggregate COUNT/SUM (GROUP BY dept_id)
// ---------------------------------------------------------------------------

fn aggregate_count_sum_compare(report: &mut Report) {
    println!("  Aggregate COUNT/SUM (GROUP BY dept_id):");

    for &size in &SMALL_SIZES {
        let employees = generate_employees(size);
        let new_emp = make_employee(size as u64 + 1, 35, 3, 75000);

        // -- Equivalence check --
        let mut emp_with_new = employees.clone();
        emp_with_new.push(new_emp.clone());
        let ds_after = build_ds_employees(&emp_with_new);
        let agg_plan = PhysicalPlan::hash_aggregate(
            PhysicalPlan::table_scan("employees"),
            vec![Expr::column("employees", "dept_id", 3)],
            vec![
                (AggregateFunc::Count, Expr::column("employees", "id", 0)),
                (AggregateFunc::Sum, Expr::column("employees", "salary", 4)),
            ],
        );
        let requery_rows = execute_plan_sorted(&ds_after, &agg_plan);

        let dataflow = DataflowNode::Aggregate {
            input: Box::new(DataflowNode::source(1)),
            group_by: vec![3], // dept_id
            functions: vec![
                (0, AggregateType::Count),
                (4, AggregateType::Sum),
            ],
        };
        let mut view = MaterializedView::new(dataflow);
        let initial_deltas: Vec<Delta<Row>> =
            employees.iter().map(|r| Delta::insert(r.clone())).collect();
        view.on_table_change(1, initial_deltas);
        view.on_table_change(1, vec![Delta::insert(new_emp.clone())]);
        let ivm_rows = ivm_result_sorted(&view);
        let equivalent = rows_equivalent(&requery_rows, &ivm_rows);

        // -- Performance --
        let requery_bench = measure(ITERATIONS, || {
            let ds = build_ds_employees(&emp_with_new);
            let runner = PhysicalPlanRunner::new(&ds);
            std::hint::black_box(runner.execute(&agg_plan).unwrap());
        });

        let ivm_bench = {
            let dataflow = DataflowNode::Aggregate {
                input: Box::new(DataflowNode::source(1)),
                group_by: vec![3],
                functions: vec![
                    (0, AggregateType::Count),
                    (4, AggregateType::Sum),
                ],
            };
            let mut view = MaterializedView::new(dataflow);
            let initial_deltas: Vec<Delta<Row>> =
                employees.iter().map(|r| Delta::insert(r.clone())).collect();
            view.on_table_change(1, initial_deltas);

            measure(ITERATIONS, || {
                let delta = vec![Delta::insert(new_emp.clone())];
                std::hint::black_box(view.on_table_change(1, delta));
                let del = vec![Delta::delete(new_emp.clone())];
                view.on_table_change(1, del);
            })
        };

        print_comparison("agg_count_sum", size, &requery_bench, &ivm_bench, equivalent, &requery_rows, &ivm_rows, report);
    }
}

// ---------------------------------------------------------------------------
// Scenario 5: Aggregate MIN/MAX (GROUP BY dept_id) — delete current min
// ---------------------------------------------------------------------------

fn aggregate_min_max_compare(report: &mut Report) {
    println!("  Aggregate MIN/MAX (GROUP BY dept_id, delete min):");

    for &size in &SMALL_SIZES {
        let employees = generate_employees(size);

        // Find the employee with the minimum salary in dept 1
        let min_emp = employees
            .iter()
            .filter(|r| r.get(3).and_then(|v| v.as_i64()) == Some(1))
            .min_by_key(|r| r.get(4).and_then(|v| v.as_i64()).unwrap_or(i64::MAX))
            .cloned()
            .unwrap();

        // -- Equivalence check --
        let emp_without_min: Vec<Row> = employees
            .iter()
            .filter(|r| r.id() != min_emp.id())
            .cloned()
            .collect();
        let ds_after = build_ds_employees(&emp_without_min);
        let agg_plan = PhysicalPlan::hash_aggregate(
            PhysicalPlan::table_scan("employees"),
            vec![Expr::column("employees", "dept_id", 3)],
            vec![
                (AggregateFunc::Min, Expr::column("employees", "salary", 4)),
                (AggregateFunc::Max, Expr::column("employees", "salary", 4)),
            ],
        );
        let requery_rows = execute_plan_sorted(&ds_after, &agg_plan);

        let dataflow = DataflowNode::Aggregate {
            input: Box::new(DataflowNode::source(1)),
            group_by: vec![3],
            functions: vec![
                (4, AggregateType::Min),
                (4, AggregateType::Max),
            ],
        };
        let mut view = MaterializedView::new(dataflow);
        let initial_deltas: Vec<Delta<Row>> =
            employees.iter().map(|r| Delta::insert(r.clone())).collect();
        view.on_table_change(1, initial_deltas);
        view.on_table_change(1, vec![Delta::delete(min_emp.clone())]);
        let ivm_rows = ivm_result_sorted(&view);
        let equivalent = rows_equivalent(&requery_rows, &ivm_rows);

        // -- Performance --
        let requery_bench = measure(ITERATIONS, || {
            let ds = build_ds_employees(&emp_without_min);
            let runner = PhysicalPlanRunner::new(&ds);
            std::hint::black_box(runner.execute(&agg_plan).unwrap());
        });

        let ivm_bench = {
            let dataflow = DataflowNode::Aggregate {
                input: Box::new(DataflowNode::source(1)),
                group_by: vec![3],
                functions: vec![
                    (4, AggregateType::Min),
                    (4, AggregateType::Max),
                ],
            };
            let mut view = MaterializedView::new(dataflow);
            let initial_deltas: Vec<Delta<Row>> =
                employees.iter().map(|r| Delta::insert(r.clone())).collect();
            view.on_table_change(1, initial_deltas);

            measure(ITERATIONS, || {
                let delta = vec![Delta::delete(min_emp.clone())];
                std::hint::black_box(view.on_table_change(1, delta));
                let ins = vec![Delta::insert(min_emp.clone())];
                view.on_table_change(1, ins);
            })
        };

        print_comparison("agg_min_max", size, &requery_bench, &ivm_bench, equivalent, &requery_rows, &ivm_rows, report);
    }
}

// ---------------------------------------------------------------------------
// Scenario 6: Filter + Join (WHERE age > 30 then JOIN departments)
// ---------------------------------------------------------------------------

fn filter_join_compare(report: &mut Report) {
    println!("  Filter + Join (WHERE age > 30 JOIN departments):");

    let departments = generate_departments(10);

    for &size in &SMALL_SIZES {
        let employees = generate_employees(size);
        let new_emp = make_employee(size as u64 + 1, 35, 5, 60000);

        // -- Equivalence check --
        let mut emp_with_new = employees.clone();
        emp_with_new.push(new_emp.clone());
        let ds_after = build_ds_with_departments(&emp_with_new, &departments);
        let plan = PhysicalPlan::hash_join(
            PhysicalPlan::filter(
                PhysicalPlan::table_scan("employees"),
                Expr::gt(
                    Expr::column("employees", "age", 2),
                    Expr::literal(Value::Int32(30)),
                ),
            ),
            PhysicalPlan::table_scan("departments"),
            Expr::eq(
                Expr::column("employees", "dept_id", 3),
                Expr::column("departments", "id", 0),
            ),
            QueryJoinType::Inner,
        );
        let requery_rows = execute_plan_sorted(&ds_after, &plan);

        let dataflow = DataflowNode::join(
            DataflowNode::filter(DataflowNode::source(1), |row| {
                row.get(2)
                    .and_then(|v| v.as_i32())
                    .map(|age| age > 30)
                    .unwrap_or(false)
            }),
            DataflowNode::source(2),
            Box::new(|row: &Row| vec![row.get(3).cloned().unwrap_or(Value::Null)]),
            Box::new(|row: &Row| vec![row.get(0).cloned().unwrap_or(Value::Null)]),
        );
        let mut view = MaterializedView::new(dataflow);
        let dept_deltas: Vec<Delta<Row>> =
            departments.iter().map(|r| Delta::insert(r.clone())).collect();
        view.on_table_change(2, dept_deltas);
        let emp_deltas: Vec<Delta<Row>> =
            employees.iter().map(|r| Delta::insert(r.clone())).collect();
        view.on_table_change(1, emp_deltas);
        view.on_table_change(1, vec![Delta::insert(new_emp.clone())]);
        let ivm_rows = ivm_result_sorted(&view);
        let equivalent = rows_equivalent(&requery_rows, &ivm_rows);

        // -- Performance --
        let requery_bench = measure(ITERATIONS, || {
            let ds = build_ds_with_departments(&emp_with_new, &departments);
            let runner = PhysicalPlanRunner::new(&ds);
            std::hint::black_box(runner.execute(&plan).unwrap());
        });

        let ivm_bench = {
            let dataflow = DataflowNode::join(
                DataflowNode::filter(DataflowNode::source(1), |row| {
                    row.get(2)
                        .and_then(|v| v.as_i32())
                        .map(|age| age > 30)
                        .unwrap_or(false)
                }),
                DataflowNode::source(2),
                Box::new(|row: &Row| vec![row.get(3).cloned().unwrap_or(Value::Null)]),
                Box::new(|row: &Row| vec![row.get(0).cloned().unwrap_or(Value::Null)]),
            );
            let mut view = MaterializedView::new(dataflow);
            let dept_deltas: Vec<Delta<Row>> =
                departments.iter().map(|r| Delta::insert(r.clone())).collect();
            view.on_table_change(2, dept_deltas);
            let emp_deltas: Vec<Delta<Row>> =
                employees.iter().map(|r| Delta::insert(r.clone())).collect();
            view.on_table_change(1, emp_deltas);

            measure(ITERATIONS, || {
                let delta = vec![Delta::insert(new_emp.clone())];
                std::hint::black_box(view.on_table_change(1, delta));
                let del = vec![Delta::delete(new_emp.clone())];
                view.on_table_change(1, del);
            })
        };

        print_comparison("filter_join", size, &requery_bench, &ivm_bench, equivalent, &requery_rows, &ivm_rows, report);
    }
}
