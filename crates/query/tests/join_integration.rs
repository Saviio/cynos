//! Integration tests for JOIN operations.
//!
//! These tests are migrated from the original Lovefield JavaScript tests:
//! - tests/proc/join_step_test.js
//! - tests/pred/join_predicate_test.js

use cynos_core::{Row, Value};
use cynos_query::ast::{ColumnRef, EvalType, JoinPredicate, JoinType};
use cynos_query::executor::join::{HashJoin, NestedLoopJoin, SortMergeJoin};
use cynos_query::executor::Relation;

/// Helper to create sample rows for TableA.
fn create_table_a_rows(count: usize) -> Vec<Row> {
    (0..count)
        .map(|i| {
            Row::new(
                i as u64,
                vec![
                    Value::Int64(i as i64),
                    Value::String(format!("dummyName{}", i)),
                ],
            )
        })
        .collect()
}

/// Helper to create sample rows for TableB.
fn create_table_b_rows(count: usize) -> Vec<Row> {
    (0..count)
        .map(|i| {
            Row::new(
                i as u64,
                vec![
                    Value::Int64(i as i64),
                    Value::String(format!("dummyName{}", i)),
                ],
            )
        })
        .collect()
}

/// Helper to create employee rows.
fn create_employee_rows(count: usize, job_count: usize, dept_count: usize) -> Vec<Row> {
    (0..count)
        .map(|i| {
            Row::new(
                i as u64,
                vec![
                    Value::Int64(i as i64),                          // id
                    Value::String(format!("Employee{}", i)),         // name
                    Value::Int64((i % job_count) as i64),            // job_id
                    Value::Int64((i % dept_count) as i64),           // department_id
                    Value::Int64(100000),                            // salary
                ],
            )
        })
        .collect()
}

/// Helper to create job rows.
fn create_job_rows(count: usize) -> Vec<Row> {
    (0..count)
        .map(|i| {
            Row::new(
                i as u64,
                vec![
                    Value::Int64(i as i64),                  // id
                    Value::String(format!("Job{}", i)),      // title
                    Value::Int64(100000),                    // min_salary
                ],
            )
        })
        .collect()
}

/// Helper to create department rows.
fn create_department_rows(count: usize) -> Vec<Row> {
    (0..count)
        .map(|i| {
            Row::new(
                i as u64,
                vec![
                    Value::Int64(i as i64),                      // id
                    Value::String(format!("Department{}", i)),   // name
                ],
            )
        })
        .collect()
}

// =============================================================================
// Tests migrated from join_step_test.js
// =============================================================================

/// Tests join for the case where the entire tableA and tableB contents are joined.
/// Migrated from: testIndexJoin_EntireTables
#[test]
fn test_join_entire_tables() {
    let table_a_rows = create_table_a_rows(3);
    let table_b_rows = create_table_b_rows(3);

    let relation_a = Relation::from_rows_owned(table_a_rows, vec!["TableA".into()]);
    let relation_b = Relation::from_rows_owned(table_b_rows, vec!["TableB".into()]);

    // Test with Hash Join
    let hash_join = HashJoin::inner(0, 0);
    let result = hash_join.execute(relation_a.clone(), relation_b.clone());
    assert_eq!(result.len(), 3);

    // Verify each joined row has matching IDs
    for entry in result.entries.iter() {
        let a_id = entry.get_field(0);
        let b_id = entry.get_field(2); // After join, TableB fields start at index 2
        assert_eq!(a_id, b_id);
    }

    // Test with Nested Loop Join
    let nested_join = NestedLoopJoin::inner(0, 0);
    let result = nested_join.execute(relation_a.clone(), relation_b.clone());
    assert_eq!(result.len(), 3);

    // Test with Sort Merge Join
    let merge_join = SortMergeJoin::inner(0, 0);
    let result = merge_join.execute(relation_a, relation_b);
    assert_eq!(result.len(), 3);
}

/// Tests join for the case where a subset of TableA is joined with the entire TableB.
/// Migrated from: testIndexJoin_PartialTable
#[test]
fn test_join_partial_table() {
    let table_a_rows = create_table_a_rows(3);
    let table_b_rows = create_table_b_rows(3);

    // Only use the last row from TableA (id=2)
    let partial_a = vec![table_a_rows[2].clone()];
    let relation_a = Relation::from_rows_owned(partial_a, vec!["TableA".into()]);
    let relation_b = Relation::from_rows_owned(table_b_rows, vec!["TableB".into()]);

    let hash_join = HashJoin::inner(0, 0);
    let result = hash_join.execute(relation_a, relation_b);

    // Should only match one row (id=2)
    assert_eq!(result.len(), 1);
    assert_eq!(result.entries[0].get_field(0), Some(&Value::Int64(2)));
}

/// Tests join for the case where an empty relation is joined.
/// Migrated from: testIndexJoin_EmptyTable
#[test]
fn test_join_empty_table() {
    let table_b_rows = create_table_b_rows(3);

    let relation_a = Relation::from_rows_owned(vec![], vec!["TableA".into()]);
    let relation_b = Relation::from_rows_owned(table_b_rows, vec!["TableB".into()]);

    let hash_join = HashJoin::inner(0, 0);
    let result = hash_join.execute(relation_a, relation_b);

    assert_eq!(result.len(), 0);
}

// =============================================================================
// Tests migrated from join_predicate_test.js
// =============================================================================

/// Tests JoinPredicate.reverse() works correctly.
/// Migrated from: testJoinPredicate_reverse
#[test]
fn test_join_predicate_reverse() {
    let left_col = ColumnRef::new("Employee", "jobId", 2);
    let right_col = ColumnRef::new("Job", "id", 0);

    // Test Lt -> Gt
    let pred_lt = JoinPredicate::new(left_col.clone(), right_col.clone(), EvalType::Lt, JoinType::Inner);
    let reversed = pred_lt.reverse();
    assert_eq!(reversed.eval_type, EvalType::Gt);
    assert_eq!(reversed.left_column.table, "Job");
    assert_eq!(reversed.right_column.table, "Employee");

    // Test Gt -> Lt
    let pred_gt = JoinPredicate::new(left_col.clone(), right_col.clone(), EvalType::Gt, JoinType::Inner);
    let reversed = pred_gt.reverse();
    assert_eq!(reversed.eval_type, EvalType::Lt);

    // Test Le -> Ge
    let pred_le = JoinPredicate::new(left_col.clone(), right_col.clone(), EvalType::Le, JoinType::Inner);
    let reversed = pred_le.reverse();
    assert_eq!(reversed.eval_type, EvalType::Ge);

    // Test Ge -> Le
    let pred_ge = JoinPredicate::new(left_col.clone(), right_col.clone(), EvalType::Ge, JoinType::Inner);
    let reversed = pred_ge.reverse();
    assert_eq!(reversed.eval_type, EvalType::Le);

    // Test Eq -> Eq (symmetric)
    let pred_eq = JoinPredicate::new(left_col.clone(), right_col.clone(), EvalType::Eq, JoinType::Inner);
    let reversed = pred_eq.reverse();
    assert_eq!(reversed.eval_type, EvalType::Eq);

    // Test Ne -> Ne (symmetric)
    let pred_ne = JoinPredicate::new(left_col, right_col, EvalType::Ne, JoinType::Inner);
    let reversed = pred_ne.reverse();
    assert_eq!(reversed.eval_type, EvalType::Ne);
}

/// Tests that evalRelations() detects which input relation should be used as "left" and "right".
/// Migrated from: testJoinPredicate_RelationsInputOrder
#[test]
fn test_join_relations_input_order() {
    let employees = create_employee_rows(1, 1, 1);
    let jobs = create_job_rows(1);

    let employee_relation = Relation::from_rows_owned(employees, vec!["Employee".into()]);
    let job_relation = Relation::from_rows_owned(jobs, vec!["Job".into()]);

    // Test Hash Join with both orderings
    let hash_join = HashJoin::inner(2, 0); // employee.job_id = job.id
    let result1 = hash_join.execute(employee_relation.clone(), job_relation.clone());
    assert_eq!(result1.len(), 1);

    // Reverse the input order - should still work
    let hash_join_rev = HashJoin::inner(0, 2);
    let result2 = hash_join_rev.execute(job_relation.clone(), employee_relation.clone());
    assert_eq!(result2.len(), 1);
}

/// Tests join with unique keys.
/// Migrated from: checkEvalRelations_UniqueKeys
#[test]
fn test_join_unique_keys() {
    let employee_count = 60;
    let job_count = 6;
    let dept_count = 3;

    let employees = create_employee_rows(employee_count, job_count, dept_count);
    let jobs = create_job_rows(job_count);

    let employee_relation = Relation::from_rows_owned(employees, vec!["Employee".into()]);
    let job_relation = Relation::from_rows_owned(jobs, vec!["Job".into()]);

    // Join on employee.job_id = job.id
    let hash_join = HashJoin::inner(2, 0);
    let result = hash_join.execute(employee_relation.clone(), job_relation.clone());
    assert_eq!(result.len(), employee_count);

    // Join on employee.id = job.id (only 6 matches since there are 6 jobs)
    let hash_join2 = HashJoin::inner(0, 0);
    let result2 = hash_join2.execute(employee_relation, job_relation);
    assert_eq!(result2.len(), job_count);
}

/// Tests join with non-unique keys.
/// Migrated from: checkEvalRelations_NonUniqueKeys
#[test]
fn test_join_non_unique_keys() {
    let employee_count = 60;
    let job_count = 6;
    let dept_count = 3;

    let employees = create_employee_rows(employee_count, job_count, dept_count);
    let jobs = create_job_rows(job_count);

    let employee_relation = Relation::from_rows_owned(employees, vec!["Employee".into()]);
    let job_relation = Relation::from_rows_owned(jobs, vec!["Job".into()]);

    // Join on employee.salary = job.min_salary (all have same salary)
    // This should produce a cartesian product since all salaries match
    let hash_join = HashJoin::inner(4, 2);
    let result = hash_join.execute(employee_relation, job_relation);
    assert_eq!(result.len(), employee_count * job_count);
}

/// Tests outer join with unique keys.
/// Migrated from: checkEvalRelations_OuterJoin_UniqueKeys
#[test]
fn test_outer_join_unique_keys() {
    let employee_count = 60;
    let job_count = 6;
    let dept_count = 3;

    let employees = create_employee_rows(employee_count, job_count, dept_count);
    // Remove the last job
    let jobs = create_job_rows(job_count - 1);

    let employee_relation = Relation::from_rows_owned(employees, vec!["Employee".into()]);
    let job_relation = Relation::from_rows_owned(jobs, vec!["Job".into()]);

    // Left outer join on employee.job_id = job.id
    let hash_join = HashJoin::left_outer(2, 0);
    let result = hash_join.execute(employee_relation, job_relation);

    // All employees should be in result
    assert_eq!(result.len(), employee_count);

    // Count entries with null job fields (employees with job_id = 5)
    let null_count = result
        .entries
        .iter()
        .filter(|e| {
            // Check if job fields are null (fields after employee fields)
            e.get_field(5).map_or(true, |v| v.is_null())
        })
        .count();

    // 10 employees per job, so 10 should have null job fields
    assert_eq!(null_count, 10);
}

/// Tests multi-table join (3 tables).
/// Migrated from: checkEvalRelations_MultiJoin
#[test]
fn test_multi_join() {
    let employee_count = 60;
    let job_count = 6;
    let dept_count = 3;

    let employees = create_employee_rows(employee_count, job_count, dept_count);
    let jobs = create_job_rows(job_count);
    let departments = create_department_rows(dept_count);

    let employee_relation = Relation::from_rows_owned(employees, vec!["Employee".into()]);
    let job_relation = Relation::from_rows_owned(jobs, vec!["Job".into()]);
    let department_relation = Relation::from_rows_owned(departments, vec!["Department".into()]);

    // First join: employee.job_id = job.id
    let hash_join1 = HashJoin::inner(2, 0);
    let result_emp_job = hash_join1.execute(employee_relation, job_relation);
    assert_eq!(result_emp_job.len(), employee_count);

    // Second join: (employee+job).department_id = department.id
    // After first join, department_id is at index 3 in the combined row
    let hash_join2 = HashJoin::inner(3, 0);
    let result_emp_job_dept = hash_join2.execute(result_emp_job, department_relation);
    assert_eq!(result_emp_job_dept.len(), employee_count);
}

/// Tests join with nullable keys - null values should not match.
/// Migrated from: checkEvalRelations_NullableKeys
#[test]
fn test_join_nullable_keys() {
    // Create rows with some null values
    let table_a_rows = vec![
        Row::new(0, vec![Value::Int64(1)]),
        Row::new(1, vec![Value::Int64(2)]),
        Row::new(2, vec![Value::Null]),  // Null key
        Row::new(3, vec![Value::Int64(3)]),
    ];

    let table_b_rows = vec![
        Row::new(0, vec![Value::Int64(1)]),
        Row::new(1, vec![Value::Int64(2)]),
        Row::new(2, vec![Value::Int64(3)]),
    ];

    let relation_a = Relation::from_rows_owned(table_a_rows, vec!["TableA".into()]);
    let relation_b = Relation::from_rows_owned(table_b_rows, vec!["TableB".into()]);

    let hash_join = HashJoin::inner(0, 0);
    let result = hash_join.execute(relation_a, relation_b);

    // Should only match 3 rows (null doesn't match anything)
    assert_eq!(result.len(), 3);
}

/// Tests that all three join algorithms produce the same results.
#[test]
fn test_join_algorithms_consistency() {
    let employee_count = 20;
    let job_count = 4;
    let dept_count = 2;

    let employees = create_employee_rows(employee_count, job_count, dept_count);
    let jobs = create_job_rows(job_count);

    let employee_relation = Relation::from_rows_owned(employees, vec!["Employee".into()]);
    let job_relation = Relation::from_rows_owned(jobs, vec!["Job".into()]);

    // Hash Join
    let hash_join = HashJoin::inner(2, 0);
    let hash_result = hash_join.execute(employee_relation.clone(), job_relation.clone());

    // Nested Loop Join
    let nested_join = NestedLoopJoin::inner(2, 0);
    let nested_result = nested_join.execute(employee_relation.clone(), job_relation.clone());

    // Sort Merge Join (use execute_with_sort since data is not pre-sorted by job_id)
    let merge_join = SortMergeJoin::inner(2, 0);
    let merge_result = merge_join.execute_with_sort(employee_relation, job_relation);

    // All should have the same number of results
    assert_eq!(hash_result.len(), nested_result.len());
    assert_eq!(hash_result.len(), merge_result.len());
    assert_eq!(hash_result.len(), employee_count);
}

/// Tests range join with nested loop (non-equi join).
#[test]
fn test_range_join() {
    let table_a_rows = vec![
        Row::new(0, vec![Value::Int64(10)]),
        Row::new(1, vec![Value::Int64(20)]),
        Row::new(2, vec![Value::Int64(30)]),
    ];

    let table_b_rows = vec![
        Row::new(0, vec![Value::Int64(15)]),
        Row::new(1, vec![Value::Int64(25)]),
    ];

    let relation_a = Relation::from_rows_owned(table_a_rows, vec!["TableA".into()]);
    let relation_b = Relation::from_rows_owned(table_b_rows, vec!["TableB".into()]);

    // Join where TableA.value < TableB.value
    let nested_join = NestedLoopJoin::inner(0, 0);
    let result = nested_join.execute_with_predicate(relation_a, relation_b, |a, b| a < b);

    // Expected matches:
    // 10 < 15 ✓, 10 < 25 ✓
    // 20 < 25 ✓
    // 30 < nothing
    assert_eq!(result.len(), 3);
}
