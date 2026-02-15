//! Property-based tests for JOIN algorithms.
//!
//! These tests verify that all JOIN algorithms produce consistent results
//! for randomly generated inputs.

use cynos_core::{Row, Value};
use cynos_query::executor::join::{HashJoin, NestedLoopJoin, SortMergeJoin};
use cynos_query::executor::Relation;
use proptest::prelude::*;
use std::collections::HashSet;

/// Strategy for generating random i64 values within a reasonable range.
fn value_strategy() -> impl Strategy<Value = i64> {
    -1000i64..1000i64
}

/// Strategy for generating a vector of rows with a single key column.
fn rows_strategy(max_rows: usize) -> impl Strategy<Value = Vec<Row>> {
    prop::collection::vec(value_strategy(), 0..max_rows).prop_map(|values| {
        values
            .into_iter()
            .enumerate()
            .map(|(i, v)| Row::new(i as u64, vec![Value::Int64(v)]))
            .collect()
    })
}

/// Strategy for generating rows with multiple columns.
fn multi_column_rows_strategy(max_rows: usize) -> impl Strategy<Value = Vec<Row>> {
    prop::collection::vec(
        (value_strategy(), value_strategy()),
        0..max_rows,
    )
    .prop_map(|values| {
        values
            .into_iter()
            .enumerate()
            .map(|(i, (k, v))| Row::new(i as u64, vec![Value::Int64(k), Value::Int64(v)]))
            .collect()
    })
}

/// Extracts the set of (left_key, right_key) pairs from join results.
fn extract_key_pairs(result: &Relation, left_idx: usize, right_idx: usize) -> HashSet<(i64, i64)> {
    result
        .entries
        .iter()
        .filter_map(|e| {
            let left = e.get_field(left_idx).and_then(|v| v.as_i64())?;
            let right = e.get_field(right_idx).and_then(|v| v.as_i64())?;
            Some((left, right))
        })
        .collect()
}

proptest! {
    /// Property: Hash join and nested loop join produce the same results for inner joins.
    #[test]
    fn hash_join_equals_nested_loop_join(
        left_rows in rows_strategy(50),
        right_rows in rows_strategy(50),
    ) {
        let left = Relation::from_rows_owned(left_rows, vec!["left".into()]);
        let right = Relation::from_rows_owned(right_rows, vec!["right".into()]);

        let hash_join = HashJoin::inner(0, 0);
        let hash_result = hash_join.execute(left.clone(), right.clone());

        let nested_join = NestedLoopJoin::inner(0, 0);
        let nested_result = nested_join.execute(left, right);

        // Both should have the same number of results
        prop_assert_eq!(hash_result.len(), nested_result.len());

        // Both should have the same key pairs
        let hash_pairs = extract_key_pairs(&hash_result, 0, 1);
        let nested_pairs = extract_key_pairs(&nested_result, 0, 1);
        prop_assert_eq!(hash_pairs, nested_pairs);
    }

    /// Property: Hash join and sort-merge join produce the same results for inner joins.
    #[test]
    fn hash_join_equals_sort_merge_join(
        left_rows in rows_strategy(50),
        right_rows in rows_strategy(50),
    ) {
        let left = Relation::from_rows_owned(left_rows, vec!["left".into()]);
        let right = Relation::from_rows_owned(right_rows, vec!["right".into()]);

        let hash_join = HashJoin::inner(0, 0);
        let hash_result = hash_join.execute(left.clone(), right.clone());

        let merge_join = SortMergeJoin::inner(0, 0);
        let merge_result = merge_join.execute_with_sort(left, right);

        // Both should have the same number of results
        prop_assert_eq!(hash_result.len(), merge_result.len());

        // Both should have the same key pairs
        let hash_pairs = extract_key_pairs(&hash_result, 0, 1);
        let merge_pairs = extract_key_pairs(&merge_result, 0, 1);
        prop_assert_eq!(hash_pairs, merge_pairs);
    }

    /// Property: All three join algorithms produce the same results.
    #[test]
    fn all_joins_consistent(
        left_rows in rows_strategy(30),
        right_rows in rows_strategy(30),
    ) {
        let left = Relation::from_rows_owned(left_rows, vec!["left".into()]);
        let right = Relation::from_rows_owned(right_rows, vec!["right".into()]);

        let hash_join = HashJoin::inner(0, 0);
        let hash_result = hash_join.execute(left.clone(), right.clone());

        let nested_join = NestedLoopJoin::inner(0, 0);
        let nested_result = nested_join.execute(left.clone(), right.clone());

        let merge_join = SortMergeJoin::inner(0, 0);
        let merge_result = merge_join.execute_with_sort(left, right);

        // All should have the same count
        prop_assert_eq!(hash_result.len(), nested_result.len());
        prop_assert_eq!(hash_result.len(), merge_result.len());
    }

    /// Property: Inner join result count equals the sum of matching pairs.
    #[test]
    fn inner_join_count_correct(
        left_rows in rows_strategy(30),
        right_rows in rows_strategy(30),
    ) {
        let left = Relation::from_rows_owned(left_rows.clone(), vec!["left".into()]);
        let right = Relation::from_rows_owned(right_rows.clone(), vec!["right".into()]);

        // Calculate expected count manually
        let mut expected_count = 0;
        for l in &left_rows {
            let l_key = l.get(0).and_then(|v| v.as_i64());
            for r in &right_rows {
                let r_key = r.get(0).and_then(|v| v.as_i64());
                if l_key == r_key && l_key.is_some() {
                    expected_count += 1;
                }
            }
        }

        let hash_join = HashJoin::inner(0, 0);
        let result = hash_join.execute(left, right);

        prop_assert_eq!(result.len(), expected_count);
    }

    /// Property: Left outer join preserves all left rows (at least one result per left row).
    #[test]
    fn left_outer_join_preserves_left(
        left_rows in rows_strategy(30),
        right_rows in rows_strategy(30),
    ) {
        let left_count = left_rows.len();
        // Collect left values (not IDs, since IDs may change during join)
        let left_values: Vec<_> = left_rows.iter()
            .filter_map(|r| r.get(0).and_then(|v| v.as_i64()))
            .collect();
        let left = Relation::from_rows_owned(left_rows, vec!["left".into()]);
        let right = Relation::from_rows_owned(right_rows, vec!["right".into()]);

        let hash_join = HashJoin::left_outer(0, 0);
        let result = hash_join.execute(left, right);

        // Result should have at least as many rows as left
        prop_assert!(result.len() >= left_count);

        // Count occurrences of each left value in results
        let mut result_left_values: Vec<_> = result
            .entries
            .iter()
            .filter_map(|e| e.get_field(0).and_then(|v| v.as_i64()))
            .collect();
        result_left_values.sort();

        let mut sorted_left_values = left_values.clone();
        sorted_left_values.sort();

        // Every left value should appear at least once in results
        // (may appear more times if there are multiple matches on right)
        for val in &sorted_left_values {
            let left_occurrences = sorted_left_values.iter().filter(|v| *v == val).count();
            let result_occurrences = result_left_values.iter().filter(|v| *v == val).count();
            prop_assert!(
                result_occurrences >= left_occurrences,
                "Left value {} appears {} times in left but only {} times in result",
                val, left_occurrences, result_occurrences
            );
        }
    }

    /// Property: Empty left relation produces empty result for inner join.
    #[test]
    fn empty_left_produces_empty_result(
        right_rows in rows_strategy(30),
    ) {
        let left = Relation::from_rows_owned(vec![], vec!["left".into()]);
        let right = Relation::from_rows_owned(right_rows, vec!["right".into()]);

        let hash_join = HashJoin::inner(0, 0);
        let result = hash_join.execute(left, right);

        prop_assert_eq!(result.len(), 0);
    }

    /// Property: Empty right relation produces empty result for inner join.
    #[test]
    fn empty_right_produces_empty_result(
        left_rows in rows_strategy(30),
    ) {
        let left = Relation::from_rows_owned(left_rows, vec!["left".into()]);
        let right = Relation::from_rows_owned(vec![], vec!["right".into()]);

        let hash_join = HashJoin::inner(0, 0);
        let result = hash_join.execute(left, right);

        prop_assert_eq!(result.len(), 0);
    }

    /// Property: Join is commutative for inner joins (same count, different order).
    #[test]
    fn inner_join_commutative_count(
        left_rows in rows_strategy(30),
        right_rows in rows_strategy(30),
    ) {
        let left = Relation::from_rows_owned(left_rows, vec!["left".into()]);
        let right = Relation::from_rows_owned(right_rows, vec!["right".into()]);

        let hash_join_lr = HashJoin::inner(0, 0);
        let result_lr = hash_join_lr.execute(left.clone(), right.clone());

        let hash_join_rl = HashJoin::inner(0, 0);
        let result_rl = hash_join_rl.execute(right, left);

        // Same count regardless of order
        prop_assert_eq!(result_lr.len(), result_rl.len());
    }

    /// Property: Nested loop join with custom predicate works correctly.
    #[test]
    fn nested_loop_range_join(
        left_rows in rows_strategy(20),
        right_rows in rows_strategy(20),
    ) {
        let left = Relation::from_rows_owned(left_rows.clone(), vec!["left".into()]);
        let right = Relation::from_rows_owned(right_rows.clone(), vec!["right".into()]);

        // Calculate expected count for less-than join
        let mut expected_count = 0;
        for l in &left_rows {
            let l_key = l.get(0).and_then(|v| v.as_i64());
            for r in &right_rows {
                let r_key = r.get(0).and_then(|v| v.as_i64());
                if let (Some(lk), Some(rk)) = (l_key, r_key) {
                    if lk < rk {
                        expected_count += 1;
                    }
                }
            }
        }

        let nested_join = NestedLoopJoin::inner(0, 0);
        let result = nested_join.execute_with_predicate(left, right, |a, b| a < b);

        prop_assert_eq!(result.len(), expected_count);
    }
}
