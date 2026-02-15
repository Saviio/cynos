//! Nested Loop Join implementation.

use crate::executor::{Relation, RelationEntry};
use alloc::vec::Vec;
use cynos_core::Value;

/// Nested Loop Join executor.
///
/// The simplest join algorithm that compares every pair of rows.
/// Best for small relations or non-equi joins.
pub struct NestedLoopJoin {
    /// Column index for the left relation.
    left_key_index: usize,
    /// Column index for the right relation.
    right_key_index: usize,
    /// Whether this is an outer join.
    is_outer_join: bool,
}

impl NestedLoopJoin {
    /// Creates a new nested loop join executor.
    pub fn new(left_key_index: usize, right_key_index: usize, is_outer_join: bool) -> Self {
        Self {
            left_key_index,
            right_key_index,
            is_outer_join,
        }
    }

    /// Creates an inner nested loop join.
    pub fn inner(left_key_index: usize, right_key_index: usize) -> Self {
        Self::new(left_key_index, right_key_index, false)
    }

    /// Creates a left outer nested loop join.
    pub fn left_outer(left_key_index: usize, right_key_index: usize) -> Self {
        Self::new(left_key_index, right_key_index, true)
    }

    /// Executes the nested loop join with equality comparison.
    pub fn execute(&self, left: Relation, right: Relation) -> Relation {
        self.execute_with_predicate(left, right, |l, r| l == r)
    }

    /// Executes the nested loop join with a custom predicate.
    pub fn execute_with_predicate<F>(&self, left: Relation, right: Relation, predicate: F) -> Relation
    where
        F: Fn(&Value, &Value) -> bool,
    {
        let mut result_entries = Vec::new();
        let left_tables = left.tables().to_vec();
        let right_tables = right.tables().to_vec();
        let right_col_count = right
            .entries
            .first()
            .map(|e| e.row.len())
            .unwrap_or(0);

        // Block-based nested loop for better cache performance
        const BLOCK_SIZE: usize = 256;
        let right_entries: Vec<_> = right.entries.iter().collect();
        let block_count = (right_entries.len() + BLOCK_SIZE - 1) / BLOCK_SIZE;

        for left_entry in left.iter() {
            let mut match_found = false;
            let left_value = left_entry.get_field(self.left_key_index);

            // Skip if left value is null (nulls don't match)
            if left_value.map(|v| v.is_null()).unwrap_or(true) {
                if self.is_outer_join {
                    let combined = RelationEntry::combine_with_null(
                        left_entry,
                        &left_tables,
                        right_col_count,
                        &right_tables,
                    );
                    result_entries.push(combined);
                }
                continue;
            }

            let left_val = left_value.unwrap();

            // Process in blocks for better cache locality
            for block in 0..block_count {
                let start = block * BLOCK_SIZE;
                let end = core::cmp::min(start + BLOCK_SIZE, right_entries.len());

                for right_entry in &right_entries[start..end] {
                    if let Some(right_val) = right_entry.get_field(self.right_key_index) {
                        if !right_val.is_null() && predicate(left_val, right_val) {
                            match_found = true;
                            let combined = RelationEntry::combine(
                                left_entry,
                                &left_tables,
                                right_entry,
                                &right_tables,
                            );
                            result_entries.push(combined);
                        }
                    }
                }
            }

            // For outer join, add unmatched left entries with nulls
            if self.is_outer_join && !match_found {
                let combined = RelationEntry::combine_with_null(
                    left_entry,
                    &left_tables,
                    right_col_count,
                    &right_tables,
                );
                result_entries.push(combined);
            }
        }

        let mut tables = left_tables;
        tables.extend(right_tables);

        // Compute combined table column counts
        let mut table_column_counts = left.table_column_counts().to_vec();
        table_column_counts.extend(right.table_column_counts().iter().cloned());

        Relation {
            entries: result_entries,
            tables,
            table_column_counts,
        }
    }
}

/// Performs a nested loop join using a predicate function.
pub fn nested_loop_join<L, R, O, P, OF>(
    left: &[L],
    right: &[R],
    predicate: P,
    output_fn: OF,
) -> Vec<O>
where
    P: Fn(&L, &R) -> bool,
    OF: Fn(&L, &R) -> O,
{
    let mut results = Vec::new();

    for l in left {
        for r in right {
            if predicate(l, r) {
                results.push(output_fn(l, r));
            }
        }
    }

    results
}

#[cfg(test)]
mod tests {
    use super::*;
    use cynos_core::Row;
    use alloc::vec;

    #[test]
    fn test_nested_loop_join_inner() {
        let left_rows = vec![
            Row::new(0, vec![Value::Int64(1), Value::String("A".into())]),
            Row::new(1, vec![Value::Int64(2), Value::String("B".into())]),
            Row::new(2, vec![Value::Int64(3), Value::String("C".into())]),
        ];
        let right_rows = vec![
            Row::new(10, vec![Value::Int64(1), Value::String("X".into())]),
            Row::new(11, vec![Value::Int64(2), Value::String("Y".into())]),
            Row::new(12, vec![Value::Int64(4), Value::String("Z".into())]),
        ];

        let left = Relation::from_rows_owned(left_rows, vec!["left".into()]);
        let right = Relation::from_rows_owned(right_rows, vec!["right".into()]);

        let join = NestedLoopJoin::inner(0, 0);
        let result = join.execute(left, right);

        // Should match on keys 1 and 2
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn test_nested_loop_join_range() {
        let left_rows = vec![
            Row::new(0, vec![Value::Int64(10)]),
            Row::new(1, vec![Value::Int64(20)]),
        ];
        let right_rows = vec![
            Row::new(10, vec![Value::Int64(5)]),
            Row::new(11, vec![Value::Int64(15)]),
            Row::new(12, vec![Value::Int64(25)]),
        ];

        let left = Relation::from_rows_owned(left_rows, vec!["left".into()]);
        let right = Relation::from_rows_owned(right_rows, vec!["right".into()]);

        let join = NestedLoopJoin::inner(0, 0);
        // left.value > right.value
        let result = join.execute_with_predicate(left, right, |l, r| l > r);

        // 10 > 5, 20 > 5, 20 > 15 = 3 matches
        assert_eq!(result.len(), 3);
    }

    #[test]
    fn test_nested_loop_join_left_outer() {
        let left_rows = vec![
            Row::new(0, vec![Value::Int64(1)]),
            Row::new(1, vec![Value::Int64(2)]),
            Row::new(2, vec![Value::Int64(3)]),
        ];
        let right_rows = vec![
            Row::new(10, vec![Value::Int64(1)]),
        ];

        let left = Relation::from_rows_owned(left_rows, vec!["left".into()]);
        let right = Relation::from_rows_owned(right_rows, vec!["right".into()]);

        let join = NestedLoopJoin::left_outer(0, 0);
        let result = join.execute(left, right);

        // 1 match + 2 unmatched with nulls
        assert_eq!(result.len(), 3);
    }

    #[test]
    fn test_nested_loop_join_function() {
        let left = vec![(1, "A"), (2, "B"), (3, "C")];
        let right = vec![(1, "X"), (2, "Y"), (4, "Z")];

        let result = nested_loop_join(
            &left,
            &right,
            |l, r| l.0 == r.0,
            |l, r| (l.1, r.1),
        );

        assert_eq!(result.len(), 2);
        assert!(result.contains(&("A", "X")));
        assert!(result.contains(&("B", "Y")));
    }

    #[test]
    fn test_nested_loop_join_with_nulls() {
        let left_rows = vec![
            Row::new(0, vec![Value::Int64(1)]),
            Row::new(1, vec![Value::Null]),
        ];
        let right_rows = vec![
            Row::new(10, vec![Value::Int64(1)]),
            Row::new(11, vec![Value::Null]),
        ];

        let left = Relation::from_rows_owned(left_rows, vec!["left".into()]);
        let right = Relation::from_rows_owned(right_rows, vec!["right".into()]);

        let join = NestedLoopJoin::inner(0, 0);
        let result = join.execute(left, right);

        // NULL values should not match
        assert_eq!(result.len(), 1);
    }
}
