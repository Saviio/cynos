//! Sort-Merge Join implementation.

use crate::executor::{Relation, RelationEntry};
use alloc::vec::Vec;
use cynos_core::Value;
use core::cmp::Ordering;

/// Sort-Merge Join executor.
///
/// Efficient for joining pre-sorted relations or when both inputs
/// can be sorted efficiently.
pub struct SortMergeJoin {
    /// Column index for the left relation.
    left_key_index: usize,
    /// Column index for the right relation.
    right_key_index: usize,
    /// Whether this is an outer join.
    is_outer_join: bool,
}

impl SortMergeJoin {
    /// Creates a new sort-merge join executor.
    pub fn new(left_key_index: usize, right_key_index: usize, is_outer_join: bool) -> Self {
        Self {
            left_key_index,
            right_key_index,
            is_outer_join,
        }
    }

    /// Creates an inner sort-merge join.
    pub fn inner(left_key_index: usize, right_key_index: usize) -> Self {
        Self::new(left_key_index, right_key_index, false)
    }

    /// Creates a left outer sort-merge join.
    pub fn left_outer(left_key_index: usize, right_key_index: usize) -> Self {
        Self::new(left_key_index, right_key_index, true)
    }

    /// Executes the sort-merge join.
    /// Assumes both inputs are already sorted by their join keys.
    pub fn execute(&self, left: Relation, right: Relation) -> Relation {
        let mut result_entries = Vec::new();
        let left_tables = left.tables().to_vec();
        let right_tables = right.tables().to_vec();
        let right_col_count = right
            .entries
            .first()
            .map(|e| e.row.len())
            .unwrap_or(0);

        let left_entries: Vec<_> = left.entries.iter().collect();
        let right_entries: Vec<_> = right.entries.iter().collect();

        let mut left_idx = 0;
        let mut right_idx = 0;

        while left_idx < left_entries.len() {
            let left_entry = left_entries[left_idx];
            let left_value = left_entry.get_field(self.left_key_index);

            // Handle null values
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
                left_idx += 1;
                continue;
            }

            let left_val = left_value.unwrap();

            // Skip right entries that are smaller than current left
            while right_idx < right_entries.len() {
                let right_value = right_entries[right_idx].get_field(self.right_key_index);
                if right_value.map(|v| v.is_null()).unwrap_or(true) {
                    right_idx += 1;
                    continue;
                }
                if right_value.unwrap() < left_val {
                    right_idx += 1;
                } else {
                    break;
                }
            }

            // Find all matching right entries
            let mut match_found = false;
            let mut right_scan = right_idx;

            while right_scan < right_entries.len() {
                let right_entry = right_entries[right_scan];
                let right_value = right_entry.get_field(self.right_key_index);

                if right_value.map(|v| v.is_null()).unwrap_or(true) {
                    right_scan += 1;
                    continue;
                }

                let right_val = right_value.unwrap();

                match left_val.cmp(right_val) {
                    Ordering::Equal => {
                        match_found = true;
                        let combined = RelationEntry::combine(
                            left_entry,
                            &left_tables,
                            right_entry,
                            &right_tables,
                        );
                        result_entries.push(combined);
                        right_scan += 1;
                    }
                    Ordering::Less => break,
                    Ordering::Greater => {
                        right_scan += 1;
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

            left_idx += 1;
        }

        let mut tables = left_tables;
        tables.extend(right_tables);

        // Compute combined table column counts
        let mut table_column_counts = left.table_column_counts.clone();
        table_column_counts.extend(right.table_column_counts.iter().cloned());

        Relation {
            entries: result_entries,
            tables,
            table_column_counts,
        }
    }

    /// Executes the sort-merge join, sorting inputs first.
    pub fn execute_with_sort(&self, mut left: Relation, mut right: Relation) -> Relation {
        // Sort both relations by their join keys
        left.entries.sort_by(|a, b| {
            let a_val = a.get_field(self.left_key_index);
            let b_val = b.get_field(self.left_key_index);
            compare_values(a_val, b_val)
        });

        right.entries.sort_by(|a, b| {
            let a_val = a.get_field(self.right_key_index);
            let b_val = b.get_field(self.right_key_index);
            compare_values(a_val, b_val)
        });

        self.execute(left, right)
    }
}

fn compare_values(a: Option<&Value>, b: Option<&Value>) -> Ordering {
    match (a, b) {
        (None, None) => Ordering::Equal,
        (None, Some(_)) => Ordering::Less,
        (Some(_), None) => Ordering::Greater,
        (Some(av), Some(bv)) => av.cmp(bv),
    }
}

/// Performs a sort-merge join on pre-sorted slices.
pub fn sort_merge_join<L, R, K, O, LK, RK, OF>(
    left: &mut [L],
    right: &mut [R],
    left_key: LK,
    right_key: RK,
    output_fn: OF,
) -> Vec<O>
where
    K: Ord,
    LK: Fn(&L) -> K,
    RK: Fn(&R) -> K,
    OF: Fn(&L, &R) -> O,
{
    // Sort both inputs
    left.sort_by(|a, b| left_key(a).cmp(&left_key(b)));
    right.sort_by(|a, b| right_key(a).cmp(&right_key(b)));

    let mut results = Vec::new();
    let mut right_idx = 0;

    for l in left.iter() {
        let left_k = left_key(l);

        // Skip right entries that are smaller
        while right_idx < right.len() && right_key(&right[right_idx]) < left_k {
            right_idx += 1;
        }

        // Find all matching right entries
        let mut scan = right_idx;
        while scan < right.len() {
            let right_k = right_key(&right[scan]);
            match left_k.cmp(&right_k) {
                Ordering::Equal => {
                    results.push(output_fn(l, &right[scan]));
                    scan += 1;
                }
                Ordering::Less => break,
                Ordering::Greater => {
                    scan += 1;
                }
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
    fn test_sort_merge_join_inner() {
        // Pre-sorted inputs
        let left_rows = vec![
            Row::new(0, vec![Value::Int64(1)]),
            Row::new(1, vec![Value::Int64(2)]),
            Row::new(2, vec![Value::Int64(3)]),
        ];
        let right_rows = vec![
            Row::new(10, vec![Value::Int64(1)]),
            Row::new(11, vec![Value::Int64(2)]),
            Row::new(12, vec![Value::Int64(4)]),
        ];

        let left = Relation::from_rows_owned(left_rows, vec!["left".into()]);
        let right = Relation::from_rows_owned(right_rows, vec!["right".into()]);

        let join = SortMergeJoin::inner(0, 0);
        let result = join.execute(left, right);

        // Should match on keys 1 and 2
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn test_sort_merge_join_with_duplicates() {
        let left_rows = vec![
            Row::new(0, vec![Value::Int64(1)]),
            Row::new(1, vec![Value::Int64(1)]),
            Row::new(2, vec![Value::Int64(2)]),
        ];
        let right_rows = vec![
            Row::new(10, vec![Value::Int64(1)]),
            Row::new(11, vec![Value::Int64(1)]),
        ];

        let left = Relation::from_rows_owned(left_rows, vec!["left".into()]);
        let right = Relation::from_rows_owned(right_rows, vec!["right".into()]);

        let join = SortMergeJoin::inner(0, 0);
        let result = join.execute(left, right);

        // 2 left rows with key 1 * 2 right rows with key 1 = 4 matches
        assert_eq!(result.len(), 4);
    }

    #[test]
    fn test_sort_merge_join_left_outer() {
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

        let join = SortMergeJoin::left_outer(0, 0);
        let result = join.execute(left, right);

        // 1 match + 2 unmatched with nulls
        assert_eq!(result.len(), 3);
    }

    #[test]
    fn test_sort_merge_join_function() {
        let mut left = vec![(3, "C"), (1, "A"), (2, "B")];
        let mut right = vec![(2, "Y"), (1, "X"), (4, "Z")];

        let result = sort_merge_join(
            &mut left,
            &mut right,
            |l| l.0,
            |r| r.0,
            |l, r| (l.1, r.1),
        );

        assert_eq!(result.len(), 2);
        assert!(result.contains(&("A", "X")));
        assert!(result.contains(&("B", "Y")));
    }

    #[test]
    fn test_sort_merge_join_with_sort() {
        // Unsorted inputs
        let left_rows = vec![
            Row::new(0, vec![Value::Int64(3)]),
            Row::new(1, vec![Value::Int64(1)]),
            Row::new(2, vec![Value::Int64(2)]),
        ];
        let right_rows = vec![
            Row::new(10, vec![Value::Int64(2)]),
            Row::new(11, vec![Value::Int64(1)]),
        ];

        let left = Relation::from_rows_owned(left_rows, vec!["left".into()]);
        let right = Relation::from_rows_owned(right_rows, vec!["right".into()]);

        let join = SortMergeJoin::inner(0, 0);
        let result = join.execute_with_sort(left, right);

        // Should match on keys 1 and 2
        assert_eq!(result.len(), 2);
    }
}
