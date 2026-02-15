//! Sort executor.

use crate::ast::SortOrder;
use crate::executor::{Relation, RelationEntry};
use alloc::vec::Vec;
use core::cmp::Ordering;

/// Sort executor - sorts rows by specified columns.
pub struct SortExecutor {
    /// Column indices and sort orders.
    order_by: Vec<(usize, SortOrder)>,
}

impl SortExecutor {
    /// Creates a new sort executor.
    pub fn new(order_by: Vec<(usize, SortOrder)>) -> Self {
        Self { order_by }
    }

    /// Executes the sort on the input relation.
    pub fn execute(&self, mut input: Relation) -> Relation {
        input.entries.sort_by(|a, b| self.compare_entries(a, b));
        input
    }

    fn compare_entries(&self, a: &RelationEntry, b: &RelationEntry) -> Ordering {
        for (col_idx, order) in &self.order_by {
            let a_val = a.get_field(*col_idx);
            let b_val = b.get_field(*col_idx);

            let cmp = match (a_val, b_val) {
                (Some(av), Some(bv)) => av.cmp(bv),
                (None, Some(_)) => Ordering::Less,
                (Some(_), None) => Ordering::Greater,
                (None, None) => Ordering::Equal,
            };

            if cmp != Ordering::Equal {
                return match order {
                    SortOrder::Asc => cmp,
                    SortOrder::Desc => cmp.reverse(),
                };
            }
        }
        Ordering::Equal
    }
}

/// Sorts a relation by a key function.
#[allow(dead_code)]
pub fn sort_relation<K, F>(mut input: Relation, key_fn: F) -> Relation
where
    K: Ord,
    F: Fn(&RelationEntry) -> K,
{
    input.entries.sort_by(|a, b| key_fn(a).cmp(&key_fn(b)));
    input
}

/// Sorts a relation by a comparison function.
#[allow(dead_code)]
pub fn sort_relation_by<F>(mut input: Relation, compare: F) -> Relation
where
    F: Fn(&RelationEntry, &RelationEntry) -> Ordering,
{
    input.entries.sort_by(compare);
    input
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloc::rc::Rc;
    use alloc::vec;
    use cynos_core::{Row, Value};

    #[test]
    fn test_sort_executor_asc() {
        let rows = vec![
            Rc::new(Row::new(1, vec![Value::Int64(30)])),
            Rc::new(Row::new(2, vec![Value::Int64(10)])),
            Rc::new(Row::new(3, vec![Value::Int64(20)])),
        ];
        let input = Relation::from_rows(rows, vec!["t".into()]);

        let executor = SortExecutor::new(vec![(0, SortOrder::Asc)]);
        let result = executor.execute(input);

        assert_eq!(result.entries[0].get_field(0), Some(&Value::Int64(10)));
        assert_eq!(result.entries[1].get_field(0), Some(&Value::Int64(20)));
        assert_eq!(result.entries[2].get_field(0), Some(&Value::Int64(30)));
    }

    #[test]
    fn test_sort_executor_desc() {
        let rows = vec![
            Rc::new(Row::new(1, vec![Value::Int64(10)])),
            Rc::new(Row::new(2, vec![Value::Int64(30)])),
            Rc::new(Row::new(3, vec![Value::Int64(20)])),
        ];
        let input = Relation::from_rows(rows, vec!["t".into()]);

        let executor = SortExecutor::new(vec![(0, SortOrder::Desc)]);
        let result = executor.execute(input);

        assert_eq!(result.entries[0].get_field(0), Some(&Value::Int64(30)));
        assert_eq!(result.entries[1].get_field(0), Some(&Value::Int64(20)));
        assert_eq!(result.entries[2].get_field(0), Some(&Value::Int64(10)));
    }

    #[test]
    fn test_sort_executor_multi_column() {
        let rows = vec![
            Rc::new(Row::new(1, vec![Value::Int64(1), Value::String("B".into())])),
            Rc::new(Row::new(2, vec![Value::Int64(1), Value::String("A".into())])),
            Rc::new(Row::new(3, vec![Value::Int64(2), Value::String("A".into())])),
        ];
        let input = Relation::from_rows(rows, vec!["t".into()]);

        let executor = SortExecutor::new(vec![(0, SortOrder::Asc), (1, SortOrder::Asc)]);
        let result = executor.execute(input);

        // Should be sorted by col 0 first, then col 1
        assert_eq!(result.entries[0].get_field(1), Some(&Value::String("A".into())));
        assert_eq!(result.entries[1].get_field(1), Some(&Value::String("B".into())));
        assert_eq!(result.entries[2].get_field(0), Some(&Value::Int64(2)));
    }
}
