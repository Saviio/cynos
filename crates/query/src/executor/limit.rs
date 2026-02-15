//! Limit executor.

use crate::executor::Relation;

/// Limit executor - applies LIMIT and OFFSET to a relation.
pub struct LimitExecutor {
    limit: usize,
    offset: usize,
}

impl LimitExecutor {
    /// Creates a new limit executor.
    pub fn new(limit: usize, offset: usize) -> Self {
        Self { limit, offset }
    }

    /// Creates a limit executor with only a limit (no offset).
    pub fn limit_only(limit: usize) -> Self {
        Self { limit, offset: 0 }
    }

    /// Executes the limit on the input relation.
    ///
    /// Note: This operation is O(offset + limit) for the actual work,
    /// but dropping the unused entries is O(n - offset - limit).
    /// In a real query engine, limit would be pushed down to avoid
    /// materializing unnecessary rows.
    pub fn execute(&self, mut input: Relation) -> Relation {
        let tables = input.tables().to_vec();
        let table_column_counts = input.table_column_counts().to_vec();
        let len = input.entries.len();
        let start = self.offset.min(len);
        let end = (self.offset + self.limit).min(len);

        // Truncate tail first (drops elements after end)
        input.entries.truncate(end);
        // Remove head elements (drops elements before start)
        if start > 0 {
            input.entries.drain(..start);
        }

        Relation {
            entries: input.entries,
            tables,
            table_column_counts,
        }
    }
}

/// Applies limit and offset to a relation.
#[allow(dead_code)]
pub fn limit_relation(input: Relation, limit: usize, offset: usize) -> Relation {
    let tables = input.tables().to_vec();
    let table_column_counts = input.table_column_counts().to_vec();
    let entries = input
        .entries
        .into_iter()
        .skip(offset)
        .take(limit)
        .collect();

    Relation { entries, tables, table_column_counts }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloc::vec;
    use alloc::vec::Vec;
    use cynos_core::{Row, Value};

    #[test]
    fn test_limit_executor() {
        let rows: Vec<Row> = (0..10)
            .map(|i| Row::new(i, vec![Value::Int64(i as i64)]))
            .collect();
        let input = Relation::from_rows_owned(rows, vec!["t".into()]);

        let executor = LimitExecutor::new(3, 2);
        let result = executor.execute(input);

        assert_eq!(result.len(), 3);
        assert_eq!(result.entries[0].get_field(0), Some(&Value::Int64(2)));
        assert_eq!(result.entries[1].get_field(0), Some(&Value::Int64(3)));
        assert_eq!(result.entries[2].get_field(0), Some(&Value::Int64(4)));
    }

    #[test]
    fn test_limit_only() {
        let rows: Vec<Row> = (0..10)
            .map(|i| Row::new(i, vec![Value::Int64(i as i64)]))
            .collect();
        let input = Relation::from_rows_owned(rows, vec!["t".into()]);

        let executor = LimitExecutor::limit_only(5);
        let result = executor.execute(input);

        assert_eq!(result.len(), 5);
        assert_eq!(result.entries[0].get_field(0), Some(&Value::Int64(0)));
    }

    #[test]
    fn test_limit_exceeds_size() {
        let rows = vec![
            Row::new(0, vec![Value::Int64(0)]),
            Row::new(1, vec![Value::Int64(1)]),
        ];
        let input = Relation::from_rows_owned(rows, vec!["t".into()]);

        let executor = LimitExecutor::new(100, 0);
        let result = executor.execute(input);

        assert_eq!(result.len(), 2);
    }

    #[test]
    fn test_offset_exceeds_size() {
        let rows = vec![
            Row::new(0, vec![Value::Int64(0)]),
            Row::new(1, vec![Value::Int64(1)]),
        ];
        let input = Relation::from_rows_owned(rows, vec!["t".into()]);

        let executor = LimitExecutor::new(10, 100);
        let result = executor.execute(input);

        assert_eq!(result.len(), 0);
    }
}
