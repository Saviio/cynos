//! Filter executor.

use crate::ast::Predicate;
use crate::executor::{Relation, RelationEntry};
use alloc::vec::Vec;

/// Filter executor - filters rows based on a predicate.
pub struct FilterExecutor<P: Predicate> {
    predicate: P,
}

impl<P: Predicate> FilterExecutor<P> {
    /// Creates a new filter executor.
    pub fn new(predicate: P) -> Self {
        Self { predicate }
    }

    /// Executes the filter on the input relation.
    pub fn execute(&self, input: Relation) -> Relation {
        let tables = input.tables().to_vec();
        let table_column_counts = input.table_column_counts().to_vec();
        let entries: Vec<RelationEntry> = input
            .into_iter()
            .filter(|entry| self.predicate.eval(&entry.row))
            .collect();

        Relation { entries, tables, table_column_counts }
    }
}

/// Filters a relation using a closure.
#[allow(dead_code)]
pub fn filter_relation<F>(input: Relation, predicate: F) -> Relation
where
    F: Fn(&RelationEntry) -> bool,
{
    let tables = input.tables().to_vec();
    let table_column_counts = input.table_column_counts().to_vec();
    let entries: Vec<RelationEntry> = input.into_iter().filter(|e| predicate(e)).collect();

    Relation { entries, tables, table_column_counts }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::{ColumnRef, EvalType, ValuePredicate};
    use alloc::vec;
    use cynos_core::{Row, Value};

    #[test]
    fn test_filter_executor() {
        let rows = vec![
            Row::new(1, vec![Value::Int64(10)]),
            Row::new(2, vec![Value::Int64(20)]),
            Row::new(3, vec![Value::Int64(30)]),
        ];
        let input = Relation::from_rows_owned(rows, vec!["t".into()]);

        let col = ColumnRef::new("t", "value", 0);
        let pred = ValuePredicate::new(col, EvalType::Gt, Value::Int64(15));
        let executor = FilterExecutor::new(pred);

        let result = executor.execute(input);
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn test_filter_relation_closure() {
        let rows = vec![
            Row::new(1, vec![Value::Int64(10)]),
            Row::new(2, vec![Value::Int64(20)]),
        ];
        let input = Relation::from_rows_owned(rows, vec!["t".into()]);

        let result = filter_relation(input, |entry| {
            entry
                .get_field(0)
                .and_then(|v| v.as_i64())
                .map(|v| v > 15)
                .unwrap_or(false)
        });

        assert_eq!(result.len(), 1);
    }
}
