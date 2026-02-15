//! Project executor.

use crate::executor::{Relation, RelationEntry, SharedTables};
use alloc::rc::Rc;
use alloc::vec;
use alloc::vec::Vec;
use cynos_core::{Row, Value};

/// Project executor - projects specific columns from rows.
pub struct ProjectExecutor {
    /// Column indices to project.
    column_indices: Vec<usize>,
}

impl ProjectExecutor {
    /// Creates a new project executor.
    pub fn new(column_indices: Vec<usize>) -> Self {
        Self { column_indices }
    }

    /// Executes the projection on the input relation.
    pub fn execute(&self, input: Relation) -> Relation {
        let tables = input.tables().to_vec();
        let shared_tables: SharedTables = tables.clone().into();
        let entries: Vec<RelationEntry> = input
            .into_iter()
            .map(|entry| {
                let values: Vec<Value> = self
                    .column_indices
                    .iter()
                    .map(|&idx| entry.get_field(idx).cloned().unwrap_or(Value::Null))
                    .collect();
                RelationEntry::new_combined(Rc::new(Row::new(entry.id(), values)), shared_tables.clone())
            })
            .collect();

        // After projection, we have a single combined result with projected columns
        let table_column_counts = vec![self.column_indices.len()];
        Relation { entries, tables, table_column_counts }
    }
}

/// Projects columns from a relation using a transformation function.
#[allow(dead_code)]
pub fn project_relation<F>(input: Relation, transform: F) -> Relation
where
    F: Fn(&RelationEntry) -> Vec<Value>,
{
    let tables = input.tables().to_vec();
    let shared_tables: SharedTables = tables.clone().into();
    let entries: Vec<RelationEntry> = input
        .into_iter()
        .map(|entry| {
            let values = transform(&entry);
            RelationEntry::new_combined(Rc::new(Row::new(entry.id(), values)), shared_tables.clone())
        })
        .collect();

    // After transform, we don't know the exact column count, use entries length as approximation
    let table_column_counts = if entries.is_empty() {
        vec![0]
    } else {
        vec![entries[0].row.len()]
    };
    Relation { entries, tables, table_column_counts }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cynos_core::Row;
    use alloc::vec;

    #[test]
    fn test_project_executor() {
        let rows = vec![
            Rc::new(Row::new(1, vec![Value::Int64(1), Value::String("Alice".into()), Value::Int64(25)])),
            Rc::new(Row::new(2, vec![Value::Int64(2), Value::String("Bob".into()), Value::Int64(30)])),
        ];
        let input = Relation::from_rows(rows, vec!["users".into()]);

        // Project only columns 0 and 2 (id and age)
        let executor = ProjectExecutor::new(vec![0, 2]);
        let result = executor.execute(input);

        assert_eq!(result.len(), 2);
        let first = &result.entries[0];
        assert_eq!(first.row.len(), 2);
        assert_eq!(first.get_field(0), Some(&Value::Int64(1)));
        assert_eq!(first.get_field(1), Some(&Value::Int64(25)));
    }

    #[test]
    fn test_project_relation_transform() {
        let rows = vec![Rc::new(Row::new(1, vec![Value::Int64(10), Value::Int64(20)]))];
        let input = Relation::from_rows(rows, vec!["t".into()]);

        let result = project_relation(input, |entry| {
            let a = entry.get_field(0).and_then(|v| v.as_i64()).unwrap_or(0);
            let b = entry.get_field(1).and_then(|v| v.as_i64()).unwrap_or(0);
            vec![Value::Int64(a + b)]
        });

        assert_eq!(result.len(), 1);
        assert_eq!(result.entries[0].get_field(0), Some(&Value::Int64(30)));
    }
}
