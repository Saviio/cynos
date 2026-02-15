//! Table and index scan executors.

use crate::executor::Relation;
use alloc::rc::Rc;
use alloc::string::String;
use alloc::vec::Vec;
use cynos_core::Row;

/// Table scan executor - scans all rows from a table.
pub struct TableScanExecutor {
    table: String,
    rows: Vec<Rc<Row>>,
}

impl TableScanExecutor {
    /// Creates a new table scan executor.
    pub fn new(table: impl Into<String>, rows: Vec<Rc<Row>>) -> Self {
        Self {
            table: table.into(),
            rows,
        }
    }

    /// Executes the scan and returns the relation.
    pub fn execute(&self) -> Relation {
        Relation::from_rows(self.rows.clone(), alloc::vec![self.table.clone()])
    }
}

/// Index scan executor - scans rows using an index.
pub struct IndexScanExecutor {
    table: String,
    rows: Vec<Rc<Row>>,
}

impl IndexScanExecutor {
    /// Creates a new index scan executor with pre-fetched rows.
    pub fn new(table: impl Into<String>, rows: Vec<Rc<Row>>) -> Self {
        Self {
            table: table.into(),
            rows,
        }
    }

    /// Executes the scan and returns the relation.
    pub fn execute(&self) -> Relation {
        Relation::from_rows(self.rows.clone(), alloc::vec![self.table.clone()])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cynos_core::Value;
    use alloc::vec;

    #[test]
    fn test_table_scan() {
        let rows = vec![
            Rc::new(Row::new(1, vec![Value::Int64(1), Value::String("Alice".into())])),
            Rc::new(Row::new(2, vec![Value::Int64(2), Value::String("Bob".into())])),
        ];
        let executor = TableScanExecutor::new("users", rows);
        let result = executor.execute();

        assert_eq!(result.len(), 2);
        assert_eq!(result.tables(), &["users"]);
    }

    #[test]
    fn test_index_scan() {
        let rows = vec![Rc::new(Row::new(1, vec![Value::Int64(42)]))];
        let executor = IndexScanExecutor::new("users", rows);
        let result = executor.execute();

        assert_eq!(result.len(), 1);
    }
}
