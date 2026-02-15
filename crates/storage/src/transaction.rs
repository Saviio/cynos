//! Transaction management for Cynos database.
//!
//! This module provides transaction support with isolation and rollback capabilities.

use crate::cache::TableCache;
use crate::journal::{Journal, JournalEntry};
use alloc::vec::Vec;
use cynos_core::{Error, Result, Row, RowId};
use core::sync::atomic::{AtomicU64, Ordering};

/// Global transaction ID counter.
static NEXT_TX_ID: AtomicU64 = AtomicU64::new(1);

/// Transaction ID type.
pub type TransactionId = u64;

/// Transaction state.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TransactionState {
    /// Transaction is active and can perform operations.
    Active,
    /// Transaction has been committed.
    Committed,
    /// Transaction has been rolled back.
    RolledBack,
}

/// A database transaction.
pub struct Transaction {
    /// Unique transaction ID.
    id: TransactionId,
    /// Journal for tracking changes.
    journal: Journal,
    /// Current state.
    state: TransactionState,
}

impl Transaction {
    /// Creates a new transaction.
    pub fn begin() -> Self {
        Self {
            id: NEXT_TX_ID.fetch_add(1, Ordering::SeqCst),
            journal: Journal::new(),
            state: TransactionState::Active,
        }
    }

    /// Returns the transaction ID.
    pub fn id(&self) -> TransactionId {
        self.id
    }

    /// Returns the current state.
    pub fn state(&self) -> TransactionState {
        self.state
    }

    /// Returns true if the transaction is active.
    pub fn is_active(&self) -> bool {
        self.state == TransactionState::Active
    }

    /// Checks if the transaction is active, returns error if not.
    fn check_active(&self) -> Result<()> {
        if self.state != TransactionState::Active {
            return Err(Error::invalid_operation("Transaction is not active"));
        }
        Ok(())
    }

    /// Inserts a row within this transaction.
    pub fn insert(&mut self, cache: &mut TableCache, table: &str, row: Row) -> Result<RowId> {
        self.check_active()?;

        let store = cache.get_table_mut(table).ok_or_else(|| Error::table_not_found(table))?;
        let row_id = store.insert(row.clone())?;

        self.journal.record_insert(table, row);
        Ok(row_id)
    }

    /// Updates a row within this transaction.
    pub fn update(&mut self, cache: &mut TableCache, table: &str, row_id: RowId, new_row: Row) -> Result<()> {
        self.check_active()?;

        let store = cache.get_table_mut(table).ok_or_else(|| Error::table_not_found(table))?;
        let old_row = store.get(row_id).ok_or_else(|| {
            Error::not_found(table, cynos_core::Value::Int64(row_id as i64))
        })?;

        let old_row_owned = (*old_row).clone();
        store.update(row_id, new_row.clone())?;
        self.journal.record_update(table, old_row_owned, new_row);
        Ok(())
    }

    /// Deletes a row within this transaction.
    pub fn delete(&mut self, cache: &mut TableCache, table: &str, row_id: RowId) -> Result<Row> {
        self.check_active()?;

        let store = cache.get_table_mut(table).ok_or_else(|| Error::table_not_found(table))?;
        let row = store.delete(row_id)?;

        let row_owned = (*row).clone();
        self.journal.record_delete(table, row_owned.clone());
        Ok(row_owned)
    }

    /// Commits the transaction.
    pub fn commit(mut self) -> Result<Vec<JournalEntry>> {
        self.check_active()?;
        self.state = TransactionState::Committed;
        Ok(self.journal.commit())
    }

    /// Rolls back the transaction.
    pub fn rollback(mut self, cache: &mut TableCache) -> Result<()> {
        self.check_active()?;
        self.state = TransactionState::RolledBack;
        self.journal.rollback(cache)
    }

    /// Returns the journal entries.
    pub fn get_changes(&self) -> &[JournalEntry] {
        self.journal.get_entries()
    }

    /// Returns the journal.
    pub fn journal(&self) -> &Journal {
        &self.journal
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cynos_core::schema::TableBuilder;
    use cynos_core::{DataType, Value};
    use alloc::format;
    use alloc::vec;

    fn test_schema() -> cynos_core::schema::Table {
        TableBuilder::new("test")
            .unwrap()
            .add_column("id", DataType::Int64)
            .unwrap()
            .add_column("name", DataType::String)
            .unwrap()
            .add_primary_key(&["id"], false)
            .unwrap()
            .build()
            .unwrap()
    }

    #[test]
    fn test_transaction_begin() {
        let tx = Transaction::begin();
        assert!(tx.is_active());
        assert!(tx.id() > 0);
    }

    #[test]
    fn test_transaction_insert_commit() {
        let mut cache = TableCache::new();
        cache.create_table(test_schema()).unwrap();

        let mut tx = Transaction::begin();
        let row = Row::new(1, vec![Value::Int64(1), Value::String("test".into())]);
        tx.insert(&mut cache, "test", row).unwrap();

        let entries = tx.commit().unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(cache.get_table("test").unwrap().len(), 1);
    }

    #[test]
    fn test_transaction_rollback() {
        let mut cache = TableCache::new();
        cache.create_table(test_schema()).unwrap();

        let mut tx = Transaction::begin();
        let row = Row::new(1, vec![Value::Int64(1), Value::String("test".into())]);
        tx.insert(&mut cache, "test", row).unwrap();

        assert_eq!(cache.get_table("test").unwrap().len(), 1);

        tx.rollback(&mut cache).unwrap();
        assert_eq!(cache.get_table("test").unwrap().len(), 0);
    }

    #[test]
    fn test_transaction_update() {
        let mut cache = TableCache::new();
        cache.create_table(test_schema()).unwrap();

        // Insert initial row
        let row = Row::new(1, vec![Value::Int64(1), Value::String("initial".into())]);
        cache.get_table_mut("test").unwrap().insert(row).unwrap();

        // Update in transaction
        let mut tx = Transaction::begin();
        let new_row = Row::new(1, vec![Value::Int64(1), Value::String("updated".into())]);
        tx.update(&mut cache, "test", 1, new_row).unwrap();

        let entries = tx.commit().unwrap();
        assert_eq!(entries.len(), 1);

        let stored = cache.get_table("test").unwrap().get(1).unwrap();
        assert_eq!(stored.get(1), Some(&Value::String("updated".into())));
    }

    #[test]
    fn test_transaction_delete() {
        let mut cache = TableCache::new();
        cache.create_table(test_schema()).unwrap();

        // Insert initial row
        let row = Row::new(1, vec![Value::Int64(1), Value::String("test".into())]);
        cache.get_table_mut("test").unwrap().insert(row).unwrap();

        // Delete in transaction
        let mut tx = Transaction::begin();
        tx.delete(&mut cache, "test", 1).unwrap();

        let entries = tx.commit().unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(cache.get_table("test").unwrap().len(), 0);
    }

    #[test]
    fn test_transaction_state_after_commit() {
        let mut cache = TableCache::new();
        cache.create_table(test_schema()).unwrap();

        let tx = Transaction::begin();
        let _ = tx.commit();
        // Transaction is consumed after commit
    }

    #[test]
    fn test_multiple_operations() {
        let mut cache = TableCache::new();
        cache.create_table(test_schema()).unwrap();

        let mut tx = Transaction::begin();

        // Insert multiple rows
        for i in 1..=3 {
            let row = Row::new(i, vec![Value::Int64(i as i64), Value::String(format!("row{}", i))]);
            tx.insert(&mut cache, "test", row).unwrap();
        }

        // Update one
        let updated = Row::new(2, vec![Value::Int64(2), Value::String("updated".into())]);
        tx.update(&mut cache, "test", 2, updated).unwrap();

        // Delete one
        tx.delete(&mut cache, "test", 3).unwrap();

        let entries = tx.commit().unwrap();
        assert_eq!(entries.len(), 5); // 3 inserts + 1 update + 1 delete
        assert_eq!(cache.get_table("test").unwrap().len(), 2);
    }

    #[test]
    fn test_transaction_rollback_update() {
        let mut cache = TableCache::new();
        cache.create_table(test_schema()).unwrap();

        // Insert initial row
        let row = Row::new(1, vec![Value::Int64(1), Value::String("original".into())]);
        cache.get_table_mut("test").unwrap().insert(row).unwrap();

        // Update in transaction
        let mut tx = Transaction::begin();
        let new_row = Row::new(1, vec![Value::Int64(1), Value::String("modified".into())]);
        tx.update(&mut cache, "test", 1, new_row).unwrap();

        // Verify change is visible
        assert_eq!(
            cache.get_table("test").unwrap().get(1).unwrap().get(1),
            Some(&Value::String("modified".into()))
        );

        // Rollback
        tx.rollback(&mut cache).unwrap();

        // Verify original value is restored
        assert_eq!(
            cache.get_table("test").unwrap().get(1).unwrap().get(1),
            Some(&Value::String("original".into()))
        );
    }

    #[test]
    fn test_transaction_rollback_delete() {
        let mut cache = TableCache::new();
        cache.create_table(test_schema()).unwrap();

        // Insert initial row
        let row = Row::new(1, vec![Value::Int64(1), Value::String("test".into())]);
        cache.get_table_mut("test").unwrap().insert(row).unwrap();

        // Delete in transaction
        let mut tx = Transaction::begin();
        tx.delete(&mut cache, "test", 1).unwrap();

        // Verify row is deleted
        assert!(cache.get_table("test").unwrap().get(1).is_none());

        // Rollback
        tx.rollback(&mut cache).unwrap();

        // Verify row is restored
        assert!(cache.get_table("test").unwrap().get(1).is_some());
    }

    #[test]
    fn test_transaction_complex_rollback() {
        let mut cache = TableCache::new();
        cache.create_table(test_schema()).unwrap();

        // Insert initial rows
        let row1 = Row::new(1, vec![Value::Int64(1), Value::String("row1".into())]);
        let row2 = Row::new(2, vec![Value::Int64(2), Value::String("row2".into())]);
        cache.get_table_mut("test").unwrap().insert(row1).unwrap();
        cache.get_table_mut("test").unwrap().insert(row2).unwrap();

        // Start transaction with multiple operations
        let mut tx = Transaction::begin();

        // Insert new row
        let row3 = Row::new(3, vec![Value::Int64(3), Value::String("row3".into())]);
        tx.insert(&mut cache, "test", row3).unwrap();

        // Update existing row
        let updated_row1 = Row::new(1, vec![Value::Int64(1), Value::String("updated".into())]);
        tx.update(&mut cache, "test", 1, updated_row1).unwrap();

        // Delete existing row
        tx.delete(&mut cache, "test", 2).unwrap();

        // Verify intermediate state
        assert_eq!(cache.get_table("test").unwrap().len(), 2); // row1, row3 (row2 deleted)

        // Rollback all changes
        tx.rollback(&mut cache).unwrap();

        // Verify original state is restored
        assert_eq!(cache.get_table("test").unwrap().len(), 2); // row1, row2
        assert_eq!(
            cache.get_table("test").unwrap().get(1).unwrap().get(1),
            Some(&Value::String("row1".into()))
        );
        assert!(cache.get_table("test").unwrap().get(2).is_some());
        assert!(cache.get_table("test").unwrap().get(3).is_none());
    }

    #[test]
    fn test_transaction_error_on_inactive() {
        let mut cache = TableCache::new();
        cache.create_table(test_schema()).unwrap();

        let tx = Transaction::begin();
        let _ = tx.commit();

        // Cannot use committed transaction - it's consumed
        // This is enforced by Rust's ownership system
    }

    #[test]
    fn test_transaction_journal_entries() {
        let mut cache = TableCache::new();
        cache.create_table(test_schema()).unwrap();

        let mut tx = Transaction::begin();

        let row = Row::new(1, vec![Value::Int64(1), Value::String("test".into())]);
        tx.insert(&mut cache, "test", row).unwrap();

        // Check journal has the entry
        let changes = tx.get_changes();
        assert_eq!(changes.len(), 1);
        assert!(matches!(changes[0], JournalEntry::Insert { .. }));

        tx.commit().unwrap();
    }
}
