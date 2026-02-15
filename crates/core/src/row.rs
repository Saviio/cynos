//! Row structure for Cynos database.
//!
//! This module defines the `Row` struct which represents a single row in a table.

use crate::value::Value;
use alloc::vec::Vec;
use core::sync::atomic::{AtomicU64, Ordering};

/// Unique identifier for a row.
pub type RowId = u64;

/// A dummy row ID used for rows that don't correspond to a DB entry
/// (e.g., the result of joining two rows).
pub const DUMMY_ROW_ID: RowId = u64::MAX;

/// Global row ID counter for generating unique row IDs.
static NEXT_ROW_ID: AtomicU64 = AtomicU64::new(0);

/// Gets the next unique row ID.
pub fn next_row_id() -> RowId {
    NEXT_ROW_ID.fetch_add(1, Ordering::SeqCst)
}

/// Reserves a range of row IDs and returns the starting ID.
/// This is useful for bulk inserts where we need to allocate multiple IDs at once.
pub fn reserve_row_ids(count: u64) -> RowId {
    NEXT_ROW_ID.fetch_add(count, Ordering::SeqCst)
}

/// Sets the next row ID. Used by storage backends during initialization.
pub fn set_next_row_id(id: RowId) {
    NEXT_ROW_ID.store(id, Ordering::SeqCst);
}

/// Sets the next row ID only if it's greater than the current value.
pub fn set_next_row_id_if_greater(id: RowId) {
    NEXT_ROW_ID.fetch_max(id, Ordering::SeqCst);
}

/// A row in a database table.
#[derive(Clone, Debug)]
pub struct Row {
    /// Unique identifier for this row.
    id: RowId,
    /// Version number for change detection. Incremented on each update.
    /// For JOIN results, this is the sum of source row versions.
    version: u64,
    /// Values stored in this row, indexed by column position.
    values: Vec<Value>,
}

impl Row {
    /// Creates a new row with the given ID and values.
    /// Version defaults to 1 for new rows.
    pub fn new(id: RowId, values: Vec<Value>) -> Self {
        Self { id, version: 1, values }
    }

    /// Creates a new row with the given ID, version, and values.
    pub fn new_with_version(id: RowId, version: u64, values: Vec<Value>) -> Self {
        Self { id, version, values }
    }

    /// Creates a new row with an automatically assigned ID.
    pub fn create(values: Vec<Value>) -> Self {
        Self::new(next_row_id(), values)
    }

    /// Creates a dummy row (for join results, etc.).
    pub fn dummy(values: Vec<Value>) -> Self {
        Self::new(DUMMY_ROW_ID, values)
    }

    /// Creates a dummy row with a specific version (for join results).
    pub fn dummy_with_version(version: u64, values: Vec<Value>) -> Self {
        Self::new_with_version(DUMMY_ROW_ID, version, values)
    }

    /// Returns the row ID.
    #[inline]
    pub fn id(&self) -> RowId {
        self.id
    }

    /// Sets the row ID.
    pub fn set_id(&mut self, id: RowId) {
        self.id = id;
    }

    /// Returns the version number.
    #[inline]
    pub fn version(&self) -> u64 {
        self.version
    }

    /// Sets the version number.
    #[inline]
    pub fn set_version(&mut self, version: u64) {
        self.version = version;
    }

    /// Increments the version number and returns the new value.
    #[inline]
    pub fn increment_version(&mut self) -> u64 {
        self.version = self.version.wrapping_add(1);
        self.version
    }

    /// Returns a reference to the values.
    #[inline]
    pub fn values(&self) -> &[Value] {
        &self.values
    }

    /// Returns a mutable reference to the values.
    #[inline]
    pub fn values_mut(&mut self) -> &mut Vec<Value> {
        &mut self.values
    }

    /// Gets a value at the given column index.
    pub fn get(&self, index: usize) -> Option<&Value> {
        self.values.get(index)
    }

    /// Gets a mutable reference to a value at the given column index.
    pub fn get_mut(&mut self, index: usize) -> Option<&mut Value> {
        self.values.get_mut(index)
    }

    /// Sets a value at the given column index.
    pub fn set(&mut self, index: usize, value: Value) -> bool {
        if index < self.values.len() {
            self.values[index] = value;
            true
        } else {
            false
        }
    }

    /// Returns the number of values in this row.
    #[inline]
    pub fn len(&self) -> usize {
        self.values.len()
    }

    /// Returns true if this row has no values.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    /// Returns true if this is a dummy row.
    #[inline]
    pub fn is_dummy(&self) -> bool {
        self.id == DUMMY_ROW_ID
    }
}

impl PartialEq for Row {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id && self.values == other.values
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloc::vec;

    #[test]
    fn test_row_new() {
        let row = Row::new(1, vec![Value::Int64(42), Value::String("Alice".into())]);
        assert_eq!(row.id(), 1);
        assert_eq!(row.version(), 1);
        assert_eq!(row.len(), 2);
    }

    #[test]
    fn test_row_get_value() {
        let row = Row::new(1, vec![Value::Int64(1), Value::String("Alice".into())]);
        assert_eq!(row.get(0), Some(&Value::Int64(1)));
        assert_eq!(row.get(1), Some(&Value::String("Alice".into())));
        assert_eq!(row.get(2), None);
    }

    #[test]
    fn test_row_set_value() {
        let mut row = Row::new(1, vec![Value::Int64(1), Value::String("Alice".into())]);
        assert!(row.set(0, Value::Int64(100)));
        assert_eq!(row.get(0), Some(&Value::Int64(100)));
        assert!(!row.set(10, Value::Int64(999)));
    }

    #[test]
    fn test_row_create() {
        set_next_row_id(100);
        let row1 = Row::create(vec![Value::Int32(1)]);
        let row2 = Row::create(vec![Value::Int32(2)]);
        assert_eq!(row1.id(), 100);
        assert_eq!(row2.id(), 101);
    }

    #[test]
    fn test_row_dummy() {
        let row = Row::dummy(vec![Value::Int32(1)]);
        assert!(row.is_dummy());
        assert_eq!(row.id(), DUMMY_ROW_ID);
    }

    #[test]
    fn test_row_equality() {
        let row1 = Row::new(1, vec![Value::Int32(42)]);
        let row2 = Row::new(1, vec![Value::Int32(42)]);
        let row3 = Row::new(2, vec![Value::Int32(42)]);
        assert_eq!(row1, row2);
        assert_ne!(row1, row3);
    }

    #[test]
    fn test_row_version() {
        let mut row = Row::new(1, vec![Value::Int32(42)]);
        assert_eq!(row.version(), 1);

        row.increment_version();
        assert_eq!(row.version(), 2);

        row.set_version(10);
        assert_eq!(row.version(), 10);
    }

    #[test]
    fn test_row_dummy_with_version() {
        let row = Row::dummy_with_version(5, vec![Value::Int32(1)]);
        assert!(row.is_dummy());
        assert_eq!(row.version(), 5);
    }
}
