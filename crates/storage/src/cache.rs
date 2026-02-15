//! Cache management for Cynos database.
//!
//! This module provides the `TableCache` struct which manages multiple table stores.

use crate::row_store::RowStore;
use alloc::collections::BTreeMap;
use alloc::format;
use alloc::rc::Rc;
use alloc::string::{String, ToString};
use alloc::vec::Vec;
use cynos_core::schema::Table;
use cynos_core::{Error, Result, Row, RowId};

/// Cache for managing multiple table stores.
pub struct TableCache {
    /// Table name → RowStore mapping.
    tables: BTreeMap<String, RowStore>,
}

impl TableCache {
    /// Creates a new empty table cache.
    pub fn new() -> Self {
        Self {
            tables: BTreeMap::new(),
        }
    }

    /// Creates a table in the cache.
    pub fn create_table(&mut self, schema: Table) -> Result<()> {
        let name = schema.name().to_string();
        if self.tables.contains_key(&name) {
            return Err(Error::invalid_schema(format!(
                "Table already exists: {}",
                name
            )));
        }
        self.tables.insert(name, RowStore::new(schema));
        Ok(())
    }

    /// Drops a table from the cache.
    pub fn drop_table(&mut self, name: &str) -> Result<()> {
        if self.tables.remove(name).is_none() {
            return Err(Error::table_not_found(name));
        }
        Ok(())
    }

    /// Gets a reference to a table store.
    pub fn get_table(&self, name: &str) -> Option<&RowStore> {
        self.tables.get(name)
    }

    /// Gets a mutable reference to a table store.
    pub fn get_table_mut(&mut self, name: &str) -> Option<&mut RowStore> {
        self.tables.get_mut(name)
    }

    /// Returns the number of tables.
    pub fn table_count(&self) -> usize {
        self.tables.len()
    }

    /// Returns all table names.
    pub fn table_names(&self) -> Vec<&str> {
        self.tables.keys().map(|s| s.as_str()).collect()
    }

    /// Returns the total row count across all tables.
    pub fn total_row_count(&self) -> usize {
        self.tables.values().map(|t| t.len()).sum()
    }

    /// Gets a row by table name and row ID.
    pub fn get_row(&self, table: &str, row_id: RowId) -> Option<Rc<Row>> {
        self.tables.get(table).and_then(|t| t.get(row_id))
    }

    /// Gets multiple rows by table name and row IDs.
    pub fn get_many(&self, table: &str, row_ids: &[RowId]) -> Vec<Option<Rc<Row>>> {
        if let Some(store) = self.tables.get(table) {
            store.get_many(row_ids)
        } else {
            row_ids.iter().map(|_| None).collect()
        }
    }

    /// Checks if a table exists.
    pub fn has_table(&self, name: &str) -> bool {
        self.tables.contains_key(name)
    }

    /// Clears all tables.
    pub fn clear(&mut self) {
        for store in self.tables.values_mut() {
            store.clear();
        }
    }

    /// Clears a specific table.
    pub fn clear_table(&mut self, name: &str) -> Result<()> {
        if let Some(store) = self.tables.get_mut(name) {
            store.clear();
            Ok(())
        } else {
            Err(Error::table_not_found(name))
        }
    }
}

impl Default for TableCache {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cynos_core::schema::TableBuilder;
    use cynos_core::{DataType, Value};
    use alloc::vec;

    fn test_schema(name: &str) -> Table {
        TableBuilder::new(name)
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
    fn test_cache_create_table() {
        let mut cache = TableCache::new();
        let result = cache.create_table(test_schema("users"));
        assert!(result.is_ok());
        assert!(cache.has_table("users"));
    }

    #[test]
    fn test_cache_create_duplicate_table() {
        let mut cache = TableCache::new();
        cache.create_table(test_schema("users")).unwrap();
        let result = cache.create_table(test_schema("users"));
        assert!(result.is_err());
    }

    #[test]
    fn test_cache_drop_table() {
        let mut cache = TableCache::new();
        cache.create_table(test_schema("users")).unwrap();
        let result = cache.drop_table("users");
        assert!(result.is_ok());
        assert!(!cache.has_table("users"));
    }

    #[test]
    fn test_cache_drop_nonexistent_table() {
        let mut cache = TableCache::new();
        let result = cache.drop_table("nonexistent");
        assert!(result.is_err());
    }

    #[test]
    fn test_cache_get_table() {
        let mut cache = TableCache::new();
        cache.create_table(test_schema("users")).unwrap();

        assert!(cache.get_table("users").is_some());
        assert!(cache.get_table("nonexistent").is_none());
    }

    #[test]
    fn test_cache_insert_and_get() {
        let mut cache = TableCache::new();
        cache.create_table(test_schema("users")).unwrap();

        let row = Row::new(1, vec![Value::Int64(1), Value::String("Alice".into())]);
        cache.get_table_mut("users").unwrap().insert(row).unwrap();

        let retrieved = cache.get_row("users", 1);
        assert!(retrieved.is_some());
    }

    #[test]
    fn test_cache_total_row_count() {
        let mut cache = TableCache::new();
        cache.create_table(test_schema("users")).unwrap();
        cache.create_table(test_schema("orders")).unwrap();

        cache.get_table_mut("users").unwrap()
            .insert(Row::new(1, vec![Value::Int64(1), Value::String("Alice".into())]))
            .unwrap();
        cache.get_table_mut("users").unwrap()
            .insert(Row::new(2, vec![Value::Int64(2), Value::String("Bob".into())]))
            .unwrap();
        cache.get_table_mut("orders").unwrap()
            .insert(Row::new(3, vec![Value::Int64(1), Value::String("Order1".into())]))
            .unwrap();

        assert_eq!(cache.total_row_count(), 3);
    }

    #[test]
    fn test_cache_clear() {
        let mut cache = TableCache::new();
        cache.create_table(test_schema("users")).unwrap();
        cache.get_table_mut("users").unwrap()
            .insert(Row::new(1, vec![Value::Int64(1), Value::String("Alice".into())]))
            .unwrap();

        cache.clear();
        assert_eq!(cache.total_row_count(), 0);
        // Tables still exist, just empty
        assert!(cache.has_table("users"));
    }
}
