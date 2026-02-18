//! Row storage for Cynos database.
//!
//! This module provides the `RowStore` struct which manages rows for a single table,
//! including primary key and secondary index maintenance.

use alloc::collections::BTreeMap;
use alloc::format;
use alloc::rc::Rc;
use alloc::string::{String, ToString};
use alloc::vec::Vec;
use cynos_core::schema::Table;
use cynos_core::{Error, Result, Row, RowId, Value};
use cynos_incremental::Delta;
use cynos_index::{BTreeIndex, GinIndex, HashIndex, Index, KeyRange, RangeIndex};

/// Row storage backend: HashMap (O(1) lookup) or BTreeMap (O(log n) lookup).
#[cfg(feature = "hash-store")]
type RowMap = hashbrown::HashMap<RowId, Rc<Row>>;
#[cfg(not(feature = "hash-store"))]
type RowMap = BTreeMap<RowId, Rc<Row>>;

/// Trait for index storage that supports both point and range queries.
pub trait IndexStore {
    /// Adds a key-value pair to the index.
    fn add(&mut self, key: Value, row_id: RowId) -> core::result::Result<(), cynos_index::IndexError>;
    /// Sets a key-value pair, replacing any existing values.
    fn set(&mut self, key: Value, row_id: RowId);
    /// Gets all row IDs for a key.
    fn get(&self, key: &Value) -> Vec<RowId>;
    /// Removes a key-value pair.
    fn remove(&mut self, key: &Value, row_id: Option<RowId>);
    /// Checks if the index contains a key.
    fn contains_key(&self, key: &Value) -> bool;
    /// Returns the number of entries.
    fn len(&self) -> usize;
    /// Returns true if empty.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
    /// Returns whether this is a unique index.
    fn is_unique(&self) -> bool;
    /// Clears all entries.
    fn clear(&mut self);
    /// Gets range of row IDs.
    fn get_range(&self, range: Option<&KeyRange<Value>>, reverse: bool, limit: Option<usize>, skip: usize) -> Vec<RowId>;
    /// Returns all row IDs in the index.
    fn get_all(&self) -> Vec<RowId>;
}

/// Wrapper for BTreeIndex that implements IndexStore.
pub struct BTreeIndexStore {
    inner: BTreeIndex<Value>,
}

impl BTreeIndexStore {
    /// Creates a new BTree index store.
    pub fn new(unique: bool) -> Self {
        Self {
            inner: BTreeIndex::new(64, unique),
        }
    }
}

impl IndexStore for BTreeIndexStore {
    fn add(&mut self, key: Value, row_id: RowId) -> core::result::Result<(), cynos_index::IndexError> {
        self.inner.add(key, row_id)
    }

    fn set(&mut self, key: Value, row_id: RowId) {
        self.inner.set(key, row_id);
    }

    fn get(&self, key: &Value) -> Vec<RowId> {
        self.inner.get(key)
    }

    fn remove(&mut self, key: &Value, row_id: Option<RowId>) {
        self.inner.remove(key, row_id);
    }

    fn contains_key(&self, key: &Value) -> bool {
        self.inner.contains_key(key)
    }

    fn len(&self) -> usize {
        self.inner.len()
    }

    fn is_unique(&self) -> bool {
        self.inner.is_unique()
    }

    fn clear(&mut self) {
        self.inner.clear();
    }

    fn get_range(&self, range: Option<&KeyRange<Value>>, reverse: bool, limit: Option<usize>, skip: usize) -> Vec<RowId> {
        self.inner.get_range(range, reverse, limit, skip)
    }

    fn get_all(&self) -> Vec<RowId> {
        self.inner.get_range(None, false, None, 0)
    }
}

/// Wrapper for HashIndex that implements IndexStore.
pub struct HashIndexStore {
    inner: HashIndex<Value>,
}

impl HashIndexStore {
    /// Creates a new Hash index store.
    pub fn new(unique: bool) -> Self {
        Self {
            inner: HashIndex::new(unique),
        }
    }
}

impl IndexStore for HashIndexStore {
    fn add(&mut self, key: Value, row_id: RowId) -> core::result::Result<(), cynos_index::IndexError> {
        self.inner.add(key, row_id)
    }

    fn set(&mut self, key: Value, row_id: RowId) {
        self.inner.set(key, row_id);
    }

    fn get(&self, key: &Value) -> Vec<RowId> {
        self.inner.get(key)
    }

    fn remove(&mut self, key: &Value, row_id: Option<RowId>) {
        self.inner.remove(key, row_id);
    }

    fn contains_key(&self, key: &Value) -> bool {
        self.inner.contains_key(key)
    }

    fn len(&self) -> usize {
        self.inner.len()
    }

    fn is_unique(&self) -> bool {
        self.inner.is_unique()
    }

    fn clear(&mut self) {
        self.inner.clear();
    }

    fn get_range(&self, _range: Option<&KeyRange<Value>>, _reverse: bool, _limit: Option<usize>, _skip: usize) -> Vec<RowId> {
        self.get_all()
    }

    fn get_all(&self) -> Vec<RowId> {
        self.inner.get_all_row_ids()
    }
}

/// Extracts the key value from a row for the given column indices.
fn extract_key(row: &Row, col_indices: &[usize]) -> Value {
    if col_indices.len() == 1 {
        row.get(col_indices[0]).cloned().unwrap_or(Value::Null)
    } else {
        let values: Vec<Value> = col_indices
            .iter()
            .map(|&i| row.get(i).cloned().unwrap_or(Value::Null))
            .collect();
        let key_str: String = values
            .iter()
            .map(|v| format!("{:?}", v))
            .collect::<Vec<_>>()
            .join("|");
        Value::String(key_str)
    }
}

/// Row storage for a single table.
pub struct RowStore {
    schema: Table,
    rows: RowMap,
    row_id_index: BTreeIndexStore,
    primary_index: Option<BTreeIndexStore>,
    pk_columns: Vec<usize>,
    secondary_indices: BTreeMap<String, BTreeIndexStore>,
    index_columns: BTreeMap<String, Vec<usize>>,
    /// GIN indexes for JSONB columns
    gin_indices: BTreeMap<String, GinIndex>,
    /// Column indices for GIN indexes
    gin_index_columns: BTreeMap<String, usize>,
}

impl RowStore {
    /// Creates a new row store for the given table schema.
    pub fn new(schema: Table) -> Self {
        let mut store = Self {
            schema: schema.clone(),
            rows: RowMap::default(),
            row_id_index: BTreeIndexStore::new(true),
            primary_index: None,
            pk_columns: Vec::new(),
            secondary_indices: BTreeMap::new(),
            index_columns: BTreeMap::new(),
            gin_indices: BTreeMap::new(),
            gin_index_columns: BTreeMap::new(),
        };

        if let Some(pk) = schema.primary_key() {
            store.primary_index = Some(BTreeIndexStore::new(true));
            store.pk_columns = pk
                .columns()
                .iter()
                .filter_map(|c| schema.get_column_index(&c.name))
                .collect();
        }

        for idx in schema.indices() {
            let cols: Vec<usize> = idx
                .columns()
                .iter()
                .filter_map(|c| schema.get_column_index(&c.name))
                .collect();

            // Check if this is a GIN index (for JSONB columns)
            if idx.get_index_type() == cynos_core::schema::IndexType::Gin {
                if let Some(&col_idx) = cols.first() {
                    store.gin_indices.insert(idx.name().to_string(), GinIndex::new());
                    store.gin_index_columns.insert(idx.name().to_string(), col_idx);
                }
            } else {
                store.secondary_indices.insert(
                    idx.name().to_string(),
                    BTreeIndexStore::new(idx.is_unique()),
                );
                store.index_columns.insert(idx.name().to_string(), cols);
            }
        }

        store
    }

    /// Returns the table schema.
    pub fn schema(&self) -> &Table {
        &self.schema
    }

    /// Returns the number of rows.
    pub fn len(&self) -> usize {
        self.rows.len()
    }

    /// Returns true if the store is empty.
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    /// Inserts a row into the store.
    pub fn insert(&mut self, row: Row) -> Result<RowId> {
        let row_id = row.id();

        if self.rows.contains_key(&row_id) {
            return Err(Error::invalid_operation("Row ID already exists"));
        }

        // Check primary key uniqueness
        let pk_value = if !self.pk_columns.is_empty() {
            let pk = extract_key(&row, &self.pk_columns);
            if let Some(ref pk_index) = self.primary_index {
                if pk_index.contains_key(&pk) {
                    return Err(Error::UniqueConstraint {
                        column: "primary_key".into(),
                        value: pk,
                    });
                }
            }
            Some(pk)
        } else {
            None
        };

        // Add to row ID index
        self.row_id_index
            .add(Value::Int64(row_id as i64), row_id)
            .map_err(|_| Error::invalid_operation("Failed to add to row ID index"))?;

        // Add to primary key index
        if let (Some(ref mut pk_index), Some(pk)) = (&mut self.primary_index, pk_value.clone()) {
            if pk_index.add(pk.clone(), row_id).is_err() {
                self.row_id_index.remove(&Value::Int64(row_id as i64), Some(row_id));
                return Err(Error::UniqueConstraint {
                    column: "primary_key".into(),
                    value: pk,
                });
            }
        }

        // Add to secondary indices
        // Collect index names first to avoid borrow conflict
        let index_names: Vec<String> = self.index_columns.keys().cloned().collect();
        for idx_name in &index_names {
            let cols = &self.index_columns[idx_name];
            let key = extract_key(&row, cols);
            if let Some(idx) = self.secondary_indices.get_mut(idx_name) {
                if idx.add(key.clone(), row_id).is_err() {
                    self.rollback_insert(row_id, &row);
                    return Err(Error::UniqueConstraint {
                        column: idx_name.clone(),
                        value: key,
                    });
                }
            }
        }

        // Add to GIN indices
        let gin_index_names: Vec<String> = self.gin_index_columns.keys().cloned().collect();
        for idx_name in &gin_index_names {
            let col_idx = self.gin_index_columns[idx_name];
            if let Some(gin_idx) = self.gin_indices.get_mut(idx_name) {
                if let Some(value) = row.get(col_idx) {
                    Self::index_jsonb_value(gin_idx, value, row_id);
                }
            }
        }

        self.rows.insert(row_id, Rc::new(row));
        Ok(row_id)
    }

    fn rollback_insert(&mut self, row_id: RowId, row: &Row) {
        self.row_id_index.remove(&Value::Int64(row_id as i64), Some(row_id));

        if let Some(ref mut pk_index) = self.primary_index {
            let pk_value = extract_key(row, &self.pk_columns);
            pk_index.remove(&pk_value, Some(row_id));
        }

        let index_names: Vec<String> = self.index_columns.keys().cloned().collect();
        for idx_name in &index_names {
            let cols = &self.index_columns[idx_name];
            let key = extract_key(row, cols);
            if let Some(idx) = self.secondary_indices.get_mut(idx_name) {
                idx.remove(&key, Some(row_id));
            }
        }
    }

    /// Updates a row in the store.
    pub fn update(&mut self, row_id: RowId, new_row: Row) -> Result<()> {
        let old_row = self.rows.get(&row_id).ok_or_else(|| {
            Error::not_found(self.schema.name(), Value::Int64(row_id as i64))
        })?.clone();

        // Check primary key uniqueness if PK changed
        if !self.pk_columns.is_empty() {
            let old_pk = extract_key(&old_row, &self.pk_columns);
            let new_pk = extract_key(&new_row, &self.pk_columns);
            if let Some(ref pk_index) = self.primary_index {
                if old_pk != new_pk && pk_index.contains_key(&new_pk) {
                    return Err(Error::UniqueConstraint {
                        column: "primary_key".into(),
                        value: new_pk,
                    });
                }
            }
        }

        // Check secondary index uniqueness (only for unique indexes)
        for (idx_name, cols) in &self.index_columns {
            let old_key = extract_key(&old_row, cols);
            let new_key = extract_key(&new_row, cols);
            if let Some(idx) = self.secondary_indices.get(idx_name) {
                if idx.is_unique() && old_key != new_key && idx.contains_key(&new_key) {
                    return Err(Error::UniqueConstraint {
                        column: idx_name.clone(),
                        value: new_key,
                    });
                }
            }
        }

        // Update primary key index
        if !self.pk_columns.is_empty() {
            let old_pk = extract_key(&old_row, &self.pk_columns);
            let new_pk = extract_key(&new_row, &self.pk_columns);
            if let Some(ref mut pk_index) = self.primary_index {
                if old_pk != new_pk {
                    pk_index.remove(&old_pk, Some(row_id));
                    let _ = pk_index.add(new_pk, row_id);
                }
            }
        }

        // Update secondary indices
        let index_names: Vec<String> = self.index_columns.keys().cloned().collect();
        for idx_name in &index_names {
            let cols = &self.index_columns[idx_name];
            let old_key = extract_key(&old_row, cols);
            let new_key = extract_key(&new_row, cols);
            if let Some(idx) = self.secondary_indices.get_mut(idx_name) {
                if old_key != new_key {
                    idx.remove(&old_key, Some(row_id));
                    let _ = idx.add(new_key, row_id);
                }
            }
        }

        self.rows.insert(row_id, Rc::new(new_row));
        Ok(())
    }

    /// Deletes a row from the store.
    pub fn delete(&mut self, row_id: RowId) -> Result<Rc<Row>> {
        let row = self.rows.remove(&row_id).ok_or_else(|| {
            Error::not_found(self.schema.name(), Value::Int64(row_id as i64))
        })?;

        self.row_id_index.remove(&Value::Int64(row_id as i64), Some(row_id));

        if !self.pk_columns.is_empty() {
            let pk_value = extract_key(&row, &self.pk_columns);
            if let Some(ref mut pk_index) = self.primary_index {
                pk_index.remove(&pk_value, Some(row_id));
            }
        }

        let index_names: Vec<String> = self.index_columns.keys().cloned().collect();
        for idx_name in &index_names {
            let cols = &self.index_columns[idx_name];
            let key = extract_key(&row, cols);
            if let Some(idx) = self.secondary_indices.get_mut(idx_name) {
                idx.remove(&key, Some(row_id));
            }
        }

        Ok(row)
    }

    /// Gets a row by ID.
    pub fn get(&self, row_id: RowId) -> Option<Rc<Row>> {
        self.rows.get(&row_id).cloned()
    }

    /// Gets a mutable reference to a row by ID (requires exclusive access).
    /// Note: This clones the Rc and returns a new Row if mutation is needed.
    pub fn get_mut(&mut self, row_id: RowId) -> Option<&mut Row> {
        self.rows.get_mut(&row_id).map(|rc| Rc::make_mut(rc))
    }

    /// Returns an iterator over all rows.
    pub fn scan(&self) -> impl Iterator<Item = Rc<Row>> + '_ {
        self.rows.values().cloned()
    }

    /// Returns all row IDs.
    pub fn row_ids(&self) -> Vec<RowId> {
        self.rows.keys().copied().collect()
    }

    /// Gets rows by primary key value.
    pub fn get_by_pk(&self, pk_value: &Value) -> Vec<Rc<Row>> {
        if let Some(ref pk_index) = self.primary_index {
            pk_index
                .get(pk_value)
                .iter()
                .filter_map(|&id| self.rows.get(&id).cloned())
                .collect()
        } else {
            Vec::new()
        }
    }

    /// Finds existing row ID by primary key.
    pub fn find_row_id_by_pk(&self, row: &Row) -> Option<RowId> {
        if let Some(ref pk_index) = self.primary_index {
            let pk_value = extract_key(row, &self.pk_columns);
            pk_index.get(&pk_value).first().copied()
        } else {
            None
        }
    }

    /// Checks if a primary key value exists.
    pub fn pk_exists(&self, pk_value: &Value) -> bool {
        if let Some(ref pk_index) = self.primary_index {
            pk_index.contains_key(pk_value)
        } else {
            false
        }
    }

    /// Gets rows by index scan.
    pub fn index_scan(&self, index_name: &str, range: Option<&KeyRange<Value>>) -> Vec<Rc<Row>> {
        if let Some(idx) = self.secondary_indices.get(index_name) {
            idx.get_range(range, false, None, 0)
                .iter()
                .filter_map(|&id| self.rows.get(&id).cloned())
                .collect()
        } else {
            Vec::new()
        }
    }

    /// Gets rows by index scan with limit.
    pub fn index_scan_with_limit(
        &self,
        index_name: &str,
        range: Option<&KeyRange<Value>>,
        limit: Option<usize>,
    ) -> Vec<Rc<Row>> {
        self.index_scan_with_limit_offset(index_name, range, limit, 0)
    }

    /// Gets rows by index scan with limit and offset.
    /// This enables true pushdown of LIMIT/OFFSET to the storage layer.
    pub fn index_scan_with_limit_offset(
        &self,
        index_name: &str,
        range: Option<&KeyRange<Value>>,
        limit: Option<usize>,
        offset: usize,
    ) -> Vec<Rc<Row>> {
        self.index_scan_with_options(index_name, range, limit, offset, false)
    }

    /// Gets rows by index scan with limit, offset, and reverse option.
    /// This enables true pushdown of LIMIT/OFFSET/ORDER to the storage layer.
    pub fn index_scan_with_options(
        &self,
        index_name: &str,
        range: Option<&KeyRange<Value>>,
        limit: Option<usize>,
        offset: usize,
        reverse: bool,
    ) -> Vec<Rc<Row>> {
        if let Some(idx) = self.secondary_indices.get(index_name) {
            idx.get_range(range, reverse, limit, offset)
                .iter()
                .filter_map(|&id| self.rows.get(&id).cloned())
                .collect()
        } else {
            Vec::new()
        }
    }

    /// Clears all rows and indices.
    pub fn clear(&mut self) {
        self.rows.clear();
        self.row_id_index.clear();
        if let Some(ref mut pk_index) = self.primary_index {
            pk_index.clear();
        }
        for idx in self.secondary_indices.values_mut() {
            idx.clear();
        }
    }

    /// Gets multiple rows by IDs.
    pub fn get_many(&self, row_ids: &[RowId]) -> Vec<Option<Rc<Row>>> {
        row_ids.iter().map(|&id| self.rows.get(&id).cloned()).collect()
    }

    /// Inserts a row or replaces an existing row with the same primary key.
    /// Returns the row ID and whether it was a replacement.
    pub fn insert_or_replace(&mut self, row: Row) -> Result<(RowId, bool)> {
        // Check if a row with the same PK already exists
        if let Some(existing_row_id) = self.find_row_id_by_pk(&row) {
            // Replace: update the existing row, preserving the original row ID
            let updated_row = Row::new(existing_row_id, row.values().to_vec());
            self.update(existing_row_id, updated_row)?;
            Ok((existing_row_id, true))
        } else {
            // Insert: add as new row
            let row_id = self.insert(row)?;
            Ok((row_id, false))
        }
    }

    /// Checks if a secondary index contains a key (for unique constraint checking).
    pub fn secondary_index_contains(&self, index_name: &str, key: &Value) -> bool {
        if let Some(idx) = self.secondary_indices.get(index_name) {
            idx.contains_key(key)
        } else {
            false
        }
    }

    /// Gets the primary key columns indices.
    pub fn pk_columns(&self) -> &[usize] {
        &self.pk_columns
    }

    /// Extracts the primary key value from a row.
    pub fn extract_pk(&self, row: &Row) -> Option<Value> {
        if self.pk_columns.is_empty() {
            None
        } else {
            Some(extract_key(row, &self.pk_columns))
        }
    }

    /// Inserts a row and returns a Delta for IVM propagation.
    pub fn insert_with_delta(&mut self, row: Row) -> Result<Delta<Row>> {
        let row_clone = row.clone();
        self.insert(row)?;
        Ok(Delta::insert(row_clone))
    }

    /// Deletes a row and returns a Delta for IVM propagation.
    pub fn delete_with_delta(&mut self, row_id: RowId) -> Result<Delta<Row>> {
        let row = self.delete(row_id)?;
        Ok(Delta::delete((*row).clone()))
    }

    /// Updates a row and returns Deltas for IVM propagation (delete old + insert new).
    pub fn update_with_delta(&mut self, row_id: RowId, new_row: Row) -> Result<(Delta<Row>, Delta<Row>)> {
        let old_row = self.rows.get(&row_id).ok_or_else(|| {
            Error::not_found(self.schema.name(), Value::Int64(row_id as i64))
        })?.clone();
        let new_row_clone = new_row.clone();
        self.update(row_id, new_row)?;
        Ok((Delta::delete((*old_row).clone()), Delta::insert(new_row_clone)))
    }

    // ========== GIN Index Methods ==========

    /// Indexes a JSONB value into the GIN index.
    fn index_jsonb_value(gin_idx: &mut GinIndex, value: &Value, row_id: RowId) {
        if let Value::Jsonb(jsonb) = value {
            // Parse JSON and extract key-value pairs
            if let Ok(json_str) = core::str::from_utf8(&jsonb.0) {
                Self::extract_and_index_json(gin_idx, json_str, row_id);
            }
        }
    }

    /// Extracts key-value pairs from JSON and adds them to the GIN index.
    fn extract_and_index_json(gin_idx: &mut GinIndex, json_str: &str, row_id: RowId) {
        // Simple JSON parsing for top-level key-value pairs
        let trimmed = json_str.trim();
        if !trimmed.starts_with('{') || !trimmed.ends_with('}') {
            return;
        }

        let inner = &trimmed[1..trimmed.len() - 1];
        let mut depth = 0;
        let mut in_string = false;
        let mut escape = false;
        let mut start = 0;

        for (i, c) in inner.char_indices() {
            if escape {
                escape = false;
                continue;
            }
            match c {
                '\\' if in_string => escape = true,
                '"' => in_string = !in_string,
                '{' | '[' if !in_string => depth += 1,
                '}' | ']' if !in_string => depth -= 1,
                ',' if !in_string && depth == 0 => {
                    Self::parse_and_index_pair(gin_idx, &inner[start..i], row_id);
                    start = i + 1;
                }
                _ => {}
            }
        }
        // Parse last pair
        if start < inner.len() {
            Self::parse_and_index_pair(gin_idx, &inner[start..], row_id);
        }
    }

    /// Parses a key-value pair and adds it to the GIN index.
    fn parse_and_index_pair(gin_idx: &mut GinIndex, pair: &str, row_id: RowId) {
        let pair = pair.trim();
        if let Some(colon_pos) = pair.find(':') {
            let key = pair[..colon_pos].trim().trim_matches('"');
            let value = pair[colon_pos + 1..].trim();

            // Add key to index
            gin_idx.add_key(key.into(), row_id);

            // Add key-value pair to index (for string values)
            let value_str = if value.starts_with('"') && value.ends_with('"') {
                &value[1..value.len() - 1]
            } else {
                value
            };
            gin_idx.add_key_value(key.into(), value_str.into(), row_id);
        }
    }

    /// Removes JSONB value from the GIN index.
    #[allow(dead_code)]
    fn remove_jsonb_from_gin(gin_idx: &mut GinIndex, value: &Value, row_id: RowId) {
        if let Value::Jsonb(jsonb) = value {
            if let Ok(json_str) = core::str::from_utf8(&jsonb.0) {
                Self::extract_and_remove_json(gin_idx, json_str, row_id);
            }
        }
    }

    /// Extracts key-value pairs from JSON and removes them from the GIN index.
    fn extract_and_remove_json(gin_idx: &mut GinIndex, json_str: &str, row_id: RowId) {
        let trimmed = json_str.trim();
        if !trimmed.starts_with('{') || !trimmed.ends_with('}') {
            return;
        }

        let inner = &trimmed[1..trimmed.len() - 1];
        let mut depth = 0;
        let mut in_string = false;
        let mut escape = false;
        let mut start = 0;

        for (i, c) in inner.char_indices() {
            if escape {
                escape = false;
                continue;
            }
            match c {
                '\\' if in_string => escape = true,
                '"' => in_string = !in_string,
                '{' | '[' if !in_string => depth += 1,
                '}' | ']' if !in_string => depth -= 1,
                ',' if !in_string && depth == 0 => {
                    Self::parse_and_remove_pair(gin_idx, &inner[start..i], row_id);
                    start = i + 1;
                }
                _ => {}
            }
        }
        if start < inner.len() {
            Self::parse_and_remove_pair(gin_idx, &inner[start..], row_id);
        }
    }

    /// Parses a key-value pair and removes it from the GIN index.
    fn parse_and_remove_pair(gin_idx: &mut GinIndex, pair: &str, row_id: RowId) {
        let pair = pair.trim();
        if let Some(colon_pos) = pair.find(':') {
            let key = pair[..colon_pos].trim().trim_matches('"');
            let value = pair[colon_pos + 1..].trim();

            gin_idx.remove_key(key, row_id);

            let value_str = if value.starts_with('"') && value.ends_with('"') {
                &value[1..value.len() - 1]
            } else {
                value
            };
            gin_idx.remove_key_value(key, value_str, row_id);
        }
    }

    /// Queries the GIN index by key-value pair.
    pub fn gin_index_get_by_key_value(&self, index_name: &str, key: &str, value: &str) -> Vec<Rc<Row>> {
        if let Some(gin_idx) = self.gin_indices.get(index_name) {
            let row_ids = gin_idx.get_by_key_value(key, value);
            row_ids.iter().filter_map(|&id| self.rows.get(&id).cloned()).collect()
        } else {
            Vec::new()
        }
    }

    /// Queries the GIN index by key existence.
    pub fn gin_index_get_by_key(&self, index_name: &str, key: &str) -> Vec<Rc<Row>> {
        if let Some(gin_idx) = self.gin_indices.get(index_name) {
            let row_ids = gin_idx.get_by_key(key);
            row_ids.iter().filter_map(|&id| self.rows.get(&id).cloned()).collect()
        } else {
            Vec::new()
        }
    }

    /// Queries the GIN index by multiple key-value pairs (AND query).
    /// Returns rows that match ALL of the given key-value pairs.
    pub fn gin_index_get_by_key_values_all(&self, index_name: &str, pairs: &[(&str, &str)]) -> Vec<Rc<Row>> {
        if let Some(gin_idx) = self.gin_indices.get(index_name) {
            let row_ids = gin_idx.get_by_key_values_all(pairs);
            row_ids.iter().filter_map(|&id| self.rows.get(&id).cloned()).collect()
        } else {
            Vec::new()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cynos_core::schema::TableBuilder;
    use cynos_core::DataType;
    use alloc::vec;

    fn test_schema() -> Table {
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

    fn test_schema_with_index() -> Table {
        TableBuilder::new("test")
            .unwrap()
            .add_column("id", DataType::Int64)
            .unwrap()
            .add_column("value", DataType::Int64)
            .unwrap()
            .add_primary_key(&["id"], false)
            .unwrap()
            .add_index("idx_value", &["value"], false)
            .unwrap()
            .build()
            .unwrap()
    }

    #[test]
    fn test_row_store_insert() {
        let mut store = RowStore::new(test_schema());
        let row = Row::new(1, vec![Value::Int64(1), Value::String("Alice".into())]);
        assert!(store.insert(row).is_ok());
        assert_eq!(store.len(), 1);
    }

    #[test]
    fn test_row_store_get() {
        let mut store = RowStore::new(test_schema());
        let row = Row::new(1, vec![Value::Int64(1), Value::String("Alice".into())]);
        store.insert(row).unwrap();
        let retrieved = store.get(1);
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().get(1), Some(&Value::String("Alice".into())));
    }

    #[test]
    fn test_row_store_update() {
        let mut store = RowStore::new(test_schema());
        let row = Row::new(1, vec![Value::Int64(1), Value::String("Alice".into())]);
        store.insert(row).unwrap();
        let new_row = Row::new(1, vec![Value::Int64(1), Value::String("Bob".into())]);
        assert!(store.update(1, new_row).is_ok());
        let retrieved = store.get(1);
        assert_eq!(retrieved.unwrap().get(1), Some(&Value::String("Bob".into())));
    }

    #[test]
    fn test_row_store_delete() {
        let mut store = RowStore::new(test_schema());
        let row = Row::new(1, vec![Value::Int64(1), Value::String("Alice".into())]);
        store.insert(row).unwrap();
        assert!(store.delete(1).is_ok());
        assert_eq!(store.len(), 0);
        assert!(store.get(1).is_none());
    }

    #[test]
    fn test_row_store_pk_uniqueness() {
        let mut store = RowStore::new(test_schema());
        let row1 = Row::new(1, vec![Value::Int64(1), Value::String("Alice".into())]);
        let row2 = Row::new(2, vec![Value::Int64(1), Value::String("Bob".into())]);
        store.insert(row1).unwrap();
        assert!(store.insert(row2).is_err());
    }

    #[test]
    fn test_row_store_scan() {
        let mut store = RowStore::new(test_schema());
        store.insert(Row::new(1, vec![Value::Int64(1), Value::String("Alice".into())])).unwrap();
        store.insert(Row::new(2, vec![Value::Int64(2), Value::String("Bob".into())])).unwrap();
        let rows: Vec<_> = store.scan().collect();
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn test_row_store_index_maintenance() {
        let mut store = RowStore::new(test_schema_with_index());
        let row = Row::new(1, vec![Value::Int64(1), Value::Int64(100)]);
        store.insert(row).unwrap();
        let results = store.index_scan("idx_value", Some(&KeyRange::only(Value::Int64(100))));
        assert_eq!(results.len(), 1);
        store.delete(1).unwrap();
        let results = store.index_scan("idx_value", Some(&KeyRange::only(Value::Int64(100))));
        assert_eq!(results.len(), 0);
    }

    #[test]
    fn test_row_store_clear() {
        let mut store = RowStore::new(test_schema());
        store.insert(Row::new(1, vec![Value::Int64(1), Value::String("Alice".into())])).unwrap();
        store.insert(Row::new(2, vec![Value::Int64(2), Value::String("Bob".into())])).unwrap();
        store.clear();
        assert!(store.is_empty());
    }

    // === Additional tests for better coverage ===

    fn test_schema_composite_pk() -> Table {
        TableBuilder::new("test")
            .unwrap()
            .add_column("id1", DataType::String)
            .unwrap()
            .add_column("id2", DataType::Int64)
            .unwrap()
            .add_column("name", DataType::String)
            .unwrap()
            .add_primary_key(&["id1", "id2"], false)
            .unwrap()
            .build()
            .unwrap()
    }

    fn test_schema_with_unique_index() -> Table {
        TableBuilder::new("test")
            .unwrap()
            .add_column("id", DataType::Int64)
            .unwrap()
            .add_column("email", DataType::String)
            .unwrap()
            .add_primary_key(&["id"], false)
            .unwrap()
            .add_index("idx_email", &["email"], true) // unique index
            .unwrap()
            .build()
            .unwrap()
    }

    #[test]
    fn test_composite_primary_key() {
        let mut store = RowStore::new(test_schema_composite_pk());

        let row1 = Row::new(1, vec![
            Value::String("pk1".into()),
            Value::Int64(100),
            Value::String("Name1".into())
        ]);
        assert!(store.insert(row1).is_ok());

        // Same id1, different id2 - should succeed
        let row2 = Row::new(2, vec![
            Value::String("pk1".into()),
            Value::Int64(200),
            Value::String("Name2".into())
        ]);
        assert!(store.insert(row2).is_ok());

        // Same composite key - should fail
        let row3 = Row::new(3, vec![
            Value::String("pk1".into()),
            Value::Int64(100),
            Value::String("Name3".into())
        ]);
        assert!(store.insert(row3).is_err());
    }

    #[test]
    fn test_insert_or_replace_new() {
        let mut store = RowStore::new(test_schema());
        let row = Row::new(1, vec![Value::Int64(1), Value::String("Alice".into())]);

        let (row_id, replaced) = store.insert_or_replace(row).unwrap();
        assert_eq!(row_id, 1);
        assert!(!replaced);
        assert_eq!(store.len(), 1);
    }

    #[test]
    fn test_insert_or_replace_existing() {
        let mut store = RowStore::new(test_schema());

        // Insert first row
        let row1 = Row::new(1, vec![Value::Int64(1), Value::String("Alice".into())]);
        store.insert(row1).unwrap();

        // Replace with same PK but different row ID
        let row2 = Row::new(2, vec![Value::Int64(1), Value::String("Updated".into())]);
        let (row_id, replaced) = store.insert_or_replace(row2).unwrap();

        assert_eq!(row_id, 1); // Should preserve original row ID
        assert!(replaced);
        assert_eq!(store.len(), 1);

        let stored = store.get(1).unwrap();
        assert_eq!(stored.get(1), Some(&Value::String("Updated".into())));
    }

    #[test]
    fn test_update_pk_violation() {
        let mut store = RowStore::new(test_schema());

        // Insert two rows
        let row1 = Row::new(1, vec![Value::Int64(1), Value::String("Alice".into())]);
        let row2 = Row::new(2, vec![Value::Int64(2), Value::String("Bob".into())]);
        store.insert(row1).unwrap();
        store.insert(row2).unwrap();

        // Try to update row2 to have the same PK as row1
        let row2_updated = Row::new(2, vec![Value::Int64(1), Value::String("Bob Updated".into())]);
        let result = store.update(2, row2_updated);
        assert!(result.is_err());
    }

    #[test]
    fn test_unique_index_violation() {
        let mut store = RowStore::new(test_schema_with_unique_index());

        let row1 = Row::new(1, vec![Value::Int64(1), Value::String("alice@test.com".into())]);
        store.insert(row1).unwrap();

        // Try to insert with same email (unique index violation)
        let row2 = Row::new(2, vec![Value::Int64(2), Value::String("alice@test.com".into())]);
        let result = store.insert(row2);
        assert!(result.is_err());
    }

    #[test]
    fn test_unique_index_update_violation() {
        let mut store = RowStore::new(test_schema_with_unique_index());

        let row1 = Row::new(1, vec![Value::Int64(1), Value::String("alice@test.com".into())]);
        let row2 = Row::new(2, vec![Value::Int64(2), Value::String("bob@test.com".into())]);
        store.insert(row1).unwrap();
        store.insert(row2).unwrap();

        // Try to update row2 to have the same email as row1
        let row2_updated = Row::new(2, vec![Value::Int64(2), Value::String("alice@test.com".into())]);
        let result = store.update(2, row2_updated);
        assert!(result.is_err());
    }

    #[test]
    fn test_delete_then_insert_same_pk() {
        let mut store = RowStore::new(test_schema());

        // Insert and delete
        let row1 = Row::new(1, vec![Value::Int64(100), Value::String("Alice".into())]);
        store.insert(row1).unwrap();
        store.delete(1).unwrap();

        // Insert with same PK should succeed
        let row2 = Row::new(2, vec![Value::Int64(100), Value::String("Bob".into())]);
        assert!(store.insert(row2).is_ok());
        assert_eq!(store.len(), 1);
    }

    #[test]
    fn test_index_update_maintenance() {
        let mut store = RowStore::new(test_schema_with_index());

        // Insert row
        let row = Row::new(1, vec![Value::Int64(1), Value::Int64(100)]);
        store.insert(row).unwrap();

        // Verify index has the value
        let results = store.index_scan("idx_value", Some(&KeyRange::only(Value::Int64(100))));
        assert_eq!(results.len(), 1);

        // Update the indexed value
        let updated = Row::new(1, vec![Value::Int64(1), Value::Int64(200)]);
        store.update(1, updated).unwrap();

        // Old value should not be in index
        let results = store.index_scan("idx_value", Some(&KeyRange::only(Value::Int64(100))));
        assert_eq!(results.len(), 0);

        // New value should be in index
        let results = store.index_scan("idx_value", Some(&KeyRange::only(Value::Int64(200))));
        assert_eq!(results.len(), 1);
    }

    // === Delta integration tests ===

    #[test]
    fn test_insert_with_delta() {
        let mut store = RowStore::new(test_schema());
        let row = Row::new(1, vec![Value::Int64(1), Value::String("Alice".into())]);
        let delta = store.insert_with_delta(row.clone()).unwrap();

        assert_eq!(delta.diff(), 1);
        assert_eq!(delta.data(), &row);
        assert_eq!(store.len(), 1);
    }

    #[test]
    fn test_delete_with_delta() {
        let mut store = RowStore::new(test_schema());
        let row = Row::new(1, vec![Value::Int64(1), Value::String("Alice".into())]);
        store.insert(row.clone()).unwrap();

        let delta = store.delete_with_delta(1).unwrap();
        assert_eq!(delta.diff(), -1);
        assert_eq!(delta.data(), &row);
        assert_eq!(store.len(), 0);
    }

    #[test]
    fn test_update_with_delta() {
        let mut store = RowStore::new(test_schema());
        let old_row = Row::new(1, vec![Value::Int64(1), Value::String("Alice".into())]);
        store.insert(old_row.clone()).unwrap();

        let new_row = Row::new(1, vec![Value::Int64(1), Value::String("Bob".into())]);
        let (delete_delta, insert_delta) = store.update_with_delta(1, new_row.clone()).unwrap();

        assert_eq!(delete_delta.diff(), -1);
        assert_eq!(delete_delta.data(), &old_row);
        assert_eq!(insert_delta.diff(), 1);
        assert_eq!(insert_delta.data(), &new_row);
        assert_eq!(store.len(), 1);
    }
}
