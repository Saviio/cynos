//! GIN (Generalized Inverted Index) implementation.
//!
//! GIN indexes are used for indexing composite values like JSONB,
//! arrays, and full-text search documents.

mod posting;

pub use posting::PostingList;

use crate::stats::IndexStats;
use alloc::collections::BTreeMap;
use alloc::string::String;
use alloc::vec::Vec;
use cynos_core::RowId;

/// A GIN index for JSONB and other composite types.
///
/// This index maintains two inverted indexes:
/// - `key_index`: Maps keys to row IDs (for `?` key existence queries)
/// - `key_value_index`: Maps (key, value) pairs to row IDs (for `@>` containment queries)
#[derive(Debug, Clone)]
pub struct GinIndex {
    /// Key → Row IDs (for key existence queries)
    key_index: BTreeMap<String, PostingList>,
    /// (Key, Value) → Row IDs (for containment queries)
    key_value_index: BTreeMap<(String, String), PostingList>,
    /// Statistics
    stats: IndexStats,
}

impl GinIndex {
    /// Creates a new empty GIN index.
    pub fn new() -> Self {
        Self {
            key_index: BTreeMap::new(),
            key_value_index: BTreeMap::new(),
            stats: IndexStats::new(),
        }
    }

    /// Returns the statistics for this index.
    pub fn stats(&self) -> &IndexStats {
        &self.stats
    }

    /// Indexes a key for a given row.
    pub fn add_key(&mut self, key: String, row_id: RowId) {
        self.key_index
            .entry(key)
            .or_insert_with(PostingList::new)
            .add(row_id);
        self.stats.add_rows(1);
    }

    /// Indexes a key-value pair for a given row.
    pub fn add_key_value(&mut self, key: String, value: String, row_id: RowId) {
        self.key_value_index
            .entry((key, value))
            .or_insert_with(PostingList::new)
            .add(row_id);
    }

    /// Indexes multiple keys for a given row.
    pub fn add_keys(&mut self, keys: impl IntoIterator<Item = String>, row_id: RowId) {
        for key in keys {
            self.add_key(key, row_id);
        }
    }

    /// Indexes multiple key-value pairs for a given row.
    pub fn add_key_values(
        &mut self,
        pairs: impl IntoIterator<Item = (String, String)>,
        row_id: RowId,
    ) {
        for (key, value) in pairs {
            self.add_key_value(key, value, row_id);
        }
    }

    /// Removes a key entry for a given row.
    pub fn remove_key(&mut self, key: &str, row_id: RowId) {
        if let Some(posting) = self.key_index.get_mut(key) {
            if posting.remove(row_id) {
                self.stats.remove_rows(1);
            }
            if posting.is_empty() {
                self.key_index.remove(key);
            }
        }
    }

    /// Removes a key-value entry for a given row.
    pub fn remove_key_value(&mut self, key: &str, value: &str, row_id: RowId) {
        let pair = (key.into(), value.into());
        if let Some(posting) = self.key_value_index.get_mut(&pair) {
            posting.remove(row_id);
            if posting.is_empty() {
                self.key_value_index.remove(&pair);
            }
        }
    }

    /// Checks if a key exists in any row (for `?` operator).
    pub fn contains_key(&self, key: &str) -> bool {
        self.key_index.contains_key(key)
    }

    /// Gets all row IDs that contain the given key (for `?` operator).
    pub fn get_by_key(&self, key: &str) -> Vec<RowId> {
        self.key_index
            .get(key)
            .map(|p| p.to_vec())
            .unwrap_or_default()
    }

    /// Gets all row IDs that contain the given key-value pair (for `@>` operator).
    pub fn get_by_key_value(&self, key: &str, value: &str) -> Vec<RowId> {
        let pair = (key.into(), value.into());
        self.key_value_index
            .get(&pair)
            .map(|p| p.to_vec())
            .unwrap_or_default()
    }

    /// Gets all row IDs that contain ALL of the given keys (AND query).
    pub fn get_by_keys_all(&self, keys: &[&str]) -> Vec<RowId> {
        if keys.is_empty() {
            return Vec::new();
        }

        let mut result: Option<PostingList> = None;

        for key in keys {
            match self.key_index.get(*key) {
                Some(posting) => {
                    result = Some(match result {
                        Some(r) => r.intersect(posting),
                        None => posting.clone(),
                    });
                }
                None => return Vec::new(), // Key not found, no matches
            }
        }

        result.map(|p| p.to_vec()).unwrap_or_default()
    }

    /// Gets all row IDs that contain ANY of the given keys (OR query).
    pub fn get_by_keys_any(&self, keys: &[&str]) -> Vec<RowId> {
        let mut result = PostingList::new();

        for key in keys {
            if let Some(posting) = self.key_index.get(*key) {
                result = result.union(posting);
            }
        }

        result.to_vec()
    }

    /// Gets all row IDs that contain ALL of the given key-value pairs (AND query).
    pub fn get_by_key_values_all(&self, pairs: &[(&str, &str)]) -> Vec<RowId> {
        if pairs.is_empty() {
            return Vec::new();
        }

        let mut result: Option<PostingList> = None;

        for (key, value) in pairs {
            let pair = ((*key).into(), (*value).into());
            match self.key_value_index.get(&pair) {
                Some(posting) => {
                    result = Some(match result {
                        Some(r) => r.intersect(posting),
                        None => posting.clone(),
                    });
                }
                None => return Vec::new(),
            }
        }

        result.map(|p| p.to_vec()).unwrap_or_default()
    }

    /// Returns the number of unique keys in the index.
    pub fn key_count(&self) -> usize {
        self.key_index.len()
    }

    /// Returns the number of unique key-value pairs in the index.
    pub fn key_value_count(&self) -> usize {
        self.key_value_index.len()
    }

    /// Clears the index.
    pub fn clear(&mut self) {
        self.key_index.clear();
        self.key_value_index.clear();
        self.stats.clear();
    }

    /// Returns true if the index is empty.
    pub fn is_empty(&self) -> bool {
        self.key_index.is_empty() && self.key_value_index.is_empty()
    }

    /// Estimates the cost of a key lookup.
    pub fn cost_key(&self, key: &str) -> usize {
        self.key_index.get(key).map(|p| p.len()).unwrap_or(0)
    }

    /// Estimates the cost of a key-value lookup.
    pub fn cost_key_value(&self, key: &str, value: &str) -> usize {
        let pair = (key.into(), value.into());
        self.key_value_index
            .get(&pair)
            .map(|p| p.len())
            .unwrap_or(0)
    }
}

impl Default for GinIndex {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloc::vec;

    #[test]
    fn test_gin_new() {
        let gin = GinIndex::new();
        assert!(gin.is_empty());
        assert_eq!(gin.key_count(), 0);
        assert_eq!(gin.key_value_count(), 0);
    }

    #[test]
    fn test_gin_add_key() {
        let mut gin = GinIndex::new();

        gin.add_key("name".into(), 1);
        gin.add_key("name".into(), 2);
        gin.add_key("age".into(), 1);

        assert!(gin.contains_key("name"));
        assert!(gin.contains_key("age"));
        assert!(!gin.contains_key("email"));

        assert_eq!(gin.get_by_key("name"), vec![1, 2]);
        assert_eq!(gin.get_by_key("age"), vec![1]);
    }

    #[test]
    fn test_gin_add_key_value() {
        let mut gin = GinIndex::new();

        gin.add_key_value("status".into(), "active".into(), 1);
        gin.add_key_value("status".into(), "active".into(), 2);
        gin.add_key_value("status".into(), "inactive".into(), 3);

        assert_eq!(gin.get_by_key_value("status", "active"), vec![1, 2]);
        assert_eq!(gin.get_by_key_value("status", "inactive"), vec![3]);
        assert_eq!(gin.get_by_key_value("status", "pending"), Vec::<RowId>::new());
    }

    #[test]
    fn test_gin_remove_key() {
        let mut gin = GinIndex::new();

        gin.add_key("name".into(), 1);
        gin.add_key("name".into(), 2);

        gin.remove_key("name", 1);
        assert_eq!(gin.get_by_key("name"), vec![2]);

        gin.remove_key("name", 2);
        assert!(!gin.contains_key("name"));
    }

    #[test]
    fn test_gin_get_by_keys_all() {
        let mut gin = GinIndex::new();

        // Row 1 has keys: name, age
        gin.add_key("name".into(), 1);
        gin.add_key("age".into(), 1);

        // Row 2 has keys: name, email
        gin.add_key("name".into(), 2);
        gin.add_key("email".into(), 2);

        // Row 3 has keys: name, age, email
        gin.add_key("name".into(), 3);
        gin.add_key("age".into(), 3);
        gin.add_key("email".into(), 3);

        // Query: rows with both "name" AND "age"
        let result = gin.get_by_keys_all(&["name", "age"]);
        assert_eq!(result, vec![1, 3]);

        // Query: rows with "name", "age", AND "email"
        let result = gin.get_by_keys_all(&["name", "age", "email"]);
        assert_eq!(result, vec![3]);

        // Query: rows with non-existent key
        let result = gin.get_by_keys_all(&["name", "nonexistent"]);
        assert!(result.is_empty());
    }

    #[test]
    fn test_gin_get_by_keys_any() {
        let mut gin = GinIndex::new();

        gin.add_key("name".into(), 1);
        gin.add_key("age".into(), 2);
        gin.add_key("email".into(), 3);

        // Query: rows with "name" OR "age"
        let result = gin.get_by_keys_any(&["name", "age"]);
        assert_eq!(result, vec![1, 2]);

        // Query: rows with "email" OR "nonexistent"
        let result = gin.get_by_keys_any(&["email", "nonexistent"]);
        assert_eq!(result, vec![3]);
    }

    #[test]
    fn test_gin_get_by_key_values_all() {
        let mut gin = GinIndex::new();

        // Row 1: status=active, type=user
        gin.add_key_value("status".into(), "active".into(), 1);
        gin.add_key_value("type".into(), "user".into(), 1);

        // Row 2: status=active, type=admin
        gin.add_key_value("status".into(), "active".into(), 2);
        gin.add_key_value("type".into(), "admin".into(), 2);

        // Row 3: status=inactive, type=user
        gin.add_key_value("status".into(), "inactive".into(), 3);
        gin.add_key_value("type".into(), "user".into(), 3);

        // Query: status=active AND type=user
        let result = gin.get_by_key_values_all(&[("status", "active"), ("type", "user")]);
        assert_eq!(result, vec![1]);

        // Query: status=active (any type)
        let result = gin.get_by_key_values_all(&[("status", "active")]);
        assert_eq!(result, vec![1, 2]);
    }

    #[test]
    fn test_gin_clear() {
        let mut gin = GinIndex::new();

        gin.add_key("name".into(), 1);
        gin.add_key_value("status".into(), "active".into(), 1);

        gin.clear();

        assert!(gin.is_empty());
        assert_eq!(gin.key_count(), 0);
        assert_eq!(gin.key_value_count(), 0);
    }

    #[test]
    fn test_gin_cost() {
        let mut gin = GinIndex::new();

        gin.add_key("name".into(), 1);
        gin.add_key("name".into(), 2);
        gin.add_key("name".into(), 3);
        gin.add_key("age".into(), 1);

        assert_eq!(gin.cost_key("name"), 3);
        assert_eq!(gin.cost_key("age"), 1);
        assert_eq!(gin.cost_key("nonexistent"), 0);
    }
}
