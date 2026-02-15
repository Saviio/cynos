//! Nullable index wrapper implementation.
//!
//! This module provides a wrapper around any index that allows null keys.
//! Null keys are stored separately from the main index.

use crate::stats::IndexStats;
use crate::traits::{Index, IndexError, KeyRange, RangeIndex};
use alloc::vec::Vec;
use cynos_core::RowId;

/// A wrapper around an index that allows null keys.
///
/// This index stores null keys separately from the main index,
/// allowing multiple null keys even in unique indexes (matching SQL behcynos).
#[derive(Debug)]
pub struct NullableIndex<K, I: Index<K>> {
    /// The underlying index for non-null keys.
    inner: I,
    /// Row IDs associated with null keys.
    null_values: Vec<RowId>,
    /// Statistics for this index.
    stats: IndexStats,
    /// Phantom data for the key type.
    _marker: core::marker::PhantomData<K>,
}

impl<K: Clone + Ord, I: Index<K>> NullableIndex<K, I> {
    /// Creates a new nullable index wrapping the given index.
    pub fn new(inner: I) -> Self {
        Self {
            inner,
            null_values: Vec::new(),
            stats: IndexStats::new(),
            _marker: core::marker::PhantomData,
        }
    }

    /// Returns the statistics for this index.
    pub fn stats(&self) -> &IndexStats {
        &self.stats
    }

    /// Adds a null key with the given value.
    pub fn add_null(&mut self, value: RowId) {
        self.null_values.push(value);
        self.stats.add_rows(1);
    }

    /// Gets all values associated with null keys.
    pub fn get_null(&self) -> Vec<RowId> {
        self.null_values.clone()
    }

    /// Removes a null key (and optionally a specific value).
    pub fn remove_null(&mut self, value: Option<RowId>) {
        match value {
            Some(v) => {
                let original_len = self.null_values.len();
                self.null_values.retain(|&x| x != v);
                let removed = original_len - self.null_values.len();
                self.stats.remove_rows(removed);
            }
            None => {
                let removed = self.null_values.len();
                self.null_values.clear();
                self.stats.remove_rows(removed);
            }
        }
    }

    /// Sets the null key to a single value, replacing any existing values.
    pub fn set_null(&mut self, value: RowId) {
        let removed = self.null_values.len();
        self.null_values.clear();
        self.null_values.push(value);
        // Adjust stats: remove old, add new
        if removed > 0 {
            self.stats.remove_rows(removed);
        }
        self.stats.add_rows(1);
    }

    /// Checks if there are any null keys.
    pub fn contains_null(&self) -> bool {
        !self.null_values.is_empty()
    }

    /// Returns the number of null values.
    pub fn null_count(&self) -> usize {
        self.null_values.len()
    }
}

impl<K: Clone + Ord, I: Index<K>> Index<K> for NullableIndex<K, I> {
    fn add(&mut self, key: K, value: RowId) -> Result<(), IndexError> {
        let result = self.inner.add(key, value);
        if result.is_ok() {
            self.stats.add_rows(1);
        }
        result
    }

    fn set(&mut self, key: K, value: RowId) {
        // Get current count for this key
        let old_count = self.inner.get(&key).len();
        self.inner.set(key, value);
        // Adjust stats
        if old_count > 0 {
            self.stats.remove_rows(old_count);
        }
        self.stats.add_rows(1);
    }

    fn get(&self, key: &K) -> Vec<RowId> {
        self.inner.get(key)
    }

    fn remove(&mut self, key: &K, value: Option<RowId>) {
        let old_count = match value {
            Some(_) => 1, // Assume we're removing one value
            None => self.inner.get(key).len(),
        };
        self.inner.remove(key, value);
        // Check how many were actually removed
        let new_count = self.inner.get(key).len();
        let removed = if value.is_some() {
            if new_count < old_count { 1 } else { 0 }
        } else {
            old_count
        };
        self.stats.remove_rows(removed);
    }

    fn contains_key(&self, key: &K) -> bool {
        self.inner.contains_key(key)
    }

    fn len(&self) -> usize {
        self.stats.total_rows()
    }

    fn clear(&mut self) {
        self.inner.clear();
        self.null_values.clear();
        self.stats.clear();
    }

    fn min(&self) -> Option<(&K, Vec<RowId>)> {
        self.inner.min()
    }

    fn max(&self) -> Option<(&K, Vec<RowId>)> {
        self.inner.max()
    }

    fn cost(&self, range: &KeyRange<K>) -> usize {
        self.inner.cost(range)
    }
}

impl<K: Clone + Ord, I: RangeIndex<K>> RangeIndex<K> for NullableIndex<K, I> {
    fn get_range(
        &self,
        range: Option<&KeyRange<K>>,
        reverse: bool,
        limit: Option<usize>,
        skip: usize,
    ) -> Vec<RowId> {
        // Get results from inner index
        let mut results = self.inner.get_range(range, reverse, limit, skip);

        // If no range specified (all), include null values at the end
        if range.is_none() || matches!(range, Some(KeyRange::All)) {
            // Null values are appended at the end
            let remaining_limit = limit.map(|l| l.saturating_sub(results.len()));
            let remaining_skip = skip.saturating_sub(results.len());

            if remaining_limit.map_or(true, |l| l > 0) {
                let null_iter = self.null_values.iter().skip(remaining_skip);
                if let Some(lim) = remaining_limit {
                    results.extend(null_iter.take(lim).copied());
                } else {
                    results.extend(null_iter.copied());
                }
            }
        }

        results
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::btree::BTreeIndex;
    use alloc::vec;

    #[test]
    fn test_nullable_index_basic() {
        let inner: BTreeIndex<i32> = BTreeIndex::new(5, false);
        let mut index = NullableIndex::new(inner);

        index.add(1, 2).unwrap();
        index.add(1, 3).unwrap();
        index.add(2, 4).unwrap();
        index.add(3, 5).unwrap();
        index.add_null(7);
        index.add_null(8);

        assert_eq!(index.get(&1), vec![2, 3]);
        assert_eq!(index.get_null(), vec![7, 8]);

        assert_eq!(*index.min().unwrap().0, 1);
        assert_eq!(*index.max().unwrap().0, 3);

        assert_eq!(index.cost(&KeyRange::All), 4); // Non-null count
    }

    #[test]
    fn test_nullable_index_remove() {
        let inner: BTreeIndex<i32> = BTreeIndex::new(5, false);
        let mut index = NullableIndex::new(inner);

        index.add(1, 2).unwrap();
        index.add(1, 3).unwrap();
        index.add(2, 4).unwrap();
        index.add_null(7);
        index.add_null(8);

        // Remove non-null key
        index.remove(&2, None);
        assert_eq!(index.get(&2), Vec::<RowId>::new());

        // Remove specific null value
        index.remove_null(Some(7));
        assert_eq!(index.get_null(), vec![8]);
    }

    #[test]
    fn test_nullable_index_set() {
        let inner: BTreeIndex<i32> = BTreeIndex::new(5, false);
        let mut index = NullableIndex::new(inner);

        index.add(1, 2).unwrap();
        index.add(1, 3).unwrap();
        index.add_null(7);
        index.add_null(8);

        // Set replaces all values for the key
        index.set(1, 10);
        assert_eq!(index.get(&1), vec![10]);

        // Set null replaces all null values
        index.set_null(9);
        assert_eq!(index.get_null(), vec![9]);
    }

    #[test]
    fn test_nullable_index_contains() {
        let inner: BTreeIndex<i32> = BTreeIndex::new(5, false);
        let mut index = NullableIndex::new(inner);

        index.add(1, 2).unwrap();
        index.add_null(7);

        assert!(index.contains_key(&1));
        assert!(index.contains_null());
        assert!(!index.contains_key(&2));

        index.remove_null(None);
        assert!(!index.contains_null());
    }

    #[test]
    fn test_nullable_index_clear() {
        let inner: BTreeIndex<i32> = BTreeIndex::new(5, false);
        let mut index = NullableIndex::new(inner);

        index.add(1, 2).unwrap();
        index.add_null(7);

        index.clear();

        assert!(!index.contains_key(&1));
        assert!(!index.contains_null());
        assert_eq!(index.len(), 0);
    }

    #[test]
    fn test_nullable_index_range() {
        let inner: BTreeIndex<i32> = BTreeIndex::new(5, false);
        let mut index = NullableIndex::new(inner);

        index.add(1, 10).unwrap();
        index.add(2, 20).unwrap();
        index.add(3, 30).unwrap();
        index.add_null(100);
        index.add_null(200);

        // Range query with bounds excludes null
        let range = KeyRange::bound(2, 3, false, false);
        let result = index.get_range(Some(&range), false, None, 0);
        assert_eq!(result, vec![20, 30]);

        // All range includes null at the end
        let result = index.get_range(None, false, None, 0);
        assert_eq!(result, vec![10, 20, 30, 100, 200]);
    }

    /// Test that unique nullable index allows multiple null keys
    /// (matching SQL behcynos)
    #[test]
    fn test_nullable_index_unique_allows_multiple_nulls() {
        let inner: BTreeIndex<i32> = BTreeIndex::new(5, true);
        let mut index = NullableIndex::new(inner);

        index.add_null(1);
        index.add(1, 2).unwrap();
        index.add_null(3);

        assert_eq!(index.get_null(), vec![1, 3]);
    }

    #[test]
    fn test_nullable_index_stats() {
        let inner: BTreeIndex<i32> = BTreeIndex::new(5, false);
        let mut index = NullableIndex::new(inner);

        index.add_null(1);
        index.add_null(2);
        index.add_null(7);
        index.add(1, 3).unwrap();
        index.add(1, 4).unwrap();
        index.add(1, 8).unwrap();
        index.add(2, 5).unwrap();
        assert_eq!(index.stats().total_rows(), 7);

        index.remove_null(Some(2));
        assert_eq!(index.stats().total_rows(), 6);

        index.remove_null(None);
        assert_eq!(index.stats().total_rows(), 4);

        index.set_null(22);
        assert_eq!(index.stats().total_rows(), 5);

        index.add_null(33);
        assert_eq!(index.stats().total_rows(), 6);

        index.remove_null(None);
        assert_eq!(index.stats().total_rows(), 4);

        index.clear();
        assert_eq!(index.stats().total_rows(), 0);
    }
}
