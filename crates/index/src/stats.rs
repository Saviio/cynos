//! Index statistics for Cynos database.
//!
//! This module provides statistics tracking for indexes.

use core::sync::atomic::{AtomicUsize, Ordering};

/// Statistics for an index.
#[derive(Debug)]
pub struct IndexStats {
    /// Total number of rows in the index.
    total_rows: AtomicUsize,
    /// Maximum key encountered (for numeric keys).
    max_key_encountered: AtomicUsize,
}

impl IndexStats {
    /// Creates a new empty stats instance.
    pub fn new() -> Self {
        Self {
            total_rows: AtomicUsize::new(0),
            max_key_encountered: AtomicUsize::new(0),
        }
    }

    /// Creates stats with initial values.
    pub fn with_values(total_rows: usize, max_key: usize) -> Self {
        Self {
            total_rows: AtomicUsize::new(total_rows),
            max_key_encountered: AtomicUsize::new(max_key),
        }
    }

    /// Returns the total number of rows.
    pub fn total_rows(&self) -> usize {
        self.total_rows.load(Ordering::Relaxed)
    }

    /// Returns the maximum key encountered.
    pub fn max_key_encountered(&self) -> usize {
        self.max_key_encountered.load(Ordering::Relaxed)
    }

    /// Increments the row count by the given amount.
    pub fn add_rows(&self, count: usize) {
        self.total_rows.fetch_add(count, Ordering::Relaxed);
    }

    /// Decrements the row count by the given amount.
    pub fn remove_rows(&self, count: usize) {
        self.total_rows.fetch_sub(count, Ordering::Relaxed);
    }

    /// Sets the total row count.
    pub fn set_total_rows(&self, count: usize) {
        self.total_rows.store(count, Ordering::Relaxed);
    }

    /// Updates the maximum key if the given key is larger.
    pub fn update_max_key(&self, key: usize) {
        self.max_key_encountered.fetch_max(key, Ordering::Relaxed);
    }

    /// Resets the row count to zero.
    pub fn clear(&self) {
        self.total_rows.store(0, Ordering::Relaxed);
    }
}

impl Default for IndexStats {
    fn default() -> Self {
        Self::new()
    }
}

impl Clone for IndexStats {
    fn clone(&self) -> Self {
        Self {
            total_rows: AtomicUsize::new(self.total_rows.load(Ordering::Relaxed)),
            max_key_encountered: AtomicUsize::new(self.max_key_encountered.load(Ordering::Relaxed)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_stats_new() {
        let stats = IndexStats::new();
        assert_eq!(stats.total_rows(), 0);
        assert_eq!(stats.max_key_encountered(), 0);
    }

    #[test]
    fn test_stats_add_remove_rows() {
        let stats = IndexStats::new();
        stats.add_rows(10);
        assert_eq!(stats.total_rows(), 10);
        stats.add_rows(5);
        assert_eq!(stats.total_rows(), 15);
        stats.remove_rows(3);
        assert_eq!(stats.total_rows(), 12);
    }

    #[test]
    fn test_stats_max_key() {
        let stats = IndexStats::new();
        stats.update_max_key(100);
        assert_eq!(stats.max_key_encountered(), 100);
        stats.update_max_key(50);
        assert_eq!(stats.max_key_encountered(), 100); // Should not decrease
        stats.update_max_key(200);
        assert_eq!(stats.max_key_encountered(), 200);
    }

    #[test]
    fn test_stats_clear() {
        let stats = IndexStats::new();
        stats.add_rows(100);
        stats.update_max_key(50);
        stats.clear();
        assert_eq!(stats.total_rows(), 0);
        assert_eq!(stats.max_key_encountered(), 50); // Max key is preserved
    }

    #[test]
    fn test_stats_clone() {
        let stats = IndexStats::with_values(100, 50);
        let cloned = stats.clone();
        assert_eq!(cloned.total_rows(), 100);
        assert_eq!(cloned.max_key_encountered(), 50);
    }
}
