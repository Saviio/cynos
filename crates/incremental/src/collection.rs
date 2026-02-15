//! Differential collection for Incremental View Maintenance.
//!
//! A DiffCollection maintains both a snapshot of the current state and
//! pending changes that haven't been committed yet.

use crate::delta::Delta;
use alloc::vec::Vec;
use core::hash::Hash;
use hashbrown::HashMap;

/// A collection that tracks both current state and pending changes.
///
/// This is the fundamental data structure for IVM, allowing efficient
/// incremental updates while maintaining a consistent snapshot.
#[derive(Clone, Debug)]
pub struct DiffCollection<T> {
    /// Current committed snapshot
    snapshot: Vec<T>,
    /// Pending changes not yet committed
    pending: Vec<Delta<T>>,
}

impl<T> Default for DiffCollection<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> DiffCollection<T> {
    /// Creates a new empty collection.
    pub fn new() -> Self {
        Self {
            snapshot: Vec::new(),
            pending: Vec::new(),
        }
    }

    /// Creates a collection from an initial snapshot.
    pub fn from_snapshot(snapshot: Vec<T>) -> Self {
        Self {
            snapshot,
            pending: Vec::new(),
        }
    }

    /// Returns a reference to the current snapshot.
    #[inline]
    pub fn snapshot(&self) -> &[T] {
        &self.snapshot
    }

    /// Returns a reference to pending changes.
    #[inline]
    pub fn pending(&self) -> &[Delta<T>] {
        &self.pending
    }

    /// Returns the number of items in the snapshot.
    #[inline]
    pub fn len(&self) -> usize {
        self.snapshot.len()
    }

    /// Returns true if the snapshot is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.snapshot.is_empty()
    }

    /// Returns true if there are pending changes.
    #[inline]
    pub fn has_pending(&self) -> bool {
        !self.pending.is_empty()
    }

    /// Adds a delta to the pending changes.
    pub fn apply(&mut self, delta: Delta<T>) {
        self.pending.push(delta);
    }

    /// Adds multiple deltas to the pending changes.
    pub fn apply_batch(&mut self, deltas: impl IntoIterator<Item = Delta<T>>) {
        self.pending.extend(deltas);
    }

    /// Clears all pending changes without committing.
    pub fn rollback(&mut self) {
        self.pending.clear();
    }

    /// Takes the pending changes, leaving an empty pending list.
    pub fn take_pending(&mut self) -> Vec<Delta<T>> {
        core::mem::take(&mut self.pending)
    }
}

impl<T: Clone + PartialEq> DiffCollection<T> {
    /// Commits pending changes to the snapshot.
    ///
    /// Insertions are added to the snapshot, deletions are removed.
    pub fn commit(&mut self) {
        for delta in self.pending.drain(..) {
            if delta.is_insert() {
                self.snapshot.push(delta.data);
            } else if delta.is_delete() {
                if let Some(pos) = self.snapshot.iter().position(|x| *x == delta.data) {
                    self.snapshot.remove(pos);
                }
            }
        }
    }
}

/// A collection that consolidates deltas by key.
///
/// This is useful when you need to track net changes per key,
/// automatically canceling out insert/delete pairs.
#[derive(Clone, Debug)]
pub struct ConsolidatedCollection<K, V> {
    /// Map from key to (value, net_diff)
    data: HashMap<K, (V, i32)>,
}

impl<K, V> Default for ConsolidatedCollection<K, V>
where
    K: Eq + Hash,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<K, V> ConsolidatedCollection<K, V>
where
    K: Eq + Hash,
{
    /// Creates a new empty consolidated collection.
    pub fn new() -> Self {
        Self {
            data: HashMap::new(),
        }
    }

    /// Returns the number of entries with non-zero diff.
    pub fn len(&self) -> usize {
        self.data.values().filter(|(_, diff)| *diff != 0).count()
    }

    /// Returns true if there are no entries with non-zero diff.
    pub fn is_empty(&self) -> bool {
        self.data.values().all(|(_, diff)| *diff == 0)
    }
}

impl<K, V> ConsolidatedCollection<K, V>
where
    K: Eq + Hash + Clone,
    V: Clone,
{
    /// Applies a delta, consolidating with existing entries.
    pub fn apply(&mut self, key: K, value: V, diff: i32) {
        self.data
            .entry(key)
            .and_modify(|(_, existing_diff)| *existing_diff += diff)
            .or_insert((value, diff));
    }

    /// Drains all entries with non-zero diff as deltas.
    pub fn drain_deltas<F>(&mut self, mut key_fn: F) -> Vec<Delta<V>>
    where
        F: FnMut(&K, &V) -> V,
    {
        let mut result = Vec::new();
        self.data.retain(|k, (v, diff)| {
            if *diff != 0 {
                result.push(Delta::new(key_fn(k, v), *diff));
            }
            false
        });
        result
    }

    /// Returns an iterator over entries with non-zero diff.
    pub fn iter(&self) -> impl Iterator<Item = (&K, &V, i32)> {
        self.data
            .iter()
            .filter(|(_, (_, diff))| *diff != 0)
            .map(|(k, (v, diff))| (k, v, *diff))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloc::vec;

    #[test]
    fn test_diff_collection_new() {
        let coll: DiffCollection<i32> = DiffCollection::new();
        assert!(coll.is_empty());
        assert!(!coll.has_pending());
    }

    #[test]
    fn test_diff_collection_from_snapshot() {
        let coll = DiffCollection::from_snapshot(vec![1, 2, 3]);
        assert_eq!(coll.len(), 3);
        assert_eq!(coll.snapshot(), &[1, 2, 3]);
    }

    #[test]
    fn test_diff_collection_apply_and_commit() {
        let mut coll: DiffCollection<i32> = DiffCollection::new();

        coll.apply(Delta::insert(1));
        coll.apply(Delta::insert(2));
        assert!(coll.has_pending());
        assert_eq!(coll.pending().len(), 2);

        coll.commit();
        assert!(!coll.has_pending());
        assert_eq!(coll.snapshot(), &[1, 2]);
    }

    #[test]
    fn test_diff_collection_delete() {
        let mut coll = DiffCollection::from_snapshot(vec![1, 2, 3]);

        coll.apply(Delta::delete(2));
        coll.commit();

        assert_eq!(coll.snapshot(), &[1, 3]);
    }

    #[test]
    fn test_diff_collection_rollback() {
        let mut coll = DiffCollection::from_snapshot(vec![1, 2, 3]);

        coll.apply(Delta::insert(4));
        coll.apply(Delta::delete(1));
        assert!(coll.has_pending());

        coll.rollback();
        assert!(!coll.has_pending());
        assert_eq!(coll.snapshot(), &[1, 2, 3]);
    }

    #[test]
    fn test_consolidated_collection() {
        let mut coll: ConsolidatedCollection<i32, &str> = ConsolidatedCollection::new();

        coll.apply(1, "a", 1);
        coll.apply(1, "a", 1);
        coll.apply(1, "a", -1);

        // Net diff for key 1 should be 1
        let entries: Vec<_> = coll.iter().collect();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].2, 1);
    }

    #[test]
    fn test_consolidated_collection_cancel_out() {
        let mut coll: ConsolidatedCollection<i32, &str> = ConsolidatedCollection::new();

        coll.apply(1, "a", 1);
        coll.apply(1, "a", -1);

        // Net diff should be 0, so it's effectively empty
        assert!(coll.is_empty());
    }
}