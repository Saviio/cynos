//! Delta type for Incremental View Maintenance.
//!
//! A Delta represents a change to a data item, with a diff value indicating
//! whether it's an insertion (+1) or deletion (-1).

use alloc::vec::Vec;

/// A differential change to a data item.
///
/// The `diff` field indicates the multiplicity of the change:
/// - `+1` means insertion
/// - `-1` means deletion
/// - Other values can represent multiple insertions/deletions
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Delta<T> {
    /// The data being changed
    pub data: T,
    /// The differential: +1 for insert, -1 for delete
    pub diff: i32,
}

impl<T> Delta<T> {
    /// Creates a new delta with the given data and diff.
    #[inline]
    pub fn new(data: T, diff: i32) -> Self {
        Self { data, diff }
    }

    /// Creates an insertion delta (+1).
    #[inline]
    pub fn insert(data: T) -> Self {
        Self { data, diff: 1 }
    }

    /// Creates a deletion delta (-1).
    #[inline]
    pub fn delete(data: T) -> Self {
        Self { data, diff: -1 }
    }

    /// Returns true if this is an insertion (diff > 0).
    #[inline]
    pub fn is_insert(&self) -> bool {
        self.diff > 0
    }

    /// Returns true if this is a deletion (diff < 0).
    #[inline]
    pub fn is_delete(&self) -> bool {
        self.diff < 0
    }

    /// Returns true if this delta has no effect (diff == 0).
    #[inline]
    pub fn is_noop(&self) -> bool {
        self.diff == 0
    }

    /// Maps the data to a new type.
    #[inline]
    pub fn map<U, F>(self, f: F) -> Delta<U>
    where
        F: FnOnce(T) -> U,
    {
        Delta {
            data: f(self.data),
            diff: self.diff,
        }
    }

    /// Returns a reference to the data.
    #[inline]
    pub fn data(&self) -> &T {
        &self.data
    }

    /// Returns the diff value.
    #[inline]
    pub fn diff(&self) -> i32 {
        self.diff
    }

    /// Negates the diff (turns insert into delete and vice versa).
    #[inline]
    pub fn negate(self) -> Self {
        Self {
            data: self.data,
            diff: -self.diff,
        }
    }
}

impl<T: Clone> Delta<T> {
    /// Creates a negated copy of this delta.
    #[inline]
    pub fn negated(&self) -> Self {
        Self {
            data: self.data.clone(),
            diff: -self.diff,
        }
    }
}

/// A batch of deltas.
pub type DeltaBatch<T> = Vec<Delta<T>>;

/// Extension trait for working with delta batches.
pub trait DeltaBatchExt<T> {
    /// Filters out no-op deltas (diff == 0).
    fn compact(self) -> Self;

    /// Returns the net effect count (sum of all diffs).
    fn net_count(&self) -> i64;
}

impl<T> DeltaBatchExt<T> for DeltaBatch<T> {
    fn compact(self) -> Self {
        self.into_iter().filter(|d| d.diff != 0).collect()
    }

    fn net_count(&self) -> i64 {
        self.iter().map(|d| d.diff as i64).sum()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloc::vec;

    #[test]
    fn test_delta_insert() {
        let d = Delta::insert(42);
        assert!(d.is_insert());
        assert!(!d.is_delete());
        assert!(!d.is_noop());
        assert_eq!(d.diff, 1);
        assert_eq!(d.data, 42);
    }

    #[test]
    fn test_delta_delete() {
        let d = Delta::delete(42);
        assert!(!d.is_insert());
        assert!(d.is_delete());
        assert!(!d.is_noop());
        assert_eq!(d.diff, -1);
    }

    #[test]
    fn test_delta_negate() {
        let d = Delta::insert(42);
        let neg = d.negate();
        assert!(neg.is_delete());
        assert_eq!(neg.diff, -1);
        assert_eq!(neg.data, 42);
    }

    #[test]
    fn test_delta_map() {
        let d = Delta::insert(42);
        let mapped = d.map(|x| x * 2);
        assert_eq!(mapped.data, 84);
        assert_eq!(mapped.diff, 1);
    }

    #[test]
    fn test_delta_batch_compact() {
        let batch: DeltaBatch<i32> = vec![
            Delta::insert(1),
            Delta::new(2, 0),
            Delta::delete(3),
        ];
        let compacted = batch.compact();
        assert_eq!(compacted.len(), 2);
    }

    #[test]
    fn test_delta_batch_net_count() {
        let batch: DeltaBatch<i32> = vec![
            Delta::insert(1),
            Delta::insert(2),
            Delta::delete(3),
        ];
        assert_eq!(batch.net_count(), 1);
    }
}