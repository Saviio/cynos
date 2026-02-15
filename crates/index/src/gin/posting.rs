//! Posting list implementation for GIN index.
//!
//! A posting list is a sorted list of row IDs that contain a particular key.

use alloc::collections::BTreeSet;
use alloc::vec::Vec;
use cynos_core::RowId;

/// A posting list storing row IDs in sorted order.
///
/// Uses `BTreeSet` for `no_std` compatibility instead of `RoaringBitmap`.
/// For production use with large datasets, consider using `roaring` crate.
#[derive(Debug, Clone, Default)]
pub struct PostingList {
    /// Sorted set of row IDs.
    rows: BTreeSet<RowId>,
}

impl PostingList {
    /// Creates a new empty posting list.
    pub fn new() -> Self {
        Self {
            rows: BTreeSet::new(),
        }
    }

    /// Adds a row ID to the posting list.
    pub fn add(&mut self, row_id: RowId) {
        self.rows.insert(row_id);
    }

    /// Removes a row ID from the posting list.
    /// Returns true if the row was present.
    pub fn remove(&mut self, row_id: RowId) -> bool {
        self.rows.remove(&row_id)
    }

    /// Checks if the posting list contains a row ID.
    pub fn contains(&self, row_id: RowId) -> bool {
        self.rows.contains(&row_id)
    }

    /// Returns the number of row IDs in the posting list.
    pub fn len(&self) -> usize {
        self.rows.len()
    }

    /// Returns true if the posting list is empty.
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    /// Converts the posting list to a vector.
    pub fn to_vec(&self) -> Vec<RowId> {
        self.rows.iter().copied().collect()
    }

    /// Returns an iterator over the row IDs.
    pub fn iter(&self) -> impl Iterator<Item = RowId> + '_ {
        self.rows.iter().copied()
    }

    /// Computes the intersection of two posting lists.
    pub fn intersect(&self, other: &PostingList) -> PostingList {
        PostingList {
            rows: self.rows.intersection(&other.rows).copied().collect(),
        }
    }

    /// Computes the union of two posting lists.
    pub fn union(&self, other: &PostingList) -> PostingList {
        PostingList {
            rows: self.rows.union(&other.rows).copied().collect(),
        }
    }

    /// Computes the difference of two posting lists (self - other).
    pub fn difference(&self, other: &PostingList) -> PostingList {
        PostingList {
            rows: self.rows.difference(&other.rows).copied().collect(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloc::vec;

    #[test]
    fn test_posting_list_new() {
        let pl = PostingList::new();
        assert!(pl.is_empty());
        assert_eq!(pl.len(), 0);
    }

    #[test]
    fn test_posting_list_add() {
        let mut pl = PostingList::new();
        pl.add(1);
        pl.add(3);
        pl.add(2);

        assert_eq!(pl.len(), 3);
        assert!(pl.contains(1));
        assert!(pl.contains(2));
        assert!(pl.contains(3));
        assert!(!pl.contains(4));
    }

    #[test]
    fn test_posting_list_add_duplicate() {
        let mut pl = PostingList::new();
        pl.add(1);
        pl.add(1);
        pl.add(1);

        assert_eq!(pl.len(), 1);
    }

    #[test]
    fn test_posting_list_remove() {
        let mut pl = PostingList::new();
        pl.add(1);
        pl.add(2);
        pl.add(3);

        assert!(pl.remove(2));
        assert!(!pl.remove(2)); // Already removed
        assert_eq!(pl.len(), 2);
        assert!(!pl.contains(2));
    }

    #[test]
    fn test_posting_list_to_vec() {
        let mut pl = PostingList::new();
        pl.add(3);
        pl.add(1);
        pl.add(2);

        let vec = pl.to_vec();
        assert_eq!(vec, vec![1, 2, 3]); // Sorted
    }

    #[test]
    fn test_posting_list_intersect() {
        let mut pl1 = PostingList::new();
        pl1.add(1);
        pl1.add(2);
        pl1.add(3);

        let mut pl2 = PostingList::new();
        pl2.add(2);
        pl2.add(3);
        pl2.add(4);

        let result = pl1.intersect(&pl2);
        assert_eq!(result.to_vec(), vec![2, 3]);
    }

    #[test]
    fn test_posting_list_union() {
        let mut pl1 = PostingList::new();
        pl1.add(1);
        pl1.add(2);

        let mut pl2 = PostingList::new();
        pl2.add(2);
        pl2.add(3);

        let result = pl1.union(&pl2);
        assert_eq!(result.to_vec(), vec![1, 2, 3]);
    }

    #[test]
    fn test_posting_list_difference() {
        let mut pl1 = PostingList::new();
        pl1.add(1);
        pl1.add(2);
        pl1.add(3);

        let mut pl2 = PostingList::new();
        pl2.add(2);

        let result = pl1.difference(&pl2);
        assert_eq!(result.to_vec(), vec![1, 3]);
    }

    #[test]
    fn test_posting_list_iter() {
        let mut pl = PostingList::new();
        pl.add(3);
        pl.add(1);
        pl.add(2);

        let collected: Vec<_> = pl.iter().collect();
        assert_eq!(collected, vec![1, 2, 3]);
    }
}
