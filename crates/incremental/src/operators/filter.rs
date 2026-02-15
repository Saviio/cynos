//! Incremental filter operator.

use crate::delta::Delta;
use alloc::vec::Vec;

/// Applies a filter predicate to a batch of deltas.
///
/// Only deltas whose data satisfies the predicate are passed through.
/// The diff values are preserved.
///
/// # Example
///
/// ```ignore
/// let deltas = vec![
///     Delta::insert(10),
///     Delta::insert(5),
///     Delta::delete(20),
/// ];
/// let filtered = filter_incremental(&deltas, |&x| x > 8);
/// // Result: [Delta::insert(10), Delta::delete(20)]
/// ```
pub fn filter_incremental<T, F>(input: &[Delta<T>], predicate: F) -> Vec<Delta<T>>
where
    T: Clone,
    F: Fn(&T) -> bool,
{
    input
        .iter()
        .filter(|d| predicate(&d.data))
        .cloned()
        .collect()
}

/// Applies a filter predicate to a batch of deltas, consuming the input.
#[allow(dead_code)]
pub fn filter_incremental_owned<T, F>(input: Vec<Delta<T>>, predicate: F) -> Vec<Delta<T>>
where
    F: Fn(&T) -> bool,
{
    input.into_iter().filter(|d| predicate(&d.data)).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloc::vec;

    #[test]
    fn test_filter_incremental_basic() {
        let deltas = vec![
            Delta::insert(10),
            Delta::insert(5),
            Delta::insert(15),
            Delta::delete(20),
        ];

        let filtered = filter_incremental(&deltas, |&x| x > 8);

        assert_eq!(filtered.len(), 3);
        assert!(filtered.iter().all(|d| d.data > 8));
    }

    #[test]
    fn test_filter_incremental_preserves_diff() {
        let deltas = vec![Delta::insert(10), Delta::delete(20)];

        let filtered = filter_incremental(&deltas, |&x| x > 5);

        assert_eq!(filtered.len(), 2);
        assert!(filtered[0].is_insert());
        assert!(filtered[1].is_delete());
    }

    #[test]
    fn test_filter_incremental_empty() {
        let deltas: Vec<Delta<i32>> = vec![];
        let filtered = filter_incremental(&deltas, |&x| x > 0);
        assert!(filtered.is_empty());
    }

    #[test]
    fn test_filter_incremental_none_match() {
        let deltas = vec![Delta::insert(1), Delta::insert(2), Delta::insert(3)];

        let filtered = filter_incremental(&deltas, |&x| x > 100);
        assert!(filtered.is_empty());
    }
}
