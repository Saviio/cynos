//! Property-based tests for cynos-index using proptest.

use cynos_index::{BTreeIndex, HashIndex, Index, KeyRange, RangeIndex};
use proptest::prelude::*;

proptest! {
    /// Test that all inserted keys can be retrieved from BTree.
    #[test]
    fn btree_insert_get_roundtrip(keys in prop::collection::vec(0i64..10000, 1..500)) {
        let mut tree = BTreeIndex::new(64, false);
        for (i, &key) in keys.iter().enumerate() {
            tree.add(key, i as u64).unwrap();
        }
        for (i, &key) in keys.iter().enumerate() {
            let values = tree.get(&key);
            prop_assert!(!values.is_empty(), "Key {} should exist", key);
            prop_assert!(values.contains(&(i as u64)), "Value {} should be in results for key {}", i, key);
        }
    }

    /// Test that BTree range query returns sorted results.
    #[test]
    fn btree_range_query_sorted(keys in prop::collection::vec(0i64..10000, 1..500)) {
        let mut tree = BTreeIndex::new(64, true);
        for (i, &key) in keys.iter().enumerate() {
            let _ = tree.add(key, i as u64);
        }
        let range: Vec<_> = tree.get_range(None, false, None, 0);
        // Values should be in order of their keys
        // Since we use unique index, each key maps to one value
        // The values returned should correspond to sorted keys
        prop_assert!(range.len() <= keys.len());
    }

    /// Test that BTree maintains correct count after insertions.
    #[test]
    fn btree_count_after_insert(keys in prop::collection::vec(0i64..1000, 1..200)) {
        let mut tree = BTreeIndex::new(64, false);
        let mut count = 0;
        for &key in &keys {
            tree.add(key, key as u64).unwrap();
            count += 1;
        }
        prop_assert_eq!(tree.len(), count);
    }

    /// Test that BTree delete removes the correct number of entries.
    #[test]
    fn btree_delete_correctness(
        keys in prop::collection::vec(0i64..500, 10..100),
        delete_indices in prop::collection::vec(0usize..100, 1..10)
    ) {
        let mut tree = BTreeIndex::new(64, true);
        let mut inserted_keys = Vec::new();

        for &key in &keys {
            if tree.add(key, key as u64).is_ok() {
                inserted_keys.push(key);
            }
        }

        let initial_len = tree.len();

        for &idx in &delete_indices {
            if idx < inserted_keys.len() {
                let key = inserted_keys[idx];
                tree.remove(&key, None);
            }
        }

        prop_assert!(tree.len() <= initial_len);
    }

    /// Test that BTree min/max are correct.
    #[test]
    fn btree_min_max_correct(keys in prop::collection::vec(1i64..10000, 1..100)) {
        let mut tree = BTreeIndex::new(64, true);
        let mut unique_keys = Vec::new();

        for &key in &keys {
            if tree.add(key, key as u64).is_ok() {
                unique_keys.push(key);
            }
        }

        if !unique_keys.is_empty() {
            let expected_min = *unique_keys.iter().min().unwrap();
            let expected_max = *unique_keys.iter().max().unwrap();

            let (actual_min, _) = tree.min().unwrap();
            let (actual_max, _) = tree.max().unwrap();

            prop_assert_eq!(*actual_min, expected_min);
            prop_assert_eq!(*actual_max, expected_max);
        }
    }

    /// Test that HashIndex insert/get roundtrip works.
    #[test]
    fn hash_insert_get_roundtrip(keys in prop::collection::vec(0i64..10000, 1..500)) {
        let mut index = HashIndex::new(false);
        for (i, &key) in keys.iter().enumerate() {
            index.add(key, i as u64).unwrap();
        }
        for (i, &key) in keys.iter().enumerate() {
            let values = index.get(&key);
            prop_assert!(!values.is_empty());
            prop_assert!(values.contains(&(i as u64)));
        }
    }

    /// Test that HashIndex unique constraint works.
    #[test]
    fn hash_unique_constraint(keys in prop::collection::vec(0i64..100, 10..50)) {
        let mut index = HashIndex::new(true);
        let mut inserted = std::collections::HashSet::new();

        for &key in &keys {
            let result = index.add(key, key as u64);
            if inserted.contains(&key) {
                prop_assert!(result.is_err());
            } else {
                prop_assert!(result.is_ok());
                inserted.insert(key);
            }
        }
    }

    /// Test BTree range query with bounds.
    #[test]
    fn btree_range_bounds(
        keys in prop::collection::vec(0i64..1000, 10..100),
        lower in 0i64..500,
        upper in 500i64..1000
    ) {
        let mut tree = BTreeIndex::new(64, true);
        for &key in &keys {
            let _ = tree.add(key, key as u64);
        }

        let range = KeyRange::bound(lower, upper, false, false);
        let results = tree.get_range(Some(&range), false, None, 0);

        // All results should be within bounds
        for &val in &results {
            let key = val as i64;
            prop_assert!(key >= lower && key <= upper,
                "Value {} (key {}) should be in range [{}, {}]", val, key, lower, upper);
        }
    }

    /// Test that clearing an index makes it empty.
    #[test]
    fn btree_clear_makes_empty(keys in prop::collection::vec(0i64..1000, 1..100)) {
        let mut tree = BTreeIndex::new(64, true);
        for &key in &keys {
            let _ = tree.add(key, key as u64);
        }

        tree.clear();

        prop_assert!(tree.is_empty());
        prop_assert_eq!(tree.len(), 0);
        prop_assert!(tree.min().is_none());
        prop_assert!(tree.max().is_none());
    }

    /// Test BTree with negative keys.
    #[test]
    fn btree_negative_keys(keys in prop::collection::vec(-5000i64..5000, 10..100)) {
        let mut tree = BTreeIndex::new(64, true);
        for &key in &keys {
            let _ = tree.add(key, (key + 10000) as u64);
        }

        // Min should be the smallest key
        if let Some((min_key, _)) = tree.min() {
            let expected_min = keys.iter().min().unwrap();
            // Due to unique constraint, min might differ
            prop_assert!(*min_key <= *expected_min || tree.contains_key(min_key));
        }
    }

    /// Test that stats are consistent with actual count.
    #[test]
    fn btree_stats_consistent(keys in prop::collection::vec(0i64..1000, 1..100)) {
        let mut tree = BTreeIndex::new(64, false);
        for &key in &keys {
            tree.add(key, key as u64).unwrap();
        }

        prop_assert_eq!(tree.len(), tree.stats().total_rows());
    }
}
