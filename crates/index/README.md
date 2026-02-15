# cynos-index

Index implementations for Cynos database.

## Overview

This crate provides various index implementations for efficient data access:

- `HashIndex`: O(1) point queries using hash map
- `BTreeIndex`: Efficient range queries using B+Tree
- `GinIndex`: Inverted index for JSONB and composite types

## Features

- `#![no_std]` compatible
- Multiple index types for different query patterns
- Support for unique and non-unique indexes
- Nullable value handling with `NullableIndex`
- Index statistics for query optimization

## Usage

```rust
use cynos_index::{BTreeIndex, HashIndex, Index, RangeIndex, KeyRange};

// Create a B+Tree index
let mut btree: BTreeIndex<i32> = BTreeIndex::new(64, true);
btree.add(10, 100).unwrap();
btree.add(20, 200).unwrap();
btree.add(5, 50).unwrap();

// Point query
assert_eq!(btree.get(&10), vec![100]);

// Range query
let range = KeyRange::lower_bound(10, false);
let results = btree.get_range(Some(&range), false, None, 0);
assert_eq!(results, vec![100, 200]);

// Create a Hash index
let mut hash: HashIndex<i32> = HashIndex::new(true);
hash.add(10, 100).unwrap();
assert_eq!(hash.get(&10), vec![100]);
```

## Index Types

| Index Type | Point Query | Range Query | Use Case |
|------------|-------------|-------------|----------|
| HashIndex  | O(1)        | N/A         | Equality lookups |
| BTreeIndex | O(log n)    | O(log n + k)| Range queries, ordering |
| GinIndex   | O(1)        | N/A         | JSONB containment |

## License

Apache-2.0
