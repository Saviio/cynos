# cynos-index

Standalone index implementations used by Cynos storage and query layers.

## Overview

This crate provides the reusable index primitives for the workspace:

- `BTreeIndex<K>`: B+Tree for point lookups, range scans, and ordered iteration.
- `HashIndex<K>`: equality-focused hash index.
- `GinIndex`: inverted index for extracted keys and key/value pairs, primarily used with JSONB.
- `NullableIndex<K, I>`: wrapper that tracks `NULL` entries separately from an inner index.
- `KeyRange`, `Index`, `RangeIndex`, and `IndexStats`: the common abstraction layer used by other crates.

## What Is Wired Into Cynos Today

- `cynos-storage::RowStore` currently materializes B+Tree secondary indexes and GIN indexes from table schema definitions.
- `HashIndex` and `HashIndexStore` are still useful as standalone building blocks, but they are not the default secondary-index path in `RowStore` today.

## Typical Use Cases

| Type | Best at | Notes |
| --- | --- | --- |
| `BTreeIndex` | Equality + range + ordered scans | Supports `RangeIndex` |
| `HashIndex` | Fast equality lookups | Range scans degrade to full traversal |
| `GinIndex` | Key existence and key/value containment | Designed around inverted posting lists |
| `NullableIndex` | Keeping null semantics explicit | Wraps another index implementation |

## Complexity At A Glance

These are the typical costs for the current data structures:

| Type | Point lookup | Range scan | Insert / delete | Notes |
| --- | --- | --- | --- | --- |
| `BTreeIndex` | `O(log n)` | `O(log n + k)` | `O(log n)` | Good default for ordered access |
| `HashIndex` | Average `O(1)` | Full traversal | Average `O(1)` | Best for equality-heavy access patterns |
| `GinIndex` | Proportional to lookup token count + posting-list work | N/A | Proportional to extracted entries | Inverted-index style behavior rather than a plain map lookup |
| `NullableIndex` | Underlying index + null bookkeeping | Underlying index | Underlying index | Keeps `NULL` handling explicit |

## Example

```rust
use cynos_index::{BTreeIndex, GinIndex, HashIndex, Index, KeyRange, RangeIndex};

fn main() -> Result<(), cynos_index::IndexError> {
    let mut btree = BTreeIndex::new(64, false);
    btree.add(10, 100)?;
    btree.add(20, 200)?;
    btree.add(30, 300)?;

    let range = KeyRange::bound(10, 20, false, false);
    let ids = btree.get_range(Some(&range), false, None, 0);
    assert_eq!(ids, vec![100, 200]);

    let mut hash = HashIndex::new(true);
    hash.add("alice", 1)?;
    assert_eq!(hash.get(&"alice"), vec![1]);

    let mut gin = GinIndex::new();
    gin.add_key("city".into(), 1);
    gin.add_key_value("city".into(), "shanghai".into(), 1);

    assert_eq!(gin.get_by_key("city"), vec![1]);
    assert_eq!(gin.get_by_key_value("city", "shanghai"), vec![1]);
    Ok(())
}
```

## Notes

- `GinIndex` works with extracted tokens; it is not a generic full SQL index by itself.
- `contains_trigrams()`, `contains_trigram_key()`, and `contains_trigram_pairs()` are helper utilities used for JSONB containment prefilters.
- `IndexStats` tracks logical entry counts for the planner and diagnostics.

## License

Apache-2.0
