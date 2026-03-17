# cynos-storage

In-memory row storage, table cache management, constraint checking, and journaled transactions for Cynos.

## Overview

`cynos-storage` sits between schema definitions and query execution. Its main pieces are:

- `RowStore`: owns rows for one table and keeps indexes in sync.
- `TableCache`: registry for multiple `RowStore` instances.
- `ConstraintChecker`: validates not-null and foreign-key rules.
- `Journal` / `TableDiff`: records inserts, updates, and deletes.
- `Transaction`: commit/rollback wrapper over the journal.
- `LockManager`: standalone lock bookkeeping utility.

## What `RowStore` Handles

- Insert, update, delete, and batch delete operations.
- Primary-key lookups and secondary index maintenance.
- B+Tree-backed secondary indexes plus GIN indexes for JSONB columns defined in the schema.
- Scan APIs, PK existence checks, index scans, and JSONB/GIN retrieval helpers.
- Delta-producing helpers such as `insert_with_delta()` / `update_with_delta()` / `delete_with_delta()` for reactive integrations.

## Transaction Model

Transactions are journal-based and operate on in-memory state.

- Mutations are applied eagerly to the underlying store.
- `commit()` finalizes the journal and returns the ordered change list.
- `rollback()` replays the reverse diff to restore the previous state.
- There is no durable storage layer, WAL, MVCC, or integrated lock scheduling yet.

## Example

```rust
use cynos_core::schema::TableBuilder;
use cynos_core::{DataType, Row, Value};
use cynos_storage::{TableCache, Transaction};

fn main() -> cynos_core::Result<()> {
    let schema = TableBuilder::new("users")?
        .add_column("id", DataType::Int64)?
        .add_column("name", DataType::String)?
        .add_primary_key(&["id"], false)?
        .add_index("idx_name", &["name"], false)?
        .build()?;

    let mut cache = TableCache::new();
    cache.create_table(schema)?;

    let mut tx = Transaction::begin();
    tx.insert(
        &mut cache,
        "users",
        Row::new(
            1,
            vec![Value::Int64(1), Value::String("Alice".into())],
        ),
    )?;

    let journal = tx.commit()?;
    assert_eq!(journal.len(), 1);
    assert_eq!(cache.get_table("users").map(|table| table.len()), Some(1));
    Ok(())
}
```

## Notes

- Row IDs are internal identifiers and are distinct from logical primary-key column values.
- `LockManager` is exposed for higher-level coordination, but `Transaction` does not automatically acquire or release locks.
- If you want planning/execution, pair this crate with `cynos-query`.

## License

Apache-2.0
