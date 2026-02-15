# cynos-storage

Storage layer for Cynos in-memory database.

## Overview

This crate provides the storage layer including:

- `RowStore`: Row storage with index maintenance
- `TableCache`: Multi-table cache management
- `Journal`: Change tracking for transactions
- `Transaction`: Transaction management with rollback support
- `ConstraintChecker`: Constraint validation
- `LockManager`: Concurrent access control

## Features

- `#![no_std]` compatible
- ACID transaction support
- Automatic index maintenance
- Constraint enforcement (primary key, unique, foreign key)
- Optional hash-based storage via `hash-store` feature

## Usage

```rust
use cynos_storage::{TableCache, Transaction};
use cynos_core::schema::TableBuilder;
use cynos_core::{DataType, Row, Value};

// Create a cache and table
let mut cache = TableCache::new();
let schema = TableBuilder::new("users")
    .unwrap()
    .add_column("id", DataType::Int64)
    .unwrap()
    .add_column("name", DataType::String)
    .unwrap()
    .add_primary_key(&["id"], false)
    .unwrap()
    .build()
    .unwrap();
cache.create_table(schema).unwrap();

// Use a transaction
let mut tx = Transaction::begin();
let row = Row::new(1, vec![Value::Int64(1), Value::String("Alice".into())]);
tx.insert(&mut cache, "users", row).unwrap();
tx.commit().unwrap();

assert_eq!(cache.get_table("users").unwrap().len(), 1);
```

## Transaction Support

- `begin()`: Start a new transaction
- `insert()`, `update()`, `delete()`: DML operations
- `commit()`: Commit changes
- `rollback()`: Rollback changes

## License

Apache-2.0
