# cynos-core

Core types and schema definitions for Cynos database.

## Overview

This crate provides the foundational types for the Cynos in-memory database:

- `DataType`: Supported data types (Boolean, Int32, Int64, Float64, String, DateTime, Bytes, Jsonb)
- `Value`: Runtime values that can be stored in the database
- `Row`: A row of values with a unique identifier
- `schema`: Schema definitions (Column, Table, Index, Constraints)
- `Error`: Error types for database operations

## Features

- `#![no_std]` compatible - works in embedded and WASM environments
- Zero external dependencies for minimal footprint
- Type-safe schema definitions with builder pattern

## Usage

```rust
use cynos_core::{DataType, Value, Row};
use cynos_core::schema::{TableBuilder, Column};

// Create a table schema
let table = TableBuilder::new("users")
    .unwrap()
    .add_column("id", DataType::Int64)
    .unwrap()
    .add_column("name", DataType::String)
    .unwrap()
    .add_primary_key(&["id"], true)
    .unwrap()
    .build()
    .unwrap();

// Create a row
let row = Row::new(1, vec![
    Value::Int64(1),
    Value::String("Alice".into()),
]);

assert_eq!(row.id(), 1);
assert_eq!(row.get(1), Some(&Value::String("Alice".into())));
```

## License

Apache-2.0
