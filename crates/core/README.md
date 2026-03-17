# cynos-core

Core types, row model, schema definitions, and shared error types for Cynos.

## Overview

`cynos-core` is the foundation used by every other Rust crate in the workspace. It owns:

- `DataType`: the logical types Cynos understands.
- `Value`: the runtime cell value enum used across storage, query, and reactive layers.
- `Row`: the row container, including row IDs and version tracking.
- `schema`: columns, tables, indexes, constraints, and the `TableBuilder` API.
- `Error` / `Result`: shared error surface for low-level operations.

## Design Notes

- `#![no_std]` + `alloc` friendly.
- `Bytes` and `Jsonb` columns are nullable by default; scalar types are not.
- `cynos_core::JsonbValue` is intentionally an opaque byte carrier for the generic `Value::Jsonb` variant. The parsed JSONB value model lives in `cynos-jsonb`.
- `TableBuilder` validates identifiers, supports primary keys, unique constraints, secondary indexes, and foreign keys.
- `TableBuilder::add_index()` automatically marks JSONB indexes as GIN at the schema layer.
- Auto-increment is only valid on a single-column `Int32` or `Int64` primary key.

## Example

```rust
use cynos_core::schema::TableBuilder;
use cynos_core::{DataType, Row, Value};

fn main() -> cynos_core::Result<()> {
    let users = TableBuilder::new("users")?
        .add_column("id", DataType::Int64)?
        .add_column("name", DataType::String)?
        .add_column("profile", DataType::Jsonb)?
        .add_primary_key(&["id"], true)?
        .add_index("idx_name", &["name"], false)?
        .add_index("idx_profile", &["profile"], false)?
        .build()?;

    let row = Row::new(
        42,
        vec![
            Value::Int64(42),
            Value::String("Alice".into()),
            Value::Null,
        ],
    );

    assert_eq!(users.columns().len(), 3);
    assert_eq!(row.id(), 42);
    assert_eq!(row.version(), 1);
    Ok(())
}
```

`idx_profile` is represented as a GIN index definition because it targets a JSONB column.

## Related Crates

- Use `cynos-storage` to actually store rows described by these schemas.
- Use `cynos-jsonb` when you need a structured JSONB value model instead of opaque bytes.

## License

Apache-2.0
