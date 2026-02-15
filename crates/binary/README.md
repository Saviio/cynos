# cynos-binary

Binary protocol for high-performance row serialization.

## Overview

Provides a compact binary encoding format for transferring row data between Rust and JavaScript. When the `wasm` feature is enabled, includes zero-copy WASM bindings for direct JS access to encoded buffers.

## Binary Format (Row-Major)

```text
Header: 16 bytes
+----------+----------+------------+-------+
| row_count| row_stride| var_offset | flags |
| u32      | u32       | u32        | u32   |
+----------+----------+------------+-------+

Fixed Section (row-major):
Row 0: [null_mask: ceil(cols/8) bytes][col0][col1][col2]
Row 1: [null_mask][col0][col1][col2]
...

Variable Section:
[string bytes][bytes data][jsonb data]
```

## Features

- `#![no_std]` compatible
- Zero-copy access from JavaScript via `asView()`
- Efficient encoding for all Cynos data types
- Optional WASM bindings via `wasm` feature

## Usage

```rust
use cynos_binary::{BinaryEncoder, SchemaLayout};
use cynos_core::Row;

// Create encoder with schema layout
let layout = SchemaLayout::from_schema(&table_schema);
let encoder = BinaryEncoder::new(&layout);

// Encode rows
let rows: Vec<Row> = /* ... */;
let result = encoder.encode(&rows);

// Access binary data
let ptr = result.ptr();
let len = result.len();
```

## License

Apache-2.0
