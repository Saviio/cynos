# Cynos

> ⚠️ **Development Status**: This project is currently in active development and not yet ready for production use. APIs may change without notice.

A reactive in-memory database written in Rust with first-class WASM support.

## Features

- **Reactive Queries**: Subscribe to query results and receive incremental updates when data changes
- **Incremental View Maintenance (IVM)**: Based on Differential Dataflow concepts for efficient delta propagation
- **WASM Support**: Full browser and Node.js support with zero-copy binary protocol
- **`#![no_std]` Compatible**: Works in embedded and WASM environments
- **Rich Data Types**: Boolean, Int32, Int64, Float64, String, DateTime, Bytes, JSONB
- **Multiple Index Types**: Hash (O(1) lookups), B+Tree (range queries), GIN (JSONB containment)
- **ACID Transactions**: Full transaction support with rollback capability
- **Query Optimization**: Cost-based optimizer with predicate pushdown, join reordering, and index selection

## Quick Start

### JavaScript/TypeScript

```javascript
import { Database, DataType, col } from '@cynos/core';

const db = await Database.create('mydb');

// Create a table
db.createTable('users')
  .column('id', DataType.Int64, { primaryKey: true })
  .column('name', DataType.String)
  .column('age', DataType.Int32)
  .build();

// Insert data
await db.insert('users').values([
  { id: 1, name: 'Alice', age: 25 },
  { id: 2, name: 'Bob', age: 30 },
]).exec();

// Query data
const results = await db.select()
  .from('users')
  .where(col('age').gt(25))
  .exec();
```

### Reactive Queries

```javascript
const query = db.select()
  .from('users')
  .where(col('age').gt(18))
  .observe();

query.subscribe((changes) => {
  console.log('Added:', changes.added);
  console.log('Removed:', changes.removed);
});
```

### Rust

```rust
use cynos_core::{DataType, Value, Row};
use cynos_core::schema::TableBuilder;

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
```

## Architecture

Cynos is organized as a Rust workspace with the following crates:

| Crate | Description |
|-------|-------------|
| `cynos-core` | Core types, schema definitions, and error types |
| `cynos-index` | Index implementations (Hash, B+Tree, GIN) |
| `cynos-storage` | Storage layer with transaction support |
| `cynos-query` | Query engine with optimizer and executors |
| `cynos-incremental` | Incremental View Maintenance (IVM) |
| `cynos-reactive` | Reactive query system with subscriptions |
| `cynos-jsonb` | JSONB data type with JSONPath support |
| `cynos-binary` | Binary protocol for WASM data transfer |
| `cynos-database` | WASM API bindings and JavaScript API |
| `cynos-perf` | Performance benchmark suite |

## Index Types

| Index Type | Point Query | Range Query | Use Case |
|------------|-------------|-------------|----------|
| HashIndex  | O(1)        | N/A         | Equality lookups |
| BTreeIndex | O(log n)    | O(log n + k)| Range queries, ordering |
| GinIndex   | O(1)        | N/A         | JSONB containment |

## Query Operators

| Operator | Description |
|----------|-------------|
| Scan | Full table scan or index scan |
| Filter | Row filtering with predicates |
| Project | Column projection |
| Join | Hash/Merge/Nested loop joins |
| Aggregate | GROUP BY with COUNT, SUM, AVG, MIN, MAX |
| Sort | ORDER BY implementation |
| Limit | LIMIT/OFFSET implementation |

## JSONB Support

Full PostgreSQL-compatible JSONB implementation with:

- JSONPath query language (`$.key`, `[n]`, `[*]`, `..key`, filter expressions)
- Containment operators (`@>`, `<@`, `?`, `?|`, `?&`)
- GIN index for efficient querying
- Compact binary encoding

```rust
use cynos_jsonb::{JsonbValue, JsonPath};

let json = JsonbValue::parse(r#"{"name": "Alice", "age": 25}"#).unwrap();
let path = JsonPath::parse("$.name").unwrap();
let results = json.query(&path);
```

## Performance

Run the benchmark suite:

```bash
cargo run -p cynos-perf --release
```

Benchmark categories:
- Index operations (BTree, Hash, GIN)
- Storage operations (row store, table cache)
- Query execution (filter, project, sort, limit)
- Join performance (hash, merge, nested loop)
- Incremental computation (IVM delta propagation)
- Reactive queries (subscription, change notification)
- JSONB operations (path queries, containment)

## Building

```bash
# Build all crates
cargo build --release

# Run tests
cargo test

# Build WASM (requires wasm-pack)
cd crates/database
wasm-pack build --target web
```

## Browser Compatibility

Cynos uses **WebAssembly MVP** (no advanced features like SIMD, threads, or bulk-memory), ensuring broad browser support.

### Minimum Browser Versions

| Browser | Version | Release Date |
|---------|---------|--------------|
| Chrome  | 67+     | June 2018    |
| Firefox | 68+     | July 2019    |
| Safari  | 14+     | Sept 2020    |
| Edge    | 79+     | Jan 2020     |

### Required Web APIs

| API | Usage | Fallback |
|-----|-------|----------|
| WebAssembly | Core runtime | Required |
| BigInt | Int64 data type | Required |
| TypedArrays / DataView | Binary protocol | Required |
| TextDecoder | String decoding | Required |
| Promise | Async operations | Required |
| `new Function()` | Compiled row decoder | Graceful (slower fallback) |

### Notes

- **No polyfills needed** for modern browsers (2020+)
- **CSP-friendly**: Works with strict Content Security Policy (compiled decoder falls back to interpreted mode)
- **Node.js**: Requires v12+ with WebAssembly support

## License

Apache-2.0
