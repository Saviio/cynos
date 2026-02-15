# Binary Protocol Design Document

## Goal

Eliminate WASM↔JS serialization overhead (currently 90%+ of query time) with a zero-copy binary protocol.

## Binary Format (Row-Major)

```
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

### Header Fields

- `row_count`: Number of rows (u32)
- `row_stride`: Bytes per row (u32)
- `var_offset`: Variable section start offset (u32)
- `flags`: bit0=has_nulls, others reserved (u32)

### Type Encoding

| Type | Fixed Size | Storage |
|------|-----------|---------|
| Boolean | 1B | u8 (0/1) |
| Int32 | 4B | i32 LE |
| Int64 | 8B | f64 LE (JS Number compatible) |
| Float64 | 8B | f64 LE |
| DateTime | 8B | f64 LE (ms timestamp) |
| String | 8B | (offset: u32, len: u32) → variable section |
| Bytes | 8B | (offset: u32, len: u32) → variable section |
| Jsonb | 8B | (offset: u32, len: u32) → variable section (JSON string) |

### NULL Handling

Each row starts with a `null_mask`, 1 bit per column, bit=1 means NULL.

## Schema Layout

Schema is obtained separately via `getSchemaLayout()` and can be cached on JS side:

```rust
#[wasm_bindgen]
pub struct SchemaLayout {
    columns: Vec<ColumnLayout>,
    row_stride: usize,           // Total bytes per row
    null_mask_size: usize,       // ceil(cols/8)
}

pub struct ColumnLayout {
    name: String,
    data_type: BinaryDataType,
    offset: usize,               // Column offset within row (after null_mask)
    is_nullable: bool,
}
```

## JS Usage

```typescript
// Get schema layout (cacheable)
const layout = db.select('*').from('users').getSchemaLayout();

// Execute query returning binary buffer
const result = await db.select('*').from('users').execBinary();

// Create ResultSet (zero-copy access to WASM memory)
const rs = new ResultSet(result, layout);

// Zero-copy numeric access
for (let i = 0; i < rs.length; i++) {
  const id = rs.getNumber(i, 0);    // Direct DataView read
  const price = rs.getInt32(i, 3);  // No object creation
}

// Or convert to object array (has decoding overhead)
const rows = rs.toArray();

// Free memory
rs.free();
```

## Performance Benchmarks

> Test Environment: MacBook Air M4, 32GB RAM, macOS, Chromium (vitest browser mode)
> Dataset: 100,000 rows, mixed types (Int64, String, Float64, JSONB)
> Reference: `cynos/js/tests/binary-benchmark.test.ts`

### Benchmark Results

| Scenario | Rows | JSON (ms) | Binary (ms) | Speedup |
|----------|------|-----------|-------------|---------|
| **Point Query** |
| PK Lookup | 1 | 0.05 | 0.04 | 1.4x |
| Indexed + LIMIT 10 | 10 | 0.05 | 0.03 | 1.4x |
| Indexed No LIMIT (~20%) | 20,000 | 43.10 | 8.92 | 4.8x |
| **Range Query** |
| BETWEEN + LIMIT 10 | 10 | 0.05 | 0.03 | 1.5x |
| BETWEEN No LIMIT (~45%) | 45,000 | 98.54 | 19.52 | 5.0x |
| gt (~10%) | 10,000 | 23.52 | 4.19 | 5.6x |
| **JSONB Query** |
| Single Predicate (~20%) | 20,000 | 27.14 | 13.88 | 2.0x |
| 2 Predicates AND (~7%) | 6,700 | 9.10 | 4.38 | 2.1x |
| **Result Size Scaling** |
| 10 rows | 10 | 0.04 | 0.03 | 1.4x |
| 100 rows | 100 | 0.19 | 0.06 | 2.9x |
| 1,000 rows | 1,000 | 1.88 | 0.46 | 4.1x |
| 10,000 rows | 10,000 | 19.80 | 4.58 | 4.3x |
| 50,000 rows | 50,000 | 109.94 | 22.10 | 5.0x |
| **Lazy Access (Zero-Copy)** |
| Numeric Aggregation (SUM) | 100,000 | 227.14 | 6.66 | **34x** |
| First Row Only | 1 | 232.81 | 4.93 | **47x** |
| Multi-Column Calc | 100,000 | 233.61 | 6.76 | **35x** |
| Conditional Filter (~50%) | 50,000 | 222.63 | 35.26 | 6.3x |
| Numeric-Only toArray | 100,000 | 136.02 | 13.42 | **10x** |

**Summary**: Average speedup 9.2x, best case 47x (first row access), Binary wins 19/19 tests

### Performance Characteristics

1. **Small result sets (< 100 rows)**: 1.4-2.9x speedup, serialization overhead is small
2. **Medium result sets (1K-10K rows)**: 4-5x speedup, Binary advantage is clear
3. **Large result sets (50K+ rows)**: 5x+ speedup, approaching theoretical limit
4. **Lazy Access scenarios**: 30-47x speedup, true zero-copy advantage

### Why is toArray() speedup limited?

`toArray()` must create JS objects, which is unavoidable overhead:

```
JSON path:   Rust serde serialize → JSON.parse() → JS object array
Binary path: Binary encode → DataView read → JS object creation
```

Both paths ultimately create JS objects. Binary saves Rust→JS serialization overhead,
but JS object creation cost is the same. Measured toArray() speedup is ~4-10x.

**True Zero-Copy advantage is in Lazy Access scenarios**:
- No JS object creation needed
- Direct numeric reads via DataView
- Ideal for aggregation, filtering, pagination
- Measured speedup up to 30-47x

## File Structure

```
binary_protocol/
├── mod.rs              # Module exports, BinaryResult, BinaryDataType
├── encoder.rs          # BinaryEncoder high-performance encoding
├── schema_layout.rs    # SchemaLayout precomputed offsets
└── PROTOCOL.md         # This document
```
