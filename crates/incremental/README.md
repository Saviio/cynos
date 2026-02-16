# cynos-incremental

Incremental View Maintenance (IVM) for Cynos database.

## Overview

This crate implements Incremental View Maintenance (IVM) based on DBSP (Database Stream Processing) theory. Instead of recomputing query results from scratch on every data change, it propagates only deltas through a dataflow graph — achieving O(|Δoutput|) complexity per update rather than O(|result_set|).

## Core Concepts

- `Delta<T>`: Represents a change to data (+1 for insert, -1 for delete)
- `DiffCollection<T>`: A collection that tracks both snapshot and pending changes
- `DataflowNode`: Composable nodes in a dataflow graph representing query operators (filter, map, join, aggregate)
- `MaterializedView`: A cached query result that updates incrementally via delta propagation

## Incremental Operators

- `filter_incremental`: Filters deltas based on a predicate — O(|Δinput|)
- `map_incremental`: Transforms deltas using a mapper function — O(|Δinput|)
- `project_incremental`: Projects specific columns from row deltas — O(|Δinput|)
- `IncrementalHashJoin`: Maintains join results incrementally with hash-indexed state — O(|Δinput| × |matching keys|)
- `IncrementalCount/Sum/Avg`: Incremental aggregate functions — O(|Δinput|) per group
- `IncrementalMin/Max`: Incremental min/max with fallback re-scan on current-extremum deletion

## Features

- `#![no_std]` compatible
- DBSP-based delta propagation through composable dataflow graphs
- Support for complex query patterns (filter, map, join, aggregate)
- Composable operators: `Filter → Join → Aggregate` pipelines work incrementally end-to-end

## Usage

```rust
use cynos_incremental::{Delta, MaterializedView, DataflowNode};
use cynos_core::{Row, Value};

// Create a dataflow: Source -> Filter(age > 18)
let dataflow = DataflowNode::filter(
    DataflowNode::source(1),
    |row| row.get(1).and_then(|v| v.as_i64()).map(|age| age > 18).unwrap_or(false)
);

let mut view = MaterializedView::new(dataflow);

// Insert a row that passes the filter
let deltas = vec![Delta::insert(Row::new(1, vec![Value::Int64(1), Value::Int64(25)]))];
let output = view.on_table_change(1, deltas);

assert_eq!(output.len(), 1);
assert_eq!(view.len(), 1);
```

## License

Apache-2.0
