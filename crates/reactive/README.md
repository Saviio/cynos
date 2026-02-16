# cynos-reactive

Reactive query system for Cynos database.

## Overview

This crate implements the reactive query layer that bridges `cynos-incremental` (DBSP dataflow) with subscriber notifications. It wraps `MaterializedView` into `ObservableQuery`, managing subscriptions and delivering `ChangeSet` deltas to callbacks.

## Core Concepts

- `ChangeSet`: Delta output from IVM — contains `added` and `removed` rows after a table change
- `ObservableQuery`: Wraps a `MaterializedView`, manages subscriptions, and delivers `ChangeSet` to callbacks on each table change
- `QueryRegistry`: Routes table-level DML events to dependent `ObservableQuery` instances; supports both IVM and re-query paths

## Key APIs

- `ObservableQuery::subscribe(callback)`: Register a callback that receives `ChangeSet` on each delta propagation
- `ObservableQuery::on_table_change(table_id, deltas)`: Feed deltas from DML into the dataflow, propagate, and notify subscribers
- `ObservableQuery::result()`: Get the current materialized result set

## Features

- `#![no_std]` compatible
- Efficient change propagation
- Multiple subscription patterns (callback, iterator)

## Usage

```rust
use cynos_reactive::{ObservableQuery, ChangeSet};
use cynos_incremental::{DataflowNode, Delta};
use cynos_core::{Row, Value};

// Create an observable query with a filter dataflow
let dataflow = DataflowNode::filter(
    DataflowNode::source(1),
    |row| row.get(1).and_then(|v| v.as_i64()).map(|age| age > 18).unwrap_or(false)
);

let mut query = ObservableQuery::new(dataflow);

// Subscribe to delta changes
query.subscribe(|change_set: &ChangeSet| {
    println!("Added: {}, Removed: {}", change_set.added.len(), change_set.removed.len());
});

// Feed a table change — delta propagates through dataflow, subscribers notified
let deltas = vec![Delta::insert(Row::new(1, vec![Value::Int64(1), Value::Int64(25)]))];
query.on_table_change(1, deltas);
```

## License

Apache-2.0
