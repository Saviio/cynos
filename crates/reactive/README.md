# cynos-reactive

Delta-oriented observable queries built on top of `cynos-incremental`.

## Overview

`cynos-reactive` wraps `MaterializedView` with subscriptions and change-set delivery. It is the incremental, delta-based reactive layer used by the WASM API's `trace()` mode.

Main types:

- `ObservableQuery`: owns a `MaterializedView` and a `SubscriptionManager`.
- `ChangeSet`: carries `added`, `removed`, `modified`, and optional `current_result` data.
- `Changes`: helper for explicit initial-state + incremental processing.
- `QueryRegistry`: routes table-level deltas to registered `ObservableQuery` instances.

## Important Scope Boundary

This crate is the delta-based path only.

- Re-query observables that re-execute a physical plan on every change are implemented in `cynos-database`.
- `cynos-reactive::QueryRegistry` knows only about `ObservableQuery` values from this crate.

## Example

```rust
use cynos_core::{Row, Value};
use cynos_incremental::{DataflowNode, Delta};
use cynos_reactive::{ChangeSet, ObservableQuery};

let dataflow = DataflowNode::filter(
    DataflowNode::source(1),
    |row| row.get(1).and_then(|v| v.as_i64()).map(|age| age > 18).unwrap_or(false),
);

let mut query = ObservableQuery::new(dataflow);
let _sub = query.subscribe(|changes: &ChangeSet| {
    println!("added={}, removed={}", changes.added.len(), changes.removed.len());
});

query.on_table_change(
    1,
    vec![Delta::insert(Row::new(
        1,
        vec![Value::Int64(1), Value::Int64(25)],
    ))],
);
```

## Notes

- `ObservableQuery::initialize()` can seed an initial result and notify subscribers.
- `Changes::initial()` returns the initial result as additions; `Changes::process()` returns a `ChangeSet` that also includes `current_result`.
- The callback path used by `ObservableQuery::on_table_change()` is optimized for delta delivery and does not populate `current_result` unless you compute it explicitly.

## License

Apache-2.0
