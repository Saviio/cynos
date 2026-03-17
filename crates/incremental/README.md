# cynos-incremental

Incremental dataflow and materialized views for Cynos.

## Overview

`cynos-incremental` provides the DBSP-style dataflow layer used for incremental view maintenance (IVM). Instead of re-running a full query after every change, it propagates `Delta<T>` updates through a graph of incremental operators.

Core pieces:

- `Delta<T>` / `DeltaBatch<T>`: insert/delete change representation.
- `DataflowNode`: source, filter, project, map, join, and aggregate nodes.
- `MaterializedView`: current result state plus operator-specific maintenance state.
- `JoinState` / aggregate state types: internal structures that keep joins and aggregates incremental.
- `DiffCollection` / `ConsolidatedCollection`: lightweight multiset-oriented helpers.

## Supported Operators

- Filter, project, and map.
- Inner, left outer, right outer, and full outer joins.
- Aggregates: `COUNT`, `SUM`, `AVG`, `MIN`, and `MAX`.
- `MIN`/`MAX` use ordered multisets internally so deletes do not require a full rescan.

## Typical Update Costs

`cynos-incremental` is useful precisely because different operators scale with the touched delta rather than the whole result:

| Operator family | Typical cost | Notes |
| --- | --- | --- |
| Filter / map / project | `O(|Δinput|)` | Processes only changed rows |
| Hash-based join state updates | Proportional to rows matched by the touched keys | Join fan-out dominates the actual cost |
| `COUNT` / `SUM` / `AVG` | `O(|Δinput|)` over affected groups | Running aggregate state is updated in place |
| `MIN` / `MAX` | `O(log group_size)` per delta | Backed by ordered `BTreeMap` multisets |
| End-to-end incremental plan | Roughly `O(|Δoutput|)` delivery when every node is incremental | Only applies when the full plan can stay on the incremental path |

## Example

```rust
use cynos_core::{Row, Value};
use cynos_incremental::{DataflowNode, Delta, MaterializedView};

let dataflow = DataflowNode::filter(
    DataflowNode::source(1),
    |row| row.get(1).and_then(|v| v.as_i64()).map(|age| age > 18).unwrap_or(false),
);

let mut view = MaterializedView::new(dataflow);
let output = view.on_table_change(
    1,
    vec![Delta::insert(Row::new(
        1,
        vec![Value::Int64(1), Value::Int64(25)],
    ))],
);

assert_eq!(output.len(), 1);
assert_eq!(view.len(), 1);
```

## Notes

- This crate is delta-oriented and intentionally lower level than the JS-facing reactive APIs.
- `cynos-database` compiles incrementalizable physical plans into `DataflowNode` graphs via `dataflow_compiler`.
- `cynos-reactive` wraps `MaterializedView` with subscriptions and change-set delivery.

## License

Apache-2.0
