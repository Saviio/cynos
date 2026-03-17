# cynos-query

AST, planner, optimizer passes, physical plans, and executors for Cynos queries.

## Overview

`cynos-query` does not own storage by itself. Instead, it defines the query language and the execution machinery that runs against a `DataSource` implementation.

Main modules:

- `ast`: expression trees, predicates, aggregates, and join types.
- `planner`: logical plans, physical plans, plan properties, and the `QueryPlanner` pipeline.
- `optimizer`: logical and physical optimization passes.
- `executor`: relational executors and the `PhysicalPlanRunner`.
- `context`: table statistics and index metadata used by context-aware planning.
- `plan_cache`: fingerprints and a simple LRU-style cache for compiled plans.

## Planning Model

The planner is rule/heuristic-based rather than a full cost-based optimizer.

Default pipeline in `QueryPlanner`:

1. Logical rewrites: `NotSimplification`, `AndPredicatePass`, `CrossProductPass`, `ImplicitJoinsPass`, `OuterJoinSimplification`, `PredicatePushdown`, `JoinReorder`.
2. Context-aware logical optimization: `IndexSelection`.
3. Logical -> physical conversion.
4. Physical rewrites: `TopNPushdown`, `OrderByIndexPass`, `LimitSkipByIndexPass`.

## Execution Notes

- The executor supports filter, projection, aggregation, sort, limit/offset, cross product, and multiple join operators.
- The execution layer includes hash join, sort-merge join, and nested-loop join implementations.
- The default planner currently chooses hash join for equi-joins and nested-loop join otherwise.
- `PhysicalPlan::is_incrementalizable()` is used by the WASM layer to decide whether a plan can be lowered into incremental dataflow.

## Example

```rust
use cynos_query::ast::Expr;
use cynos_query::context::{ExecutionContext, IndexInfo, TableStats};
use cynos_query::planner::{LogicalPlan, QueryPlanner};

let plan = LogicalPlan::project(
    LogicalPlan::filter(
        LogicalPlan::scan("users"),
        Expr::gt(
            Expr::column("users", "age", 1),
            Expr::literal(18_i64),
        ),
    ),
    vec![Expr::column("users", "name", 0)],
);

let mut ctx = ExecutionContext::new();
ctx.register_table(
    "users",
    TableStats {
        row_count: 10_000,
        is_sorted: false,
        indexes: vec![IndexInfo::new("idx_age", vec!["age".into()], false)],
    },
);

let planner = QueryPlanner::new(ctx);
let physical = planner.plan(plan);

assert!(physical.is_incrementalizable());
```

## Related Crates

- `cynos-storage` provides the table data and index access used by the runner.
- `cynos-database` adapts this crate to the WASM/JS API and wires in plan caching.
- `cynos-incremental` reuses physical-plan structure for the incremental path.

## License

Apache-2.0
