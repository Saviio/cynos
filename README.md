# Cynos
[![Ask DeepWiki](https://deepwiki.com/badge.svg)](https://deepwiki.com/Saviio/cynos)

> Warning: Cynos is still under active development and is not production-ready. APIs, crate boundaries, and performance characteristics may change.

Cynos is a Rust workspace for an in-memory relational engine, a rule/heuristic query planner, incremental dataflow, reactive subscriptions, and a WASM-backed JavaScript/TypeScript package.

The public JS package lives in `js/packages/core` as `@cynos/core`. The Rust crates are organized so the lower layers stay reusable and mostly `#![no_std]`, while the host-facing WASM and benchmark crates depend on a full runtime.

## What Exists Today

- In-memory row storage with schema enforcement, primary-key lookups, secondary B+Tree indexes, and JSONB-oriented GIN indexes.
- Rule/heuristic query planning with predicate pushdown, implicit join recognition, join reordering by row-count estimates, index selection, TopN pushdown, order-by-index rewriting, and limit/offset pushdown into scans.
- Reactive query APIs with three distinct modes:
  - `observe()`: re-query, callback receives the full current result set when it changes.
  - `changes()`: re-query, callback receives the full current result set immediately and on later changes.
  - `trace()`: incremental view maintenance (IVM), callback receives `{ added, removed }` deltas for incrementalizable queries.
- Binary query results via `execBinary()` + `getSchemaLayout()` + `ResultSet` for low-overhead WASM-to-JS transfer.
- JSONB building blocks including a compact binary codec, a JSONPath subset parser/evaluator, JSONB operators, and extraction helpers for GIN indexing.
- Journaled in-memory transactions with commit/rollback APIs.

## Repository Layout

| Path | Purpose |
| --- | --- |
| `crates/core` | Shared types, schema definitions, rows, and errors |
| `crates/index` | Standalone B+Tree, hash, nullable, and GIN index primitives |
| `crates/storage` | Row store, table cache, constraint checking, and transaction journal |
| `crates/query` | AST, planner, optimizer passes, physical plans, and executors |
| `crates/incremental` | DBSP-style incremental dataflow and materialized views |
| `crates/reactive` | Delta-based observable queries on top of incremental views |
| `crates/jsonb` | JSONB value model, binary codec, JSONPath subset, and operators |
| `crates/binary` | Compact binary row encoding for WASM/JS transfer |
| `crates/database` | WASM/JS-facing database API that stitches the other crates together |
| `crates/perf` | Custom benchmark runner for the workspace |
| `js/packages/core` | Published NPM package `@cynos/core` |
| `example` | Vite demo app for live queries, binary protocol, and performance experiments |

## Architecture Overview

At a high level, Cynos is layered so the storage, planning, incremental, and host-facing pieces stay separable:

```text
Application code
  -> @cynos/core (js/packages/core)
      -> cynos-database (wasm-bindgen host API)
          -> cynos-query        -> logical plan / physical plan / executors
          -> cynos-storage      -> row store / constraints / transaction journal
          -> cynos-index        -> B+Tree / hash / GIN primitives
          -> cynos-jsonb        -> JSONB values / JSONPath / operators / key extraction
          -> cynos-binary       -> compact result encoding for WASM -> JS
          -> cynos-incremental  -> incremental dataflow for eligible plans
          -> cynos-reactive     -> subscriptions on top of incremental views
```

Operationally there are two query delivery paths:

1. Re-query path:
   - `SelectBuilder` produces a logical plan.
   - `cynos-query` rewrites and lowers it to a physical plan.
   - The physical plan runs against `cynos-storage`.
   - Results are delivered either as JS objects or as a binary buffer decoded by `ResultSet`.
2. Incremental path:
   - If `PhysicalPlan::is_incrementalizable()` is true, `cynos-database` lowers the plan into `cynos-incremental` dataflow.
   - `cynos-reactive` subscribes to the materialized view and emits row-level deltas.
   - If the plan is not incrementalizable, callers should use the re-query APIs instead.

Cross-cutting responsibilities:

- `cynos-core` defines the shared schema/value/error model used everywhere else.
- `cynos-jsonb` feeds both query semantics and GIN-style indexing.
- `cynos-perf` exercises the full stack with custom benchmark scenarios.

## JavaScript / TypeScript Quick Start

```ts
import {
  ColumnOptions,
  JsDataType,
  col,
  createDatabase,
  initCynos,
} from '@cynos/core';

await initCynos();
const db = createDatabase('demo');

const users = db.createTable('users')
  .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
  .column('name', JsDataType.String)
  .column('age', JsDataType.Int32)
  .index('idx_age', 'age');

db.registerTable(users);

await db.insert('users').values([
  { id: 1, name: 'Alice', age: 25 },
  { id: 2, name: 'Bob', age: 30 },
]).exec();

const rows = await db
  .select('*')
  .from('users')
  .where(col('age').gt(25))
  .exec();

console.log(rows);
```

Notes:

- Table builders must be passed to `db.registerTable(...)`. Calling `createTable(...).column(...);` alone does not install the schema.
- `select()` accepts `'*'`, a single column name, an array of column names, or multiple variadic column arguments.
- For joins, use column references on both sides, for example `col('orders.user_id').eq(col('users.id'))`.

## Reactive Modes

| API | Callback payload | Best for | Notes |
| --- | --- | --- | --- |
| `observe()` | Full current result set | Imperative listeners that can fetch the initial state manually | No initial callback; call `getResult()` first if needed |
| `changes()` | Full current result set | UI state updates such as React `setState` | Emits the initial result immediately |
| `trace()` | `{ added, removed }` delta object | Incremental UIs or downstream consumers that want O(delta) updates | Only works for incrementalizable plans; `ORDER BY`, `LIMIT`, `OFFSET`, and other non-streamable operators fall back to `observe()`/`changes()` |

```ts
import { col } from '@cynos/core';

const stream = db
  .select('*')
  .from('users')
  .where(col('age').gte(18))
  .changes();

const stopStream = stream.subscribe((currentRows) => {
  console.log('current result', currentRows);
});

const trace = db
  .select('*')
  .from('users')
  .where(col('age').gte(18))
  .trace();

console.log('initial trace result', trace.getResult());

const stopTrace = trace.subscribe((delta) => {
  console.log('added', delta.added);
  console.log('removed', delta.removed);
});
```

## DBSP-Style IVM

`trace()` is Cynos's DBSP-style incremental view maintenance path. It is not a separate query language; it reuses the normal planner pipeline and then lowers an eligible physical plan into a delta-oriented dataflow graph.

How it works today:

1. Build the normal logical plan from the query builder.
2. Run the regular planner/optimizer pipeline and produce a physical plan.
3. Execute that physical plan once to bootstrap the initial result.
4. If `PhysicalPlan::is_incrementalizable()` is true, compile the plan into `cynos-incremental::DataflowNode`.
5. Route table-level `Delta<Row>` updates through `cynos-reactive::ObservableQuery`, which emits `{ added, removed }` changes to subscribers.

That design means the incremental path shares the same query-building surface as the re-query path, but uses a different maintenance strategy after the initial snapshot.

Current capabilities that are grounded in the implemented compiler/runtime:

- Incrementalizable scan family: table scans, index scans/gets, `IN` index gets, and GIN scan variants can participate in the bootstrap plan and dependency graph.
- Incrementalizable relational operators: filter, projection, map-style computed projections, joins, cross product, and hash aggregate.
- Current aggregate coverage on the incremental path: `COUNT`, `SUM`, `AVG`, `MIN`, and `MAX`.
- `MIN` and `MAX` are maintained with ordered multisets internally, so deletes do not force a full recomputation of the group.
- Join plans are lowered into incremental join state, so multi-table live queries can stay on the delta path when the rest of the plan is eligible.

Current boundaries:

- `ORDER BY`, `LIMIT`, and `TopN` are explicit blockers for `trace()`.
- When a query cannot be incrementalized, `trace()` returns an error and the caller should use `observe()` or `changes()` instead.
- The callback payload is a delta object, not a fully materialized result set, so consumers must maintain local state or re-read with `getResult()`.

## Choosing A Live Query Strategy

Cynos intentionally exposes two live-query families because they optimize for different things.

| Strategy | API | Strengths | Tradeoffs | Good fit |
| --- | --- | --- | --- | --- |
| Re-query | `observe()` / `changes()` | Works with the full query surface, easy to consume, always delivers the full current state, naturally fits UI state setters | Re-executes the query and rematerializes the result when data changes; cost scales with query/result size | Dashboards, CRUD tables, queries with `ORDER BY` / `LIMIT`, simple app code, correctness-first integrations |
| Incremental plan | `trace()` | Reuses the planned query shape but propagates deltas after bootstrap; lower steady-state cost on frequent writes; payload is small when output churn is small | Restricted to incrementalizable plans; consumer must apply `{ added, removed }`; less convenient for naive UI binding | High-frequency subscriptions, collaborative/live views, pipelines where the query is mostly filter/join/group-by without ordering/windowing |

Practical guidance:

- Use `changes()` when you want the simplest "always give me the current rows" API, especially in React/Vue-style state updates.
- Use `observe()` when you want the same re-query semantics but prefer to fetch the initial state manually.
- Use `trace()` when the query stays within the supported incremental subset and update frequency is high enough that full re-query work becomes the bottleneck.
- If you need sorted, paginated, or otherwise non-streamable output, treat re-query as the default strategy.

## Local Bench Snapshot

To make this tradeoff less abstract, benchmarks were conducted natively at the Rust layer on a Mac mini M4, before cross-compilation to WASM. The absolute numbers will differ in WASM environments (browsers, runtimes), but the relative performance characteristics shown here reflect the underlying algorithmic tradeoffs.

```bash
cargo run -p cynos-perf --release
```

The relevant `IVM vs RE-QUERY COMPARISON` scenarios in that run all reported equivalent results, then measured steady-state maintenance cost for the same logical workload:

| Scenario | Size | Re-query | IVM | Speedup |
| --- | --- | --- | --- | --- |
| Filter (`WHERE age > 30`) | 10K rows | `1.71 ms` | `516 ns` | `3322.8x` |
| Inner join | 10K rows | `3.50 ms` | `3.20 us` | `1093.5x` |
| Left outer join | 10K rows | `3.69 ms` | `2.56 us` | `1441.8x` |
| `GROUP BY` + `COUNT` / `SUM` | 10K rows | `3.51 ms` | `1.50 us` | `2336.6x` |
| `GROUP BY` + `MIN` / `MAX` | 10K rows | `3.61 ms` | `2.14 us` | `1686.1x` |
| Filter + join | 10K rows | `3.27 ms` | `2.60 us` | `1254.5x` |

What these numbers mean in practice:

- On this machine, once a query is incrementalizable, the steady-state delta path is dramatically cheaper than re-running the whole query.
- The speedup tends to grow with table size because the re-query path scales with the full plan/result maintenance cost, while the incremental path scales mostly with the touched delta and affected join/group state.
- The benefit is especially visible for grouped live queries and join-heavy subscriptions, where the delta path avoids rebuilding the full result on each change.

Important caveats:

- These are Rust-side harness numbers, not browser end-to-end UI timings.
- The comparison is intentionally fair to re-query: it uses a pre-compiled physical plan, so optimizer overhead is already excluded.
- The IVM side measures delta maintenance, but `trace()` still requires the consumer to apply `{ added, removed }` on the application side.
- If your real UI needs sorted/paginated output, the query may not be incrementalizable at all, in which case the re-query APIs remain the right path.

## Complexity At A Glance

These are the typical asymptotics for the current implementation, not hard real-time guarantees:

| Area | Typical cost | Notes |
| --- | --- | --- |
| Primary-key / B+Tree lookup | `O(log n)` | Backed by `RowStore` + B+Tree indexes |
| B+Tree range scan | `O(log n + k)` | `k` is the number of returned row IDs |
| Standalone hash-index lookup | Average `O(1)` | Implemented in `cynos-index`, but not the default secondary-index path in `RowStore` today |
| GIN-style key lookup | Proportional to extracted keys + posting-list work | Best thought of as inverted-index style rather than a plain `O(1)` hash lookup |
| `observe()` / `changes()` | Re-executes the query and materializes the current result | Cost scales with the full query path, not just the delta |
| `trace()` | `O(Δoutput)` delivery after incremental compilation | Only for plans that can be lowered to incremental dataflow |
| Incremental `COUNT` / `SUM` / `AVG` | `O(Δinput)` over affected groups | Maintains running aggregate state |
| Incremental `MIN` / `MAX` | `O(log group_size)` per delta | Implemented with ordered multisets in `cynos-incremental` |

## Binary Results

The JS package can read query results through a compact binary buffer instead of JSON object materialization.

```ts
import { ResultSet } from '@cynos/core';

const query = db.select('*').from('users');
const layout = query.getSchemaLayout();
const buffer = await query.execBinary();

const rs = new ResultSet(buffer, layout);
console.log(rs.length);
console.log(rs.getString(0, 1));

const materialized = rs.toArray();
rs.free();
```

Implementation details:

- The binary format is row-major with a 16-byte header plus a fixed-width section and a variable-width section.
- `Int64` and `DateTime` are encoded as `f64` values in the binary payload for JS interop.
- `ResultSet` uses a compiled decoder when `new Function()` is allowed and falls back to an interpreted path under stricter CSP settings.

## Rust Quick Start

```rust
use cynos_core::schema::TableBuilder;
use cynos_core::{DataType, Row, Value};
use cynos_storage::RowStore;

fn main() -> cynos_core::Result<()> {
    let schema = TableBuilder::new("users")?
        .add_column("id", DataType::Int64)?
        .add_column("name", DataType::String)?
        .add_primary_key(&["id"], false)?
        .build()?;

    let mut store = RowStore::new(schema);
    store.insert(Row::new(
        1,
        vec![Value::Int64(1), Value::String("Alice".into())],
    ))?;

    assert_eq!(store.len(), 1);
    Ok(())
}
```

## Build And Test

```bash
# Rust workspace
cargo test --workspace

# Benchmark binary
cargo run -p cynos-perf --release

# Raw WASM build for the database crate
cd crates/database
wasm-pack build --target web

# JS package workspace
cd ../../js
pnpm install
pnpm build
```

## Browser / Runtime Compatibility

Compilation to WASM and execution in browser-like environments are first-class features of cynos. To provide a complete picture, this section presents additional compatibility data.

The current JS package and generated glue code rely on these platform features:

| Capability | Where it is used | Requirement |
| --- | --- | --- |
| `WebAssembly` | Loading and instantiating `cynos.wasm` | Required |
| `Promise` | `initCynos()` and async query APIs | Required |
| `TypedArray` / `DataView` | Binary protocol and WASM memory access | Required |
| `TextDecoder` | String and JSONB decoding | Required |
| `BigInt` | Exact `Int64` inputs outside the JS safe-integer range | Optional for basic usage, required for precise 64-bit literals |
| `new Function()` | Fast compiled decoder in `ResultSet` | Optional; there is an interpreted fallback |

Notes:

- The generated wasm-bindgen loader already falls back from `WebAssembly.instantiateStreaming(...)` to byte-buffer instantiation when streaming or MIME setup is unavailable.
- The current WASM target does not depend on threads or SIMD-specific browser features.
- `@cynos/core` is ESM-first and targets modern browsers plus recent Node.js runtimes with standard WebAssembly APIs.

## Current Limits

- Cynos is in-memory only. There is no durable on-disk storage engine yet.
- Transactions are journaled commit/rollback over in-memory state; this is not durable storage in the traditional ACID database sense.
- `trace()` only works for plans that the physical planner can lower to incremental dataflow.
- Storage/query integration currently materializes B+Tree and GIN indexes from schema definitions. A standalone hash index implementation also exists in the workspace, but it is not the default secondary-index path in `RowStore` today.
- JavaScript `Int64` values are exposed through JS-friendly paths as numbers, so values outside the safe integer range lose precision unless the calling pattern is designed around that limitation.

## License

Apache-2.0
