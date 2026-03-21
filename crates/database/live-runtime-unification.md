# Live Runtime Unification Design

Status: proposed
Owner: cynos-database / cynos-gql
Scope: `crates/database`, `crates/gql`, `js/packages/core`

## 1. Context

Cynos currently exposes four live-oriented APIs:

- `observe()`
- `changes()`
- `trace()`
- `subscribeGraphql()` / `PreparedGraphqlQuery.subscribe()`

At the execution-kernel level, these APIs are backed by two different mechanisms:

1. Re-query / cached-plan / reactive-patch
2. DBSP-style incremental view maintenance (`trace()` / dataflow / delta propagation)

At the runtime/control-plane level, however, the implementation has already started to diverge into three live shapes:

- SQL rows snapshot live (`observe()` / `changes()`)
- SQL rows delta live (`trace()`)
- GraphQL payload snapshot live (`subscribeGraphql()`)

This split is visible in the current code layout:

- `crates/database/src/query_builder.rs`
  - `observe()` / `changes()` build `ReQueryObservable`
  - `trace()` builds `ObservableQuery`
- `crates/database/src/database.rs`
  - `subscribeGraphql()` binds GraphQL and creates a GraphQL-specific subscription path
- `crates/database/src/reactive_bridge.rs`
  - contains SQL re-query runtime pieces
  - contains IVM registration path
  - currently also contains GraphQL-specific subscription runtime pieces
- `crates/gql/src/plan.rs`
  - lowers GraphQL root fields to existing planner-backed logical plans
- `crates/gql/src/execute.rs`
  - renders rows into GraphQL payload trees
- `crates/gql/src/bind.rs`
  - now also knows how to collect GraphQL dependency tables from nested selections

This works, but if left unchecked it will gradually produce an expensive maintenance shape:

- duplicate invalidation logic
- duplicate subscription lifecycle management
- duplicate flush/GC behavior
- duplicated performance work across SQL and GraphQL live paths
- pressure to create a third de facto runtime instead of keeping GraphQL as an upper-layer bridge

The long-term direction should be:

- exactly two live execution kernels
- one shared live runtime control plane
- SQL and GraphQL implemented as adapters on top of the shared runtime
- GraphQL able to choose `Snapshot` or `Delta` backend per query shape

## 2. Problem Statement

We need a unified live abstraction that satisfies all of the following:

- does not regress `observe()` / `changes()` performance
- does not regress `trace()` / DBSP-IVM performance
- does not break existing unit, wasm, browser, or performance tests
- moves GraphQL to a bridge/compiler/adapter role instead of an independent runtime role
- allows GraphQL live execution to select `requery` or `ivm` backend per query
- preserves correctness for nested GraphQL relations and directive-pruned selections

The key architectural constraint is:

- unify the control plane
- do not flatten the hot execution paths into one generic slow abstraction

## 3. Goals

### 3.1 Primary goals

- Introduce a shared live runtime control plane for all live APIs.
- Preserve exactly two execution kernels:
  - `SnapshotKernel`
  - `DeltaKernel`
- Move GraphQL live from a dedicated runtime lane to an adapter-driven model.
- Add a backend selector for GraphQL live queries.
- Keep current API surfaces stable.

### 3.2 Secondary goals

- Reduce long-term maintenance risk by centralizing:
  - dependency registration
  - pending-change batching
  - flush scheduling
  - subscription lifecycle
  - keepalive/GC
- Make backend selection explicit and testable.
- Establish a capability matrix for GraphQL-on-IVM instead of ad hoc special cases.

## 4. Non-Goals

The initial unification is not required to deliver all of the following on day one:

- full GraphQL tree delta patch output to JS
- IVM support for all GraphQL query shapes
- multi-root GraphQL subscriptions
- GraphQL fragment support
- GraphQL payload delta protocol (`path/op/value`) for JS consumers

The first complete version may legitimately do this:

- unify runtime control flow immediately
- allow GraphQL to select `SnapshotKernel` or `DeltaKernel`
- have `DeltaKernel` support only a restricted GraphQL subset at first
- automatically fall back to `SnapshotKernel` for the rest

## 5. Design Principles

1. Control-plane unification, hot-path specialization
   - shared registry and lifecycle management
   - specialized snapshot and delta kernels

2. Cold-path selection, not hot-path guessing
   - backend selection happens at compile/register time
   - no per-row or per-delta backend branching in hot loops

3. Strongly typed kernel plans
   - avoid trait-object-heavy execution in hot paths
   - prefer enums and concrete structs for runtime dispatch boundaries

4. GraphQL remains a semantic adapter
   - GraphQL owns selection semantics and payload assembly
   - GraphQL does not own an independent live runtime

5. Performance gates are first-class requirements
   - benchmark regressions fail the work
   - compatibility tests remain green throughout

## 6. Current-State Summary

### 6.1 SQL snapshot live

Current path:

- `SelectBuilder.observe()` in `crates/database/src/query_builder.rs`
- compiles `LogicalPlan` -> `CompiledPhysicalPlan`
- initializes `ReQueryObservable`
- registers dependencies into `QueryRegistry`
- `changes()` simply wraps `observe()` into `JsChangesStream`

Properties:

- full current-result snapshot delivery
- re-query on change, with row-local patch fast path when available
- performance-sensitive and already tuned

### 6.2 SQL delta live

Current path:

- `SelectBuilder.trace()` in `crates/database/src/query_builder.rs`
- compiles physical plan -> dataflow via `compile_to_dataflow()`
- initializes `ObservableQuery`
- registers with `register_ivm()`

Properties:

- delta-oriented
- truly distinct execution kernel
- must not be slowed down by snapshot semantics

### 6.3 GraphQL live

Current path:

- `Database.subscribeGraphql()` in `crates/database/src/database.rs`
- bind GraphQL -> `BoundOperation`
- root field -> planner-backed plan via `crates/gql/src/plan.rs`
- nested payload assembly via `crates/gql/src/execute.rs`
- dependency collection via `crates/gql/src/bind.rs`
- GraphQL subscription runtime currently hosted in `crates/database/src/reactive_bridge.rs`

Properties:

- root query can reuse planner/cached-plan machinery
- nested relations currently require payload re-rendering semantics
- GraphQL already differs from SQL live in payload shape, not just transport shape

## 7. Target Architecture

### 7.1 High-level structure

```text
API Surfaces
├─ observe() / changes()
├─ trace()
└─ subscribeGraphql()

Shared Live Runtime Control Plane
├─ LivePlan
├─ LiveRegistry
├─ LiveHandle / subscription lifecycle
├─ pending change batching
├─ flush scheduling
└─ GC / keepalive

Execution Kernels
├─ SnapshotKernel  (requery / cached-plan / patch)
└─ DeltaKernel     (DBSP-IVM / ObservableQuery / dataflow)

Adapters
├─ RowsSnapshotAdapter
├─ RowsDeltaAdapter
├─ GraphqlSnapshotAdapter
└─ GraphqlDeltaAdapter
```

### 7.2 Rule of ownership

- kernels own execution
- runtime owns lifecycle and routing
- adapters own output shaping
- GraphQL compiler owns semantic analysis and backend selection

## 8. Proposed Core Types

### 8.1 `LiveEngineKind`

```rust
pub enum LiveEngineKind {
    Snapshot,
    Delta,
}
```

### 8.2 `LiveOutputKind`

```rust
pub enum LiveOutputKind {
    RowsSnapshot,
    RowsDelta,
    GraphqlSnapshot,
    GraphqlDelta,
}
```

`GraphqlDelta` is included in the design even if the first complete implementation only emits full GraphQL payload snapshots for delta-backed subscriptions.

### 8.3 `LiveDependencySet`

```rust
pub struct LiveDependencySet {
    pub tables: Vec<TableId>,
    pub root_tables: Vec<TableId>,
}
```

Notes:

- `tables` is the complete invalidation set.
- `root_tables` is specifically needed by GraphQL snapshot adapters to distinguish:
  - root-row maintenance events
  - nested relation invalidation events

### 8.4 `KernelPlan`

```rust
pub enum KernelPlan {
    Snapshot(SnapshotPlan),
    Delta(DeltaPlan),
}
```

`SnapshotPlan` should wrap existing planner-backed artifacts directly.
`DeltaPlan` should wrap existing dataflow compilation output directly.

### 8.5 `AdapterPlan`

```rust
pub enum AdapterPlan {
    RowsSnapshot(RowsSnapshotAdapterPlan),
    RowsDelta(RowsDeltaAdapterPlan),
    GraphqlSnapshot(GraphqlSnapshotAdapterPlan),
    GraphqlDelta(GraphqlDeltaAdapterPlan),
}
```

This plan is built once and used to instantiate concrete live subscriptions.

### 8.6 `LivePlan`

```rust
pub struct LivePlan {
    pub engine: LiveEngineKind,
    pub output: LiveOutputKind,
    pub dependencies: LiveDependencySet,
    pub kernel: KernelPlan,
    pub adapter: AdapterPlan,
}
```

## 9. Shared Runtime Control Plane

### 9.1 `LiveRegistry`

`LiveRegistry` replaces the current role split inside `QueryRegistry` without flattening the kernels into one path.

Responsibilities:

- register subscriptions against dependency tables
- accumulate pending changes by table
- schedule microtask flushes in wasm
- perform synchronous flushes in tests/native paths
- drop dead subscriptions
- coordinate keepalive semantics

Internal shape should still preserve two execution lanes:

- snapshot lane
- delta lane

Important detail:

- unification happens at registration/routing/flush boundaries
- not inside the hot row-processing or delta-propagation loops

### 9.2 Subscription lifecycle

The runtime should define a shared subscription-handle model:

- `LiveHandle`
- keepalive subscription
- unsubscribe function generation
- active subscription counting
- GC after flush

This consolidates the currently duplicated lifecycle behavior across SQL and GraphQL live code.

### 9.3 Flush protocol

A single flush cycle should do this:

1. drain pending deltas/changes
2. dispatch to delta lane
3. dispatch to snapshot lane
4. run adapter-level diffing and notification
5. GC dead subscriptions

The exact ordering may be tuned, but it must remain deterministic and benchmarked.

## 10. SnapshotKernel

### 10.1 Scope

`SnapshotKernel` is the formalized home for the current re-query runtime.

It should directly preserve the current performance-critical pieces:

- `CompiledPhysicalPlan`
- `execute_compiled_physical_plan_with_summary()`
- `apply_reactive_patch()`
- row-summary comparison

### 10.2 Expected behavior

For SQL rows adapters:

- no semantic change from current `observe()` / `changes()` behavior

For GraphQL adapters:

- root-table changes:
  - try root rows patch
  - if unsupported, re-execute root query
- non-root dependency changes:
  - do not force root query re-execution
  - re-render GraphQL payload from cached root rows
- notify only if final payload changes

### 10.3 Performance contract

The current fast path for SQL snapshot live must remain intact.
No GraphQL-specific logic may leak into the SQL rows hot path.

## 11. DeltaKernel

### 11.1 Scope

`DeltaKernel` is the formalized home for the current `trace()` / DBSP-IVM runtime.

It should directly preserve:

- `compile_to_dataflow()`
- `ObservableQuery`
- current delta propagation path
- `register_ivm()`-style dependency extraction

### 11.2 Expected behavior

For SQL rows delta adapters:

- identical semantics to current `trace()`

For GraphQL delta adapters:

- only enabled when both relational and GraphQL payload capability checks succeed
- otherwise not instantiated at all; the selector falls back to `SnapshotKernel`

### 11.3 Performance contract

The current `trace()` / IVM path must not pay for snapshot or GraphQL logic in its delta hot loops.

## 12. Adapter Layer

### 12.1 `RowsSnapshotAdapter`

Used by:

- `observe()`
- `changes()`

Responsibilities:

- expose current rows snapshot
- deliver rows snapshot callbacks
- preserve current `JsObservableQuery` / `JsChangesStream` behavior

### 12.2 `RowsDeltaAdapter`

Used by:

- `trace()`

Responsibilities:

- expose `{ added, removed }`
- preserve current `JsIvmObservableQuery` behavior

### 12.3 `GraphqlSnapshotAdapter`

Used by:

- GraphQL subscriptions that fall back to `SnapshotKernel`
- potentially all GraphQL subscriptions in the first unification step

Responsibilities:

- render root rows into GraphQL payload tree
- differentiate root-table vs non-root-table invalidation
- reuse cached root rows whenever possible
- maintain payload summary and exact equality fallback
- emit full GraphQL payload snapshots

### 12.4 `GraphqlDeltaAdapter`

Used by:

- GraphQL subscriptions eligible for `DeltaKernel`

Responsibilities:

- consume relational deltas
- maintain GraphQL node/bucket state incrementally
- update only affected subtrees internally
- emit full GraphQL payload snapshots externally in v1
- optionally evolve into true GraphQL delta output in a future phase

## 13. GraphQL Live Compiler

GraphQL should move toward a compiler role, not a runtime-owner role.

### 13.1 Inputs

- bound GraphQL operation
- GraphQL catalog
- table id map
- schema cache state

### 13.2 Outputs

A `GraphqlLivePlan` that contains:

- root field analysis
- dependency set from the full selection tree
- root relational plan candidate
- GraphQL payload adapter plan
- backend selection result

### 13.3 Existing code that can be reused

- root planner lowering from `crates/gql/src/plan.rs`
- dependency collection from `crates/gql/src/bind.rs`
- payload rendering semantics from `crates/gql/src/execute.rs`

### 13.4 Selector split

The selector should be explicit and testable:

- `relational_incrementalizable`
- `graphql_payload_incrementalizable`

Backend choice rule:

```text
if relational_incrementalizable && graphql_payload_incrementalizable:
    use DeltaKernel
else:
    use SnapshotKernel
```

## 14. GraphQL IVM Capability Matrix (v1)

The first version must be strict.
It is better to miss some IVM opportunities than to produce an unstable or slow hybrid path.

### 14.1 Eligible in v1

- exactly one concrete root subscription field
- root field is planner-lowerable query/subscription read
- root relational plan is incrementalizable by current dataflow compiler
- selections consist of:
  - scalar columns
  - aliases
  - `__typename`
  - directive-pruned fields (`@include` / `@skip` already resolved during binding)
  - simple FK-backed relations
- nested relations do not use unsupported per-parent pagination semantics
- nested filters are within the current incrementalizable predicate subset
- ordering is absent or within a specifically supported stable subset

### 14.2 Fallback to SnapshotKernel in v1

- multi-root subscriptions
- unsupported directives
- unsupported nested ordering/pagination
- unsupported nested filter shapes
- any GraphQL tree shape whose incremental payload maintenance is not explicitly implemented

## 15. Data Flow by Surface

### 15.1 `observe()` / `changes()`

```text
SelectBuilder
  -> LivePlan(engine = Snapshot, output = RowsSnapshot)
  -> SnapshotKernel
  -> RowsSnapshotAdapter
  -> JsObservableQuery / JsChangesStream
```

### 15.2 `trace()`

```text
SelectBuilder
  -> LivePlan(engine = Delta, output = RowsDelta)
  -> DeltaKernel
  -> RowsDeltaAdapter
  -> JsIvmObservableQuery
```

### 15.3 GraphQL fallback snapshot path

```text
subscribeGraphql()
  -> bind GraphQL
  -> GraphqlLiveCompiler
  -> selector chooses Snapshot
  -> LivePlan(engine = Snapshot, output = GraphqlSnapshot)
  -> SnapshotKernel (root rows)
  -> GraphqlSnapshotAdapter (payload tree)
  -> JsGraphqlSubscription
```

### 15.4 GraphQL eligible delta path

```text
subscribeGraphql()
  -> bind GraphQL
  -> GraphqlLiveCompiler
  -> selector chooses Delta
  -> LivePlan(engine = Delta, output = GraphqlSnapshot/GraphqlDelta)
  -> DeltaKernel (relational delta)
  -> GraphqlDeltaAdapter (tree maintenance)
  -> JsGraphqlSubscription
```

## 16. Migration Plan

The target architecture should be delivered through bounded implementation steps, but the intended end-state is a single unified runtime model.

### Step 1: Introduce `LivePlan` and shared runtime skeleton

- add shared types
- add shared registry lifecycle model
- keep existing surfaces working
- do not change kernel hot paths yet

### Step 2: Move SQL snapshot live onto the shared runtime

- adapt `observe()` and `changes()`
- preserve `ReQueryObservable` semantics internally as `SnapshotKernel`
- prove zero semantic drift with existing tests

### Step 3: Move SQL delta live onto the shared runtime

- adapt `trace()`
- preserve `ObservableQuery` semantics internally as `DeltaKernel`
- keep current incremental performance characteristics intact

### Step 4: Move GraphQL live to `GraphqlSnapshotAdapter` on unified runtime

- remove GraphQL-specific runtime ownership
- keep current correctness for nested relation invalidation
- preserve current payload semantics

### Step 5: Introduce GraphQL backend selector

- add capability analysis
- wire `Snapshot` vs `Delta` backend decision into GraphQL live compiler
- default to conservative fallback

### Step 6: Implement first `GraphqlDeltaAdapter` subset

- restricted query shape only
- preserve full GraphQL payload snapshot API externally
- validate equivalence against snapshot backend and one-shot GraphQL execution

### Step 7: Remove obsolete GraphQL-specific runtime branches

- final cleanup in `reactive_bridge.rs`
- GraphQL remains compiler/adapter only

## 17. Testing Plan

### 17.1 Compatibility tests

Must remain green:

- all current Rust unit tests
- all wasm tests
- all browser tests
- all GraphQL tests

### 17.2 New runtime tests

Add tests for:

- unified registry batching semantics
- subscription lifecycle and GC
- identical `observe()` behavior pre/post runtime unification
- identical `trace()` behavior pre/post runtime unification

### 17.3 GraphQL backend-selection tests

Add tests that assert a query shape selects:

- `SnapshotKernel` when expected
- `DeltaKernel` when expected

### 17.4 GraphQL equivalence tests

For GraphQL queries eligible for `DeltaKernel`:

- one-shot `graphql()` result
- snapshot-backed live result
- delta-backed live result

must match after each mutation sequence.

### 17.5 Randomized regression tests

For a fixed schema and random mutation stream:

- apply random inserts/updates/deletes
- compare live GraphQL state against fresh one-shot execution
- run for both eligible and fallback query shapes

## 18. Performance Gates

Performance is a release gate for this work.

### 18.1 No-regression gates

- `observe()` benchmark median must not regress beyond noise threshold
- `changes()` benchmark median must not regress beyond noise threshold
- `trace()` benchmark median must not regress beyond noise threshold
- GraphQL snapshot fallback path must not regress relative to the current implementation

### 18.2 Positive expectation gates

- GraphQL delta-backed eligible queries should outperform snapshot fallback on mutation-heavy workloads
- non-eligible GraphQL queries must still benefit from root-row reuse and relation-only re-rendering

### 18.3 Practical enforcement

Use existing benchmark/test surfaces where possible:

- `js/packages/core/tests/performance.test.ts`
- `js/packages/core/tests/live-query-throughput.test.ts`
- `js/packages/core/tests/comprehensive-perf.test.ts`
- `js/packages/core/tests/graphql.test.ts`
- relevant Rust-side unit and wasm tests in `crates/database` and `crates/gql`

## 19. Risks and Mitigations

### Risk 1: Over-abstracting the hot path

Mitigation:

- keep kernels concrete
- use typed `KernelPlan` enums
- avoid per-row trait-object dispatch

### Risk 2: GraphQL IVM capability surface is over-claimed

Mitigation:

- strict capability matrix
- explicit fallback rules
- contract tests for backend selection

### Risk 3: Runtime unification accidentally changes subscription lifecycle semantics

Mitigation:

- dedicated lifecycle tests
- preserve keepalive/unsubscribe behavior during migration

### Risk 4: GraphQL remains effectively a third runtime in disguise

Mitigation:

- force GraphQL onto shared `LivePlan` / `LiveRegistry`
- keep GraphQL code limited to compiler + adapter modules

## 20. Acceptance Criteria

This design is considered implemented when all of the following are true:

- there is one shared live runtime control plane
- there are exactly two execution kernels (`SnapshotKernel`, `DeltaKernel`)
- `observe()` and `changes()` run through the shared runtime and keep their current performance/behavior
- `trace()` runs through the shared runtime and keeps its current performance/behavior
- GraphQL subscriptions no longer own an independent runtime lane
- GraphQL subscriptions are compiled into adapter-backed live plans
- GraphQL backend selection exists and is tested
- unsupported GraphQL-on-IVM shapes correctly fall back to snapshot backend
- all existing tests remain green
- performance benchmarks show no meaningful regressions on current hot paths

## 21. Long-Term Outcome

If this design is followed, Cynos ends up with:

- two kernel implementations, not three
- one shared live runtime, not per-surface lifecycle logic
- GraphQL as a true upper-layer bridge
- a clean path for GraphQL to choose `requery` or `ivm` by query shape
- a sustainable foundation for future GraphQL live work without permanently fragmenting the runtime
