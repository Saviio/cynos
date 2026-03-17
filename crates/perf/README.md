# cynos-perf

Custom benchmark runner for the Cynos workspace.

## Overview

`cynos-perf` is a non-published binary crate that exercises the rest of the workspace through a custom reporting harness. It is useful for quick local comparisons without wiring up Criterion dashboards.

Current benchmark groups:

1. Index operations (`BTreeIndex`, `HashIndex`)
2. Storage operations (`RowStore` insert/scan/filter/update/delete)
3. Query executor operators (filter, sort, project, limit, combined pipelines)
4. Join algorithms (hash, sort-merge, nested-loop)
5. Incremental operators and materialized views
6. Reactive query creation and propagation
7. JSONB parsing, codec, query, and operator costs
8. End-to-end workflows
9. IVM-vs-re-query correctness/performance comparisons across several scenarios

## Running

```bash
cargo run -p cynos-perf --release
```

## What The Report Contains

The benchmark binary prints a console summary that includes:

- Mean duration per benchmark entry
- Optional throughput numbers
- Optional target checks (pass/fail) for a subset of latency-sensitive cases
- Results grouped by benchmark category

It does **not** currently produce percentile histograms, memory profiles, or persisted machine-readable reports.

## Notes

- This crate uses a hand-rolled harness in `src/utils.rs` and `src/report.rs` rather than Criterion output files.
- Benchmarks are intended for local regression spotting and relative comparisons, not as a substitute for production load testing.
- The IVM comparison suite checks result equivalence before comparing latency.

## License

Apache-2.0
