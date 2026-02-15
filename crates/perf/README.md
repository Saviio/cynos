# cynos-perf

Comprehensive performance benchmarks for Cynos database.

## Overview

This crate provides a comprehensive benchmark suite for measuring Cynos database performance across all components.

## Benchmark Categories

1. **Index Performance** - BTree, Hash, and GIN index operations
2. **Storage Performance** - Row store operations, table cache
3. **Query Execution** - Filter, project, sort, limit operations
4. **Join Performance** - Hash join, merge join, nested loop join
5. **Incremental Computation** - IVM delta propagation
6. **Reactive Query** - Subscription and change notification
7. **JSONB Performance** - JSON operations and path queries
8. **End-to-End Scenarios** - Real-world usage patterns

## Running Benchmarks

```bash
# From workspace root
cargo run -p cynos-perf --release

# Or with alias (if configured)
cargo perf
```

## Output

The benchmark produces a detailed report with:
- Operation throughput (ops/sec)
- Latency percentiles (p50, p95, p99)
- Memory usage statistics
- Comparison across different data sizes

## License

Apache-2.0
