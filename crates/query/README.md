# cynos-query

Query engine for Cynos in-memory database.

## Overview

This crate provides the query execution engine including:

- `ast`: Expression and predicate AST definitions
- `planner`: Logical and physical query plans
- `optimizer`: Query optimization passes
- `executor`: Query execution operators
- `context`: Execution context
- `plan_cache`: Query plan caching for repeated queries

## Features

- `#![no_std]` compatible
- Cost-based query optimization
- Multiple join algorithms (hash join, merge join, nested loop)
- Aggregate functions (COUNT, SUM, AVG, MIN, MAX)
- Sort and limit operations
- Plan caching for repeated queries

## Query Operators

| Operator | Description |
|----------|-------------|
| Scan | Full table scan or index scan |
| Filter | Row filtering with predicates |
| Project | Column projection |
| Join | Hash/Merge/Nested loop joins |
| Aggregate | Group by with aggregations |
| Sort | ORDER BY implementation |
| Limit | LIMIT/OFFSET implementation |

## Optimization Passes

- Predicate pushdown
- Join reordering
- Index selection
- Projection pruning

## License

Apache-2.0
