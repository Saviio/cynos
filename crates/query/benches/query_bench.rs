//! Benchmarks for query operations.
//!
//! These benchmarks measure pure execution time by:
//! 1. Using iter_batched to exclude setup from measurement
//! 2. Using shuffled data to avoid sorted-input optimizations
//! 3. Measuring actual executor operations, not clone/drop overhead
//!
//! End-to-end benchmarks use PhysicalPlanRunner to measure the full query
//! execution pipeline including plan interpretation overhead.

use cynos_core::{Row, Value};
use cynos_query::ast::{ColumnRef, Expr, JoinType, SortOrder, ValuePredicate};
use cynos_query::executor::join::{HashJoin, NestedLoopJoin, SortMergeJoin};
use cynos_query::executor::{
    FilterExecutor, InMemoryDataSource, LimitExecutor, PhysicalPlanRunner, ProjectExecutor,
    Relation, SortExecutor,
};
use cynos_query::optimizer::Optimizer;
use cynos_query::planner::{LogicalPlan, PhysicalPlan};
use criterion::{black_box, criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion};

// ============================================================================
// Data Generation Utilities
// ============================================================================

/// Simple LCG for reproducible pseudo-random shuffling
fn shuffle_indices(count: usize, seed: u64) -> Vec<usize> {
    let mut indices: Vec<usize> = (0..count).collect();
    let mut s = seed;
    for i in (1..count).rev() {
        s = s.wrapping_mul(6364136223846793005).wrapping_add(1);
        let j = (s as usize) % (i + 1);
        indices.swap(i, j);
    }
    indices
}

/// Creates rows with shuffled order for realistic sort benchmarks
fn create_shuffled_rows(count: usize) -> Vec<Row> {
    shuffle_indices(count, 12345)
        .into_iter()
        .map(|i| {
            Row::new(
                i as u64,
                vec![
                    Value::Int64(i as i64),
                    Value::String(format!("name_{}", i)),
                    Value::Int64((i % 100) as i64),
                ],
            )
        })
        .collect()
}

/// Creates rows for join benchmarks with controlled key distribution
fn create_join_rows(count: usize, key_range: usize, seed: u64) -> Vec<Row> {
    shuffle_indices(count, seed)
        .into_iter()
        .map(|i| {
            Row::new(
                i as u64,
                vec![
                    Value::Int64((i % key_range) as i64),
                    Value::String(format!("value_{}", i)),
                ],
            )
        })
        .collect()
}

// ============================================================================
// JOIN Benchmarks - Measure pure join execution
// ============================================================================

fn bench_hash_join(c: &mut Criterion) {
    let mut group = c.benchmark_group("hash_join");

    for size in [100, 1000, 10000].iter() {
        let key_range = size / 10; // 10% selectivity
        let left_rows = create_join_rows(*size, key_range, 12345);
        let right_rows = create_join_rows(*size, key_range, 67890);

        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, _| {
            b.iter_batched(
                || {
                    (
                        Relation::from_rows_owned(left_rows.clone(), vec!["left".into()]),
                        Relation::from_rows_owned(right_rows.clone(), vec!["right".into()]),
                    )
                },
                |(left, right)| {
                    let join = HashJoin::inner(0, 0);
                    black_box(join.execute(left, right))
                },
                BatchSize::SmallInput,
            )
        });
    }

    group.finish();
}

fn bench_nested_loop_join(c: &mut Criterion) {
    let mut group = c.benchmark_group("nested_loop_join");

    // Smaller sizes for O(n*m) algorithm
    for size in [100, 500, 1000].iter() {
        let key_range = size / 10;
        let left_rows = create_join_rows(*size, key_range, 12345);
        let right_rows = create_join_rows(*size, key_range, 67890);

        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, _| {
            b.iter_batched(
                || {
                    (
                        Relation::from_rows_owned(left_rows.clone(), vec!["left".into()]),
                        Relation::from_rows_owned(right_rows.clone(), vec!["right".into()]),
                    )
                },
                |(left, right)| {
                    let join = NestedLoopJoin::inner(0, 0);
                    black_box(join.execute(left, right))
                },
                BatchSize::SmallInput,
            )
        });
    }

    group.finish();
}

fn bench_sort_merge_join(c: &mut Criterion) {
    let mut group = c.benchmark_group("sort_merge_join");

    for size in [100, 1000, 10000].iter() {
        let key_range = size / 10;
        let left_rows = create_join_rows(*size, key_range, 12345);
        let right_rows = create_join_rows(*size, key_range, 67890);

        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, _| {
            b.iter_batched(
                || {
                    (
                        Relation::from_rows_owned(left_rows.clone(), vec!["left".into()]),
                        Relation::from_rows_owned(right_rows.clone(), vec!["right".into()]),
                    )
                },
                |(left, right)| {
                    let join = SortMergeJoin::inner(0, 0);
                    black_box(join.execute_with_sort(left, right))
                },
                BatchSize::SmallInput,
            )
        });
    }

    group.finish();
}

// ============================================================================
// Single-Table Operation Benchmarks
// ============================================================================

fn bench_filter(c: &mut Criterion) {
    let mut group = c.benchmark_group("filter");

    for size in [100, 1000, 10000].iter() {
        let rows = create_shuffled_rows(*size);
        let threshold = (*size / 2) as i64; // 50% selectivity

        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, _| {
            b.iter_batched(
                || Relation::from_rows_owned(rows.clone(), vec!["table".into()]),
                |relation| {
                    let col = ColumnRef::new("table", "id", 0);
                    let pred = ValuePredicate::gt(col, Value::Int64(threshold));
                    let executor = FilterExecutor::new(pred);
                    black_box(executor.execute(relation))
                },
                BatchSize::SmallInput,
            )
        });
    }

    group.finish();
}

fn bench_sort(c: &mut Criterion) {
    let mut group = c.benchmark_group("sort");

    for size in [100, 1000, 10000].iter() {
        // Use shuffled data to measure real sorting work
        let rows = create_shuffled_rows(*size);

        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, _| {
            b.iter_batched(
                || Relation::from_rows_owned(rows.clone(), vec!["table".into()]),
                |relation| {
                    let executor = SortExecutor::new(vec![(0, SortOrder::Asc)]);
                    black_box(executor.execute(relation))
                },
                BatchSize::SmallInput,
            )
        });
    }

    group.finish();
}

fn bench_project(c: &mut Criterion) {
    let mut group = c.benchmark_group("project");

    for size in [100, 1000, 10000].iter() {
        let rows = create_shuffled_rows(*size);

        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, _| {
            b.iter_batched(
                || Relation::from_rows_owned(rows.clone(), vec!["table".into()]),
                |relation| {
                    let executor = ProjectExecutor::new(vec![0, 2]);
                    black_box(executor.execute(relation))
                },
                BatchSize::SmallInput,
            )
        });
    }

    group.finish();
}

fn bench_limit(c: &mut Criterion) {
    let mut group = c.benchmark_group("limit");

    for size in [100, 1000, 10000].iter() {
        let rows = create_shuffled_rows(*size);

        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, _| {
            b.iter_batched(
                || Relation::from_rows_owned(rows.clone(), vec!["table".into()]),
                |relation| {
                    let executor = LimitExecutor::new(10, 5);
                    black_box(executor.execute(relation))
                },
                BatchSize::SmallInput,
            )
        });
    }

    group.finish();
}

// ============================================================================
// Combined Query Benchmarks
// ============================================================================

/// Benchmark: SELECT id, name FROM table WHERE id > N ORDER BY id LIMIT 10
fn bench_simple_query(c: &mut Criterion) {
    let mut group = c.benchmark_group("simple_query");

    for size in [1000, 10000].iter() {
        let rows = create_shuffled_rows(*size);
        let threshold = (*size / 2) as i64;

        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, _| {
            b.iter_batched(
                || Relation::from_rows_owned(rows.clone(), vec!["table".into()]),
                |relation| {
                    // Filter: WHERE id > threshold
                    let col = ColumnRef::new("table", "id", 0);
                    let pred = ValuePredicate::gt(col, Value::Int64(threshold));
                    let filter = FilterExecutor::new(pred);
                    let filtered = filter.execute(relation);

                    // Sort: ORDER BY id
                    let sort = SortExecutor::new(vec![(0, SortOrder::Asc)]);
                    let sorted = sort.execute(filtered);

                    // Limit: LIMIT 10
                    let limit = LimitExecutor::new(10, 0);
                    let limited = limit.execute(sorted);

                    // Project: SELECT id, name
                    let project = ProjectExecutor::new(vec![0, 1]);
                    black_box(project.execute(limited))
                },
                BatchSize::SmallInput,
            )
        });
    }

    group.finish();
}

/// Benchmark: Two-table join with filter
/// SELECT * FROM left JOIN right ON left.key = right.key WHERE left.key > N
fn bench_join_with_filter(c: &mut Criterion) {
    let mut group = c.benchmark_group("join_with_filter");

    for size in [100, 1000].iter() {
        let key_range = size / 10;
        let left_rows = create_join_rows(*size, key_range, 12345);
        let right_rows = create_join_rows(*size, key_range, 67890);
        let threshold = (key_range / 2) as i64;

        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, _| {
            b.iter_batched(
                || {
                    (
                        Relation::from_rows_owned(left_rows.clone(), vec!["left".into()]),
                        Relation::from_rows_owned(right_rows.clone(), vec!["right".into()]),
                    )
                },
                |(left, right)| {
                    // Filter left side first (predicate pushdown simulation)
                    let col = ColumnRef::new("left", "key", 0);
                    let pred = ValuePredicate::gt(col, Value::Int64(threshold));
                    let filter = FilterExecutor::new(pred);
                    let filtered_left = filter.execute(left);

                    // Join
                    let join = HashJoin::inner(0, 0);
                    black_box(join.execute(filtered_left, right))
                },
                BatchSize::SmallInput,
            )
        });
    }

    group.finish();
}

// ============================================================================
// End-to-End Benchmarks using PhysicalPlanRunner
// ============================================================================

/// Creates an InMemoryDataSource with test data
fn create_data_source(size: usize) -> InMemoryDataSource {
    let mut ds = InMemoryDataSource::new();

    // Main table with shuffled data: id, name, category
    let rows: Vec<Row> = shuffle_indices(size, 12345)
        .into_iter()
        .map(|i| {
            Row::new(
                i as u64,
                vec![
                    Value::Int64(i as i64),
                    Value::String(format!("name_{}", i)),
                    Value::Int64((i % 100) as i64),
                ],
            )
        })
        .collect();
    ds.add_table("users", rows, 3);
    ds.create_index("users", "idx_id", 0).unwrap();
    ds.create_index("users", "idx_category", 2).unwrap();

    // Secondary table for joins: id, description
    let orders: Vec<Row> = shuffle_indices(size, 67890)
        .into_iter()
        .map(|i| {
            Row::new(
                i as u64,
                vec![
                    Value::Int64((i % (size / 10).max(1)) as i64), // Foreign key
                    Value::String(format!("order_{}", i)),
                ],
            )
        })
        .collect();
    ds.add_table("orders", orders, 2);
    ds.create_index("orders", "idx_user_id", 0).unwrap();

    ds
}

/// End-to-end: SELECT * FROM users WHERE id > N
fn bench_e2e_filter(c: &mut Criterion) {
    let mut group = c.benchmark_group("e2e_filter");

    for size in [1000, 10000].iter() {
        let ds = create_data_source(*size);
        let threshold = (*size / 2) as i64;

        let plan = PhysicalPlan::filter(
            PhysicalPlan::table_scan("users"),
            Expr::gt(
                Expr::column("users", "id", 0),
                Expr::literal(Value::Int64(threshold)),
            ),
        );

        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, _| {
            let runner = PhysicalPlanRunner::new(&ds);
            b.iter(|| black_box(runner.execute(&plan).unwrap()))
        });
    }

    group.finish();
}

/// End-to-end: SELECT * FROM users ORDER BY id
fn bench_e2e_sort(c: &mut Criterion) {
    let mut group = c.benchmark_group("e2e_sort");

    for size in [1000, 10000].iter() {
        let ds = create_data_source(*size);

        let plan = PhysicalPlan::sort(
            PhysicalPlan::table_scan("users"),
            vec![(Expr::column("users", "id", 0), SortOrder::Asc)],
        );

        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, _| {
            let runner = PhysicalPlanRunner::new(&ds);
            b.iter(|| black_box(runner.execute(&plan).unwrap()))
        });
    }

    group.finish();
}

/// End-to-end: SELECT id, name FROM users WHERE id > N ORDER BY id LIMIT 10
fn bench_e2e_simple_query(c: &mut Criterion) {
    let mut group = c.benchmark_group("e2e_simple_query");

    for size in [1000, 10000].iter() {
        let ds = create_data_source(*size);
        let threshold = (*size / 2) as i64;

        let plan = PhysicalPlan::limit(
            PhysicalPlan::sort(
                PhysicalPlan::project(
                    PhysicalPlan::filter(
                        PhysicalPlan::table_scan("users"),
                        Expr::gt(
                            Expr::column("users", "id", 0),
                            Expr::literal(Value::Int64(threshold)),
                        ),
                    ),
                    vec![
                        Expr::column("users", "id", 0),
                        Expr::column("users", "name", 1),
                    ],
                ),
                vec![(Expr::column("users", "id", 0), SortOrder::Asc)],
            ),
            10,
            0,
        );

        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, _| {
            let runner = PhysicalPlanRunner::new(&ds);
            b.iter(|| black_box(runner.execute(&plan).unwrap()))
        });
    }

    group.finish();
}

/// End-to-end: Hash join between users and orders
fn bench_e2e_hash_join(c: &mut Criterion) {
    let mut group = c.benchmark_group("e2e_hash_join");

    for size in [1000, 10000].iter() {
        let ds = create_data_source(*size);

        let plan = PhysicalPlan::hash_join(
            PhysicalPlan::table_scan("users"),
            PhysicalPlan::table_scan("orders"),
            Expr::eq(
                Expr::column("users", "id", 0),
                Expr::column("orders", "user_id", 0),
            ),
            JoinType::Inner,
        );

        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, _| {
            let runner = PhysicalPlanRunner::new(&ds);
            b.iter(|| black_box(runner.execute(&plan).unwrap()))
        });
    }

    group.finish();
}

/// End-to-end: Sort-merge join between users and orders
fn bench_e2e_sort_merge_join(c: &mut Criterion) {
    let mut group = c.benchmark_group("e2e_sort_merge_join");

    for size in [1000, 10000].iter() {
        let ds = create_data_source(*size);

        let plan = PhysicalPlan::sort_merge_join(
            PhysicalPlan::table_scan("users"),
            PhysicalPlan::table_scan("orders"),
            Expr::eq(
                Expr::column("users", "id", 0),
                Expr::column("orders", "user_id", 0),
            ),
            JoinType::Inner,
        );

        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, _| {
            let runner = PhysicalPlanRunner::new(&ds);
            b.iter(|| black_box(runner.execute(&plan).unwrap()))
        });
    }

    group.finish();
}

/// End-to-end: Complex query with join, filter, sort, and limit
/// SELECT u.id, u.name, o.description
/// FROM users u
/// JOIN orders o ON u.id = o.user_id
/// WHERE u.category > 50
/// ORDER BY u.id
/// LIMIT 100
fn bench_e2e_complex_query(c: &mut Criterion) {
    let mut group = c.benchmark_group("e2e_complex_query");

    for size in [1000, 10000].iter() {
        let ds = create_data_source(*size);

        let plan = PhysicalPlan::limit(
            PhysicalPlan::sort(
                PhysicalPlan::project(
                    PhysicalPlan::hash_join(
                        PhysicalPlan::filter(
                            PhysicalPlan::table_scan("users"),
                            Expr::gt(
                                Expr::column("users", "category", 2),
                                Expr::literal(Value::Int64(50)),
                            ),
                        ),
                        PhysicalPlan::table_scan("orders"),
                        Expr::eq(
                            Expr::column("users", "id", 0),
                            Expr::column("orders", "user_id", 0),
                        ),
                        JoinType::Inner,
                    ),
                    vec![
                        Expr::column("users", "id", 0),
                        Expr::column("users", "name", 1),
                        Expr::column("orders", "description", 1),
                    ],
                ),
                vec![(Expr::column("users", "id", 0), SortOrder::Asc)],
            ),
            100,
            0,
        );

        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, _| {
            let runner = PhysicalPlanRunner::new(&ds);
            b.iter(|| black_box(runner.execute(&plan).unwrap()))
        });
    }

    group.finish();
}

/// End-to-end: Index scan benchmark
fn bench_e2e_index_scan(c: &mut Criterion) {
    let mut group = c.benchmark_group("e2e_index_scan");

    for size in [1000, 10000].iter() {
        let ds = create_data_source(*size);
        let range_end = (*size / 10) as i64;

        let plan = PhysicalPlan::IndexScan {
            table: "users".into(),
            index: "idx_id".into(),
            range_start: Some(Value::Int64(0)),
            range_end: Some(Value::Int64(range_end)),
            include_start: true,
            include_end: true,
            limit: None,
            offset: None,
            reverse: false,
        };

        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, _| {
            let runner = PhysicalPlanRunner::new(&ds);
            b.iter(|| black_box(runner.execute(&plan).unwrap()))
        });
    }

    group.finish();
}

/// End-to-end: Index point lookup
fn bench_e2e_index_get(c: &mut Criterion) {
    let mut group = c.benchmark_group("e2e_index_get");

    for size in [1000, 10000].iter() {
        let ds = create_data_source(*size);

        let plan = PhysicalPlan::index_get("users", "idx_id", Value::Int64(500));

        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, _| {
            let runner = PhysicalPlanRunner::new(&ds);
            b.iter(|| black_box(runner.execute(&plan).unwrap()))
        });
    }

    group.finish();
}

// ============================================================================
// Optimized End-to-End Benchmarks (with Optimizer)
// ============================================================================

/// Optimized e2e: SELECT * FROM users WHERE id > N
/// Tests: PredicatePushdown
fn bench_optimized_filter(c: &mut Criterion) {
    let mut group = c.benchmark_group("optimized_filter");

    for size in [1000, 10000].iter() {
        let ds = create_data_source(*size);
        let threshold = (*size / 2) as i64;

        // Build LogicalPlan
        let logical = LogicalPlan::filter(
            LogicalPlan::scan("users"),
            Expr::gt(
                Expr::column("users", "id", 0),
                Expr::literal(Value::Int64(threshold)),
            ),
        );

        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, _| {
            let optimizer = Optimizer::new();
            let runner = PhysicalPlanRunner::new(&ds);
            b.iter(|| {
                let optimized = optimizer.optimize(logical.clone());
                let physical = optimizer.to_physical(optimized);
                black_box(runner.execute(&physical).unwrap())
            })
        });
    }

    group.finish();
}

/// Optimized e2e: SELECT id, name FROM users ORDER BY id LIMIT 10
/// Tests: TopNPushdown (Limit + Sort -> TopN)
fn bench_optimized_topn(c: &mut Criterion) {
    let mut group = c.benchmark_group("optimized_topn");

    for size in [1000, 10000].iter() {
        let ds = create_data_source(*size);

        // Build LogicalPlan: Limit -> Sort -> Scan
        let logical = LogicalPlan::limit(
            LogicalPlan::sort(
                LogicalPlan::scan("users"),
                vec![(Expr::column("users", "id", 0), SortOrder::Asc)],
            ),
            10,
            0,
        );

        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, _| {
            let optimizer = Optimizer::new();
            let runner = PhysicalPlanRunner::new(&ds);
            b.iter(|| {
                let optimized = optimizer.optimize(logical.clone());
                let physical = optimizer.to_physical(optimized);
                black_box(runner.execute(&physical).unwrap())
            })
        });
    }

    group.finish();
}

/// Benchmark: TopN heap vs Sort+Limit comparison
/// Compares the performance of heap-based TopN (O(n log k)) vs Sort+Limit (O(n log n))
fn bench_topn_heap_vs_sort(c: &mut Criterion) {
    let mut group = c.benchmark_group("topn_heap_vs_sort");

    // Test with varying data sizes and k values
    for (size, k) in [(1000, 10), (10000, 10), (10000, 100), (100000, 10), (100000, 100)].iter() {
        let ds = {
            let mut ds = InMemoryDataSource::new();
            let rows: Vec<Row> = shuffle_indices(*size, 12345)
                .into_iter()
                .map(|i| {
                    Row::new(
                        i as u64,
                        vec![
                            Value::Int64(i as i64),
                            Value::String(format!("name_{}", i)),
                        ],
                    )
                })
                .collect();
            ds.add_table("data", rows, 2);
            ds
        };

        // TopN (uses heap for large datasets)
        let topn_plan = PhysicalPlan::TopN {
            input: Box::new(PhysicalPlan::table_scan("data")),
            order_by: vec![(Expr::column("data", "id", 0), SortOrder::Asc)],
            limit: *k,
            offset: 0,
        };

        // Sort + Limit (traditional approach)
        let sort_limit_plan = PhysicalPlan::limit(
            PhysicalPlan::sort(
                PhysicalPlan::table_scan("data"),
                vec![(Expr::column("data", "id", 0), SortOrder::Asc)],
            ),
            *k,
            0,
        );

        let label = format!("n={}_k={}", size, k);

        group.bench_with_input(BenchmarkId::new("topn_heap", &label), &label, |b, _| {
            let runner = PhysicalPlanRunner::new(&ds);
            b.iter(|| black_box(runner.execute(&topn_plan).unwrap()))
        });

        group.bench_with_input(BenchmarkId::new("sort_limit", &label), &label, |b, _| {
            let runner = PhysicalPlanRunner::new(&ds);
            b.iter(|| black_box(runner.execute(&sort_limit_plan).unwrap()))
        });
    }

    group.finish();
}

/// Optimized e2e: Three-way join with different table sizes
/// Tests: JoinReorder (should reorder to join smaller tables first)
fn bench_optimized_join_reorder(c: &mut Criterion) {
    let mut group = c.benchmark_group("optimized_join_reorder");

    for size in [100, 500].iter() {
        let mut ds = InMemoryDataSource::new();

        // Create tables with different sizes: small (size/10), medium (size), large (size*2)
        let small_size = size / 10;
        let medium_size = *size;
        let large_size = size * 2;

        // Small table
        let small_rows: Vec<Row> = (0..small_size)
            .map(|i| Row::new(i as u64, vec![Value::Int64(i as i64), Value::String(format!("small_{}", i))]))
            .collect();
        ds.add_table("small", small_rows, 2);

        // Medium table
        let medium_rows: Vec<Row> = (0..medium_size)
            .map(|i| Row::new(i as u64, vec![Value::Int64((i % small_size) as i64), Value::String(format!("medium_{}", i))]))
            .collect();
        ds.add_table("medium", medium_rows, 2);

        // Large table
        let large_rows: Vec<Row> = (0..large_size)
            .map(|i| Row::new(i as u64, vec![Value::Int64((i % medium_size) as i64), Value::String(format!("large_{}", i))]))
            .collect();
        ds.add_table("large", large_rows, 2);

        // Build LogicalPlan: large JOIN medium JOIN small (suboptimal order)
        let logical = LogicalPlan::inner_join(
            LogicalPlan::inner_join(
                LogicalPlan::scan("large"),
                LogicalPlan::scan("medium"),
                Expr::eq(
                    Expr::column("large", "id", 0),
                    Expr::column("medium", "id", 0),
                ),
            ),
            LogicalPlan::scan("small"),
            Expr::eq(
                Expr::column("medium", "id", 0),
                Expr::column("small", "id", 0),
            ),
        );

        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, _| {
            let optimizer = Optimizer::new();
            let runner = PhysicalPlanRunner::new(&ds);
            b.iter(|| {
                let optimized = optimizer.optimize(logical.clone());
                let physical = optimizer.to_physical(optimized);
                black_box(runner.execute(&physical).unwrap())
            })
        });
    }

    group.finish();
}

/// Optimized e2e: Join with filter that can be pushed down
/// Tests: PredicatePushdown into Join
fn bench_optimized_join_filter_pushdown(c: &mut Criterion) {
    let mut group = c.benchmark_group("optimized_join_filter_pushdown");

    for size in [100, 1000].iter() {
        let ds = create_data_source(*size);
        let threshold = (*size / 2) as i64;

        // Build LogicalPlan: Filter above Join (filter should be pushed into left side)
        let logical = LogicalPlan::filter(
            LogicalPlan::inner_join(
                LogicalPlan::scan("users"),
                LogicalPlan::scan("orders"),
                Expr::eq(
                    Expr::column("users", "id", 0),
                    Expr::column("orders", "user_id", 0),
                ),
            ),
            Expr::gt(
                Expr::column("users", "id", 0),
                Expr::literal(Value::Int64(threshold)),
            ),
        );

        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, _| {
            let optimizer = Optimizer::new();
            let runner = PhysicalPlanRunner::new(&ds);
            b.iter(|| {
                let optimized = optimizer.optimize(logical.clone());
                let physical = optimizer.to_physical(optimized);
                black_box(runner.execute(&physical).unwrap())
            })
        });
    }

    group.finish();
}

/// Optimized e2e: Left outer join that can be simplified to inner join
/// Tests: OuterJoinSimplification
fn bench_optimized_outer_join_simplification(c: &mut Criterion) {
    let mut group = c.benchmark_group("optimized_outer_join_simplify");

    for size in [100, 1000].iter() {
        let ds = create_data_source(*size);

        // Build LogicalPlan: Filter with IS NOT NULL on right side of left outer join
        // This should convert the left outer join to inner join
        let logical = LogicalPlan::filter(
            LogicalPlan::left_join(
                LogicalPlan::scan("users"),
                LogicalPlan::scan("orders"),
                Expr::eq(
                    Expr::column("users", "id", 0),
                    Expr::column("orders", "user_id", 0),
                ),
            ),
            Expr::is_not_null(Expr::column("orders", "user_id", 0)),
        );

        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, _| {
            let optimizer = Optimizer::new();
            let runner = PhysicalPlanRunner::new(&ds);
            b.iter(|| {
                let optimized = optimizer.optimize(logical.clone());
                let physical = optimizer.to_physical(optimized);
                black_box(runner.execute(&physical).unwrap())
            })
        });
    }

    group.finish();
}

/// Optimized e2e: Complex query with multiple optimizations
/// SELECT u.id, u.name FROM users u JOIN orders o ON u.id = o.user_id
/// WHERE u.category > 50 ORDER BY u.id LIMIT 100
/// Tests: PredicatePushdown + TopNPushdown + JoinReorder
fn bench_optimized_complex_query(c: &mut Criterion) {
    let mut group = c.benchmark_group("optimized_complex_query");

    for size in [1000, 10000].iter() {
        let ds = create_data_source(*size);

        // Build LogicalPlan with suboptimal structure
        let logical = LogicalPlan::limit(
            LogicalPlan::sort(
                LogicalPlan::project(
                    LogicalPlan::filter(
                        LogicalPlan::inner_join(
                            LogicalPlan::scan("users"),
                            LogicalPlan::scan("orders"),
                            Expr::eq(
                                Expr::column("users", "id", 0),
                                Expr::column("orders", "user_id", 0),
                            ),
                        ),
                        Expr::gt(
                            Expr::column("users", "category", 2),
                            Expr::literal(Value::Int64(50)),
                        ),
                    ),
                    vec![
                        Expr::column("users", "id", 0),
                        Expr::column("users", "name", 1),
                    ],
                ),
                vec![(Expr::column("users", "id", 0), SortOrder::Asc)],
            ),
            100,
            0,
        );

        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, _| {
            let optimizer = Optimizer::new();
            let runner = PhysicalPlanRunner::new(&ds);
            b.iter(|| {
                let optimized = optimizer.optimize(logical.clone());
                let physical = optimizer.to_physical(optimized);
                black_box(runner.execute(&physical).unwrap())
            })
        });
    }

    group.finish();
}

criterion_group!(
    benches,
    // Direct executor benchmarks
    bench_hash_join,
    bench_nested_loop_join,
    bench_sort_merge_join,
    bench_filter,
    bench_sort,
    bench_project,
    bench_limit,
    bench_simple_query,
    bench_join_with_filter,
    // End-to-end benchmarks using PhysicalPlanRunner (no optimizer)
    bench_e2e_filter,
    bench_e2e_sort,
    bench_e2e_simple_query,
    bench_e2e_hash_join,
    bench_e2e_sort_merge_join,
    bench_e2e_complex_query,
    bench_e2e_index_scan,
    bench_e2e_index_get,
    // Optimized end-to-end benchmarks (with optimizer)
    bench_optimized_filter,
    bench_optimized_topn,
    bench_topn_heap_vs_sort,
    bench_optimized_join_reorder,
    bench_optimized_join_filter_pushdown,
    bench_optimized_outer_join_simplification,
    bench_optimized_complex_query,
);

criterion_main!(benches);
