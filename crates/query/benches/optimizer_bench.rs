use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use cynos_core::{Row, Value};
use cynos_query::ast::{Expr, SortOrder};
use cynos_query::context::{ExecutionContext, IndexInfo, TableStats};
use cynos_query::executor::{InMemoryDataSource, PhysicalPlanRunner};
use cynos_query::optimizer::OrderByIndexPass;
use cynos_query::planner::{IndexBounds, LogicalPlan, PhysicalPlan, QueryPlanner};
use std::boxed::Box;

fn create_join_reorder_case(
    large_rows: usize,
    medium_rows: usize,
    small_rows: usize,
) -> (InMemoryDataSource, PhysicalPlan, PhysicalPlan) {
    let large: Vec<Row> = (0..large_rows)
        .map(|id| {
            Row::new(
                id as u64,
                vec![
                    Value::Int64(id as i64),
                    Value::Int64((id % medium_rows) as i64),
                    Value::Int64((id % 100) as i64),
                ],
            )
        })
        .collect();

    let medium: Vec<Row> = (0..medium_rows)
        .map(|id| {
            Row::new(
                (10_000 + id) as u64,
                vec![
                    Value::Int64(id as i64),
                    Value::Int64((id % small_rows) as i64),
                ],
            )
        })
        .collect();

    let small: Vec<Row> = (0..small_rows)
        .map(|id| Row::new((20_000 + id) as u64, vec![Value::Int64(id as i64)]))
        .collect();

    let mut ds = InMemoryDataSource::new();
    ds.add_table("large", large, 3);
    ds.add_table("medium", medium, 2);
    ds.add_table("small", small, 1);

    let large_medium = Expr::eq(
        Expr::column("large", "medium_id", 1),
        Expr::column("medium", "id", 0),
    );
    let medium_small = Expr::eq(
        Expr::column("medium", "small_id", 1),
        Expr::column("small", "id", 0),
    );

    let bad_plan = PhysicalPlan::hash_join(
        PhysicalPlan::hash_join(
            PhysicalPlan::table_scan("large"),
            PhysicalPlan::table_scan("medium"),
            large_medium.clone(),
            cynos_query::ast::JoinType::Inner,
        ),
        PhysicalPlan::table_scan("small"),
        medium_small.clone(),
        cynos_query::ast::JoinType::Inner,
    );

    let logical = LogicalPlan::inner_join(
        LogicalPlan::inner_join(
            LogicalPlan::scan("large"),
            LogicalPlan::scan("medium"),
            large_medium,
        ),
        LogicalPlan::scan("small"),
        medium_small,
    );

    let mut ctx = ExecutionContext::new();
    ctx.register_table(
        "large",
        TableStats {
            row_count: large_rows,
            is_sorted: false,
            indexes: vec![],
        },
    );
    ctx.register_table(
        "medium",
        TableStats {
            row_count: medium_rows,
            is_sorted: false,
            indexes: vec![],
        },
    );
    ctx.register_table(
        "small",
        TableStats {
            row_count: small_rows,
            is_sorted: false,
            indexes: vec![],
        },
    );

    let planner = QueryPlanner::new(ctx);
    let optimized_plan = planner.plan(logical);

    assert!(
        matches!(optimized_plan, PhysicalPlan::HashJoin { .. }),
        "expected planner to keep hash joins, got {:?}",
        optimized_plan
    );

    (ds, bad_plan, optimized_plan)
}

fn bench_join_reorder_execution(c: &mut Criterion) {
    let mut group = c.benchmark_group("optimizer_join_reorder_execution");

    for &(large_rows, medium_rows, small_rows) in &[(20_000, 2_000, 50), (100_000, 10_000, 100)] {
        let (ds, bad_plan, optimized_plan) =
            create_join_reorder_case(large_rows, medium_rows, small_rows);
        let runner = PhysicalPlanRunner::new(&ds);
        let case_id = format!("{}_{}_{}", large_rows, medium_rows, small_rows);

        group.bench_with_input(BenchmarkId::new("bad_order", &case_id), &case_id, |b, _| {
            b.iter(|| black_box(runner.execute(&bad_plan).unwrap()))
        });

        group.bench_with_input(
            BenchmarkId::new("planner_reordered", &case_id),
            &case_id,
            |b, _| b.iter(|| black_box(runner.execute(&optimized_plan).unwrap())),
        );
    }

    group.finish();
}

fn create_ordering_properties_case(
    rows: usize,
) -> (InMemoryDataSource, PhysicalPlan, PhysicalPlan) {
    let data: Vec<Row> = (0..rows)
        .map(|id| {
            Row::new(
                id as u64,
                vec![
                    Value::Int64(id as i64),
                    Value::Int64((rows - id) as i64),
                    Value::Int64((id % 2) as i64),
                ],
            )
        })
        .collect();

    let mut ds = InMemoryDataSource::new();
    ds.add_table("scores", data, 3);
    ds.create_index("scores", "idx_score", 1).unwrap();

    let filter = PhysicalPlan::Filter {
        input: Box::new(PhysicalPlan::IndexScan {
            table: "scores".into(),
            index: "idx_score".into(),
            bounds: IndexBounds::Scalar(cynos_index::KeyRange::bound(
                Value::Int64((rows / 10) as i64),
                Value::Int64((rows - rows / 10) as i64),
                false,
                false,
            )),
            limit: None,
            offset: None,
            reverse: false,
        }),
        predicate: Expr::eq(Expr::column("scores", "bucket", 2), Expr::literal(1i64)),
    };

    let baseline = PhysicalPlan::Sort {
        input: Box::new(filter),
        order_by: vec![(Expr::column("scores", "score", 1), SortOrder::Asc)],
    };

    let mut ctx = ExecutionContext::new();
    ctx.register_table(
        "scores",
        TableStats {
            row_count: rows,
            is_sorted: false,
            indexes: vec![IndexInfo::new("idx_score", vec!["score".into()], false)],
        },
    );

    let optimized = OrderByIndexPass::new(&ctx).optimize(baseline.clone());
    assert!(
        !matches!(optimized, PhysicalPlan::Sort { .. }),
        "expected ordering properties to remove the redundant sort, got {:?}",
        optimized
    );

    let runner = PhysicalPlanRunner::new(&ds);
    let baseline_result = runner.execute(&baseline).unwrap();
    let optimized_result = runner.execute(&optimized).unwrap();
    assert_eq!(baseline_result.len(), optimized_result.len());
    let baseline_scores: Vec<_> = baseline_result
        .entries
        .iter()
        .map(|entry| entry.get_field(1).cloned())
        .collect();
    let optimized_scores: Vec<_> = optimized_result
        .entries
        .iter()
        .map(|entry| entry.get_field(1).cloned())
        .collect();
    assert_eq!(baseline_scores, optimized_scores);

    (ds, baseline, optimized)
}

fn bench_ordering_properties_execution(c: &mut Criterion) {
    let mut group = c.benchmark_group("optimizer_ordering_properties_execution");

    for &rows in &[50_000, 200_000] {
        let (ds, baseline, optimized) = create_ordering_properties_case(rows);
        let runner = PhysicalPlanRunner::new(&ds);

        group.bench_with_input(BenchmarkId::new("baseline_sort", rows), &rows, |b, _| {
            b.iter(|| black_box(runner.execute(&baseline).unwrap()))
        });

        group.bench_with_input(
            BenchmarkId::new("properties_elided_sort", rows),
            &rows,
            |b, _| b.iter(|| black_box(runner.execute(&optimized).unwrap())),
        );
    }

    group.finish();
}

fn create_composite_bounds_case(
    rows_per_region: usize,
) -> (InMemoryDataSource, PhysicalPlan, PhysicalPlan) {
    let regions = ["amer", "apac", "emea"];
    let mut rows = Vec::with_capacity(rows_per_region * regions.len());
    let mut row_id = 1_u64;
    for region in regions {
        for score in 0..rows_per_region {
            rows.push(Row::new(
                row_id,
                vec![
                    Value::String(region.into()),
                    Value::Int64(score as i64),
                    Value::Int64((score % 32) as i64),
                ],
            ));
            row_id += 1;
        }
    }

    let mut ds = InMemoryDataSource::new();
    ds.add_table("scores", rows, 3);
    ds.create_composite_index("scores", "idx_region_score", &[0, 1])
        .unwrap();

    let predicate = Expr::and(
        Expr::eq(
            Expr::column("scores", "region", 0),
            Expr::literal(Value::String("apac".into())),
        ),
        Expr::and(
            Expr::ge(
                Expr::column("scores", "score", 1),
                Expr::literal((rows_per_region / 3) as i64),
            ),
            Expr::le(
                Expr::column("scores", "score", 1),
                Expr::literal((rows_per_region / 3 + 256) as i64),
            ),
        ),
    );

    let baseline = PhysicalPlan::Filter {
        input: Box::new(PhysicalPlan::table_scan("scores")),
        predicate: predicate.clone(),
    };

    let mut ctx = ExecutionContext::new();
    ctx.register_table(
        "scores",
        TableStats {
            row_count: rows_per_region * regions.len(),
            is_sorted: false,
            indexes: vec![IndexInfo::new(
                "idx_region_score",
                vec!["region".into(), "score".into()],
                false,
            )],
        },
    );

    let optimized =
        QueryPlanner::new(ctx).plan(LogicalPlan::filter(LogicalPlan::scan("scores"), predicate));
    assert!(
        matches!(optimized, PhysicalPlan::IndexScan { .. }),
        "expected composite bounds to produce an index scan, got {:?}",
        optimized
    );

    let runner = PhysicalPlanRunner::new(&ds);
    let baseline_result = runner.execute(&baseline).unwrap();
    let optimized_result = runner.execute(&optimized).unwrap();
    assert_eq!(baseline_result.len(), optimized_result.len());

    (ds, baseline, optimized)
}

fn bench_composite_tuple_bounds_execution(c: &mut Criterion) {
    let mut group = c.benchmark_group("optimizer_composite_tuple_bounds_execution");

    for &rows_per_region in &[20_000, 80_000] {
        let (ds, baseline, optimized) = create_composite_bounds_case(rows_per_region);
        let runner = PhysicalPlanRunner::new(&ds);

        group.bench_with_input(
            BenchmarkId::new("table_scan_filter", rows_per_region),
            &rows_per_region,
            |b, _| b.iter(|| black_box(runner.execute(&baseline).unwrap())),
        );

        group.bench_with_input(
            BenchmarkId::new("tuple_bound_index_scan", rows_per_region),
            &rows_per_region,
            |b, _| b.iter(|| black_box(runner.execute(&optimized).unwrap())),
        );
    }

    group.finish();
}

criterion_group!(
    optimizer_benches,
    bench_join_reorder_execution,
    bench_ordering_properties_execution,
    bench_composite_tuple_bounds_execution
);
criterion_main!(optimizer_benches);
