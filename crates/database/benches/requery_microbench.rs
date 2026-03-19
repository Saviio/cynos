use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use cynos_core::schema::{Table, TableBuilder};
use cynos_core::{DataType, Row, Value};
use cynos_database::query_engine::{
    compile_cached_plan, compile_plan, execute_compiled_physical_plan,
    execute_compiled_physical_plan_with_summary, execute_physical_plan, CompiledPhysicalPlan,
    TableCacheDataSource,
};
use cynos_database::reactive_bridge::{QueryRegistry, ReQueryObservable};
use cynos_incremental::TableId;
use cynos_query::ast::{AggregateFunc, Expr, JoinType};
use cynos_query::plan_cache::{compute_plan_fingerprint, PlanCache};
use cynos_query::planner::{LogicalPlan, PhysicalPlan};
use cynos_storage::{RowStore, TableCache};
use hashbrown::HashSet;
use std::cell::RefCell;
use std::rc::Rc;

const TABLE_NAME: &str = "users";
const DEPARTMENTS_TABLE_NAME: &str = "departments";
const TABLE_ID: TableId = 1;
const HIT_ROW_ID: u64 = 9;
const MISS_ROW_ID: u64 = 8;
type PlanCompiler = fn(&TableCache) -> CompiledPhysicalPlan;

fn test_schema() -> Table {
    TableBuilder::new(TABLE_NAME)
        .unwrap()
        .add_column("id", DataType::Int64)
        .unwrap()
        .add_column("name", DataType::String)
        .unwrap()
        .add_column("age", DataType::Int32)
        .unwrap()
        .add_column("department", DataType::String)
        .unwrap()
        .add_column("salary", DataType::Int64)
        .unwrap()
        .add_primary_key(&["id"], false)
        .unwrap()
        .build()
        .unwrap()
}

fn indexed_test_schema() -> Table {
    TableBuilder::new(TABLE_NAME)
        .unwrap()
        .add_column("id", DataType::Int64)
        .unwrap()
        .add_column("name", DataType::String)
        .unwrap()
        .add_column("age", DataType::Int32)
        .unwrap()
        .add_column("department", DataType::String)
        .unwrap()
        .add_column("salary", DataType::Int64)
        .unwrap()
        .add_primary_key(&["id"], false)
        .unwrap()
        .add_index("idx_salary", &["salary"], false)
        .unwrap()
        .build()
        .unwrap()
}

fn join_user_schema() -> Table {
    TableBuilder::new(TABLE_NAME)
        .unwrap()
        .add_column("id", DataType::Int64)
        .unwrap()
        .add_column("dept_id", DataType::Int64)
        .unwrap()
        .add_column("age", DataType::Int32)
        .unwrap()
        .add_primary_key(&["id"], false)
        .unwrap()
        .build()
        .unwrap()
}

fn department_schema() -> Table {
    TableBuilder::new(DEPARTMENTS_TABLE_NAME)
        .unwrap()
        .add_column("id", DataType::Int64)
        .unwrap()
        .add_column("name", DataType::String)
        .unwrap()
        .add_primary_key(&["id"], false)
        .unwrap()
        .build()
        .unwrap()
}

fn populate_users(store: &mut RowStore, size: usize) {
    let departments = ["Engineering", "Sales", "Marketing", "HR", "Finance"];
    for i in 0..size {
        store
            .insert(Row::new(
                i as u64,
                vec![
                    Value::Int64((i + 1) as i64),
                    Value::String(format!("User {}", i + 1).into()),
                    Value::Int32((20 + (i % 50)) as i32),
                    Value::String(departments[i % departments.len()].into()),
                    Value::Int64((50_000 + (i % 100) as i64 * 1_000) as i64),
                ],
            ))
            .unwrap();
    }
}

fn build_cache(size: usize) -> TableCache {
    let mut cache = TableCache::new();
    cache.create_table(test_schema()).unwrap();

    let store = cache.get_table_mut(TABLE_NAME).unwrap();
    populate_users(store, size);

    cache
}

fn build_indexed_cache(size: usize) -> TableCache {
    let mut cache = TableCache::new();
    cache.create_table(indexed_test_schema()).unwrap();

    let store = cache.get_table_mut(TABLE_NAME).unwrap();
    populate_users(store, size);

    cache
}

fn build_join_cache(size: usize) -> TableCache {
    let mut cache = TableCache::new();
    cache.create_table(join_user_schema()).unwrap();
    cache.create_table(department_schema()).unwrap();

    {
        let users = cache.get_table_mut(TABLE_NAME).unwrap();
        for i in 0..size {
            users
                .insert(Row::new(
                    i as u64,
                    vec![
                        Value::Int64((i + 1) as i64),
                        Value::Int64((i % 5 + 1) as i64),
                        Value::Int32((20 + (i % 50)) as i32),
                    ],
                ))
                .unwrap();
        }
    }

    {
        let departments = cache.get_table_mut(DEPARTMENTS_TABLE_NAME).unwrap();
        for (id, name) in [
            (1, "Engineering"),
            (2, "Sales"),
            (3, "Marketing"),
            (4, "HR"),
            (5, "Finance"),
        ] {
            departments
                .insert(Row::new(
                    id,
                    vec![Value::Int64(id as i64), Value::String(name.into())],
                ))
                .unwrap();
        }
    }

    cache
}

fn filter_plan() -> LogicalPlan {
    LogicalPlan::filter(
        LogicalPlan::scan(TABLE_NAME),
        Expr::gt(
            Expr::column(TABLE_NAME, "age", 2),
            Expr::literal(Value::Int32(30)),
        ),
    )
}

fn filter_project_limit_plan() -> LogicalPlan {
    LogicalPlan::limit(
        LogicalPlan::project(
            LogicalPlan::filter(
                LogicalPlan::scan(TABLE_NAME),
                Expr::gt(
                    Expr::column(TABLE_NAME, "age", 2),
                    Expr::literal(Value::Int32(30)),
                ),
            ),
            vec![
                Expr::column(TABLE_NAME, "id", 0),
                Expr::column(TABLE_NAME, "age", 2),
                Expr::column(TABLE_NAME, "salary", 4),
            ],
        ),
        100,
        0,
    )
}

fn between_filter_plan() -> LogicalPlan {
    LogicalPlan::filter(
        LogicalPlan::scan(TABLE_NAME),
        Expr::between(
            Expr::column(TABLE_NAME, "salary", 4),
            Expr::literal(Value::Int64(70_000)),
            Expr::literal(Value::Int64(90_000)),
        ),
    )
}

fn in_list_filter_plan() -> LogicalPlan {
    LogicalPlan::filter(
        LogicalPlan::scan(TABLE_NAME),
        Expr::in_list(
            Expr::column(TABLE_NAME, "department", 3),
            vec![
                Value::String("Engineering".into()),
                Value::String("Sales".into()),
            ],
        ),
    )
}

fn compound_filter_plan() -> LogicalPlan {
    LogicalPlan::filter(
        LogicalPlan::scan(TABLE_NAME),
        Expr::and(
            Expr::gt(
                Expr::column(TABLE_NAME, "age", 2),
                Expr::literal(Value::Int32(30)),
            ),
            Expr::or(
                Expr::in_list(
                    Expr::column(TABLE_NAME, "department", 3),
                    vec![
                        Value::String("Engineering".into()),
                        Value::String("Sales".into()),
                    ],
                ),
                Expr::not(Expr::between(
                    Expr::column(TABLE_NAME, "salary", 4),
                    Expr::literal(Value::Int64(70_000)),
                    Expr::literal(Value::Int64(90_000)),
                )),
            ),
        ),
    )
}

fn aggregate_plan() -> LogicalPlan {
    LogicalPlan::aggregate(
        LogicalPlan::scan(TABLE_NAME),
        vec![Expr::column(TABLE_NAME, "department", 3)],
        vec![
            (AggregateFunc::Count, Expr::column(TABLE_NAME, "id", 0)),
            (AggregateFunc::Sum, Expr::column(TABLE_NAME, "salary", 4)),
            (AggregateFunc::Avg, Expr::column(TABLE_NAME, "age", 2)),
        ],
    )
}

fn join_plan() -> LogicalPlan {
    LogicalPlan::join(
        LogicalPlan::scan(TABLE_NAME),
        LogicalPlan::scan(DEPARTMENTS_TABLE_NAME),
        Expr::eq(
            Expr::column(TABLE_NAME, "dept_id", 1),
            Expr::column(DEPARTMENTS_TABLE_NAME, "id", 0),
        ),
        JoinType::Inner,
    )
}

fn join_project_limit_plan() -> LogicalPlan {
    LogicalPlan::limit(
        LogicalPlan::project(
            join_plan(),
            vec![
                Expr::column(TABLE_NAME, "id", 0),
                Expr::column(DEPARTMENTS_TABLE_NAME, "name", 1),
            ],
        ),
        100,
        0,
    )
}

fn compiled_filter_plan(cache: &TableCache) -> CompiledPhysicalPlan {
    compile_cached_plan(cache, TABLE_NAME, filter_plan())
}

fn compiled_filter_project_limit_plan(cache: &TableCache) -> CompiledPhysicalPlan {
    compile_cached_plan(cache, TABLE_NAME, filter_project_limit_plan())
}

fn compiled_between_filter_plan(cache: &TableCache) -> CompiledPhysicalPlan {
    compile_cached_plan(cache, TABLE_NAME, between_filter_plan())
}

fn compiled_in_list_filter_plan(cache: &TableCache) -> CompiledPhysicalPlan {
    compile_cached_plan(cache, TABLE_NAME, in_list_filter_plan())
}

fn compiled_compound_filter_plan(cache: &TableCache) -> CompiledPhysicalPlan {
    compile_cached_plan(cache, TABLE_NAME, compound_filter_plan())
}

fn compiled_aggregate_plan(cache: &TableCache) -> CompiledPhysicalPlan {
    compile_cached_plan(cache, TABLE_NAME, aggregate_plan())
}

fn compiled_join_plan(cache: &TableCache) -> CompiledPhysicalPlan {
    compile_cached_plan(cache, TABLE_NAME, join_plan())
}

fn compiled_join_project_limit_plan(cache: &TableCache) -> CompiledPhysicalPlan {
    compile_cached_plan(cache, TABLE_NAME, join_project_limit_plan())
}

fn update_user<F>(cache: &Rc<RefCell<TableCache>>, row_id: u64, mutator: F)
where
    F: FnOnce(&mut Vec<Value>),
{
    let mut cache = cache.borrow_mut();
    let store = cache.get_table_mut(TABLE_NAME).unwrap();
    let current = store.get(row_id).unwrap();
    let mut values = current.values().to_vec();
    mutator(&mut values);
    let next = Row::new_with_version(row_id, current.version().wrapping_add(1), values);
    store.update(row_id, next).unwrap();
}

fn build_observable_with_plan(
    cache: Rc<RefCell<TableCache>>,
    compile: PlanCompiler,
) -> (Rc<RefCell<ReQueryObservable>>, QueryRegistry) {
    let compiled_plan = {
        let cache_ref = cache.borrow();
        compile(&cache_ref)
    };

    let initial_output = {
        let cache_ref = cache.borrow();
        execute_compiled_physical_plan_with_summary(&cache_ref, &compiled_plan).unwrap()
    };

    let observable = Rc::new(RefCell::new(ReQueryObservable::new_with_summary(
        compiled_plan,
        cache.clone(),
        initial_output.rows,
        initial_output.summary,
    )));
    observable.borrow_mut().subscribe(|rows| {
        black_box(rows.len());
    });

    let mut registry = QueryRegistry::new();
    registry.register(observable.clone(), TABLE_ID);

    (observable, registry)
}

fn build_plan_cache_entry(cache: &TableCache, plan: LogicalPlan) -> (PlanCache, u64) {
    let fingerprint = compute_plan_fingerprint(&plan);
    let mut plan_cache = PlanCache::new(8);
    let _ = plan_cache.get_or_insert_with(fingerprint, || compile_plan(cache, TABLE_NAME, plan));
    (plan_cache, fingerprint)
}

fn build_compiled_plan_cache_entry(cache: &TableCache, plan: LogicalPlan) -> (PlanCache, u64) {
    let fingerprint = compute_plan_fingerprint(&plan);
    let mut plan_cache = PlanCache::new(8);
    let _ = plan_cache
        .get_or_insert_compiled_with(fingerprint, || compile_cached_plan(cache, TABLE_NAME, plan));
    (plan_cache, fingerprint)
}

struct ReQueryBenchState {
    cache: Rc<RefCell<TableCache>>,
    observable: Rc<RefCell<ReQueryObservable>>,
    registry: QueryRegistry,
    changed_ids: HashSet<u64>,
    hit_in_result: bool,
    miss_salary_flip: bool,
}

impl ReQueryBenchState {
    fn new_with_plan(size: usize, compile: PlanCompiler) -> Self {
        let cache = Rc::new(RefCell::new(build_cache(size)));
        let (observable, registry) = build_observable_with_plan(cache.clone(), compile);
        let mut changed_ids = HashSet::new();
        changed_ids.insert(HIT_ROW_ID);
        Self {
            cache,
            observable,
            registry,
            changed_ids,
            hit_in_result: false,
            miss_salary_flip: false,
        }
    }

    fn new(size: usize) -> Self {
        Self::new_with_plan(size, compiled_filter_plan)
    }

    fn prepare_hit_change(&mut self) {
        let next_age = if self.hit_in_result { 29 } else { 31 };
        self.hit_in_result = !self.hit_in_result;
        self.changed_ids.clear();
        self.changed_ids.insert(HIT_ROW_ID);
        update_user(&self.cache, HIT_ROW_ID, |values| {
            values[2] = Value::Int32(next_age);
        });
    }

    fn prepare_miss_change(&mut self) {
        let next_salary = if self.miss_salary_flip {
            50_000
        } else {
            51_000
        };
        self.miss_salary_flip = !self.miss_salary_flip;
        self.changed_ids.clear();
        self.changed_ids.insert(MISS_ROW_ID);
        update_user(&self.cache, MISS_ROW_ID, |values| {
            values[4] = Value::Int64(next_salary);
        });
    }

    fn requery(&mut self) -> usize {
        self.registry.on_table_change(TABLE_ID, &self.changed_ids);
        self.observable.borrow().len()
    }
}

fn bench_single_query(c: &mut Criterion) {
    let mut group = c.benchmark_group("single_query_filter_execute");

    for size in [10_000usize, 100_000usize] {
        let cache = build_cache(size);
        let compiled_plan = compiled_filter_plan(&cache);

        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, _| {
            b.iter(|| {
                let rows = execute_compiled_physical_plan(&cache, &compiled_plan).unwrap();
                black_box(rows.len())
            })
        });
    }

    group.finish();
}

fn bench_single_query_pipeline(c: &mut Criterion) {
    let mut group = c.benchmark_group("single_query_filter_project_limit_execute");

    for size in [10_000usize, 100_000usize] {
        let cache = build_cache(size);
        let compiled_plan = compiled_filter_project_limit_plan(&cache);

        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, _| {
            b.iter(|| {
                let rows = execute_compiled_physical_plan(&cache, &compiled_plan).unwrap();
                black_box(rows.len())
            })
        });
    }

    group.finish();
}

fn bench_single_query_compound_filter(c: &mut Criterion) {
    let mut group = c.benchmark_group("single_query_compound_filter_execute");

    for size in [10_000usize, 100_000usize] {
        let cache = build_cache(size);
        let compiled_plan = compiled_compound_filter_plan(&cache);

        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, _| {
            b.iter(|| {
                let rows = execute_compiled_physical_plan(&cache, &compiled_plan).unwrap();
                black_box(rows.len())
            })
        });
    }

    group.finish();
}

fn bench_single_query_typed_kernels(c: &mut Criterion) {
    let mut group = c.benchmark_group("single_query_typed_kernel_execute");

    for size in [10_000usize, 100_000usize] {
        let cache = build_cache(size);
        let compiled_between_plan = compiled_between_filter_plan(&cache);
        let compiled_in_list_plan = compiled_in_list_filter_plan(&cache);

        group.bench_with_input(BenchmarkId::new("between", size), &size, |b, _| {
            b.iter(|| {
                let rows = execute_compiled_physical_plan(&cache, &compiled_between_plan).unwrap();
                black_box(rows.len())
            })
        });

        group.bench_with_input(BenchmarkId::new("in_list", size), &size, |b, _| {
            b.iter(|| {
                let rows = execute_compiled_physical_plan(&cache, &compiled_in_list_plan).unwrap();
                black_box(rows.len())
            })
        });
    }

    group.finish();
}

fn bench_group_by_aggregate(c: &mut Criterion) {
    let mut group = c.benchmark_group("group_by_aggregate_execute");

    for size in [10_000usize, 100_000usize] {
        let cache = build_cache(size);
        let compiled_plan = compiled_aggregate_plan(&cache);

        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, _| {
            b.iter(|| {
                let rows = execute_compiled_physical_plan(&cache, &compiled_plan).unwrap();
                black_box(rows.len())
            })
        });
    }

    group.finish();
}

fn bench_hash_join(c: &mut Criterion) {
    let mut group = c.benchmark_group("hash_join_execute");

    for size in [10_000usize, 100_000usize] {
        let cache = build_join_cache(size);
        let compiled_plan = compiled_join_plan(&cache);
        let physical_plan = compile_plan(&cache, TABLE_NAME, join_plan());

        group.bench_with_input(BenchmarkId::new("physical_only", size), &size, |b, _| {
            b.iter(|| {
                let rows = execute_physical_plan(&cache, &physical_plan).unwrap();
                black_box(rows.len())
            })
        });

        group.bench_with_input(
            BenchmarkId::new("compiled_artifact", size),
            &size,
            |b, _| {
                b.iter(|| {
                    let rows = execute_compiled_physical_plan(&cache, &compiled_plan).unwrap();
                    black_box(rows.len())
                })
            },
        );
    }

    group.finish();
}

fn bench_hash_join_project_limit(c: &mut Criterion) {
    let mut group = c.benchmark_group("hash_join_project_limit_execute");

    for size in [10_000usize, 100_000usize] {
        let cache = build_join_cache(size);
        let compiled_plan = compiled_join_project_limit_plan(&cache);
        let physical_plan = compile_plan(&cache, TABLE_NAME, join_project_limit_plan());

        group.bench_with_input(BenchmarkId::new("physical_only", size), &size, |b, _| {
            b.iter(|| {
                let rows = execute_physical_plan(&cache, &physical_plan).unwrap();
                black_box(rows.len())
            })
        });

        group.bench_with_input(
            BenchmarkId::new("compiled_artifact", size),
            &size,
            |b, _| {
                b.iter(|| {
                    let rows = execute_compiled_physical_plan(&cache, &compiled_plan).unwrap();
                    black_box(rows.len())
                })
            },
        );
    }

    group.finish();
}

fn bench_index_cursor_scan(c: &mut Criterion) {
    let mut group = c.benchmark_group("index_cursor_scan_execute");

    for size in [10_000usize, 100_000usize] {
        let cache = build_indexed_cache(size);
        let physical_plan = PhysicalPlan::index_scan_with_limit(
            TABLE_NAME,
            "idx_salary",
            Some(Value::Int64(70_000)),
            Some(Value::Int64(90_000)),
            Some(128),
            Some(0),
        );
        let compiled_plan = CompiledPhysicalPlan::new_with_data_source(
            physical_plan,
            &TableCacheDataSource::new(&cache),
        );

        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, _| {
            b.iter(|| {
                let rows = execute_compiled_physical_plan(&cache, &compiled_plan).unwrap();
                black_box(rows.len())
            })
        });
    }

    group.finish();
}

fn bench_plan_cache_hit_compound_filter(c: &mut Criterion) {
    let mut group = c.benchmark_group("plan_cache_hit_compound_filter_execute");

    for size in [10_000usize, 100_000usize] {
        let cache = build_cache(size);

        let (mut physical_only_cache, physical_only_fingerprint) =
            build_plan_cache_entry(&cache, compound_filter_plan());
        group.bench_with_input(BenchmarkId::new("physical_only", size), &size, |b, _| {
            b.iter(|| {
                let compiled_plan = physical_only_cache.get(physical_only_fingerprint).unwrap();
                let rows = execute_physical_plan(&cache, compiled_plan.physical_plan()).unwrap();
                black_box(rows.len())
            })
        });

        let (mut compiled_cache, compiled_fingerprint) =
            build_compiled_plan_cache_entry(&cache, compound_filter_plan());
        group.bench_with_input(
            BenchmarkId::new("compiled_artifact", size),
            &size,
            |b, _| {
                b.iter(|| {
                    let compiled_plan = compiled_cache.get(compiled_fingerprint).unwrap();
                    let rows = execute_compiled_physical_plan(&cache, compiled_plan).unwrap();
                    black_box(rows.len())
                })
            },
        );
    }

    group.finish();
}

fn bench_requery_create(c: &mut Criterion) {
    let mut group = c.benchmark_group("requery_observe_create");

    for size in [10_000usize, 100_000usize] {
        let cache = Rc::new(RefCell::new(build_cache(size)));
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, _| {
            b.iter(|| {
                let (observable, _registry) =
                    build_observable_with_plan(cache.clone(), compiled_filter_plan);
                let len = observable.borrow().len();
                black_box(len)
            })
        });
    }

    group.finish();
}

fn bench_requery_pipeline_create(c: &mut Criterion) {
    let mut group = c.benchmark_group("requery_observe_create_filter_project_limit");

    for size in [10_000usize, 100_000usize] {
        let cache = Rc::new(RefCell::new(build_cache(size)));
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, _| {
            b.iter(|| {
                let (observable, _registry) =
                    build_observable_with_plan(cache.clone(), compiled_filter_project_limit_plan);
                let len = observable.borrow().len();
                black_box(len)
            })
        });
    }

    group.finish();
}

fn bench_requery_updates(c: &mut Criterion) {
    let mut group = c.benchmark_group("requery_on_change");

    for size in [10_000usize, 100_000usize] {
        let hit_state = Rc::new(RefCell::new(ReQueryBenchState::new(size)));
        group.bench_with_input(BenchmarkId::new("result_changes", size), &size, |b, _| {
            let hit_state = hit_state.clone();
            b.iter_batched(
                || {
                    hit_state.borrow_mut().prepare_hit_change();
                },
                |_| {
                    let len = hit_state.borrow_mut().requery();
                    black_box(len)
                },
                criterion::BatchSize::SmallInput,
            )
        });

        let miss_state = Rc::new(RefCell::new(ReQueryBenchState::new(size)));
        group.bench_with_input(BenchmarkId::new("result_unchanged", size), &size, |b, _| {
            let miss_state = miss_state.clone();
            b.iter_batched(
                || {
                    miss_state.borrow_mut().prepare_miss_change();
                },
                |_| {
                    let len = miss_state.borrow_mut().requery();
                    black_box(len)
                },
                criterion::BatchSize::SmallInput,
            )
        });
    }

    group.finish();
}

fn bench_requery_pipeline_updates(c: &mut Criterion) {
    let mut group = c.benchmark_group("requery_on_change_filter_project_limit");

    for size in [10_000usize, 100_000usize] {
        let hit_state = Rc::new(RefCell::new(ReQueryBenchState::new_with_plan(
            size,
            compiled_filter_project_limit_plan,
        )));
        group.bench_with_input(BenchmarkId::new("result_changes", size), &size, |b, _| {
            let hit_state = hit_state.clone();
            b.iter_batched(
                || {
                    hit_state.borrow_mut().prepare_hit_change();
                },
                |_| {
                    let len = hit_state.borrow_mut().requery();
                    black_box(len)
                },
                criterion::BatchSize::SmallInput,
            )
        });
    }

    group.finish();
}

fn bench_requery_compound_updates(c: &mut Criterion) {
    let mut group = c.benchmark_group("requery_on_change_compound_filter");

    for size in [10_000usize, 100_000usize] {
        let hit_state = Rc::new(RefCell::new(ReQueryBenchState::new_with_plan(
            size,
            compiled_compound_filter_plan,
        )));
        group.bench_with_input(BenchmarkId::new("result_changes", size), &size, |b, _| {
            let hit_state = hit_state.clone();
            b.iter_batched(
                || {
                    hit_state.borrow_mut().prepare_hit_change();
                },
                |_| {
                    let len = hit_state.borrow_mut().requery();
                    black_box(len)
                },
                criterion::BatchSize::SmallInput,
            )
        });

        let miss_state = Rc::new(RefCell::new(ReQueryBenchState::new_with_plan(
            size,
            compiled_compound_filter_plan,
        )));
        group.bench_with_input(BenchmarkId::new("result_unchanged", size), &size, |b, _| {
            let miss_state = miss_state.clone();
            b.iter_batched(
                || {
                    miss_state.borrow_mut().prepare_miss_change();
                },
                |_| {
                    let len = miss_state.borrow_mut().requery();
                    black_box(len)
                },
                criterion::BatchSize::SmallInput,
            )
        });
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_single_query,
    bench_single_query_pipeline,
    bench_single_query_typed_kernels,
    bench_single_query_compound_filter,
    bench_group_by_aggregate,
    bench_hash_join,
    bench_hash_join_project_limit,
    bench_index_cursor_scan,
    bench_plan_cache_hit_compound_filter,
    bench_requery_create,
    bench_requery_pipeline_create,
    bench_requery_updates,
    bench_requery_pipeline_updates,
    bench_requery_compound_updates
);
criterion_main!(benches);
