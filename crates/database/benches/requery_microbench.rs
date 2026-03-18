use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use cynos_core::schema::{Table, TableBuilder};
use cynos_core::{DataType, Row, Value};
use cynos_database::query_engine::{compile_plan, execute_physical_plan};
use cynos_database::reactive_bridge::{QueryRegistry, ReQueryObservable};
use cynos_incremental::TableId;
use cynos_query::ast::Expr;
use cynos_query::planner::{LogicalPlan, PhysicalPlan};
use cynos_storage::TableCache;
use hashbrown::HashSet;
use std::cell::RefCell;
use std::rc::Rc;

const TABLE_NAME: &str = "users";
const TABLE_ID: TableId = 1;
const HIT_ROW_ID: u64 = 9;
const MISS_ROW_ID: u64 = 8;

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

fn build_cache(size: usize) -> TableCache {
    let mut cache = TableCache::new();
    cache.create_table(test_schema()).unwrap();

    let departments = ["Engineering", "Sales", "Marketing", "HR", "Finance"];
    let store = cache.get_table_mut(TABLE_NAME).unwrap();
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

fn compiled_filter_plan(cache: &TableCache) -> PhysicalPlan {
    compile_plan(cache, TABLE_NAME, filter_plan())
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

fn build_observable(
    cache: Rc<RefCell<TableCache>>,
) -> (Rc<RefCell<ReQueryObservable>>, QueryRegistry) {
    let physical_plan = {
        let cache_ref = cache.borrow();
        compiled_filter_plan(&cache_ref)
    };

    let initial_rows = {
        let cache_ref = cache.borrow();
        execute_physical_plan(&cache_ref, &physical_plan).unwrap()
    };

    let observable = Rc::new(RefCell::new(ReQueryObservable::new(
        physical_plan,
        cache.clone(),
        initial_rows,
    )));
    observable.borrow_mut().subscribe(|rows| {
        black_box(rows.len());
    });

    let mut registry = QueryRegistry::new();
    registry.register(observable.clone(), TABLE_ID);

    (observable, registry)
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
    fn new(size: usize) -> Self {
        let cache = Rc::new(RefCell::new(build_cache(size)));
        let (observable, registry) = build_observable(cache.clone());
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
        let physical_plan = compiled_filter_plan(&cache);

        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, _| {
            b.iter(|| {
                let rows = execute_physical_plan(&cache, &physical_plan).unwrap();
                black_box(rows.len())
            })
        });
    }

    group.finish();
}

fn bench_requery_create(c: &mut Criterion) {
    let mut group = c.benchmark_group("requery_observe_create");

    for size in [10_000usize, 100_000usize] {
        let cache = Rc::new(RefCell::new(build_cache(size)));
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, _| {
            b.iter(|| {
                let (observable, _registry) = build_observable(cache.clone());
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

criterion_group!(
    benches,
    bench_single_query,
    bench_requery_create,
    bench_requery_updates
);
criterion_main!(benches);
