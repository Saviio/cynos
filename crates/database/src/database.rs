//! Database - Main entry point for Cynos database operations.
//!
//! This module provides the `Database` struct which is the primary interface
//! for creating tables, executing queries, and managing data.

use crate::binary_protocol::SchemaLayoutCache;
use crate::convert::{gql_response_to_js, js_to_gql_variables};
use crate::query_builder::{DeleteBuilder, InsertBuilder, SelectBuilder, UpdateBuilder};
use crate::reactive_bridge::{JsGraphqlSubscription, QueryRegistry, ReQueryObservable};
use crate::table::{JsTable, JsTableBuilder};
use crate::transaction::JsTransaction;
use alloc::rc::Rc;
use alloc::string::{String, ToString};
#[cfg(feature = "benchmark")]
use alloc::vec::Vec;
use core::cell::RefCell;
use cynos_core::Row;
use cynos_incremental::Delta;
use cynos_gql::{PreparedQuery as GqlPreparedQuery, SchemaCache as GraphqlSchemaCache};
use cynos_query::plan_cache::PlanCache;
use cynos_reactive::TableId;
use cynos_storage::TableCache;
use wasm_bindgen::prelude::*;

/// The main database interface.
///
/// Provides methods for:
/// - Creating and dropping tables
/// - CRUD operations (insert, select, update, delete)
/// - Transaction management
/// - Observable queries
#[wasm_bindgen]
pub struct Database {
    name: String,
    cache: Rc<RefCell<TableCache>>,
    query_registry: Rc<RefCell<QueryRegistry>>,
    table_id_map: Rc<RefCell<hashbrown::HashMap<String, TableId>>>,
    next_table_id: Rc<RefCell<TableId>>,
    schema_layout_cache: Rc<RefCell<SchemaLayoutCache>>,
    plan_cache: Rc<RefCell<PlanCache>>,
    graphql_schema_cache: Rc<RefCell<GraphqlSchemaCache>>,
    schema_epoch: Rc<RefCell<u64>>,
}

/// A prepared GraphQL query that reuses the parsed document across executions.
#[wasm_bindgen]
pub struct PreparedGraphqlQuery {
    cache: Rc<RefCell<TableCache>>,
    query_registry: Rc<RefCell<QueryRegistry>>,
    table_id_map: Rc<RefCell<hashbrown::HashMap<String, TableId>>>,
    graphql_schema_cache: Rc<RefCell<GraphqlSchemaCache>>,
    schema_epoch: Rc<RefCell<u64>>,
    prepared: GqlPreparedQuery,
}

#[wasm_bindgen]
impl Database {
    /// Creates a new database instance.
    #[wasm_bindgen(constructor)]
    pub fn new(name: &str) -> Self {
        let query_registry = Rc::new(RefCell::new(QueryRegistry::new()));
        // Set self reference for microtask scheduling
        query_registry
            .borrow_mut()
            .set_self_ref(query_registry.clone());

        Self {
            name: name.to_string(),
            cache: Rc::new(RefCell::new(TableCache::new())),
            query_registry,
            table_id_map: Rc::new(RefCell::new(hashbrown::HashMap::new())),
            next_table_id: Rc::new(RefCell::new(1)),
            schema_layout_cache: Rc::new(RefCell::new(SchemaLayoutCache::new())),
            plan_cache: Rc::new(RefCell::new(PlanCache::default_size())),
            graphql_schema_cache: Rc::new(RefCell::new(GraphqlSchemaCache::new())),
            schema_epoch: Rc::new(RefCell::new(0)),
        }
    }

    /// Async factory method for creating a database (for WASM compatibility).
    #[wasm_bindgen(js_name = create)]
    pub async fn create(name: &str) -> Result<Database, JsValue> {
        Ok(Self::new(name))
    }

    /// Returns the database name.
    #[wasm_bindgen(getter)]
    pub fn name(&self) -> String {
        self.name.clone()
    }

    /// Creates a new table builder.
    #[wasm_bindgen(js_name = createTable)]
    pub fn create_table(&self, name: &str) -> JsTableBuilder {
        JsTableBuilder::new(name)
    }

    /// Registers a table schema with the database.
    #[wasm_bindgen(js_name = registerTable)]
    pub fn register_table(&self, builder: &JsTableBuilder) -> Result<(), JsValue> {
        let schema = builder.build_internal()?;
        let table_name = schema.name().to_string();

        self.cache
            .borrow_mut()
            .create_table(schema)
            .map_err(|e| JsValue::from_str(&alloc::format!("{:?}", e)))?;

        // Assign table ID
        let table_id = *self.next_table_id.borrow();
        *self.next_table_id.borrow_mut() += 1;
        self.table_id_map.borrow_mut().insert(table_name, table_id);
        *self.schema_epoch.borrow_mut() += 1;
        self.graphql_schema_cache.borrow_mut().clear();

        Ok(())
    }

    /// Gets a table reference by name.
    pub fn table(&self, name: &str) -> Option<JsTable> {
        self.cache
            .borrow()
            .get_table(name)
            .map(|store| JsTable::new(store.schema().clone()))
    }

    /// Drops a table from the database.
    #[wasm_bindgen(js_name = dropTable)]
    pub fn drop_table(&self, name: &str) -> Result<(), JsValue> {
        self.cache
            .borrow_mut()
            .drop_table(name)
            .map_err(|e| JsValue::from_str(&alloc::format!("{:?}", e)))?;

        self.table_id_map.borrow_mut().remove(name);
        *self.schema_epoch.borrow_mut() += 1;
        self.graphql_schema_cache.borrow_mut().clear();
        Ok(())
    }

    /// Returns all table names.
    #[wasm_bindgen(js_name = tableNames)]
    pub fn table_names(&self) -> js_sys::Array {
        let arr = js_sys::Array::new();
        for name in self.cache.borrow().table_names() {
            arr.push(&JsValue::from_str(name));
        }
        arr
    }

    /// Returns the number of tables.
    #[wasm_bindgen(js_name = tableCount)]
    pub fn table_count(&self) -> usize {
        self.cache.borrow().table_count()
    }

    /// Starts a SELECT query.
    /// Accepts either:
    /// - A single string: select('*') or select('name')
    /// - Multiple strings: select('name', 'score') - passed as variadic args
    #[wasm_bindgen(variadic)]
    pub fn select(&self, columns: &JsValue) -> SelectBuilder {
        SelectBuilder::new(
            self.cache.clone(),
            self.query_registry.clone(),
            self.table_id_map.clone(),
            self.schema_layout_cache.clone(),
            self.plan_cache.clone(),
            columns.clone(),
        )
    }

    /// Starts an INSERT operation.
    pub fn insert(&self, table: &str) -> InsertBuilder {
        InsertBuilder::new(
            self.cache.clone(),
            self.query_registry.clone(),
            self.table_id_map.clone(),
            table,
        )
    }

    /// Starts an UPDATE operation.
    pub fn update(&self, table: &str) -> UpdateBuilder {
        UpdateBuilder::new(
            self.cache.clone(),
            self.query_registry.clone(),
            self.table_id_map.clone(),
            table,
        )
    }

    /// Starts a DELETE operation.
    pub fn delete(&self, table: &str) -> DeleteBuilder {
        DeleteBuilder::new(
            self.cache.clone(),
            self.query_registry.clone(),
            self.table_id_map.clone(),
            table,
        )
    }

    /// Begins a new transaction.
    pub fn transaction(&self) -> JsTransaction {
        JsTransaction::new(
            self.cache.clone(),
            self.query_registry.clone(),
            self.table_id_map.clone(),
        )
    }

    /// Clears all data from all tables.
    pub fn clear(&self) {
        self.cache.borrow_mut().clear();
    }

    /// Clears data from a specific table.
    #[wasm_bindgen(js_name = clearTable)]
    pub fn clear_table(&self, name: &str) -> Result<(), JsValue> {
        self.cache
            .borrow_mut()
            .clear_table(name)
            .map_err(|e| JsValue::from_str(&alloc::format!("{:?}", e)))
    }

    /// Returns the total row count across all tables.
    #[wasm_bindgen(js_name = totalRowCount)]
    pub fn total_row_count(&self) -> usize {
        self.cache.borrow().total_row_count()
    }

    /// Checks if a table exists.
    #[wasm_bindgen(js_name = hasTable)]
    pub fn has_table(&self, name: &str) -> bool {
        self.cache.borrow().has_table(name)
    }

    /// Renders the current GraphQL schema as SDL.
    #[wasm_bindgen(js_name = graphqlSchema)]
    pub fn graphql_schema(&self) -> String {
        let cache = self.cache.borrow();
        let epoch = *self.schema_epoch.borrow();
        self.graphql_schema_cache.borrow_mut().sdl(epoch, &cache)
    }

    /// Executes a GraphQL query against the current database snapshot.
    ///
    /// Returns a standard GraphQL payload object with a single `data` property.
    #[wasm_bindgen(js_name = graphql)]
    pub fn graphql(
        &self,
        query: &str,
        variables: Option<JsValue>,
        operation_name: Option<String>,
    ) -> Result<JsValue, JsValue> {
        let variables = js_to_gql_variables(variables.as_ref())?;
        let prepared = GqlPreparedQuery::parse_with_operation(query, operation_name.as_deref())
            .map_err(|error| JsValue::from_str(error.message()))?;

        let cache = self.cache.borrow();
        let (catalog, bound) = bind_graphql_operation(
            &prepared,
            &cache,
            &self.graphql_schema_cache,
            &self.schema_epoch,
            &variables,
        )?;
        drop(cache);

        execute_graphql_bound_operation(
            self.cache.clone(),
            self.query_registry.clone(),
            self.table_id_map.clone(),
            catalog,
            bound,
        )
    }

    /// Creates a live GraphQL subscription backed by the root query planner path.
    #[wasm_bindgen(js_name = subscribeGraphql)]
    pub fn subscribe_graphql(
        &self,
        query: &str,
        variables: Option<JsValue>,
        operation_name: Option<String>,
    ) -> Result<JsGraphqlSubscription, JsValue> {
        let variables = js_to_gql_variables(variables.as_ref())?;
        let prepared = GqlPreparedQuery::parse_with_operation(query, operation_name.as_deref())
            .map_err(|error| JsValue::from_str(error.message()))?;

        let cache = self.cache.borrow();
        let (catalog, bound) = bind_graphql_operation(
            &prepared,
            &cache,
            &self.graphql_schema_cache,
            &self.schema_epoch,
            &variables,
        )?;
        drop(cache);

        create_graphql_subscription(
            self.cache.clone(),
            self.query_registry.clone(),
            self.table_id_map.clone(),
            catalog,
            bound,
        )
    }

    /// Parses and prepares a GraphQL query for repeated execution.
    #[wasm_bindgen(js_name = prepareGraphql)]
    pub fn prepare_graphql(
        &self,
        query: &str,
        operation_name: Option<String>,
    ) -> Result<PreparedGraphqlQuery, JsValue> {
        let prepared = GqlPreparedQuery::parse_with_operation(query, operation_name.as_deref())
            .map_err(|error| JsValue::from_str(error.message()))?;
        Ok(PreparedGraphqlQuery {
            cache: self.cache.clone(),
            query_registry: self.query_registry.clone(),
            table_id_map: self.table_id_map.clone(),
            graphql_schema_cache: self.graphql_schema_cache.clone(),
            schema_epoch: self.schema_epoch.clone(),
            prepared,
        })
    }

    /// Benchmarks pure Rust insert performance without JS serialization overhead.
    ///
    /// This method generates and inserts `count` rows directly in Rust,
    /// measuring only the storage layer performance.
    ///
    /// Returns an object with:
    /// - `duration_ms`: Total time in milliseconds
    /// - `rows_per_sec`: Throughput in rows per second
    #[cfg(feature = "benchmark")]
    #[wasm_bindgen(js_name = benchmarkInsert)]
    pub fn benchmark_insert(&self, table: &str, count: u32) -> Result<JsValue, JsValue> {
        use cynos_core::Value;

        let mut cache = self.cache.borrow_mut();
        let store = cache
            .get_table_mut(table)
            .ok_or_else(|| JsValue::from_str(&alloc::format!("Table not found: {}", table)))?;

        let schema = store.schema().clone();
        let columns = schema.columns();

        // Generate rows in Rust (no JS serialization)
        let start = js_sys::Date::now();

        for i in 0..count {
            let row_id = cynos_core::next_row_id();
            let mut values = Vec::with_capacity(columns.len());

            for (col_idx, col) in columns.iter().enumerate() {
                let value = match col.data_type() {
                    cynos_core::DataType::Int64 => {
                        if col_idx == 0 {
                            // Primary key - use sequential ID
                            Value::Int64(i as i64 + 1)
                        } else {
                            Value::Int64((i % 1000) as i64)
                        }
                    }
                    cynos_core::DataType::Int32 => Value::Int32((i % 100) as i32),
                    cynos_core::DataType::String => Value::String(alloc::format!("value_{}", i)),
                    cynos_core::DataType::Boolean => Value::Boolean(i % 2 == 0),
                    cynos_core::DataType::Float64 => Value::Float64(i as f64 * 0.1),
                    cynos_core::DataType::DateTime => {
                        Value::DateTime(1700000000000 + i as i64 * 1000)
                    }
                    _ => Value::Null,
                };
                values.push(value);
            }

            let row = Row::new(row_id, values);
            store
                .insert(row)
                .map_err(|e| JsValue::from_str(&alloc::format!("{:?}", e)))?;
        }

        let end = js_sys::Date::now();
        let duration_ms = end - start;
        let rows_per_sec = if duration_ms > 0.0 {
            (count as f64 / duration_ms) * 1000.0
        } else {
            f64::INFINITY
        };

        // Return result object
        let result = js_sys::Object::new();
        js_sys::Reflect::set(
            &result,
            &JsValue::from_str("duration_ms"),
            &JsValue::from_f64(duration_ms),
        )?;
        js_sys::Reflect::set(
            &result,
            &JsValue::from_str("rows_per_sec"),
            &JsValue::from_f64(rows_per_sec),
        )?;
        js_sys::Reflect::set(
            &result,
            &JsValue::from_str("count"),
            &JsValue::from_f64(count as f64),
        )?;

        Ok(result.into())
    }

    /// Benchmarks pure Rust range query performance without JS serialization overhead.
    ///
    /// This method executes a range query (column > threshold) directly in Rust,
    /// measuring only the query execution time without serialization to JS.
    ///
    /// Parameters:
    /// - `table`: Table name to query
    /// - `column`: Column name for the range condition
    /// - `threshold`: The threshold value (column > threshold)
    ///
    /// Returns an object with:
    /// - `query_ms`: Time for query execution only (no serialization)
    /// - `serialize_ms`: Time for serialization to JS
    /// - `total_ms`: Total time including serialization
    /// - `row_count`: Number of rows returned
    /// - `serialization_overhead_pct`: Percentage of time spent on serialization
    #[cfg(feature = "benchmark")]
    #[wasm_bindgen(js_name = benchmarkRangeQuery)]
    pub fn benchmark_range_query(
        &self,
        table: &str,
        column: &str,
        threshold: f64,
    ) -> Result<JsValue, JsValue> {
        use crate::query_engine::execute_plan;
        use cynos_query::ast::{BinaryOp, Expr as AstExpr};
        use cynos_query::planner::LogicalPlan;

        let cache = self.cache.borrow();
        let store = cache
            .get_table(table)
            .ok_or_else(|| JsValue::from_str(&alloc::format!("Table not found: {}", table)))?;

        let schema = store.schema().clone();
        let col = schema
            .get_column(column)
            .ok_or_else(|| JsValue::from_str(&alloc::format!("Column not found: {}", column)))?;
        let col_idx = col.index();

        // Build logical plan: SELECT * FROM table WHERE column > threshold
        let scan = LogicalPlan::Scan {
            table: table.to_string(),
        };

        let predicate = AstExpr::BinaryOp {
            left: Box::new(AstExpr::column(table, column, col_idx)),
            op: BinaryOp::Gt,
            right: Box::new(AstExpr::Literal(cynos_core::Value::Int64(threshold as i64))),
        };

        let plan = LogicalPlan::Filter {
            input: Box::new(scan),
            predicate,
        };

        // Measure query execution time (no serialization)
        let query_start = js_sys::Date::now();
        let rows = execute_plan(&cache, table, plan)
            .map_err(|e| JsValue::from_str(&alloc::format!("Query error: {:?}", e)))?;
        let query_end = js_sys::Date::now();
        let query_ms = query_end - query_start;

        let row_count = rows.len();

        // Measure serialization time
        let serialize_start = js_sys::Date::now();
        let _js_result = crate::convert::rows_to_js_array(&rows, &schema);
        let serialize_end = js_sys::Date::now();
        let serialize_ms = serialize_end - serialize_start;

        let total_ms = query_ms + serialize_ms;
        let serialization_overhead_pct = if total_ms > 0.0 {
            (serialize_ms / total_ms) * 100.0
        } else {
            0.0
        };

        // Return result object
        let result = js_sys::Object::new();
        js_sys::Reflect::set(
            &result,
            &JsValue::from_str("query_ms"),
            &JsValue::from_f64(query_ms),
        )?;
        js_sys::Reflect::set(
            &result,
            &JsValue::from_str("serialize_ms"),
            &JsValue::from_f64(serialize_ms),
        )?;
        js_sys::Reflect::set(
            &result,
            &JsValue::from_str("total_ms"),
            &JsValue::from_f64(total_ms),
        )?;
        js_sys::Reflect::set(
            &result,
            &JsValue::from_str("row_count"),
            &JsValue::from_f64(row_count as f64),
        )?;
        js_sys::Reflect::set(
            &result,
            &JsValue::from_str("serialization_overhead_pct"),
            &JsValue::from_f64(serialization_overhead_pct),
        )?;

        Ok(result.into())
    }
}

#[wasm_bindgen]
impl PreparedGraphqlQuery {
    /// Executes the prepared GraphQL query with an optional variables object.
    pub fn exec(&self, variables: Option<JsValue>) -> Result<JsValue, JsValue> {
        let variables = js_to_gql_variables(variables.as_ref())?;
        let cache = self.cache.borrow();
        let (catalog, bound) = bind_graphql_operation(
            &self.prepared,
            &cache,
            &self.graphql_schema_cache,
            &self.schema_epoch,
            &variables,
        )?;
        drop(cache);

        execute_graphql_bound_operation(
            self.cache.clone(),
            self.query_registry.clone(),
            self.table_id_map.clone(),
            catalog,
            bound,
        )
    }

    /// Creates a live subscription from a prepared GraphQL subscription document.
    pub fn subscribe(&self, variables: Option<JsValue>) -> Result<JsGraphqlSubscription, JsValue> {
        let variables = js_to_gql_variables(variables.as_ref())?;
        let cache = self.cache.borrow();
        let (catalog, bound) = bind_graphql_operation(
            &self.prepared,
            &cache,
            &self.graphql_schema_cache,
            &self.schema_epoch,
            &variables,
        )?;
        drop(cache);

        create_graphql_subscription(
            self.cache.clone(),
            self.query_registry.clone(),
            self.table_id_map.clone(),
            catalog,
            bound,
        )
    }
}

fn bind_graphql_operation(
    prepared: &GqlPreparedQuery,
    cache: &TableCache,
    graphql_schema_cache: &Rc<RefCell<GraphqlSchemaCache>>,
    schema_epoch: &Rc<RefCell<u64>>,
    variables: &cynos_gql::VariableValues,
) -> Result<(cynos_gql::GraphqlCatalog, cynos_gql::BoundOperation), JsValue> {
    let epoch = *schema_epoch.borrow();
    let catalog = graphql_schema_cache.borrow_mut().catalog(epoch, cache);
    let bound = prepared
        .bind(&catalog, Some(variables))
        .map_err(|error| JsValue::from_str(error.message()))?;
    Ok((catalog, bound))
}

fn execute_graphql_bound_operation(
    cache: Rc<RefCell<TableCache>>,
    query_registry: Rc<RefCell<QueryRegistry>>,
    table_id_map: Rc<RefCell<hashbrown::HashMap<String, TableId>>>,
    catalog: cynos_gql::GraphqlCatalog,
    bound: cynos_gql::BoundOperation,
) -> Result<JsValue, JsValue> {
    if bound.kind == cynos_gql::OperationType::Subscription {
        return Err(JsValue::from_str(
            "subscription operations must use subscribeGraphql() or PreparedGraphqlQuery.subscribe()",
        ));
    }

    let mut cache_ref = cache.borrow_mut();
    let outcome = cynos_gql::execute::execute_bound_operation_mut(&mut cache_ref, &catalog, &bound)
        .map_err(|error| JsValue::from_str(error.message()))?;
    drop(cache_ref);

    notify_graphql_changes(query_registry, table_id_map, &outcome.changes);
    gql_response_to_js(&outcome.response)
}

fn create_graphql_subscription(
    cache: Rc<RefCell<TableCache>>,
    query_registry: Rc<RefCell<QueryRegistry>>,
    table_id_map: Rc<RefCell<hashbrown::HashMap<String, TableId>>>,
    catalog: cynos_gql::GraphqlCatalog,
    bound: cynos_gql::BoundOperation,
) -> Result<JsGraphqlSubscription, JsValue> {
    if bound.kind != cynos_gql::OperationType::Subscription {
        return Err(JsValue::from_str(
            "subscribeGraphql() only accepts subscription operations",
        ));
    }
    if bound.fields.len() != 1 {
        return Err(JsValue::from_str(
            "GraphQL subscriptions must select exactly one root field",
        ));
    }

    let field = bound
        .fields
        .into_iter()
        .next()
        .ok_or_else(|| JsValue::from_str("subscription is missing a root field"))?;
    if matches!(field.kind, cynos_gql::bind::BoundRootFieldKind::Typename) {
        return Err(JsValue::from_str(
            "GraphQL subscriptions must select a concrete root field",
        ));
    }

    let plan = cynos_gql::build_root_field_plan(&catalog, &field)
        .map_err(|error| JsValue::from_str(error.message()))?;
    let dependent_tables = plan.logical_plan.collect_tables();

    let cache_ref = cache.clone();
    let cache_borrow = cache_ref.borrow();
    let compiled_plan =
        crate::query_engine::compile_cached_plan(&cache_borrow, &plan.table_name, plan.logical_plan);
    let initial_output =
        crate::query_engine::execute_compiled_physical_plan_with_summary(&cache_borrow, &compiled_plan)
            .map_err(|error| JsValue::from_str(&alloc::format!("Query execution error: {:?}", error)))?;
    drop(cache_borrow);

    let observable = ReQueryObservable::new_with_summary(
        compiled_plan,
        cache_ref.clone(),
        initial_output.rows,
        initial_output.summary,
    );
    let observable_rc = Rc::new(RefCell::new(observable));

    {
        let table_id_map = table_id_map.borrow();
        let mut registry = query_registry.borrow_mut();
        for table in dependent_tables {
            let table_id = table_id_map
                .get(&table)
                .copied()
                .ok_or_else(|| JsValue::from_str(&alloc::format!("Table ID not found: {}", table)))?;
            registry.register(observable_rc.clone(), table_id);
        }
    }

    Ok(JsGraphqlSubscription::new(
        observable_rc,
        cache_ref,
        catalog,
        field,
    ))
}

fn notify_graphql_changes(
    query_registry: Rc<RefCell<QueryRegistry>>,
    table_id_map: Rc<RefCell<hashbrown::HashMap<String, TableId>>>,
    changes: &[cynos_gql::TableChange],
) {
    let mut aggregated: hashbrown::HashMap<String, (Vec<Delta<Row>>, hashbrown::HashSet<u64>)> =
        hashbrown::HashMap::new();

    for change in changes {
        let entry = aggregated
            .entry(change.table_name.clone())
            .or_insert_with(|| (Vec::new(), hashbrown::HashSet::new()));

        for row_change in &change.row_changes {
            match row_change {
                cynos_gql::RowChange::Insert(row) => {
                    entry.0.push(Delta::insert(row.clone()));
                    entry.1.insert(row.id());
                }
                cynos_gql::RowChange::Update { old, new } => {
                    entry.0.push(Delta::delete(old.clone()));
                    entry.0.push(Delta::insert(new.clone()));
                    entry.1.insert(old.id());
                }
                cynos_gql::RowChange::Delete(row) => {
                    entry.0.push(Delta::delete(row.clone()));
                    entry.1.insert(row.id());
                }
            }
        }
    }

    let table_id_map = table_id_map.borrow();
    let mut registry = query_registry.borrow_mut();
    for (table_name, (deltas, changed_ids)) in aggregated {
        if let Some(table_id) = table_id_map.get(&table_name).copied() {
            registry.on_table_change_ivm(table_id, deltas, &changed_ids);
        }
    }
}

#[allow(dead_code)]
impl Database {
    /// Gets the internal cache (for internal use).
    pub(crate) fn cache(&self) -> Rc<RefCell<TableCache>> {
        self.cache.clone()
    }

    /// Gets the query registry (for internal use).
    pub(crate) fn query_registry(&self) -> Rc<RefCell<QueryRegistry>> {
        self.query_registry.clone()
    }

    /// Gets the table ID for a table name.
    pub(crate) fn get_table_id(&self, name: &str) -> Option<TableId> {
        self.table_id_map.borrow().get(name).copied()
    }

    /// Notifies the query registry of table changes.
    pub(crate) fn notify_table_change(
        &self,
        table_id: TableId,
        changed_ids: &hashbrown::HashSet<u64>,
    ) {
        self.query_registry
            .borrow_mut()
            .on_table_change(table_id, changed_ids);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::table::{ColumnOptions, ForeignKeyOptions};
    use crate::JsDataType;
    use cynos_core::{Row, Value};
    use wasm_bindgen_test::*;

    wasm_bindgen_test_configure!(run_in_browser);

    #[wasm_bindgen_test]
    fn test_database_new() {
        let db = Database::new("test");
        assert_eq!(db.name(), "test");
        assert_eq!(db.table_count(), 0);
    }

    #[wasm_bindgen_test]
    fn test_database_create_table() {
        let db = Database::new("test");

        let builder = db
            .create_table("users")
            .column(
                "id",
                JsDataType::Int64,
                Some(ColumnOptions::new().set_primary_key(true)),
            )
            .column("name", JsDataType::String, None);

        db.register_table(&builder).unwrap();

        assert!(db.has_table("users"));
        assert_eq!(db.table_count(), 1);
    }

    #[wasm_bindgen_test]
    fn test_database_drop_table() {
        let db = Database::new("test");

        let builder = db.create_table("users").column(
            "id",
            JsDataType::Int64,
            Some(ColumnOptions::new().set_primary_key(true)),
        );

        db.register_table(&builder).unwrap();
        assert!(db.has_table("users"));

        db.drop_table("users").unwrap();
        assert!(!db.has_table("users"));
    }

    #[wasm_bindgen_test]
    fn test_database_table_names() {
        let db = Database::new("test");

        let builder1 = db.create_table("users").column(
            "id",
            JsDataType::Int64,
            Some(ColumnOptions::new().set_primary_key(true)),
        );
        db.register_table(&builder1).unwrap();

        let builder2 = db.create_table("orders").column(
            "id",
            JsDataType::Int64,
            Some(ColumnOptions::new().set_primary_key(true)),
        );
        db.register_table(&builder2).unwrap();

        let names = db.table_names();
        assert_eq!(names.length(), 2);
    }

    #[wasm_bindgen_test]
    fn test_database_get_table() {
        let db = Database::new("test");

        let builder = db
            .create_table("users")
            .column(
                "id",
                JsDataType::Int64,
                Some(ColumnOptions::new().set_primary_key(true)),
            )
            .column("name", JsDataType::String, None);

        db.register_table(&builder).unwrap();

        let table = db.table("users").unwrap();
        assert_eq!(table.name(), "users");
        assert_eq!(table.column_count(), 2);
    }

    #[wasm_bindgen_test]
    fn test_database_clear() {
        let db = Database::new("test");

        let builder = db.create_table("users").column(
            "id",
            JsDataType::Int64,
            Some(ColumnOptions::new().set_primary_key(true)),
        );

        db.register_table(&builder).unwrap();

        db.clear();
        assert_eq!(db.total_row_count(), 0);
        // Tables still exist after clear
        assert!(db.has_table("users"));
    }

    #[wasm_bindgen_test]
    fn test_database_graphql_executes_queries() {
        let db = Database::new("test");

        let users = db
            .create_table("users")
            .column(
                "id",
                JsDataType::Int64,
                Some(ColumnOptions::new().set_primary_key(true)),
            )
            .column("name", JsDataType::String, None);
        db.register_table(&users).unwrap();

        let orders = db
            .create_table("orders")
            .column(
                "id",
                JsDataType::Int64,
                Some(ColumnOptions::new().set_primary_key(true)),
            )
            .column("user_id", JsDataType::Int64, None)
            .column("total", JsDataType::Float64, None)
            .foreign_key(
                "fk_orders_user",
                "user_id",
                "users",
                "id",
                Some(
                    ForeignKeyOptions::new()
                        .set_field_name("buyer")
                        .set_reverse_field_name("orders"),
                ),
            );
        db.register_table(&orders).unwrap();

        db.cache
            .borrow_mut()
            .get_table_mut("users")
            .unwrap()
            .insert(Row::new(
                1,
                alloc::vec![Value::Int64(1), Value::String("Alice".into())],
            ))
            .unwrap();
        db.cache
            .borrow_mut()
            .get_table_mut("users")
            .unwrap()
            .insert(Row::new(
                2,
                alloc::vec![Value::Int64(2), Value::String("Bob".into())],
            ))
            .unwrap();
        db.cache
            .borrow_mut()
            .get_table_mut("orders")
            .unwrap()
            .insert(Row::new(
                10,
                alloc::vec![Value::Int64(10), Value::Int64(1), Value::Float64(120.0)],
            ))
            .unwrap();
        db.cache
            .borrow_mut()
            .get_table_mut("orders")
            .unwrap()
            .insert(Row::new(
                11,
                alloc::vec![Value::Int64(11), Value::Int64(2), Value::Float64(80.0)],
            ))
            .unwrap();

        let result = db
            .graphql(
                "{ orders(orderBy: [{ field: TOTAL, direction: DESC }]) { id buyer { name } } }",
                None,
                None,
            )
            .unwrap();
        let data = js_sys::Reflect::get(&result, &JsValue::from_str("data")).unwrap();
        let orders = js_sys::Reflect::get(&data, &JsValue::from_str("orders")).unwrap();
        let orders = js_sys::Array::from(&orders);
        assert_eq!(orders.length(), 2);

        let first = orders.get(0);
        let id = js_sys::Reflect::get(&first, &JsValue::from_str("id"))
            .unwrap()
            .as_f64()
            .unwrap();
        assert_eq!(id, 10.0);
        let buyer = js_sys::Reflect::get(&first, &JsValue::from_str("buyer")).unwrap();
        let name = js_sys::Reflect::get(&buyer, &JsValue::from_str("name"))
            .unwrap()
            .as_string()
            .unwrap();
        assert_eq!(name, "Alice");
    }

    #[wasm_bindgen_test]
    fn test_database_prepared_graphql_executes_with_variables() {
        let db = Database::new("test");

        let users = db
            .create_table("users")
            .column(
                "id",
                JsDataType::Int64,
                Some(ColumnOptions::new().set_primary_key(true)),
            )
            .column("name", JsDataType::String, None);
        db.register_table(&users).unwrap();

        let orders = db
            .create_table("orders")
            .column(
                "id",
                JsDataType::Int64,
                Some(ColumnOptions::new().set_primary_key(true)),
            )
            .column("user_id", JsDataType::Int64, None)
            .column("total", JsDataType::Float64, None)
            .foreign_key(
                "fk_orders_user",
                "user_id",
                "users",
                "id",
                Some(ForeignKeyOptions::new().set_reverse_field_name("orders")),
            );
        db.register_table(&orders).unwrap();

        db.cache
            .borrow_mut()
            .get_table_mut("users")
            .unwrap()
            .insert(Row::new(
                1,
                alloc::vec![Value::Int64(1), Value::String("Alice".into())],
            ))
            .unwrap();
        db.cache
            .borrow_mut()
            .get_table_mut("orders")
            .unwrap()
            .insert(Row::new(
                10,
                alloc::vec![Value::Int64(10), Value::Int64(1), Value::Float64(25.0)],
            ))
            .unwrap();
        db.cache
            .borrow_mut()
            .get_table_mut("orders")
            .unwrap()
            .insert(Row::new(
                11,
                alloc::vec![Value::Int64(11), Value::Int64(1), Value::Float64(99.0)],
            ))
            .unwrap();

        let prepared = db
            .prepare_graphql(
                "query UserOrders($userId: Long!, $min: Float = 0) { usersByPk(pk: { id: $userId }) { __typename orders(where: { total: { gte: $min } }, orderBy: [{ field: TOTAL, direction: DESC }], limit: 1) { id } } }",
                Some("UserOrders".into()),
            )
            .unwrap();

        let variables = js_sys::Object::new();
        js_sys::Reflect::set(
            &variables,
            &JsValue::from_str("userId"),
            &JsValue::from_f64(1.0),
        )
        .unwrap();
        js_sys::Reflect::set(
            &variables,
            &JsValue::from_str("min"),
            &JsValue::from_f64(60.0),
        )
        .unwrap();

        let result = prepared.exec(Some(variables.into())).unwrap();
        let data = js_sys::Reflect::get(&result, &JsValue::from_str("data")).unwrap();
        let user = js_sys::Reflect::get(&data, &JsValue::from_str("usersByPk")).unwrap();
        let typename = js_sys::Reflect::get(&user, &JsValue::from_str("__typename"))
            .unwrap()
            .as_string()
            .unwrap();
        assert_eq!(typename, "Users");
        let orders = js_sys::Reflect::get(&user, &JsValue::from_str("orders")).unwrap();
        let orders = js_sys::Array::from(&orders);
        assert_eq!(orders.length(), 1);
        let top_order = orders.get(0);
        let id = js_sys::Reflect::get(&top_order, &JsValue::from_str("id"))
            .unwrap()
            .as_f64()
            .unwrap();
        assert_eq!(id, 11.0);
    }

    #[wasm_bindgen_test]
    fn test_database_graphql_mutation_updates_subscription_result() {
        let db = Database::new("test");

        let users = db
            .create_table("users")
            .column(
                "id",
                JsDataType::Int64,
                Some(ColumnOptions::new().set_primary_key(true)),
            )
            .column("name", JsDataType::String, None);
        db.register_table(&users).unwrap();

        let subscription = db
            .subscribe_graphql(
                "subscription { users(orderBy: [{ field: ID, direction: ASC }]) { id name } }",
                None,
                None,
            )
            .unwrap();

        let initial = subscription.get_result();
        let initial_data = js_sys::Reflect::get(&initial, &JsValue::from_str("data")).unwrap();
        let initial_users = js_sys::Array::from(
            &js_sys::Reflect::get(&initial_data, &JsValue::from_str("users")).unwrap(),
        );
        assert_eq!(initial_users.length(), 0);

        let mutation = db
            .graphql(
                "mutation { insertUsers(input: [{ id: 1, name: \"Alice\" }]) { id name } }",
                None,
                None,
            )
            .unwrap();
        let mutation_data = js_sys::Reflect::get(&mutation, &JsValue::from_str("data")).unwrap();
        let inserted = js_sys::Array::from(
            &js_sys::Reflect::get(&mutation_data, &JsValue::from_str("insertUsers")).unwrap(),
        );
        assert_eq!(inserted.length(), 1);

        db.query_registry.borrow_mut().flush();

        let updated = subscription.get_result();
        let updated_data = js_sys::Reflect::get(&updated, &JsValue::from_str("data")).unwrap();
        let updated_users = js_sys::Array::from(
            &js_sys::Reflect::get(&updated_data, &JsValue::from_str("users")).unwrap(),
        );
        assert_eq!(updated_users.length(), 1);
        let first = updated_users.get(0);
        let name = js_sys::Reflect::get(&first, &JsValue::from_str("name"))
            .unwrap()
            .as_string()
            .unwrap();
        assert_eq!(name, "Alice");
    }

    #[wasm_bindgen_test]
    fn test_prepared_graphql_supports_mutation_and_subscription() {
        let db = Database::new("test");

        let users = db
            .create_table("users")
            .column(
                "id",
                JsDataType::Int64,
                Some(ColumnOptions::new().set_primary_key(true)),
            )
            .column("name", JsDataType::String, None);
        db.register_table(&users).unwrap();

        let prepared_subscription = db
            .prepare_graphql(
                "subscription UserFeed { users(orderBy: [{ field: ID, direction: ASC }]) { id name } }",
                Some("UserFeed".into()),
            )
            .unwrap();
        let subscription = prepared_subscription.subscribe(None).unwrap();

        let prepared_mutation = db
            .prepare_graphql(
                "mutation AddUser($id: Long!, $name: String!) { insertUsers(input: [{ id: $id, name: $name }]) { id name } }",
                Some("AddUser".into()),
            )
            .unwrap();

        let variables = js_sys::Object::new();
        js_sys::Reflect::set(&variables, &JsValue::from_str("id"), &JsValue::from_f64(2.0)).unwrap();
        js_sys::Reflect::set(
            &variables,
            &JsValue::from_str("name"),
            &JsValue::from_str("Bob"),
        )
        .unwrap();

        let mutation_result = prepared_mutation.exec(Some(variables.into())).unwrap();
        let mutation_data =
            js_sys::Reflect::get(&mutation_result, &JsValue::from_str("data")).unwrap();
        let inserted = js_sys::Array::from(
            &js_sys::Reflect::get(&mutation_data, &JsValue::from_str("insertUsers")).unwrap(),
        );
        assert_eq!(inserted.length(), 1);

        db.query_registry.borrow_mut().flush();

        let payload = subscription.get_result();
        let data = js_sys::Reflect::get(&payload, &JsValue::from_str("data")).unwrap();
        let users = js_sys::Array::from(&js_sys::Reflect::get(&data, &JsValue::from_str("users")).unwrap());
        assert_eq!(users.length(), 1);
        let first = users.get(0);
        let id = js_sys::Reflect::get(&first, &JsValue::from_str("id"))
            .unwrap()
            .as_f64()
            .unwrap();
        let name = js_sys::Reflect::get(&first, &JsValue::from_str("name"))
            .unwrap()
            .as_string()
            .unwrap();
        assert_eq!(id, 2.0);
        assert_eq!(name, "Bob");
    }
}
