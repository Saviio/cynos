//! Database - Main entry point for Cynos database operations.
//!
//! This module provides the `Database` struct which is the primary interface
//! for creating tables, executing queries, and managing data.

use crate::binary_protocol::SchemaLayoutCache;
use crate::query_builder::{DeleteBuilder, InsertBuilder, SelectBuilder, UpdateBuilder};
use crate::reactive_bridge::QueryRegistry;
use crate::table::{JsTable, JsTableBuilder};
use crate::transaction::JsTransaction;
use alloc::rc::Rc;
use alloc::string::{String, ToString};
#[cfg(feature = "benchmark")]
use alloc::vec::Vec;
#[cfg(feature = "benchmark")]
use cynos_core::Row;
#[allow(unused_imports)]
use cynos_incremental::Delta;
use cynos_query::plan_cache::PlanCache;
use cynos_reactive::TableId;
use cynos_storage::TableCache;
use core::cell::RefCell;
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
}

#[wasm_bindgen]
impl Database {
    /// Creates a new database instance.
    #[wasm_bindgen(constructor)]
    pub fn new(name: &str) -> Self {
        let query_registry = Rc::new(RefCell::new(QueryRegistry::new()));
        // Set self reference for microtask scheduling
        query_registry.borrow_mut().set_self_ref(query_registry.clone());

        Self {
            name: name.to_string(),
            cache: Rc::new(RefCell::new(TableCache::new())),
            query_registry,
            table_id_map: Rc::new(RefCell::new(hashbrown::HashMap::new())),
            next_table_id: Rc::new(RefCell::new(1)),
            schema_layout_cache: Rc::new(RefCell::new(SchemaLayoutCache::new())),
            plan_cache: Rc::new(RefCell::new(PlanCache::default_size())),
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
                    cynos_core::DataType::DateTime => Value::DateTime(1700000000000 + i as i64 * 1000),
                    _ => Value::Null,
                };
                values.push(value);
            }

            let row = Row::new(row_id, values);
            store.insert(row).map_err(|e| JsValue::from_str(&alloc::format!("{:?}", e)))?;
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
        js_sys::Reflect::set(&result, &JsValue::from_str("duration_ms"), &JsValue::from_f64(duration_ms))?;
        js_sys::Reflect::set(&result, &JsValue::from_str("rows_per_sec"), &JsValue::from_f64(rows_per_sec))?;
        js_sys::Reflect::set(&result, &JsValue::from_str("count"), &JsValue::from_f64(count as f64))?;

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
        use cynos_query::planner::LogicalPlan;
        use cynos_query::ast::{Expr as AstExpr, BinaryOp};

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
        js_sys::Reflect::set(&result, &JsValue::from_str("query_ms"), &JsValue::from_f64(query_ms))?;
        js_sys::Reflect::set(&result, &JsValue::from_str("serialize_ms"), &JsValue::from_f64(serialize_ms))?;
        js_sys::Reflect::set(&result, &JsValue::from_str("total_ms"), &JsValue::from_f64(total_ms))?;
        js_sys::Reflect::set(&result, &JsValue::from_str("row_count"), &JsValue::from_f64(row_count as f64))?;
        js_sys::Reflect::set(&result, &JsValue::from_str("serialization_overhead_pct"), &JsValue::from_f64(serialization_overhead_pct))?;

        Ok(result.into())
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
    pub(crate) fn notify_table_change(&self, table_id: TableId, changed_ids: &hashbrown::HashSet<u64>) {
        self.query_registry
            .borrow_mut()
            .on_table_change(table_id, changed_ids);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::table::ColumnOptions;
    use crate::JsDataType;
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

        let builder = db
            .create_table("users")
            .column(
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

        let builder1 = db
            .create_table("users")
            .column(
                "id",
                JsDataType::Int64,
                Some(ColumnOptions::new().set_primary_key(true)),
            );
        db.register_table(&builder1).unwrap();

        let builder2 = db
            .create_table("orders")
            .column(
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

        let builder = db
            .create_table("users")
            .column(
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
}
