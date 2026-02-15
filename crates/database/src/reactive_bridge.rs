//! Reactive bridge - Integration between Query and Reactive modules.
//!
//! This module provides the bridge between the query system and the reactive
//! system, enabling observable queries that update automatically when
//! underlying data changes.
//!
//! Uses re-query strategy: when data changes, re-execute the query using
//! the query optimizer and indexes for optimal performance.
//! Physical plans are cached to avoid repeated optimization overhead.

use crate::convert::{row_to_js, value_to_js};
use crate::binary_protocol::{BinaryEncoder, BinaryResult, SchemaLayout};
use crate::query_engine::execute_physical_plan;
use cynos_storage::TableCache;
use alloc::string::String;
use alloc::boxed::Box;
use alloc::rc::Rc;
use alloc::vec::Vec;
use cynos_core::schema::Table;
use cynos_core::Row;
use cynos_query::planner::PhysicalPlan;
use cynos_reactive::TableId;
use core::cell::RefCell;
use hashbrown::{HashMap, HashSet};
use wasm_bindgen::prelude::*;

/// A re-query based observable that re-executes the query on each change.
/// This leverages the query optimizer and indexes for optimal performance.
/// The physical plan is cached to avoid repeated optimization overhead.
pub struct ReQueryObservable {
    /// The cached physical plan to execute
    physical_plan: PhysicalPlan,
    /// Reference to the table cache
    cache: Rc<RefCell<TableCache>>,
    /// Current result set
    result: Vec<Rc<Row>>,
    /// Subscription callbacks
    subscriptions: Vec<(usize, Box<dyn Fn(&[Rc<Row>]) + 'static>)>,
    /// Next subscription ID
    next_sub_id: usize,
    /// Whether this is a JOIN query (results have DUMMY_ROW_ID)
    is_join_query: bool,
}

impl ReQueryObservable {
    /// Creates a new re-query observable with a pre-compiled physical plan.
    pub fn new(
        physical_plan: PhysicalPlan,
        cache: Rc<RefCell<TableCache>>,
        initial_result: Vec<Rc<Row>>,
    ) -> Self {
        // Detect JOIN query by checking if first row has dummy ID
        let is_join_query = initial_result.first().map(|r| r.is_dummy()).unwrap_or(false);
        Self {
            physical_plan,
            cache,
            result: initial_result,
            subscriptions: Vec::new(),
            next_sub_id: 0,
            is_join_query,
        }
    }

    /// Returns the current result.
    pub fn result(&self) -> &[Rc<Row>] {
        &self.result
    }

    /// Returns the number of rows.
    pub fn len(&self) -> usize {
        self.result.len()
    }

    /// Returns true if empty.
    pub fn is_empty(&self) -> bool {
        self.result.is_empty()
    }

    /// Subscribes to changes.
    pub fn subscribe<F: Fn(&[Rc<Row>]) + 'static>(&mut self, callback: F) -> usize {
        let id = self.next_sub_id;
        self.next_sub_id += 1;
        self.subscriptions.push((id, Box::new(callback)));
        id
    }

    /// Unsubscribes by ID.
    pub fn unsubscribe(&mut self, id: usize) -> bool {
        let len_before = self.subscriptions.len();
        self.subscriptions.retain(|(sub_id, _)| *sub_id != id);
        self.subscriptions.len() < len_before
    }

    /// Returns subscription count.
    pub fn subscription_count(&self) -> usize {
        self.subscriptions.len()
    }

    /// Called when the table changes - re-executes the cached physical plan.
    /// Only notifies subscribers if the result actually changed.
    /// Skips re-query entirely if there are no subscribers.
    ///
    /// `changed_ids` contains the row IDs that were modified - used to optimize
    /// comparison by only checking these rows when the result set size is unchanged.
    pub fn on_change(&mut self, changed_ids: &HashSet<u64>) {
        // Skip re-query if no subscribers - major optimization for unused observables
        if self.subscriptions.is_empty() {
            return;
        }

        // Re-execute the cached physical plan (no optimization overhead)
        let cache = self.cache.borrow();

        match execute_physical_plan(&cache, &self.physical_plan) {
            Ok(new_result) => {
                // Only notify if result changed
                if !Self::results_equal(&self.result, &new_result, changed_ids, self.is_join_query) {
                    self.result = new_result;
                    // Notify all subscribers
                    for (_, callback) in &self.subscriptions {
                        callback(&self.result);
                    }
                }
            }
            Err(_) => {
                // Query execution failed, keep old result
            }
        }
    }

    /// Compares two result sets for equality using row versions.
    /// This is O(n) where n is the number of rows, comparing only version numbers.
    /// For single-table queries, can further optimize by only checking changed_ids.
    fn results_equal(old: &[Rc<Row>], new: &[Rc<Row>], changed_ids: &HashSet<u64>, is_join_query: bool) -> bool {
        use cynos_core::DUMMY_ROW_ID;

        // Different lengths means definitely changed
        if old.len() != new.len() {
            return false;
        }

        // Empty results are equal
        if old.is_empty() {
            return true;
        }

        // Check if this is an aggregate/join result (rows have DUMMY_ROW_ID)
        let is_aggregate_result = old.first().map(|r| r.id() == DUMMY_ROW_ID).unwrap_or(false);

        if is_join_query || is_aggregate_result {
            // For JOIN/aggregate queries, compare versions (sum of source row versions)
            // If any source row changed, the sum version will be different
            for (old_row, new_row) in old.iter().zip(new.iter()) {
                if old_row.version() != new_row.version() {
                    return false;
                }
            }
        } else {
            // For single-table queries, use optimized comparison
            // Compare row IDs first (fast path)
            let ids_match = old.iter().zip(new.iter()).all(|(a, b)| a.id() == b.id());
            if !ids_match {
                return false;
            }

            // IDs match - only compare versions of changed rows
            for (old_row, new_row) in old.iter().zip(new.iter()) {
                if changed_ids.contains(&old_row.id()) {
                    if old_row.version() != new_row.version() {
                        return false;
                    }
                }
            }
        }

        true
    }
}

/// Registry for tracking re-query observables and routing table changes.
/// Supports batching of changes to avoid redundant re-queries during rapid updates.
pub struct QueryRegistry {
    /// Map from table ID to list of queries that depend on it
    queries: HashMap<TableId, Vec<Rc<RefCell<ReQueryObservable>>>>,
    /// Pending changes to be flushed (table_id -> accumulated changed_ids)
    pending_changes: Rc<RefCell<HashMap<TableId, HashSet<u64>>>>,
    /// Whether a flush is already scheduled
    flush_scheduled: Rc<RefCell<bool>>,
    /// Self reference for scheduling flush callback
    self_ref: Option<Rc<RefCell<QueryRegistry>>>,
}

impl QueryRegistry {
    /// Creates a new query registry.
    pub fn new() -> Self {
        Self {
            queries: HashMap::new(),
            pending_changes: Rc::new(RefCell::new(HashMap::new())),
            flush_scheduled: Rc::new(RefCell::new(false)),
            self_ref: None,
        }
    }

    /// Sets the self reference for scheduling flush callbacks.
    /// Must be called after wrapping in Rc<RefCell<>>.
    pub fn set_self_ref(&mut self, self_ref: Rc<RefCell<QueryRegistry>>) {
        self.self_ref = Some(self_ref);
    }

    /// Registers a query with its dependent table.
    pub fn register(&mut self, query: Rc<RefCell<ReQueryObservable>>, table_id: TableId) {
        self.queries
            .entry(table_id)
            .or_insert_with(Vec::new)
            .push(query);
    }

    /// Handles table changes by batching and scheduling a flush.
    /// Multiple rapid changes are coalesced into a single re-query.
    pub fn on_table_change(&mut self, table_id: TableId, changed_ids: &HashSet<u64>) {
        // Accumulate changes
        {
            let mut pending = self.pending_changes.borrow_mut();
            pending
                .entry(table_id)
                .or_insert_with(HashSet::new)
                .extend(changed_ids.iter().copied());
        }

        // Schedule flush if not already scheduled
        let mut scheduled = self.flush_scheduled.borrow_mut();
        if !*scheduled {
            *scheduled = true;
            drop(scheduled);
            self.schedule_flush();
        }
    }

    /// Schedules a flush to run after the current microtask.
    fn schedule_flush(&self) {
        #[cfg(target_arch = "wasm32")]
        {
            if let Some(ref self_ref) = self.self_ref {
                let self_ref_clone = self_ref.clone();
                let pending_changes = self.pending_changes.clone();
                let flush_scheduled = self.flush_scheduled.clone();

                // Use queueMicrotask via Promise.resolve().then()
                let closure = Closure::once(Box::new(move |_: JsValue| {
                    *flush_scheduled.borrow_mut() = false;
                    let changes: HashMap<TableId, HashSet<u64>> =
                        pending_changes.borrow_mut().drain().collect();

                    let registry = self_ref_clone.borrow();
                    for (table_id, changed_ids) in changes {
                        if let Some(queries) = registry.queries.get(&table_id) {
                            for query in queries {
                                query.borrow_mut().on_change(&changed_ids);
                            }
                        }
                    }
                }) as Box<dyn FnOnce(JsValue)>);

                let promise = js_sys::Promise::resolve(&JsValue::UNDEFINED);
                let _ = promise.then(&closure);
                closure.forget();
            }
        }

        #[cfg(not(target_arch = "wasm32"))]
        {
            // In non-WASM environment, flush immediately (for testing)
            self.flush_sync();
        }
    }

    /// Synchronous flush for testing in non-WASM environment.
    #[cfg(not(target_arch = "wasm32"))]
    fn flush_sync(&self) {
        *self.flush_scheduled.borrow_mut() = false;
        let changes: HashMap<TableId, HashSet<u64>> =
            self.pending_changes.borrow_mut().drain().collect();

        for (table_id, changed_ids) in changes {
            if let Some(queries) = self.queries.get(&table_id) {
                for query in queries {
                    query.borrow_mut().on_change(&changed_ids);
                }
            }
        }
    }

    /// Forces an immediate flush of all pending changes.
    /// Useful for testing or when you need synchronous behcynos.
    pub fn flush(&self) {
        *self.flush_scheduled.borrow_mut() = false;
        let changes: HashMap<TableId, HashSet<u64>> =
            self.pending_changes.borrow_mut().drain().collect();

        for (table_id, changed_ids) in changes {
            if let Some(queries) = self.queries.get(&table_id) {
                for query in queries {
                    query.borrow_mut().on_change(&changed_ids);
                }
            }
        }
    }

    /// Returns the number of registered queries.
    pub fn query_count(&self) -> usize {
        self.queries.values().map(|v| v.len()).sum()
    }

    /// Returns whether there are pending changes waiting to be flushed.
    pub fn has_pending_changes(&self) -> bool {
        !self.pending_changes.borrow().is_empty()
    }
}

impl Default for QueryRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// JavaScript-friendly observable query wrapper.
/// Uses re-query strategy for optimal performance with indexes.
#[wasm_bindgen]
pub struct JsObservableQuery {
    inner: Rc<RefCell<ReQueryObservable>>,
    schema: Table,
    /// Optional projected column names. If Some, only these columns are returned.
    projected_columns: Option<Vec<String>>,
    /// Pre-computed binary layout for getResultBinary().
    binary_layout: SchemaLayout,
    /// Optional aggregate column names. If Some, this is an aggregate query.
    aggregate_columns: Option<Vec<String>>,
}

impl JsObservableQuery {
    pub(crate) fn new(
        inner: Rc<RefCell<ReQueryObservable>>,
        schema: Table,
        binary_layout: SchemaLayout,
    ) -> Self {
        Self { inner, schema, projected_columns: None, binary_layout, aggregate_columns: None }
    }

    pub(crate) fn new_with_projection(
        inner: Rc<RefCell<ReQueryObservable>>,
        schema: Table,
        projected_columns: Vec<String>,
        binary_layout: SchemaLayout,
    ) -> Self {
        Self { inner, schema, projected_columns: Some(projected_columns), binary_layout, aggregate_columns: None }
    }

    pub(crate) fn new_with_aggregates(
        inner: Rc<RefCell<ReQueryObservable>>,
        schema: Table,
        aggregate_columns: Vec<String>,
        binary_layout: SchemaLayout,
    ) -> Self {
        Self { inner, schema, projected_columns: None, binary_layout, aggregate_columns: Some(aggregate_columns) }
    }

    /// Get the inner observable for creating JsChangesStream.
    pub(crate) fn inner(&self) -> Rc<RefCell<ReQueryObservable>> {
        self.inner.clone()
    }

    /// Get the schema.
    pub(crate) fn schema(&self) -> &Table {
        &self.schema
    }

    /// Get the projected columns.
    pub(crate) fn projected_columns(&self) -> Option<&Vec<String>> {
        self.projected_columns.as_ref()
    }

    /// Get the aggregate columns.
    pub(crate) fn aggregate_columns(&self) -> Option<&Vec<String>> {
        self.aggregate_columns.as_ref()
    }
}

#[wasm_bindgen]
impl JsObservableQuery {
    /// Subscribes to query changes.
    ///
    /// The callback receives the complete current result set as a JavaScript array.
    /// It is called whenever data changes (not immediately - use getResult for initial data).
    /// Returns an unsubscribe function.
    pub fn subscribe(&mut self, callback: js_sys::Function) -> js_sys::Function {
        let schema = self.schema.clone();
        let projected_columns = self.projected_columns.clone();
        let aggregate_columns = self.aggregate_columns.clone();

        let sub_id = self.inner.borrow_mut().subscribe(move |rows| {
            let current_data = if let Some(ref cols) = aggregate_columns {
                projected_rows_to_js_array(rows, cols)
            } else if let Some(ref cols) = projected_columns {
                projected_rows_to_js_array(rows, cols)
            } else {
                rows_to_js_array(rows, &schema)
            };
            callback.call1(&JsValue::NULL, &current_data).ok();
        });

        // Create unsubscribe function
        let inner_unsub = self.inner.clone();
        let unsubscribe = Closure::once_into_js(move || {
            inner_unsub.borrow_mut().unsubscribe(sub_id);
        });

        unsubscribe.unchecked_into()
    }

    /// Returns the current result as a JavaScript array.
    #[wasm_bindgen(js_name = getResult)]
    pub fn get_result(&self) -> JsValue {
        let inner = self.inner.borrow();
        if let Some(ref cols) = self.aggregate_columns {
            projected_rows_to_js_array(inner.result(), cols)
        } else if let Some(ref cols) = self.projected_columns {
            projected_rows_to_js_array(inner.result(), cols)
        } else {
            rows_to_js_array(inner.result(), &self.schema)
        }
    }

    /// Returns the current result as a binary buffer for zero-copy access.
    #[wasm_bindgen(js_name = getResultBinary)]
    pub fn get_result_binary(&self) -> BinaryResult {
        let inner = self.inner.borrow();
        let rows = inner.result();
        let mut encoder = BinaryEncoder::new(self.binary_layout.clone(), rows.len());
        encoder.encode_rows(rows);
        BinaryResult::new(encoder.finish())
    }

    /// Returns the schema layout for decoding binary results.
    #[wasm_bindgen(js_name = getSchemaLayout)]
    pub fn get_schema_layout(&self) -> SchemaLayout {
        self.binary_layout.clone()
    }

    /// Returns the number of rows in the result.
    #[wasm_bindgen(getter)]
    pub fn length(&self) -> usize {
        self.inner.borrow().len()
    }

    /// Returns whether the result is empty.
    #[wasm_bindgen(js_name = isEmpty)]
    pub fn is_empty(&self) -> bool {
        self.inner.borrow().is_empty()
    }

    /// Returns the number of active subscriptions.
    #[wasm_bindgen(js_name = subscriptionCount)]
    pub fn subscription_count(&self) -> usize {
        self.inner.borrow().subscription_count()
    }
}

/// JavaScript-friendly changes stream.
///
/// This provides the `changes()` API that yields the complete result set
/// whenever data changes. The callback receives the full current data,
/// not incremental changes - perfect for React's setState pattern.
#[wasm_bindgen]
pub struct JsChangesStream {
    inner: Rc<RefCell<ReQueryObservable>>,
    schema: Table,
    /// Optional projected column names. If Some, only these columns are returned.
    projected_columns: Option<Vec<String>>,
    /// Pre-computed binary layout for getResultBinary().
    binary_layout: SchemaLayout,
}

impl JsChangesStream {
    pub(crate) fn from_observable(observable: JsObservableQuery) -> Self {
        Self {
            inner: observable.inner(),
            schema: observable.schema().clone(),
            projected_columns: observable.projected_columns().cloned(),
            binary_layout: observable.binary_layout.clone(),
        }
    }
}

#[wasm_bindgen]
impl JsChangesStream {
    /// Subscribes to the changes stream.
    ///
    /// The callback receives the complete current result set as a JavaScript array.
    /// It is called immediately with the initial data, and again whenever data changes.
    /// Perfect for React: `stream.subscribe(data => setUsers(data))`
    ///
    /// Returns an unsubscribe function.
    pub fn subscribe(&self, callback: js_sys::Function) -> js_sys::Function {
        let schema = self.schema.clone();
        let inner = self.inner.clone();
        let projected_columns = self.projected_columns.clone();

        // Emit initial value immediately
        let initial_data = if let Some(ref cols) = projected_columns {
            projected_rows_to_js_array(inner.borrow().result(), cols)
        } else {
            rows_to_js_array(inner.borrow().result(), &schema)
        };
        callback.call1(&JsValue::NULL, &initial_data).ok();

        // Subscribe to subsequent changes
        let schema_clone = schema.clone();
        let projected_columns_clone = projected_columns.clone();
        let sub_id = inner.borrow_mut().subscribe(move |rows| {
            let current_data = if let Some(ref cols) = projected_columns_clone {
                projected_rows_to_js_array(rows, cols)
            } else {
                rows_to_js_array(rows, &schema_clone)
            };
            callback.call1(&JsValue::NULL, &current_data).ok();
        });

        // Create unsubscribe function
        let unsubscribe = Closure::once_into_js(move || {
            inner.borrow_mut().unsubscribe(sub_id);
        });

        unsubscribe.unchecked_into()
    }

    /// Returns the current result.
    #[wasm_bindgen(js_name = getResult)]
    pub fn get_result(&self) -> JsValue {
        let inner = self.inner.borrow();
        if let Some(ref cols) = self.projected_columns {
            projected_rows_to_js_array(inner.result(), cols)
        } else {
            rows_to_js_array(inner.result(), &self.schema)
        }
    }

    /// Returns the current result as a binary buffer for zero-copy access.
    #[wasm_bindgen(js_name = getResultBinary)]
    pub fn get_result_binary(&self) -> BinaryResult {
        let inner = self.inner.borrow();
        let rows = inner.result();
        let mut encoder = BinaryEncoder::new(self.binary_layout.clone(), rows.len());
        encoder.encode_rows(rows);
        BinaryResult::new(encoder.finish())
    }

    /// Returns the schema layout for decoding binary results.
    #[wasm_bindgen(js_name = getSchemaLayout)]
    pub fn get_schema_layout(&self) -> SchemaLayout {
        self.binary_layout.clone()
    }
}

/// Converts rows to a JavaScript array.
fn rows_to_js_array(rows: &[Rc<Row>], schema: &Table) -> JsValue {
    let arr = js_sys::Array::new_with_length(rows.len() as u32);
    for (i, row) in rows.iter().enumerate() {
        arr.set(i as u32, row_to_js(row, schema));
    }
    arr.into()
}

/// Converts projected rows to a JavaScript array.
/// Only includes the specified columns in the output.
fn projected_rows_to_js_array(rows: &[Rc<Row>], column_names: &[String]) -> JsValue {
    let arr = js_sys::Array::new_with_length(rows.len() as u32);
    for (i, row) in rows.iter().enumerate() {
        let obj = js_sys::Object::new();
        for (col_idx, col_name) in column_names.iter().enumerate() {
            if let Some(value) = row.get(col_idx) {
                let js_val = value_to_js(value);
                js_sys::Reflect::set(&obj, &JsValue::from_str(col_name), &js_val).ok();
            }
        }
        arr.set(i as u32, obj.into());
    }
    arr.into()
}

#[cfg(test)]
mod tests {
    use super::*;
    use cynos_core::schema::TableBuilder;
    use cynos_core::{DataType, Value};
    use wasm_bindgen_test::*;

    wasm_bindgen_test_configure!(run_in_browser);

    fn test_schema() -> Table {
        TableBuilder::new("users")
            .unwrap()
            .add_column("id", DataType::Int64)
            .unwrap()
            .add_column("name", DataType::String)
            .unwrap()
            .add_column("age", DataType::Int32)
            .unwrap()
            .add_primary_key(&["id"], false)
            .unwrap()
            .build()
            .unwrap()
    }

    fn make_row(id: u64, name: &str, age: i32) -> Row {
        Row::new(
            id,
            alloc::vec![
                Value::Int64(id as i64),
                Value::String(name.into()),
                Value::Int32(age),
            ],
        )
    }

    #[wasm_bindgen_test]
    fn test_query_registry_new() {
        let registry = QueryRegistry::new();
        assert_eq!(registry.query_count(), 0);
    }

    #[wasm_bindgen_test]
    fn test_rows_to_js_array() {
        let schema = test_schema();
        let rows: Vec<Rc<Row>> = alloc::vec![
            Rc::new(make_row(1, "Alice", 25)),
            Rc::new(make_row(2, "Bob", 30)),
        ];

        let js = rows_to_js_array(&rows, &schema);
        let arr = js_sys::Array::from(&js);
        assert_eq!(arr.length(), 2);
    }
}
