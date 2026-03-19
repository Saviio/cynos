//! Reactive bridge - Integration between Query and Reactive modules.
//!
//! This module provides the bridge between the query system and the reactive
//! system, enabling observable queries that update automatically when
//! underlying data changes.
//!
//! Two strategies are supported:
//! 1. Re-query: re-execute the cached physical plan on each change (original)
//! 2. IVM (DBSP): propagate deltas through a compiled dataflow graph (new)
//!
//! The IVM path is used when the query is incrementalizable (no Sort/Limit/TopN).
//! Otherwise, falls back to re-query.

use crate::binary_protocol::{BinaryEncoder, BinaryResult, SchemaLayout};
use crate::convert::{row_to_js, value_to_js};
use crate::query_engine::{
    execute_compiled_physical_plan_with_summary, CompiledPhysicalPlan, QueryResultSummary,
};
use alloc::boxed::Box;
use alloc::rc::Rc;
use alloc::string::String;
use alloc::vec::Vec;
use core::cell::RefCell;
use cynos_core::schema::Table;
use cynos_core::Row;
use cynos_incremental::{Delta, TableId};
use cynos_reactive::ObservableQuery;
use cynos_storage::TableCache;
use hashbrown::{HashMap, HashSet};
use wasm_bindgen::prelude::*;

/// A re-query based observable that re-executes the query on each change.
/// This leverages the query optimizer and indexes for optimal performance.
/// The physical plan and lowered execution artifact are cached to avoid repeated
/// optimization and predicate lowering overhead.
pub struct ReQueryObservable {
    /// The cached compiled plan to execute
    compiled_plan: CompiledPhysicalPlan,
    /// Reference to the table cache
    cache: Rc<RefCell<TableCache>>,
    /// Current result set
    result: Vec<Rc<Row>>,
    /// Summary of the current result set for fast equality checks
    result_summary: QueryResultSummary,
    /// Subscription callbacks
    subscriptions: Vec<(usize, Box<dyn Fn(&[Rc<Row>]) + 'static>)>,
    /// Next subscription ID
    next_sub_id: usize,
}

impl ReQueryObservable {
    /// Creates a new re-query observable with a pre-compiled physical plan.
    pub fn new(
        compiled_plan: CompiledPhysicalPlan,
        cache: Rc<RefCell<TableCache>>,
        initial_result: Vec<Rc<Row>>,
    ) -> Self {
        let result_summary = QueryResultSummary::from_rows(&initial_result);
        Self::new_with_summary(compiled_plan, cache, initial_result, result_summary)
    }

    #[doc(hidden)]
    pub fn new_with_summary(
        compiled_plan: CompiledPhysicalPlan,
        cache: Rc<RefCell<TableCache>>,
        initial_result: Vec<Rc<Row>>,
        result_summary: QueryResultSummary,
    ) -> Self {
        Self {
            compiled_plan,
            cache,
            result: initial_result,
            result_summary,
            subscriptions: Vec::new(),
            next_sub_id: 0,
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
    /// `changed_ids` contains the row IDs that were modified.
    /// The current implementation keeps the parameter for API compatibility,
    /// while result equality remains fully deterministic.
    pub fn on_change(&mut self, _changed_ids: &HashSet<u64>) {
        // Skip re-query if no subscribers - major optimization for unused observables
        if self.subscriptions.is_empty() {
            return;
        }

        // Re-execute the cached compiled plan (no optimization or lowering overhead)
        let cache = self.cache.borrow();

        match execute_compiled_physical_plan_with_summary(&cache, &self.compiled_plan) {
            Ok(output) => {
                // Only notify if result changed
                if !Self::results_equal(
                    &self.result_summary,
                    &output.summary,
                    &self.result,
                    &output.rows,
                ) {
                    self.result = output.rows;
                    self.result_summary = output.summary;
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

    /// Compares two result sets using a precomputed summary captured during execution.
    /// This keeps the unchanged path O(1) after the query has already been re-executed.
    fn results_equal(
        old_summary: &QueryResultSummary,
        new_summary: &QueryResultSummary,
        old: &[Rc<Row>],
        new: &[Rc<Row>],
    ) -> bool {
        if old_summary != new_summary || old.len() != new.len() {
            return false;
        }

        old.iter().zip(new.iter()).all(|(old_row, new_row)| {
            Rc::ptr_eq(old_row, new_row)
                || (old_row.id() == new_row.id()
                    && old_row.version() == new_row.version()
                    && old_row.values() == new_row.values())
        })
    }
}

/// Registry for tracking re-query observables and routing table changes.
/// Supports batching of changes to avoid redundant re-queries during rapid updates.
pub struct QueryRegistry {
    /// Map from table ID to list of queries that depend on it
    queries: HashMap<TableId, Vec<Rc<RefCell<ReQueryObservable>>>>,
    /// Map from table ID to IVM-based queries
    ivm_queries: HashMap<TableId, Vec<Rc<RefCell<ObservableQuery>>>>,
    /// Pending changes to be flushed (table_id -> accumulated changed_ids)
    pending_changes: Rc<RefCell<HashMap<TableId, HashSet<u64>>>>,
    /// Pending IVM deltas (table_id -> accumulated deltas)
    pending_ivm_deltas: Rc<RefCell<HashMap<TableId, Vec<Delta<Row>>>>>,
    /// Whether a flush is already scheduled
    flush_scheduled: Rc<RefCell<bool>>,
    /// Self reference for scheduling flush callback
    self_ref: Option<Rc<RefCell<QueryRegistry>>>,
    /// Reusable flush closure to avoid Closure::once + forget() leak per DML
    #[cfg(target_arch = "wasm32")]
    flush_closure: Option<Closure<dyn FnMut(JsValue)>>,
}

impl QueryRegistry {
    /// Creates a new query registry.
    pub fn new() -> Self {
        Self {
            queries: HashMap::new(),
            ivm_queries: HashMap::new(),
            pending_changes: Rc::new(RefCell::new(HashMap::new())),
            pending_ivm_deltas: Rc::new(RefCell::new(HashMap::new())),
            flush_scheduled: Rc::new(RefCell::new(false)),
            self_ref: None,
            #[cfg(target_arch = "wasm32")]
            flush_closure: None,
        }
    }

    /// Sets the self reference for scheduling flush callbacks.
    /// Must be called after wrapping in Rc<RefCell<>>.
    pub fn set_self_ref(&mut self, self_ref: Rc<RefCell<QueryRegistry>>) {
        self.self_ref = Some(self_ref);
    }

    /// Registers a re-query observable with its dependent table.
    pub fn register(&mut self, query: Rc<RefCell<ReQueryObservable>>, table_id: TableId) {
        self.queries
            .entry(table_id)
            .or_insert_with(Vec::new)
            .push(query);
    }

    /// Registers an IVM-based observable query.
    /// The query's dependencies are automatically extracted from its dataflow.
    pub fn register_ivm(&mut self, query: Rc<RefCell<ObservableQuery>>) {
        let deps: Vec<TableId> = query.borrow().dependencies().to_vec();
        for table_id in deps {
            self.ivm_queries
                .entry(table_id)
                .or_insert_with(Vec::new)
                .push(query.clone());
        }
    }

    fn flush_requery_changes(&self, changes: HashMap<TableId, HashSet<u64>>) {
        let mut merged: HashMap<usize, (Rc<RefCell<ReQueryObservable>>, HashSet<u64>)> =
            HashMap::new();

        for (table_id, changed_ids) in changes {
            if let Some(queries) = self.queries.get(&table_id) {
                for query in queries {
                    let key = Rc::as_ptr(query) as usize;
                    let entry = merged
                        .entry(key)
                        .or_insert_with(|| (query.clone(), HashSet::new()));
                    entry.1.extend(changed_ids.iter().copied());
                }
            }
        }

        for (_, (query, changed_ids)) in merged {
            query.borrow_mut().on_change(&changed_ids);
        }
    }

    /// Handles table changes by batching and scheduling a flush.
    /// Multiple rapid changes are coalesced into a single re-query/propagation.
    pub fn on_table_change(&mut self, table_id: TableId, changed_ids: &HashSet<u64>) {
        // Accumulate changes for re-query observables
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

    /// Handles table changes with IVM deltas.
    /// This is the new DBSP-based path that propagates deltas incrementally.
    pub fn on_table_change_ivm(
        &mut self,
        table_id: TableId,
        deltas: Vec<Delta<Row>>,
        changed_ids: &HashSet<u64>,
    ) {
        // Accumulate IVM deltas
        {
            let mut pending = self.pending_ivm_deltas.borrow_mut();
            pending
                .entry(table_id)
                .or_insert_with(Vec::new)
                .extend(deltas);
        }

        // Also accumulate for re-query observables
        {
            let mut pending = self.pending_changes.borrow_mut();
            pending
                .entry(table_id)
                .or_insert_with(HashSet::new)
                .extend(changed_ids.iter().copied());
        }

        let mut scheduled = self.flush_scheduled.borrow_mut();
        if !*scheduled {
            *scheduled = true;
            drop(scheduled);
            self.schedule_flush();
        }
    }

    /// Schedules a flush to run after the current microtask.
    fn schedule_flush(&mut self) {
        #[cfg(target_arch = "wasm32")]
        {
            // Lazily create the reusable flush closure once
            if self.flush_closure.is_none() {
                if let Some(ref self_ref) = self.self_ref {
                    let self_ref_clone = self_ref.clone();
                    let pending_changes = self.pending_changes.clone();
                    let pending_ivm_deltas = self.pending_ivm_deltas.clone();
                    let flush_scheduled = self.flush_scheduled.clone();

                    self.flush_closure = Some(Closure::new(move |_: JsValue| {
                        *flush_scheduled.borrow_mut() = false;

                        // Flush IVM deltas first (O(delta) path)
                        let ivm_changes: HashMap<TableId, Vec<Delta<Row>>> =
                            pending_ivm_deltas.borrow_mut().drain().collect();
                        {
                            let registry = self_ref_clone.borrow();
                            for (table_id, deltas) in &ivm_changes {
                                if let Some(queries) = registry.ivm_queries.get(table_id) {
                                    for query in queries {
                                        query
                                            .borrow_mut()
                                            .on_table_change(*table_id, deltas.clone());
                                    }
                                }
                            }
                        }

                        // Then flush re-query changes (O(result_set) path)
                        let changes: HashMap<TableId, HashSet<u64>> =
                            pending_changes.borrow_mut().drain().collect();
                        {
                            let registry = self_ref_clone.borrow();
                            registry.flush_requery_changes(changes);
                        }

                        // GC: remove queries with no subscribers to prevent memory leaks
                        {
                            let mut registry = self_ref_clone.borrow_mut();
                            registry.gc_dead_queries();
                        }
                    }));
                }
            }

            if let Some(ref closure) = self.flush_closure {
                let promise = js_sys::Promise::resolve(&JsValue::UNDEFINED);
                let _ = promise.then(closure);
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
    fn flush_sync(&mut self) {
        *self.flush_scheduled.borrow_mut() = false;

        // Flush IVM deltas
        let ivm_changes: HashMap<TableId, Vec<Delta<Row>>> =
            self.pending_ivm_deltas.borrow_mut().drain().collect();
        for (table_id, deltas) in &ivm_changes {
            if let Some(queries) = self.ivm_queries.get(table_id) {
                for query in queries {
                    query
                        .borrow_mut()
                        .on_table_change(*table_id, deltas.clone());
                }
            }
        }

        // Flush re-query changes
        let changes: HashMap<TableId, HashSet<u64>> =
            self.pending_changes.borrow_mut().drain().collect();
        self.flush_requery_changes(changes);

        self.gc_dead_queries();
    }

    /// Forces an immediate flush of all pending changes.
    /// Useful for testing or when you need synchronous behavior.
    pub fn flush(&mut self) {
        *self.flush_scheduled.borrow_mut() = false;

        // Flush IVM deltas
        let ivm_changes: HashMap<TableId, Vec<Delta<Row>>> =
            self.pending_ivm_deltas.borrow_mut().drain().collect();
        for (table_id, deltas) in &ivm_changes {
            if let Some(queries) = self.ivm_queries.get(table_id) {
                for query in queries {
                    query
                        .borrow_mut()
                        .on_table_change(*table_id, deltas.clone());
                }
            }
        }

        // Flush re-query changes
        let changes: HashMap<TableId, HashSet<u64>> =
            self.pending_changes.borrow_mut().drain().collect();
        self.flush_requery_changes(changes);

        self.gc_dead_queries();
    }

    /// Removes queries with no active subscribers from the registry.
    /// Called after each flush to prevent memory leaks from abandoned queries.
    fn gc_dead_queries(&mut self) {
        for queries in self.ivm_queries.values_mut() {
            queries.retain(|q| q.borrow().subscription_count() > 0);
        }
        self.ivm_queries.retain(|_, v| !v.is_empty());

        for queries in self.queries.values_mut() {
            queries.retain(|q| q.borrow().subscription_count() > 0);
        }
        self.queries.retain(|_, v| !v.is_empty());
    }

    /// Returns the number of registered queries (both re-query and IVM).
    pub fn query_count(&self) -> usize {
        let requery_count: usize = self.queries.values().map(|v| v.len()).sum();
        let ivm_count: usize = self.ivm_queries.values().map(|v| v.len()).sum();
        requery_count + ivm_count
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
        Self {
            inner,
            schema,
            projected_columns: None,
            binary_layout,
            aggregate_columns: None,
        }
    }

    pub(crate) fn new_with_projection(
        inner: Rc<RefCell<ReQueryObservable>>,
        schema: Table,
        projected_columns: Vec<String>,
        binary_layout: SchemaLayout,
    ) -> Self {
        Self {
            inner,
            schema,
            projected_columns: Some(projected_columns),
            binary_layout,
            aggregate_columns: None,
        }
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
    #[allow(dead_code)]
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
        let called = Rc::new(RefCell::new(false));
        let called_c = called.clone();
        let unsubscribe = Closure::wrap(Box::new(move || {
            let mut c = called_c.borrow_mut();
            if !*c {
                *c = true;
                inner_unsub.borrow_mut().unsubscribe(sub_id);
            }
        }) as Box<dyn FnMut()>);
        unsubscribe.into_js_value().unchecked_into()
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

/// JavaScript-friendly IVM observable query wrapper.
/// Uses DBSP-based incremental view maintenance for O(delta) updates.
#[wasm_bindgen]
pub struct JsIvmObservableQuery {
    inner: Rc<RefCell<ObservableQuery>>,
    schema: Table,
    /// Optional projected column names.
    projected_columns: Option<Vec<String>>,
    /// Pre-computed binary layout for getResultBinary().
    binary_layout: SchemaLayout,
    /// Optional aggregate column names.
    aggregate_columns: Option<Vec<String>>,
}

impl JsIvmObservableQuery {
    pub(crate) fn new(
        inner: Rc<RefCell<ObservableQuery>>,
        schema: Table,
        binary_layout: SchemaLayout,
    ) -> Self {
        Self {
            inner,
            schema,
            projected_columns: None,
            binary_layout,
            aggregate_columns: None,
        }
    }

    pub(crate) fn new_with_projection(
        inner: Rc<RefCell<ObservableQuery>>,
        schema: Table,
        projected_columns: Vec<String>,
        binary_layout: SchemaLayout,
    ) -> Self {
        Self {
            inner,
            schema,
            projected_columns: Some(projected_columns),
            binary_layout,
            aggregate_columns: None,
        }
    }
}

#[wasm_bindgen]
impl JsIvmObservableQuery {
    /// Subscribes to IVM query changes.
    ///
    /// The callback receives a delta object `{ added: Row[], removed: Row[] }`
    /// instead of the full result set. This is the true O(delta) path —
    /// the UI side should apply the delta to its own state.
    ///
    /// Use `getResult()` to get the initial full result before subscribing.
    /// Returns an unsubscribe function.
    pub fn subscribe(&mut self, callback: js_sys::Function) -> js_sys::Function {
        let schema = self.schema.clone();
        let projected_columns = self.projected_columns.clone();
        let aggregate_columns = self.aggregate_columns.clone();

        let sub_id = self.inner.borrow_mut().subscribe(move |change_set| {
            let delta_obj = js_sys::Object::new();

            // Serialize only added rows
            let added = if let Some(ref cols) = aggregate_columns {
                ivm_rows_to_js_array(&change_set.added, cols)
            } else if let Some(ref cols) = projected_columns {
                ivm_rows_to_js_array(&change_set.added, cols)
            } else {
                ivm_full_rows_to_js_array(&change_set.added, &schema)
            };

            // Serialize only removed rows
            let removed = if let Some(ref cols) = aggregate_columns {
                ivm_rows_to_js_array(&change_set.removed, cols)
            } else if let Some(ref cols) = projected_columns {
                ivm_rows_to_js_array(&change_set.removed, cols)
            } else {
                ivm_full_rows_to_js_array(&change_set.removed, &schema)
            };

            js_sys::Reflect::set(&delta_obj, &JsValue::from_str("added"), &added).ok();
            js_sys::Reflect::set(&delta_obj, &JsValue::from_str("removed"), &removed).ok();

            callback.call1(&JsValue::NULL, &delta_obj).ok();
        });

        let inner_unsub = self.inner.clone();
        let called = Rc::new(RefCell::new(false));
        let called_c = called.clone();
        let unsubscribe = Closure::wrap(Box::new(move || {
            let mut c = called_c.borrow_mut();
            if !*c {
                *c = true;
                inner_unsub.borrow_mut().unsubscribe(sub_id);
            }
        }) as Box<dyn FnMut()>);
        unsubscribe.into_js_value().unchecked_into()
    }

    /// Returns the current result as a JavaScript array.
    #[wasm_bindgen(js_name = getResult)]
    pub fn get_result(&self) -> JsValue {
        let inner = self.inner.borrow();
        let rows = inner.result();
        if let Some(ref cols) = self.aggregate_columns {
            ivm_rows_to_js_array(&rows, cols)
        } else if let Some(ref cols) = self.projected_columns {
            ivm_rows_to_js_array(&rows, cols)
        } else {
            ivm_full_rows_to_js_array(&rows, &self.schema)
        }
    }

    /// Returns the current result as a binary buffer for zero-copy access.
    #[wasm_bindgen(js_name = getResultBinary)]
    pub fn get_result_binary(&self) -> BinaryResult {
        let inner = self.inner.borrow();
        let rows = inner.result();
        let rc_rows: Vec<Rc<Row>> = rows.into_iter().map(Rc::new).collect();
        let mut encoder = BinaryEncoder::new(self.binary_layout.clone(), rc_rows.len());
        encoder.encode_rows(&rc_rows);
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

/// Converts IVM rows (owned Row, not Rc<Row>) to a JavaScript array using projected columns.
fn ivm_rows_to_js_array(rows: &[Row], column_names: &[String]) -> JsValue {
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

/// Converts IVM rows (owned Row, not Rc<Row>) to a JavaScript array using full schema.
fn ivm_full_rows_to_js_array(rows: &[Row], schema: &Table) -> JsValue {
    let arr = js_sys::Array::new_with_length(rows.len() as u32);
    for (i, row) in rows.iter().enumerate() {
        let obj = js_sys::Object::new();
        for col in schema.columns() {
            if let Some(value) = row.get(col.index()) {
                let js_val = value_to_js(value);
                js_sys::Reflect::set(&obj, &JsValue::from_str(col.name()), &js_val).ok();
            }
        }
        arr.set(i as u32, obj.into());
    }
    arr.into()
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
        let called = Rc::new(RefCell::new(false));
        let called_c = called.clone();
        let unsubscribe = Closure::wrap(Box::new(move || {
            let mut c = called_c.borrow_mut();
            if !*c {
                *c = true;
                inner.borrow_mut().unsubscribe(sub_id);
            }
        }) as Box<dyn FnMut()>);
        unsubscribe.into_js_value().unchecked_into()
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
    use cynos_query::ast::Expr;
    use cynos_query::executor::{InMemoryDataSource, PhysicalPlanRunner};
    use cynos_query::planner::PhysicalPlan;
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

    #[test]
    fn test_projection_query_preserves_version_and_live_query_detects_update() {
        let plan = PhysicalPlan::project(
            PhysicalPlan::table_scan("users"),
            alloc::vec![Expr::column("users", "name", 1)],
        );

        let mut old_ds = InMemoryDataSource::new();
        old_ds.add_table(
            "users",
            alloc::vec![Row::new_with_version(
                1,
                5,
                alloc::vec![
                    Value::Int64(1),
                    Value::String("Alice".into()),
                    Value::Int32(25),
                ],
            )],
            3,
        );
        let old_result = PhysicalPlanRunner::new(&old_ds).execute(&plan).unwrap();
        let old_rows: Vec<Rc<Row>> = old_result
            .entries
            .iter()
            .map(|entry| entry.row.clone())
            .collect();

        let mut new_ds = InMemoryDataSource::new();
        new_ds.add_table(
            "users",
            alloc::vec![Row::new_with_version(
                1,
                6,
                alloc::vec![
                    Value::Int64(1),
                    Value::String("Alicia".into()),
                    Value::Int32(25),
                ],
            )],
            3,
        );
        let new_result = PhysicalPlanRunner::new(&new_ds).execute(&plan).unwrap();
        let new_rows: Vec<Rc<Row>> = new_result
            .entries
            .iter()
            .map(|entry| entry.row.clone())
            .collect();

        assert_eq!(
            old_rows[0].version(),
            5,
            "Projection should preserve the source row version for reactive diffing",
        );
        assert_eq!(
            new_rows[0].version(),
            6,
            "Projection should preserve the source row version for reactive diffing",
        );

        let old_summary = QueryResultSummary::from_rows(&old_rows);
        let new_summary = QueryResultSummary::from_rows(&new_rows);
        assert!(
            !ReQueryObservable::results_equal(&old_summary, &new_summary, &old_rows, &new_rows),
            "Projected value changed from Alice to Alicia, so live query comparison should detect a change",
        );
    }

    #[test]
    fn test_result_comparison_falls_back_to_exact_rows_when_summary_matches() {
        let old_rows: Vec<Rc<Row>> = alloc::vec![Rc::new(Row::new_with_version(
            1,
            5,
            alloc::vec![Value::String("Alice".into())],
        ))];
        let new_rows: Vec<Rc<Row>> = alloc::vec![Rc::new(Row::new_with_version(
            1,
            5,
            alloc::vec![Value::String("Alicia".into())],
        ))];

        let colliding_summary = QueryResultSummary {
            len: 1,
            fingerprint: 42,
        };

        assert!(
            !ReQueryObservable::results_equal(
                &colliding_summary,
                &colliding_summary,
                &old_rows,
                &new_rows,
            ),
            "Row comparison must remain deterministic even if two summaries collide",
        );
    }
}
