//! Materialized view for Incremental View Maintenance.
//!
//! Based on DBSP theory: each relational operator is lifted to work on Z-sets
//! (multisets with integer multiplicities). The materialized view maintains
//! the current result and propagates deltas through the dataflow graph.

use crate::dataflow::node::JoinType;
use crate::dataflow::{AggregateType, ColumnId, DataflowNode, TableId};
use crate::delta::Delta;
use crate::operators::{filter_incremental, map_incremental, project_incremental};
use alloc::collections::BTreeMap;
use alloc::vec::Vec;
use cynos_core::{Row, RowId, Value};
use hashbrown::HashMap;

// ---------------------------------------------------------------------------
// JoinState — supports Inner, Left, Right, Full Outer joins via DBSP
// ---------------------------------------------------------------------------

/// State for incremental join operations.
/// Maintains indexes for both sides and match counts for outer join support.
pub struct JoinState {
    pub left_index: HashMap<Vec<Value>, Vec<Row>>,
    pub right_index: HashMap<Vec<Value>, Vec<Row>>,
    /// For outer joins: count of right matches per left row id
    left_match_count: HashMap<RowId, usize>,
    /// For outer joins: count of left matches per right row id
    right_match_count: HashMap<RowId, usize>,
    right_col_count: usize,
    left_col_count: usize,
}

impl JoinState {
    pub fn new() -> Self {
        Self {
            left_index: HashMap::new(),
            right_index: HashMap::new(),
            left_match_count: HashMap::new(),
            right_match_count: HashMap::new(),
            right_col_count: 0,
            left_col_count: 0,
        }
    }

    /// Creates a new join state with known column counts.
    /// Required for outer joins to correctly pad NULL columns.
    pub fn with_col_counts(left_col_count: usize, right_col_count: usize) -> Self {
        Self {
            left_index: HashMap::new(),
            right_index: HashMap::new(),
            left_match_count: HashMap::new(),
            right_match_count: HashMap::new(),
            right_col_count,
            left_col_count,
        }
    }

    /// Handles a left-side insertion. Returns inner join results.
    pub fn on_left_insert(&mut self, row: Row, key: Vec<Value>) -> Vec<Row> {
        let mut output = Vec::new();
        if self.left_col_count == 0 {
            self.left_col_count = row.len();
        }
        if let Some(right_rows) = self.right_index.get(&key) {
            for r in right_rows {
                output.push(merge_rows(&row, r));
            }
        }
        self.left_index.entry(key).or_default().push(row);
        output
    }

    /// Handles a left-side deletion. Returns inner join results to remove.
    pub fn on_left_delete(&mut self, row: &Row, key: Vec<Value>) -> Vec<Row> {
        let mut output = Vec::new();
        if let Some(right_rows) = self.right_index.get(&key) {
            for r in right_rows {
                output.push(merge_rows(row, r));
            }
        }
        if let Some(left_rows) = self.left_index.get_mut(&key) {
            left_rows.retain(|l| l.id() != row.id());
            if left_rows.is_empty() {
                self.left_index.remove(&key);
            }
        }
        output
    }

    /// Handles a right-side insertion. Returns inner join results.
    pub fn on_right_insert(&mut self, row: Row, key: Vec<Value>) -> Vec<Row> {
        let mut output = Vec::new();
        if self.right_col_count == 0 {
            self.right_col_count = row.len();
        }
        if let Some(left_rows) = self.left_index.get(&key) {
            for l in left_rows {
                output.push(merge_rows(l, &row));
            }
        }
        self.right_index.entry(key).or_default().push(row);
        output
    }

    /// Handles a right-side deletion. Returns inner join results to remove.
    pub fn on_right_delete(&mut self, row: &Row, key: Vec<Value>) -> Vec<Row> {
        let mut output = Vec::new();
        if let Some(left_rows) = self.left_index.get(&key) {
            for l in left_rows {
                output.push(merge_rows(l, row));
            }
        }
        if let Some(right_rows) = self.right_index.get_mut(&key) {
            right_rows.retain(|r| r.id() != row.id());
            if right_rows.is_empty() {
                self.right_index.remove(&key);
            }
        }
        output
    }

    pub fn left_count(&self) -> usize {
        self.left_index.values().map(|v| v.len()).sum()
    }

    pub fn right_count(&self) -> usize {
        self.right_index.values().map(|v| v.len()).sum()
    }

    // --- Outer join helpers ---

    /// Process a left insert for outer join. Returns deltas including antijoin transitions.
    fn on_left_insert_outer(
        &mut self, row: Row, key: Vec<Value>, join_type: JoinType,
    ) -> Vec<Delta<Row>> {
        let mut output = Vec::new();
        if self.left_col_count == 0 {
            self.left_col_count = row.len();
        }
        let right_matches = self.right_index.get(&key).map(|v| v.len()).unwrap_or(0);

        if right_matches > 0 {
            // Has matches → emit inner join results
            for r in self.right_index.get(&key).unwrap() {
                output.push(Delta::insert(merge_rows(&row, r)));
                // Track right match count for all outer join types
                // (needed for correct delete handling)
                let rc = self.right_match_count.entry(r.id()).or_insert(0);
                if matches!(join_type, JoinType::RightOuter | JoinType::FullOuter) && *rc == 0 {
                    output.push(Delta::delete(merge_rows_null_left(r, self.left_col_count)));
                }
                *rc += 1;
            }
            // Always track left match count so we can handle left deletes
            self.left_match_count.insert(row.id(), right_matches);
        } else if matches!(join_type, JoinType::LeftOuter | JoinType::FullOuter) {
            // No matches → emit antijoin row (left + NULLs)
            output.push(Delta::insert(merge_rows_null_right(&row, self.right_col_count)));
            self.left_match_count.insert(row.id(), 0);
        }

        self.left_index.entry(key).or_default().push(row);
        output
    }

    /// Process a left delete for outer join.
    fn on_left_delete_outer(
        &mut self, row: &Row, key: Vec<Value>, join_type: JoinType,
    ) -> Vec<Delta<Row>> {
        let mut output = Vec::new();
        let match_count = self.left_match_count.remove(&row.id()).unwrap_or(0);

        if match_count > 0 {
            // Had matches → remove inner join results
            if let Some(right_rows) = self.right_index.get(&key) {
                for r in right_rows {
                    output.push(Delta::delete(merge_rows(row, r)));
                    // Always decrement right match count
                    if let Some(rc) = self.right_match_count.get_mut(&r.id()) {
                        *rc = rc.saturating_sub(1);
                        if matches!(join_type, JoinType::RightOuter | JoinType::FullOuter) && *rc == 0 {
                            output.push(Delta::insert(merge_rows_null_left(r, self.left_col_count)));
                        }
                    }
                }
            }
        } else if matches!(join_type, JoinType::LeftOuter | JoinType::FullOuter) {
            // Was unmatched → remove antijoin row
            output.push(Delta::delete(merge_rows_null_right(row, self.right_col_count)));
        }

        if let Some(left_rows) = self.left_index.get_mut(&key) {
            left_rows.retain(|l| l.id() != row.id());
            if left_rows.is_empty() { self.left_index.remove(&key); }
        }
        output
    }

    /// Process a right insert for outer join.
    fn on_right_insert_outer(
        &mut self, row: Row, key: Vec<Value>, join_type: JoinType,
    ) -> Vec<Delta<Row>> {
        let mut output = Vec::new();
        let left_matches = self.left_index.get(&key).map(|v| v.len()).unwrap_or(0);

        if self.right_col_count == 0 {
            self.right_col_count = row.len();
        }

        if left_matches > 0 {
            for l in self.left_index.get(&key).unwrap() {
                output.push(Delta::insert(merge_rows(l, &row)));
                // Track left match count for all outer join types
                let lc = self.left_match_count.entry(l.id()).or_insert(0);
                if matches!(join_type, JoinType::LeftOuter | JoinType::FullOuter) && *lc == 0 {
                    output.push(Delta::delete(merge_rows_null_right(l, self.right_col_count)));
                }
                *lc += 1;
            }
            // Always track right match count so we can handle right deletes
            self.right_match_count.insert(row.id(), left_matches);
        } else if matches!(join_type, JoinType::RightOuter | JoinType::FullOuter) {
            output.push(Delta::insert(merge_rows_null_left(&row, self.left_col_count)));
            self.right_match_count.insert(row.id(), 0);
        }

        self.right_index.entry(key).or_default().push(row);
        output
    }

    /// Process a right delete for outer join.
    fn on_right_delete_outer(
        &mut self, row: &Row, key: Vec<Value>, join_type: JoinType,
    ) -> Vec<Delta<Row>> {
        let mut output = Vec::new();
        let match_count = self.right_match_count.remove(&row.id()).unwrap_or(0);

        if match_count > 0 {
            if let Some(left_rows) = self.left_index.get(&key) {
                for l in left_rows {
                    output.push(Delta::delete(merge_rows(l, row)));
                    // Always decrement left match count
                    if let Some(lc) = self.left_match_count.get_mut(&l.id()) {
                        *lc = lc.saturating_sub(1);
                        if matches!(join_type, JoinType::LeftOuter | JoinType::FullOuter) && *lc == 0 {
                            output.push(Delta::insert(merge_rows_null_right(l, self.right_col_count)));
                        }
                    }
                }
            }
        } else if matches!(join_type, JoinType::RightOuter | JoinType::FullOuter) {
            output.push(Delta::delete(merge_rows_null_left(row, self.left_col_count)));
        }

        if let Some(right_rows) = self.right_index.get_mut(&key) {
            right_rows.retain(|r| r.id() != row.id());
            if right_rows.is_empty() { self.right_index.remove(&key); }
        }
        output
    }
}

impl Default for JoinState {
    fn default() -> Self { Self::new() }
}

/// Merges two rows into a single joined row.
fn merge_rows(left: &Row, right: &Row) -> Row {
    let mut values = left.values().to_vec();
    values.extend(right.values().iter().cloned());
    Row::new(left.id(), values)
}

/// Merges a left row with NULL padding for right columns (left outer antijoin).
fn merge_rows_null_right(left: &Row, right_col_count: usize) -> Row {
    let mut values = left.values().to_vec();
    for _ in 0..right_col_count {
        values.push(Value::Null);
    }
    Row::new(left.id(), values)
}

/// Merges a right row with NULL padding for left columns (right outer antijoin).
fn merge_rows_null_left(right: &Row, left_col_count: usize) -> Row {
    let mut values: Vec<Value> = (0..left_col_count).map(|_| Value::Null).collect();
    values.extend(right.values().iter().cloned());
    Row::new(right.id(), values)
}

// ---------------------------------------------------------------------------
// AggregateState — DBSP-based incremental aggregation per group
// ---------------------------------------------------------------------------

/// Per-function aggregate state. Uses DBSP Z-set approach:
/// - COUNT/SUM/AVG: maintain running totals, O(1) per delta
/// - MIN/MAX: maintain ordered multiset (BTreeMap), O(log n) per delta
///   This eliminates the `needs_recompute` fallback entirely.
pub enum AggregateState {
    Count { count: i64 },
    Sum { sum: f64 },
    Avg { sum: f64, count: i64 },
    /// BTreeMap<Value, multiplicity> — ordered multiset for O(log n) min on delete
    Min { values: BTreeMap<Value, i32> },
    /// BTreeMap<Value, multiplicity> — ordered multiset for O(log n) max on delete
    Max { values: BTreeMap<Value, i32> },
}

impl AggregateState {
    pub fn new(agg_type: AggregateType) -> Self {
        match agg_type {
            AggregateType::Count => AggregateState::Count { count: 0 },
            AggregateType::Sum => AggregateState::Sum { sum: 0.0 },
            AggregateType::Avg => AggregateState::Avg { sum: 0.0, count: 0 },
            AggregateType::Min => AggregateState::Min { values: BTreeMap::new() },
            AggregateType::Max => AggregateState::Max { values: BTreeMap::new() },
        }
    }

    /// Apply a single delta to this aggregate state.
    pub fn apply(&mut self, value: &Value, diff: i32) {
        match self {
            AggregateState::Count { count } => {
                *count += diff as i64;
            }
            AggregateState::Sum { sum } => {
                *sum += extract_numeric(value) * diff as f64;
            }
            AggregateState::Avg { sum, count } => {
                *sum += extract_numeric(value) * diff as f64;
                *count += diff as i64;
            }
            AggregateState::Min { values } | AggregateState::Max { values } => {
                let entry = values.entry(value.clone()).or_insert(0);
                *entry += diff;
                if *entry <= 0 {
                    values.remove(value);
                }
            }
        }
    }

    /// Get the current aggregate value.
    pub fn get_value(&self) -> Value {
        match self {
            AggregateState::Count { count } => Value::Int64(*count),
            AggregateState::Sum { sum } => Value::Float64(*sum),
            AggregateState::Avg { sum, count } => {
                if *count == 0 {
                    Value::Null
                } else {
                    Value::Float64(*sum / *count as f64)
                }
            }
            AggregateState::Min { values } => {
                values.keys().next().cloned().unwrap_or(Value::Null)
            }
            AggregateState::Max { values } => {
                values.keys().next_back().cloned().unwrap_or(Value::Null)
            }
        }
    }

    /// Returns true if this aggregate group is empty (count dropped to 0).
    pub fn is_empty(&self) -> bool {
        match self {
            AggregateState::Count { count } => *count == 0,
            AggregateState::Avg { count, .. } => *count == 0,
            AggregateState::Sum { sum } => *sum == 0.0,
            AggregateState::Min { values } | AggregateState::Max { values } => values.is_empty(),
        }
    }
}

/// State for incremental GROUP BY aggregation.
/// Maps group_key -> (per-function states, current output row).
pub struct GroupAggregateState {
    /// group_key_values -> Vec<AggregateState> (one per aggregate function)
    groups: HashMap<Vec<Value>, Vec<AggregateState>>,
    /// The aggregate function types (for creating new states)
    functions: Vec<(ColumnId, AggregateType)>,
    /// The group-by column indices
    group_by: Vec<ColumnId>,
    /// Track the last emitted row ID per group key, so deletes use the correct ID
    last_row_ids: HashMap<Vec<Value>, RowId>,
    /// Monotonic counter for generating unique aggregate output row IDs.
    /// Uses a high base (0xA660...) to avoid collision with real row IDs.
    next_row_id: RowId,
}

impl GroupAggregateState {
    pub fn new(group_by: Vec<ColumnId>, functions: Vec<(ColumnId, AggregateType)>) -> Self {
        Self {
            groups: HashMap::new(),
            functions,
            group_by,
            last_row_ids: HashMap::new(),
            next_row_id: 0xA660_0000_0000_0000,
        }
    }

    /// Process a batch of input deltas and produce output deltas.
    /// For each affected group:
    ///   1. Emit delete(old_aggregate_row) if group existed
    ///   2. Update aggregate states
    ///   3. Emit insert(new_aggregate_row) if group still has data
    pub fn process_deltas(&mut self, deltas: &[Delta<Row>]) -> Vec<Delta<Row>> {
        // Collect deltas by group key
        let mut grouped: HashMap<Vec<Value>, Vec<(&Row, i32)>> = HashMap::new();
        for d in deltas {
            let key: Vec<Value> = self.group_by.iter()
                .map(|&col| d.data.get(col).cloned().unwrap_or(Value::Null))
                .collect();
            grouped.entry(key).or_default().push((&d.data, d.diff));
        }

        let mut output = Vec::new();

        for (key, rows) in grouped {
            let existed = self.groups.contains_key(&key);

            // Snapshot old value before update
            let old_row = if existed {
                Some(self.build_output_row(&key))
            } else {
                None
            };

            // Get or create group state
            let states = self.groups.entry(key.clone()).or_insert_with(|| {
                self.functions.iter().map(|(_, agg_type)| AggregateState::new(*agg_type)).collect()
            });

            // Apply all deltas for this group
            for (row, diff) in &rows {
                for (i, (col, _)) in self.functions.iter().enumerate() {
                    let value = row.get(*col).cloned().unwrap_or(Value::Null);
                    states[i].apply(&value, *diff);
                }
            }

            // Check if group is now empty
            let is_empty = states.iter().all(|s| s.is_empty());

            // Emit old row deletion if group existed (use tracked row ID)
            if let Some(&old_id) = self.last_row_ids.get(&key) {
                if let Some(old) = old_row {
                    let mut old_with_id = old;
                    old_with_id.set_id(old_id);
                    output.push(Delta::delete(old_with_id));
                }
            }

            // Emit new row insertion if group still has data
            if !is_empty {
                let new_row = self.build_output_row(&key);
                self.last_row_ids.insert(key.clone(), new_row.id());
                output.push(Delta::insert(new_row));
            } else {
                self.groups.remove(&key);
                self.last_row_ids.remove(&key);
            }
        }

        output
    }

    /// Build an output row from group key + aggregate values.
    fn build_output_row(&mut self, key: &[Value]) -> Row {
        let states = self.groups.get(key).unwrap();
        let mut values: Vec<Value> = key.to_vec();
        for state in states {
            values.push(state.get_value());
        }
        let id = self.next_row_id;
        self.next_row_id += 1;
        Row::new(id, values)
    }
}

fn extract_numeric(value: &Value) -> f64 {
    match value {
        Value::Int32(v) => *v as f64,
        Value::Int64(v) => *v as f64,
        Value::Float64(v) => *v,
        _ => 0.0,
    }
}

// ---------------------------------------------------------------------------
// MaterializedView — the core DBSP dataflow executor
// ---------------------------------------------------------------------------

/// A materialized view that maintains query results incrementally.
pub struct MaterializedView {
    dataflow: DataflowNode,
    result_map: HashMap<RowId, Row>,
    dependencies: Vec<TableId>,
    join_states: HashMap<usize, JoinState>,
    aggregate_states: HashMap<usize, GroupAggregateState>,
}

impl MaterializedView {
    pub fn new(dataflow: DataflowNode) -> Self {
        let dependencies = dataflow.collect_sources();
        Self {
            dataflow,
            result_map: HashMap::new(),
            dependencies,
            join_states: HashMap::new(),
            aggregate_states: HashMap::new(),
        }
    }

    pub fn with_initial(dataflow: DataflowNode, initial: Vec<Row>) -> Self {
        let dependencies = dataflow.collect_sources();
        let mut result_map = HashMap::with_capacity(initial.len());
        for row in initial {
            result_map.insert(row.id(), row);
        }
        Self {
            dataflow,
            result_map,
            dependencies,
            join_states: HashMap::new(),
            aggregate_states: HashMap::new(),
        }
    }

    pub fn initialize_join_state(
        &mut self,
        left_rows: &[Row],
        right_rows: &[Row],
        left_key_fn: impl Fn(&Row) -> Vec<Value>,
        right_key_fn: impl Fn(&Row) -> Vec<Value>,
    ) {
        let join_state = self.join_states.entry(0).or_insert_with(JoinState::new);
        for row in left_rows {
            let key = left_key_fn(row);
            join_state.left_index.entry(key).or_default().push(row.clone());
        }
        for row in right_rows {
            let key = right_key_fn(row);
            join_state.right_index.entry(key).or_default().push(row.clone());
        }
    }

    #[inline]
    pub fn result(&self) -> Vec<Row> {
        self.result_map.values().cloned().collect()
    }

    #[inline]
    pub fn len(&self) -> usize { self.result_map.len() }

    #[inline]
    pub fn is_empty(&self) -> bool { self.result_map.is_empty() }

    #[inline]
    pub fn dependencies(&self) -> &[TableId] { &self.dependencies }

    pub fn depends_on(&self, table_id: TableId) -> bool {
        self.dependencies.contains(&table_id)
    }

    /// Handles changes to a source table.
    /// Propagates deltas through the dataflow and updates the result.
    pub fn on_table_change(
        &mut self,
        table_id: TableId,
        deltas: Vec<Delta<Row>>,
    ) -> Vec<Delta<Row>> {
        if !self.depends_on(table_id) {
            return Vec::new();
        }

        let dataflow_ptr = &self.dataflow as *const DataflowNode;
        let output_deltas = unsafe {
            self.propagate_mut(&*dataflow_ptr, table_id, deltas, 0, 0).0
        };

        // Apply output deltas to result
        for delta in &output_deltas {
            if delta.is_insert() {
                self.result_map.insert(delta.data.id(), delta.data.clone());
            } else if delta.is_delete() {
                self.result_map.remove(&delta.data.id());
            }
        }

        output_deltas
    }

    /// Propagates deltas through a dataflow node.
    /// Returns (output_deltas, next_join_id, next_agg_id).
    fn propagate_mut(
        &mut self,
        node: &DataflowNode,
        source_table: TableId,
        deltas: Vec<Delta<Row>>,
        join_id: usize,
        agg_id: usize,
    ) -> (Vec<Delta<Row>>, usize, usize) {
        match node {
            DataflowNode::Source { table_id } => {
                if *table_id == source_table {
                    (deltas, join_id, agg_id)
                } else {
                    (Vec::new(), join_id, agg_id)
                }
            }

            DataflowNode::Filter { input, predicate } => {
                let (input_deltas, jid, aid) =
                    self.propagate_mut(input, source_table, deltas, join_id, agg_id);
                (filter_incremental(&input_deltas, |row| predicate(row)), jid, aid)
            }

            DataflowNode::Project { input, columns } => {
                let (input_deltas, jid, aid) =
                    self.propagate_mut(input, source_table, deltas, join_id, agg_id);
                (project_incremental(&input_deltas, columns), jid, aid)
            }

            DataflowNode::Map { input, mapper } => {
                let (input_deltas, jid, aid) =
                    self.propagate_mut(input, source_table, deltas, join_id, agg_id);
                (map_incremental(&input_deltas, |row| mapper(row)), jid, aid)
            }

            DataflowNode::Join {
                left, right, left_key, right_key, join_type,
            } => {
                let current_join_id = join_id;
                if !self.join_states.contains_key(&current_join_id) {
                    self.join_states.insert(current_join_id, JoinState::new());
                }

                let left_sources = left.collect_sources();
                let right_sources = right.collect_sources();
                let is_left_side = left_sources.contains(&source_table);
                let is_right_side = right_sources.contains(&source_table);
                let jt = *join_type;

                let mut output_deltas = Vec::new();

                if is_left_side {
                    let (left_deltas, _, _) =
                        self.propagate_mut(left, source_table, deltas.clone(), current_join_id + 1, agg_id);

                    let join_state = self.join_states.get_mut(&current_join_id).unwrap();
                    for delta in left_deltas {
                        let key = left_key(&delta.data);
                        if jt == JoinType::Inner {
                            // Fast path for inner join
                            if delta.is_insert() {
                                for row in join_state.on_left_insert(delta.data, key) {
                                    output_deltas.push(Delta::insert(row));
                                }
                            } else if delta.is_delete() {
                                for row in join_state.on_left_delete(&delta.data, key) {
                                    output_deltas.push(Delta::delete(row));
                                }
                            }
                        } else if delta.is_insert() {
                            output_deltas.extend(join_state.on_left_insert_outer(delta.data, key, jt));
                        } else if delta.is_delete() {
                            output_deltas.extend(join_state.on_left_delete_outer(&delta.data, key, jt));
                        }
                    }
                }

                if is_right_side {
                    let (right_deltas, _, _) =
                        self.propagate_mut(right, source_table, deltas, current_join_id + 1, agg_id);

                    let join_state = self.join_states.get_mut(&current_join_id).unwrap();
                    for delta in right_deltas {
                        let key = right_key(&delta.data);
                        if jt == JoinType::Inner {
                            if delta.is_insert() {
                                for row in join_state.on_right_insert(delta.data, key) {
                                    output_deltas.push(Delta::insert(row));
                                }
                            } else if delta.is_delete() {
                                for row in join_state.on_right_delete(&delta.data, key) {
                                    output_deltas.push(Delta::delete(row));
                                }
                            }
                        } else if delta.is_insert() {
                            output_deltas.extend(join_state.on_right_insert_outer(delta.data, key, jt));
                        } else if delta.is_delete() {
                            output_deltas.extend(join_state.on_right_delete_outer(&delta.data, key, jt));
                        }
                    }
                }

                (output_deltas, current_join_id + 1, agg_id)
            }

            DataflowNode::Aggregate { input, group_by, functions } => {
                let current_agg_id = agg_id;
                let (input_deltas, jid, _) =
                    self.propagate_mut(input, source_table, deltas, join_id, current_agg_id + 1);

                if input_deltas.is_empty() {
                    return (Vec::new(), jid, current_agg_id + 1);
                }

                // Get or create aggregate state
                if !self.aggregate_states.contains_key(&current_agg_id) {
                    self.aggregate_states.insert(
                        current_agg_id,
                        GroupAggregateState::new(group_by.clone(), functions.clone()),
                    );
                }

                let agg_state = self.aggregate_states.get_mut(&current_agg_id).unwrap();
                let output = agg_state.process_deltas(&input_deltas);

                (output, jid, current_agg_id + 1)
            }
        }
    }

    pub fn clear(&mut self) {
        self.result_map.clear();
    }

    pub fn set_result(&mut self, rows: Vec<Row>) {
        self.result_map.clear();
        for row in rows {
            self.result_map.insert(row.id(), row);
        }
    }
}

/// Builder for creating materialized views.
pub struct MaterializedViewBuilder {
    dataflow: Option<DataflowNode>,
    initial: Vec<Row>,
}

impl Default for MaterializedViewBuilder {
    fn default() -> Self { Self::new() }
}

impl MaterializedViewBuilder {
    pub fn new() -> Self {
        Self { dataflow: None, initial: Vec::new() }
    }

    pub fn dataflow(mut self, dataflow: DataflowNode) -> Self {
        self.dataflow = Some(dataflow);
        self
    }

    pub fn initial(mut self, rows: Vec<Row>) -> Self {
        self.initial = rows;
        self
    }

    pub fn build(self) -> Option<MaterializedView> {
        self.dataflow.map(|df| {
            if self.initial.is_empty() {
                MaterializedView::new(df)
            } else {
                MaterializedView::with_initial(df, self.initial)
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloc::boxed::Box;
    use alloc::vec;
    use cynos_core::Value;

    fn make_row(id: u64, age: i64) -> Row {
        Row::new(id, vec![Value::Int64(id as i64), Value::Int64(age)])
    }

    #[test]
    fn test_materialized_view_new() {
        let dataflow = DataflowNode::source(1);
        let view = MaterializedView::new(dataflow);
        assert!(view.is_empty());
        assert_eq!(view.dependencies(), &[1]);
    }

    #[test]
    fn test_materialized_view_source_propagation() {
        let dataflow = DataflowNode::source(1);
        let mut view = MaterializedView::new(dataflow);
        let deltas = vec![Delta::insert(make_row(1, 25)), Delta::insert(make_row(2, 30))];
        let output = view.on_table_change(1, deltas);
        assert_eq!(output.len(), 2);
        assert_eq!(view.len(), 2);
    }

    #[test]
    fn test_materialized_view_filter_propagation() {
        let dataflow = DataflowNode::filter(DataflowNode::source(1), |row| {
            row.get(1).and_then(|v| v.as_i64()).map(|age| age > 18).unwrap_or(false)
        });
        let mut view = MaterializedView::new(dataflow);
        let deltas = vec![
            Delta::insert(make_row(1, 25)),
            Delta::insert(make_row(2, 15)),
            Delta::insert(make_row(3, 30)),
        ];
        let output = view.on_table_change(1, deltas);
        assert_eq!(output.len(), 2);
        assert_eq!(view.len(), 2);
    }

    #[test]
    fn test_materialized_view_delete() {
        let dataflow = DataflowNode::source(1);
        let mut view = MaterializedView::new(dataflow);
        view.on_table_change(1, vec![Delta::insert(make_row(1, 25))]);
        assert_eq!(view.len(), 1);
        view.on_table_change(1, vec![Delta::delete(make_row(1, 25))]);
        assert_eq!(view.len(), 0);
    }

    #[test]
    fn test_materialized_view_wrong_table() {
        let dataflow = DataflowNode::source(1);
        let mut view = MaterializedView::new(dataflow);
        let output = view.on_table_change(2, vec![Delta::insert(make_row(1, 25))]);
        assert!(output.is_empty());
        assert!(view.is_empty());
    }

    fn make_employee(id: u64, name_hash: i64, dept_id: i64) -> Row {
        Row::new(id, vec![Value::Int64(id as i64), Value::Int64(name_hash), Value::Int64(dept_id)])
    }

    fn make_department(id: u64, name_hash: i64) -> Row {
        Row::new(id, vec![Value::Int64(id as i64), Value::Int64(name_hash)])
    }

    #[test]
    fn test_inner_join() {
        let dataflow = DataflowNode::Join {
            left: Box::new(DataflowNode::source(1)),
            right: Box::new(DataflowNode::source(2)),
            left_key: Box::new(|row| vec![row.get(2).cloned().unwrap_or(Value::Null)]),
            right_key: Box::new(|row| vec![row.get(0).cloned().unwrap_or(Value::Null)]),
            join_type: JoinType::Inner,
        };
        let mut view = MaterializedView::new(dataflow);

        view.on_table_change(2, vec![Delta::insert(make_department(10, 100))]);
        let output = view.on_table_change(1, vec![Delta::insert(make_employee(1, 200, 10))]);
        assert_eq!(output.len(), 1);
        assert_eq!(view.len(), 1);
    }

    #[test]
    fn test_left_outer_join_no_match() {
        let dataflow = DataflowNode::Join {
            left: Box::new(DataflowNode::source(1)),
            right: Box::new(DataflowNode::source(2)),
            left_key: Box::new(|row| vec![row.get(2).cloned().unwrap_or(Value::Null)]),
            right_key: Box::new(|row| vec![row.get(0).cloned().unwrap_or(Value::Null)]),
            join_type: JoinType::LeftOuter,
        };
        let mut view = MaterializedView::new(dataflow);

        // Insert a department first so JoinState learns right_col_count
        view.on_table_change(2, vec![Delta::insert(make_department(10, 100))]);
        view.on_table_change(2, vec![Delta::delete(make_department(10, 100))]);

        // Insert employee with no matching department
        let output = view.on_table_change(1, vec![Delta::insert(make_employee(1, 200, 99))]);
        // Should get antijoin row: employee + NULLs
        assert_eq!(output.len(), 1);
        assert!(output[0].is_insert());
        let row = &output[0].data;
        // 3 employee cols + 2 NULL cols = 5
        assert_eq!(row.len(), 5);
        assert_eq!(row.get(3), Some(&Value::Null));
        assert_eq!(row.get(4), Some(&Value::Null));
    }

    #[test]
    fn test_left_outer_join_match_then_unmatch() {
        let dataflow = DataflowNode::Join {
            left: Box::new(DataflowNode::source(1)),
            right: Box::new(DataflowNode::source(2)),
            left_key: Box::new(|row| vec![row.get(2).cloned().unwrap_or(Value::Null)]),
            right_key: Box::new(|row| vec![row.get(0).cloned().unwrap_or(Value::Null)]),
            join_type: JoinType::LeftOuter,
        };
        let mut view = MaterializedView::new(dataflow);

        // Insert employee (no dept yet → antijoin)
        // Need to set right_col_count first by inserting a dept
        view.on_table_change(2, vec![Delta::insert(make_department(10, 100))]);
        view.on_table_change(1, vec![Delta::insert(make_employee(1, 200, 10))]);
        // Should have inner join result
        assert_eq!(view.len(), 1);

        // Delete department → employee becomes unmatched
        let output = view.on_table_change(2, vec![Delta::delete(make_department(10, 100))]);
        // Should delete inner join row and insert antijoin row
        let inserts: Vec<_> = output.iter().filter(|d| d.is_insert()).collect();
        let deletes: Vec<_> = output.iter().filter(|d| d.is_delete()).collect();
        assert_eq!(deletes.len(), 1); // remove inner join
        assert_eq!(inserts.len(), 1); // add antijoin
        // Antijoin row should have NULLs for right side
        assert_eq!(inserts[0].data.get(3), Some(&Value::Null));
    }

    #[test]
    fn test_aggregate_count_sum() {
        // GROUP BY column 0, COUNT(*) and SUM(column 1)
        let dataflow = DataflowNode::Aggregate {
            input: Box::new(DataflowNode::source(1)),
            group_by: vec![0],
            functions: vec![(0, AggregateType::Count), (1, AggregateType::Sum)],
        };
        let mut view = MaterializedView::new(dataflow);

        // Insert rows with group key = 1
        let output = view.on_table_change(1, vec![
            Delta::insert(Row::new(1, vec![Value::Int64(1), Value::Int64(10)])),
            Delta::insert(Row::new(2, vec![Value::Int64(1), Value::Int64(20)])),
        ]);

        // Should have one group with count=2, sum=30
        let inserts: Vec<_> = output.iter().filter(|d| d.is_insert()).collect();
        assert!(!inserts.is_empty());
        let last_insert = inserts.last().unwrap();
        // group_key(1), count(2), sum(30)
        assert_eq!(last_insert.data.get(0), Some(&Value::Int64(1)));
        assert_eq!(last_insert.data.get(1), Some(&Value::Int64(2)));
        assert_eq!(last_insert.data.get(2), Some(&Value::Float64(30.0)));
    }

    #[test]
    fn test_aggregate_min_max_delete() {
        // GROUP BY column 0, MIN(column 1), MAX(column 1)
        let dataflow = DataflowNode::Aggregate {
            input: Box::new(DataflowNode::source(1)),
            group_by: vec![0],
            functions: vec![(1, AggregateType::Min), (1, AggregateType::Max)],
        };
        let mut view = MaterializedView::new(dataflow);

        // Insert 3 rows
        view.on_table_change(1, vec![
            Delta::insert(Row::new(1, vec![Value::Int64(1), Value::Int64(10)])),
            Delta::insert(Row::new(2, vec![Value::Int64(1), Value::Int64(30)])),
            Delta::insert(Row::new(3, vec![Value::Int64(1), Value::Int64(20)])),
        ]);

        // Delete the min value (10) — should NOT need recompute, BTreeMap handles it
        let output = view.on_table_change(1, vec![
            Delta::delete(Row::new(1, vec![Value::Int64(1), Value::Int64(10)])),
        ]);

        // Should have delete(old) + insert(new)
        let inserts: Vec<_> = output.iter().filter(|d| d.is_insert()).collect();
        assert_eq!(inserts.len(), 1);
        // New min should be 20, max still 30
        assert_eq!(inserts[0].data.get(1), Some(&Value::Int64(20)));
        assert_eq!(inserts[0].data.get(2), Some(&Value::Int64(30)));
    }

    #[test]
    fn test_builder() {
        let view = MaterializedViewBuilder::new()
            .dataflow(DataflowNode::source(1))
            .initial(vec![make_row(1, 25)])
            .build()
            .unwrap();
        assert_eq!(view.len(), 1);
    }
}
