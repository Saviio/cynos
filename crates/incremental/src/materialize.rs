//! Materialized view for Incremental View Maintenance.
//!
//! A materialized view caches the result of a query and updates it
//! incrementally when the underlying data changes.

use crate::dataflow::{DataflowNode, TableId};
use crate::delta::Delta;
use crate::operators::{filter_incremental, map_incremental, project_incremental};
use alloc::vec::Vec;
use cynos_core::{Row, RowId, Value};
use hashbrown::HashMap;

/// State for incremental join operations.
/// Maintains indexes for both sides of the join.
pub struct JoinState {
    /// Left side index: key -> list of rows
    left_index: HashMap<Vec<Value>, Vec<Row>>,
    /// Right side index: key -> list of rows
    right_index: HashMap<Vec<Value>, Vec<Row>>,
}

impl JoinState {
    /// Creates a new empty join state.
    pub fn new() -> Self {
        Self {
            left_index: HashMap::new(),
            right_index: HashMap::new(),
        }
    }

    /// Handles a left-side insertion.
    pub fn on_left_insert(&mut self, row: Row, key: Vec<Value>) -> Vec<Row> {
        let mut output = Vec::new();

        // Find matching rows from right side
        if let Some(right_rows) = self.right_index.get(&key) {
            for r in right_rows {
                output.push(merge_rows(&row, r));
            }
        }

        // Add to left index
        self.left_index.entry(key).or_default().push(row);

        output
    }

    /// Handles a left-side deletion.
    pub fn on_left_delete(&mut self, row: &Row, key: Vec<Value>) -> Vec<Row> {
        let mut output = Vec::new();

        // Find matching rows from right side
        if let Some(right_rows) = self.right_index.get(&key) {
            for r in right_rows {
                output.push(merge_rows(row, r));
            }
        }

        // Remove from left index
        if let Some(left_rows) = self.left_index.get_mut(&key) {
            left_rows.retain(|l| l.id() != row.id());
            if left_rows.is_empty() {
                self.left_index.remove(&key);
            }
        }

        output
    }

    /// Handles a right-side insertion.
    pub fn on_right_insert(&mut self, row: Row, key: Vec<Value>) -> Vec<Row> {
        let mut output = Vec::new();

        // Find matching rows from left side
        if let Some(left_rows) = self.left_index.get(&key) {
            for l in left_rows {
                output.push(merge_rows(l, &row));
            }
        }

        // Add to right index
        self.right_index.entry(key).or_default().push(row);

        output
    }

    /// Handles a right-side deletion.
    pub fn on_right_delete(&mut self, row: &Row, key: Vec<Value>) -> Vec<Row> {
        let mut output = Vec::new();

        // Find matching rows from left side
        if let Some(left_rows) = self.left_index.get(&key) {
            for l in left_rows {
                output.push(merge_rows(l, row));
            }
        }

        // Remove from right index
        if let Some(right_rows) = self.right_index.get_mut(&key) {
            right_rows.retain(|r| r.id() != row.id());
            if right_rows.is_empty() {
                self.right_index.remove(&key);
            }
        }

        output
    }

    /// Returns the number of entries in the left index.
    pub fn left_count(&self) -> usize {
        self.left_index.values().map(|v| v.len()).sum()
    }

    /// Returns the number of entries in the right index.
    pub fn right_count(&self) -> usize {
        self.right_index.values().map(|v| v.len()).sum()
    }
}

impl Default for JoinState {
    fn default() -> Self {
        Self::new()
    }
}

/// Merges two rows into a single joined row.
fn merge_rows(left: &Row, right: &Row) -> Row {
    let mut values = left.values().to_vec();
    values.extend(right.values().iter().cloned());
    // Use left row's ID for the joined row
    Row::new(left.id(), values)
}

/// A materialized view that maintains query results incrementally.
///
/// The view tracks:
/// - The dataflow definition (query plan)
/// - The current result set (stored in a HashMap for O(1) lookup/delete)
/// - Dependencies on source tables
/// - Join state for join operations
pub struct MaterializedView {
    /// The dataflow node defining this view
    dataflow: DataflowNode,
    /// Current materialized result indexed by row ID for O(1) operations
    result_map: HashMap<RowId, Row>,
    /// Tables this view depends on
    dependencies: Vec<TableId>,
    /// Join state for incremental join operations (if this view contains joins)
    join_states: HashMap<usize, JoinState>,
}

impl MaterializedView {
    /// Creates a new materialized view from a dataflow node.
    pub fn new(dataflow: DataflowNode) -> Self {
        let dependencies = dataflow.collect_sources();
        Self {
            dataflow,
            result_map: HashMap::new(),
            dependencies,
            join_states: HashMap::new(),
        }
    }

    /// Creates a materialized view with an initial result set.
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
        }
    }

    /// Initializes join state from source data.
    /// This must be called for join queries to properly track incremental changes.
    pub fn initialize_join_state(
        &mut self,
        left_rows: &[Row],
        right_rows: &[Row],
        left_key_fn: impl Fn(&Row) -> Vec<Value>,
        right_key_fn: impl Fn(&Row) -> Vec<Value>,
    ) {
        let join_state = self.join_states.entry(0).or_insert_with(JoinState::new);

        // Populate left index
        for row in left_rows {
            let key = left_key_fn(row);
            join_state.left_index.entry(key).or_default().push(row.clone());
        }

        // Populate right index
        for row in right_rows {
            let key = right_key_fn(row);
            join_state.right_index.entry(key).or_default().push(row.clone());
        }
    }

    /// Returns a reference to the current result as a Vec.
    /// Note: This creates a new Vec each time for API compatibility.
    #[inline]
    pub fn result(&self) -> Vec<Row> {
        self.result_map.values().cloned().collect()
    }

    /// Returns the number of rows in the result.
    #[inline]
    pub fn len(&self) -> usize {
        self.result_map.len()
    }

    /// Returns true if the result is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.result_map.is_empty()
    }

    /// Returns the tables this view depends on.
    #[inline]
    pub fn dependencies(&self) -> &[TableId] {
        &self.dependencies
    }

    /// Checks if this view depends on the given table.
    pub fn depends_on(&self, table_id: TableId) -> bool {
        self.dependencies.contains(&table_id)
    }

    /// Handles changes to a source table.
    ///
    /// Propagates the deltas through the dataflow and updates the result.
    /// Returns the output deltas (changes to the view result).
    pub fn on_table_change(
        &mut self,
        table_id: TableId,
        deltas: Vec<Delta<Row>>,
    ) -> Vec<Delta<Row>> {
        if !self.depends_on(table_id) {
            return Vec::new();
        }

        // Propagate through dataflow using unsafe to work around borrow checker
        // This is safe because we're only reading the dataflow while mutating join_states
        let dataflow_ptr = &self.dataflow as *const DataflowNode;
        let output_deltas = unsafe {
            self.propagate_mut(&*dataflow_ptr, table_id, deltas, 0).0
        };

        // Apply output deltas to result - O(1) operations using HashMap
        for delta in &output_deltas {
            if delta.is_insert() {
                self.result_map.insert(delta.data.id(), delta.data.clone());
            } else if delta.is_delete() {
                self.result_map.remove(&delta.data.id());
            }
        }

        output_deltas
    }

    /// Propagates deltas through a dataflow node (mutable version for join state).
    /// Returns (output_deltas, next_join_id).
    fn propagate_mut(
        &mut self,
        node: &DataflowNode,
        source_table: TableId,
        deltas: Vec<Delta<Row>>,
        join_id: usize,
    ) -> (Vec<Delta<Row>>, usize) {
        match node {
            DataflowNode::Source { table_id } => {
                if *table_id == source_table {
                    (deltas, join_id)
                } else {
                    (Vec::new(), join_id)
                }
            }

            DataflowNode::Filter { input, predicate } => {
                let (input_deltas, next_id) = self.propagate_mut(input, source_table, deltas, join_id);
                (filter_incremental(&input_deltas, |row| predicate(row)), next_id)
            }

            DataflowNode::Project { input, columns } => {
                let (input_deltas, next_id) = self.propagate_mut(input, source_table, deltas, join_id);
                (project_incremental(&input_deltas, columns), next_id)
            }

            DataflowNode::Map { input, mapper } => {
                let (input_deltas, next_id) = self.propagate_mut(input, source_table, deltas, join_id);
                (map_incremental(&input_deltas, |row| mapper(row)), next_id)
            }

            DataflowNode::Join {
                left,
                right,
                left_key,
                right_key,
            } => {
                // Get or create join state for this join node
                let current_join_id = join_id;
                if !self.join_states.contains_key(&current_join_id) {
                    self.join_states.insert(current_join_id, JoinState::new());
                }

                // Check which side the source table is on
                let left_sources = left.collect_sources();
                let right_sources = right.collect_sources();

                let is_left_side = left_sources.contains(&source_table);
                let is_right_side = right_sources.contains(&source_table);

                let mut output_deltas = Vec::new();

                if is_left_side {
                    // Propagate through left side
                    let (left_deltas, _) = self.propagate_mut(left, source_table, deltas.clone(), current_join_id + 1);

                    // Process left deltas through join
                    let join_state = self.join_states.get_mut(&current_join_id).unwrap();
                    for delta in left_deltas {
                        let key = left_key(&delta.data);
                        if delta.is_insert() {
                            let joined = join_state.on_left_insert(delta.data, key);
                            for row in joined {
                                output_deltas.push(Delta::insert(row));
                            }
                        } else if delta.is_delete() {
                            let joined = join_state.on_left_delete(&delta.data, key);
                            for row in joined {
                                output_deltas.push(Delta::delete(row));
                            }
                        }
                    }
                }

                if is_right_side {
                    // Propagate through right side
                    let (right_deltas, _) = self.propagate_mut(right, source_table, deltas, current_join_id + 1);

                    // Process right deltas through join
                    let join_state = self.join_states.get_mut(&current_join_id).unwrap();
                    for delta in right_deltas {
                        let key = right_key(&delta.data);
                        if delta.is_insert() {
                            let joined = join_state.on_right_insert(delta.data, key);
                            for row in joined {
                                output_deltas.push(Delta::insert(row));
                            }
                        } else if delta.is_delete() {
                            let joined = join_state.on_right_delete(&delta.data, key);
                            for row in joined {
                                output_deltas.push(Delta::delete(row));
                            }
                        }
                    }
                }

                (output_deltas, current_join_id + 1)
            }

            DataflowNode::Aggregate { .. } => {
                // Aggregate propagation requires maintaining aggregate state
                // This is a simplified implementation
                (Vec::new(), join_id)
            }
        }
    }

    /// Propagates deltas through a dataflow node (immutable version for non-join nodes).
    #[allow(dead_code)]
    fn propagate(
        &self,
        node: &DataflowNode,
        source_table: TableId,
        deltas: Vec<Delta<Row>>,
    ) -> Vec<Delta<Row>> {
        match node {
            DataflowNode::Source { table_id } => {
                if *table_id == source_table {
                    deltas
                } else {
                    Vec::new()
                }
            }

            DataflowNode::Filter { input, predicate } => {
                let input_deltas = self.propagate(input, source_table, deltas);
                filter_incremental(&input_deltas, |row| predicate(row))
            }

            DataflowNode::Project { input, columns } => {
                let input_deltas = self.propagate(input, source_table, deltas);
                project_incremental(&input_deltas, columns)
            }

            DataflowNode::Map { input, mapper } => {
                let input_deltas = self.propagate(input, source_table, deltas);
                map_incremental(&input_deltas, |row| mapper(row))
            }

            DataflowNode::Join { .. } => {
                // Join propagation requires mutable state - use propagate_mut instead
                Vec::new()
            }

            DataflowNode::Aggregate { .. } => {
                // Aggregate propagation requires maintaining aggregate state
                Vec::new()
            }
        }
    }

    /// Clears the result and resets the view.
    pub fn clear(&mut self) {
        self.result_map.clear();
    }

    /// Replaces the result with a new set of rows.
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
    fn default() -> Self {
        Self::new()
    }
}

impl MaterializedViewBuilder {
    /// Creates a new builder.
    pub fn new() -> Self {
        Self {
            dataflow: None,
            initial: Vec::new(),
        }
    }

    /// Sets the dataflow definition.
    pub fn dataflow(mut self, dataflow: DataflowNode) -> Self {
        self.dataflow = Some(dataflow);
        self
    }

    /// Sets the initial result.
    pub fn initial(mut self, rows: Vec<Row>) -> Self {
        self.initial = rows;
        self
    }

    /// Builds the materialized view.
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
    fn test_materialized_view_depends_on() {
        let dataflow = DataflowNode::filter(DataflowNode::source(1), |_| true);
        let view = MaterializedView::new(dataflow);

        assert!(view.depends_on(1));
        assert!(!view.depends_on(2));
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
            row.get(1)
                .and_then(|v| v.as_i64())
                .map(|age| age > 18)
                .unwrap_or(false)
        });
        let mut view = MaterializedView::new(dataflow);

        let deltas = vec![
            Delta::insert(make_row(1, 25)), // passes filter
            Delta::insert(make_row(2, 15)), // filtered out
            Delta::insert(make_row(3, 30)), // passes filter
        ];

        let output = view.on_table_change(1, deltas);

        assert_eq!(output.len(), 2);
        assert_eq!(view.len(), 2);
    }

    #[test]
    fn test_materialized_view_delete() {
        let dataflow = DataflowNode::source(1);
        let mut view = MaterializedView::new(dataflow);

        // Insert
        view.on_table_change(1, vec![Delta::insert(make_row(1, 25))]);
        assert_eq!(view.len(), 1);

        // Delete
        view.on_table_change(1, vec![Delta::delete(make_row(1, 25))]);
        assert_eq!(view.len(), 0);
    }

    #[test]
    fn test_materialized_view_wrong_table() {
        let dataflow = DataflowNode::source(1);
        let mut view = MaterializedView::new(dataflow);

        // Changes to table 2 should not affect view depending on table 1
        let output = view.on_table_change(2, vec![Delta::insert(make_row(1, 25))]);

        assert!(output.is_empty());
        assert!(view.is_empty());
    }

    #[test]
    fn test_materialized_view_builder() {
        let view = MaterializedViewBuilder::new()
            .dataflow(DataflowNode::source(1))
            .initial(vec![make_row(1, 25)])
            .build()
            .unwrap();

        assert_eq!(view.len(), 1);
    }

    // Helper to create employee row: (id, name_hash, dept_id)
    fn make_employee(id: u64, name_hash: i64, dept_id: i64) -> Row {
        Row::new(id, vec![Value::Int64(id as i64), Value::Int64(name_hash), Value::Int64(dept_id)])
    }

    // Helper to create department row: (id, name_hash)
    fn make_department(id: u64, name_hash: i64) -> Row {
        Row::new(id, vec![Value::Int64(id as i64), Value::Int64(name_hash)])
    }

    #[test]
    fn test_materialized_view_join_basic() {
        // Create join: employees JOIN departments ON employees.dept_id = departments.id
        // employees table_id = 1, departments table_id = 2
        let dataflow = DataflowNode::Join {
            left: Box::new(DataflowNode::source(1)),  // employees
            right: Box::new(DataflowNode::source(2)), // departments
            left_key: Box::new(|row| vec![row.get(2).cloned().unwrap_or(Value::Null)]),  // dept_id
            right_key: Box::new(|row| vec![row.get(0).cloned().unwrap_or(Value::Null)]), // id
        };

        let mut view = MaterializedView::new(dataflow);

        // Verify dependencies include both tables
        assert!(view.depends_on(1));
        assert!(view.depends_on(2));

        // Insert department first
        let dept_deltas = vec![Delta::insert(make_department(10, 100))]; // dept_id=10
        let output = view.on_table_change(2, dept_deltas);
        assert!(output.is_empty()); // No employees yet, no join results

        // Insert employee with matching dept_id
        let emp_deltas = vec![Delta::insert(make_employee(1, 200, 10))]; // dept_id=10
        let output = view.on_table_change(1, emp_deltas);
        assert_eq!(output.len(), 1); // Should produce one join result
        assert_eq!(view.len(), 1);

        // Verify joined row has columns from both tables
        let result = view.result();
        assert_eq!(result.len(), 1);
        let joined = &result[0];
        assert_eq!(joined.len(), 5); // 3 from employee + 2 from department
    }

    #[test]
    fn test_materialized_view_join_no_match() {
        let dataflow = DataflowNode::Join {
            left: Box::new(DataflowNode::source(1)),
            right: Box::new(DataflowNode::source(2)),
            left_key: Box::new(|row| vec![row.get(2).cloned().unwrap_or(Value::Null)]),
            right_key: Box::new(|row| vec![row.get(0).cloned().unwrap_or(Value::Null)]),
        };

        let mut view = MaterializedView::new(dataflow);

        // Insert department with id=10
        view.on_table_change(2, vec![Delta::insert(make_department(10, 100))]);

        // Insert employee with dept_id=20 (no match)
        let output = view.on_table_change(1, vec![Delta::insert(make_employee(1, 200, 20))]);
        assert!(output.is_empty());
        assert!(view.is_empty());
    }

    #[test]
    fn test_materialized_view_join_right_insert_matches_existing() {
        let dataflow = DataflowNode::Join {
            left: Box::new(DataflowNode::source(1)),
            right: Box::new(DataflowNode::source(2)),
            left_key: Box::new(|row| vec![row.get(2).cloned().unwrap_or(Value::Null)]),
            right_key: Box::new(|row| vec![row.get(0).cloned().unwrap_or(Value::Null)]),
        };

        let mut view = MaterializedView::new(dataflow);

        // Insert employee first with dept_id=20
        view.on_table_change(1, vec![Delta::insert(make_employee(1, 200, 20))]);
        assert!(view.is_empty()); // No department yet

        // Insert matching department
        let output = view.on_table_change(2, vec![Delta::insert(make_department(20, 300))]);
        assert_eq!(output.len(), 1);
        assert_eq!(view.len(), 1);
    }

    #[test]
    fn test_materialized_view_join_delete() {
        let dataflow = DataflowNode::Join {
            left: Box::new(DataflowNode::source(1)),
            right: Box::new(DataflowNode::source(2)),
            left_key: Box::new(|row| vec![row.get(2).cloned().unwrap_or(Value::Null)]),
            right_key: Box::new(|row| vec![row.get(0).cloned().unwrap_or(Value::Null)]),
        };

        let mut view = MaterializedView::new(dataflow);

        let dept = make_department(10, 100);
        let emp = make_employee(1, 200, 10);

        // Insert both
        view.on_table_change(2, vec![Delta::insert(dept.clone())]);
        view.on_table_change(1, vec![Delta::insert(emp.clone())]);
        assert_eq!(view.len(), 1);

        // Delete employee
        let output = view.on_table_change(1, vec![Delta::delete(emp)]);
        assert_eq!(output.len(), 1);
        assert!(output[0].is_delete());
        assert_eq!(view.len(), 0);
    }

    #[test]
    fn test_materialized_view_join_multiple_matches() {
        let dataflow = DataflowNode::Join {
            left: Box::new(DataflowNode::source(1)),
            right: Box::new(DataflowNode::source(2)),
            left_key: Box::new(|row| vec![row.get(2).cloned().unwrap_or(Value::Null)]),
            right_key: Box::new(|row| vec![row.get(0).cloned().unwrap_or(Value::Null)]),
        };

        let mut view = MaterializedView::new(dataflow);

        // Insert department
        view.on_table_change(2, vec![Delta::insert(make_department(10, 100))]);

        // Insert multiple employees in same department
        let output = view.on_table_change(1, vec![
            Delta::insert(make_employee(1, 200, 10)),
            Delta::insert(make_employee(2, 300, 10)),
            Delta::insert(make_employee(3, 400, 10)),
        ]);

        assert_eq!(output.len(), 3);
        assert_eq!(view.len(), 3);
    }
}
