//! Query builders for SELECT, INSERT, UPDATE, DELETE operations.
//!
//! This module provides fluent API builders for constructing and executing
//! database queries.

use crate::binary_protocol::{SchemaLayout, SchemaLayoutCache};
use crate::convert::{js_array_to_rows, js_to_value, joined_rows_to_js_array, projected_rows_to_js_array, rows_to_js_array};
use crate::expr::{Expr, ExprInner};
use crate::query_engine::{compile_plan, execute_physical_plan, execute_plan, explain_plan};
use crate::dataflow_compiler::compile_to_dataflow;
use crate::reactive_bridge::{JsChangesStream, JsIvmObservableQuery, JsObservableQuery, QueryRegistry, ReQueryObservable};
use cynos_storage::TableCache;
use crate::JsSortOrder;
use alloc::boxed::Box;
use alloc::rc::Rc;
use alloc::string::{String, ToString};
use alloc::vec::Vec;
use cynos_core::schema::Table;
use cynos_core::{reserve_row_ids, DataType, Row, Value};
use cynos_incremental::Delta;
use cynos_query::ast::{AggregateFunc, SortOrder};
use cynos_query::plan_cache::{compute_plan_fingerprint, PlanCache};
use cynos_query::planner::LogicalPlan;
use cynos_reactive::{ObservableQuery, TableId};
use core::cell::RefCell;
use wasm_bindgen::prelude::*;

/// SELECT query builder.
#[wasm_bindgen]
pub struct SelectBuilder {
    cache: Rc<RefCell<TableCache>>,
    query_registry: Rc<RefCell<QueryRegistry>>,
    table_id_map: Rc<RefCell<hashbrown::HashMap<String, TableId>>>,
    schema_layout_cache: Rc<RefCell<SchemaLayoutCache>>,
    plan_cache: Rc<RefCell<PlanCache>>,
    columns: JsValue,
    from_table: Option<String>,
    where_clause: Option<Expr>,
    order_by: Vec<(String, SortOrder)>,
    limit_val: Option<usize>,
    offset_val: Option<usize>,
    joins: Vec<JoinClause>,
    group_by_cols: Vec<String>,
    aggregates: Vec<(AggregateFunc, Option<String>)>, // (func, column_name or None for COUNT(*))
}

#[derive(Clone, Debug)]
#[allow(dead_code)]
struct JoinClause {
    table: String,       // The actual table name (for schema lookup)
    alias: Option<String>, // Optional alias (for column reference)
    condition: Expr,
    join_type: JoinType,
}

impl JoinClause {
    /// Returns the name to use for column references (alias if present, otherwise table name)
    fn reference_name(&self) -> &str {
        self.alias.as_deref().unwrap_or(&self.table)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[allow(dead_code)]
enum JoinType {
    Inner,
    Left,
    Right,
}

impl SelectBuilder {
    pub(crate) fn new(
        cache: Rc<RefCell<TableCache>>,
        query_registry: Rc<RefCell<QueryRegistry>>,
        table_id_map: Rc<RefCell<hashbrown::HashMap<String, TableId>>>,
        schema_layout_cache: Rc<RefCell<SchemaLayoutCache>>,
        plan_cache: Rc<RefCell<PlanCache>>,
        columns: JsValue,
    ) -> Self {
        Self {
            cache,
            query_registry,
            table_id_map,
            schema_layout_cache,
            plan_cache,
            columns,
            from_table: None,
            where_clause: None,
            order_by: Vec::new(),
            limit_val: None,
            offset_val: None,
            joins: Vec::new(),
            group_by_cols: Vec::new(),
            aggregates: Vec::new(),
        }
    }

    fn get_schema(&self) -> Option<Table> {
        self.from_table.as_ref().and_then(|name| {
            self.cache
                .borrow()
                .get_table(name)
                .map(|s| s.schema().clone())
        })
    }

    /// Gets column info for any table (main table or joined tables).
    /// Supports qualified column names like `users.name` and table aliases.
    fn get_column_info_any_table(&self, col_name: &str) -> Option<(String, usize, DataType)> {
        // Check if column name is qualified (contains '.')
        if let Some(dot_pos) = col_name.find('.') {
            let table_part = &col_name[..dot_pos];
            let col_part = &col_name[dot_pos + 1..];

            // First check if table_part matches the main table
            if let Some(main_table) = &self.from_table {
                if main_table == table_part {
                    if let Some(schema) = self.get_schema() {
                        if let Some(col) = schema.get_column(col_part) {
                            return Some((table_part.to_string(), col.index(), col.data_type()));
                        }
                    }
                }
            }

            // Check if table_part matches any join's alias or table name
            for join in &self.joins {
                let ref_name = join.reference_name();
                if ref_name == table_part {
                    // Use the actual table name for schema lookup
                    if let Some(store) = self.cache.borrow().get_table(&join.table) {
                        if let Some(col) = store.schema().get_column(col_part) {
                            // Return the reference name (alias if present) for consistency
                            return Some((ref_name.to_string(), col.index(), col.data_type()));
                        }
                    }
                }
            }

            // Try direct table lookup (for cases without alias)
            if let Some(info) = self.cache
                .borrow()
                .get_table(table_part)
                .and_then(|store| {
                    store.schema().get_column(col_part).map(|c| (table_part.to_string(), c.index(), c.data_type()))
                })
            {
                return Some(info);
            }
        }

        // Try the main table for unqualified column names
        if let Some(table_name) = &self.from_table {
            if let Some(schema) = self.get_schema() {
                if let Some(col) = schema.get_column(col_name) {
                    return Some((table_name.clone(), col.index(), col.data_type()));
                }
            }
        }

        // Try all joined tables
        for join in &self.joins {
            if let Some(info) = self.cache
                .borrow()
                .get_table(&join.table)
                .and_then(|store| {
                    store.schema().get_column(col_name).map(|c| (join.reference_name().to_string(), c.index(), c.data_type()))
                })
            {
                return Some(info);
            }
        }

        None
    }

    /// Parses the columns JsValue into a list of column names.
    /// Returns None if selecting all columns (empty array, undefined, or contains "*").
    fn parse_columns(&self) -> Option<Vec<String>> {
        if self.columns.is_undefined() || self.columns.is_null() {
            return None;
        }

        if let Some(arr) = self.columns.dyn_ref::<js_sys::Array>() {
            if arr.length() == 0 {
                return None; // Empty array means select all
            }

            // Check if first element is an array (nested array case from variadic)
            // e.g., select(['name', 'age']) becomes [['name', 'age']] with variadic
            let first = arr.get(0);
            if let Some(inner_arr) = first.dyn_ref::<js_sys::Array>() {
                // Handle nested array: [['name', 'age']]
                let cols: Vec<String> = inner_arr.iter().filter_map(|v| v.as_string()).collect();
                if cols.is_empty() {
                    return None;
                } else if cols.len() == 1 && cols[0] == "*" {
                    return None; // ["*"] means select all
                } else {
                    return Some(cols);
                }
            }

            // Handle flat array: ['name', 'age'] (variadic args)
            let cols: Vec<String> = arr.iter().filter_map(|v| v.as_string()).collect();
            if cols.is_empty() {
                None
            } else if cols.len() == 1 && cols[0] == "*" {
                None // ["*"] means select all
            } else {
                Some(cols)
            }
        } else if let Some(s) = self.columns.as_string() {
            if s == "*" {
                None // "*" means select all
            } else {
                Some(alloc::vec![s])
            }
        } else {
            None
        }
    }

    /// Builds a LogicalPlan from the query builder state.
    fn build_logical_plan(&self, table_name: &str) -> LogicalPlan {
        // Start with a table scan
        let mut plan = LogicalPlan::Scan {
            table: table_name.to_string(),
        };

        // Track column offsets for each table in the JOIN
        // Key: reference name (alias if present, otherwise table name), Value: starting column offset
        let mut table_offsets: hashbrown::HashMap<String, usize> = hashbrown::HashMap::new();

        // Add main table offset
        if let Some(schema) = self.get_schema() {
            table_offsets.insert(table_name.to_string(), 0);
            let mut current_offset = schema.columns().len();

            // Add JOINs if any
            for join in &self.joins {
                let right_plan = LogicalPlan::Scan {
                    table: join.table.clone(),
                };

                // Record offset using reference name (alias if present)
                let ref_name = join.reference_name().to_string();
                table_offsets.insert(ref_name.clone(), current_offset);

                // Convert join condition to AST with correct offsets
                let get_col_info = |name: &str| self.get_column_info_for_join_with_offsets_alias(name, join, &table_offsets);
                let ast_condition = join.condition.to_ast_with_table(&get_col_info);

                plan = match join.join_type {
                    JoinType::Inner => LogicalPlan::inner_join(plan, right_plan, ast_condition),
                    JoinType::Left => LogicalPlan::left_join(plan, right_plan, ast_condition),
                    JoinType::Right => {
                        // Right join is left join with swapped operands
                        LogicalPlan::left_join(right_plan, plan, ast_condition)
                    }
                };

                // Update offset for next join
                if let Some(store) = self.cache.borrow().get_table(&join.table) {
                    current_offset += store.schema().columns().len();
                }
            }
        } else {
            // Fallback: no schema available, use old logic
            for join in &self.joins {
                let right_plan = LogicalPlan::Scan {
                    table: join.table.clone(),
                };

                let get_col_info = |name: &str| self.get_column_info_for_join(name, &join.table);
                let ast_condition = join.condition.to_ast_with_table(&get_col_info);

                plan = match join.join_type {
                    JoinType::Inner => LogicalPlan::inner_join(plan, right_plan, ast_condition),
                    JoinType::Left => LogicalPlan::left_join(plan, right_plan, ast_condition),
                    JoinType::Right => {
                        LogicalPlan::left_join(right_plan, plan, ast_condition)
                    }
                };
            }
        }

        // Add filter if WHERE clause exists
        if let Some(ref predicate) = self.where_clause {
            // Use get_column_info_any_table to get the correct table-relative index
            // The optimizer may push this filter to a single table, so we need table-relative indices
            let get_col_info = |name: &str| {
                self.get_column_info_any_table(name)
            };
            let ast_predicate = predicate.to_ast_with_table(&get_col_info);
            plan = LogicalPlan::Filter {
                input: Box::new(plan),
                predicate: ast_predicate,
            };
        }

        // Add aggregate if GROUP BY or aggregate functions are specified
        if !self.group_by_cols.is_empty() || !self.aggregates.is_empty() {
            let group_by_exprs: Vec<_> = self.group_by_cols.iter().filter_map(|col| {
                self.get_column_info_for_projection(col).map(|(tbl, idx, _)| {
                    let col_name = if let Some(dot_pos) = col.find('.') {
                        &col[dot_pos + 1..]
                    } else {
                        col.as_str()
                    };
                    cynos_query::ast::Expr::column(&tbl, col_name, idx)
                })
            }).collect();

            let agg_exprs: Vec<_> = self.aggregates.iter().filter_map(|(func, col_opt)| {
                if let Some(col) = col_opt {
                    self.get_column_info_for_projection(col).map(|(tbl, idx, _)| {
                        let col_name = if let Some(dot_pos) = col.find('.') {
                            &col[dot_pos + 1..]
                        } else {
                            col.as_str()
                        };
                        (*func, cynos_query::ast::Expr::column(&tbl, col_name, idx))
                    })
                } else {
                    // COUNT(*) - use a dummy column expression
                    Some((*func, cynos_query::ast::Expr::literal(cynos_core::Value::Int64(1))))
                }
            }).collect();

            plan = LogicalPlan::aggregate(plan, group_by_exprs, agg_exprs);
        }

        // Add ORDER BY if specified
        if !self.order_by.is_empty() {
            let order_exprs: Vec<_> = self.order_by.iter().filter_map(|(col, order)| {
                // Use get_column_info_for_projection to correctly handle JOIN queries
                self.get_column_info_for_projection(col).map(|(tbl, idx, _)| {
                    // Extract just the column name if qualified
                    let col_name = if let Some(dot_pos) = col.find('.') {
                        &col[dot_pos + 1..]
                    } else {
                        col.as_str()
                    };
                    (cynos_query::ast::Expr::column(&tbl, col_name, idx), *order)
                })
            }).collect();
            plan = LogicalPlan::Sort {
                input: Box::new(plan),
                order_by: order_exprs,
            };
        }

        // Add LIMIT/OFFSET if specified
        // Use a very large limit if only offset is specified
        if self.limit_val.is_some() || self.offset_val.is_some() {
            plan = LogicalPlan::Limit {
                input: Box::new(plan),
                limit: self.limit_val.unwrap_or(1_000_000_000), // Large but safe limit
                offset: self.offset_val.unwrap_or(0),
            };
        }

        // Add projection if specific columns are selected
        if let Some(cols) = self.parse_columns() {
            let project_exprs: Vec<_> = cols
                .iter()
                .filter_map(|col| {
                    // Use get_column_info_for_projection to get the correct index for JOIN queries
                    self.get_column_info_for_projection(col).map(|(tbl, idx, _)| {
                        // Extract just the column name if qualified
                        let col_name = if let Some(dot_pos) = col.find('.') {
                            &col[dot_pos + 1..]
                        } else {
                            col.as_str()
                        };
                        cynos_query::ast::Expr::column(&tbl, col_name, idx)
                    })
                })
                .collect();

            if !project_exprs.is_empty() {
                plan = LogicalPlan::Project {
                    input: Box::new(plan),
                    columns: project_exprs,
                };
            }
        }

        plan
    }

    /// Gets column info for projection, calculating the correct index for JOIN queries.
    /// For JOIN queries, returns the table-relative index (not the absolute offset).
    /// The absolute index will be computed at runtime based on actual table order.
    /// Supports table aliases.
    fn get_column_info_for_projection(&self, col_name: &str) -> Option<(String, usize, DataType)> {
        // Check if column name is qualified (contains '.')
        let (target_table, target_col) = if let Some(dot_pos) = col_name.find('.') {
            (Some(&col_name[..dot_pos]), &col_name[dot_pos + 1..])
        } else {
            (None, col_name)
        };

        // First check the main table
        if let Some(main_table) = &self.from_table {
            if let Some(schema) = self.get_schema() {
                if target_table.is_none() || target_table == Some(main_table.as_str()) {
                    if let Some(col) = schema.get_column(target_col) {
                        // Return table-relative index, not absolute offset
                        return Some((main_table.clone(), col.index(), col.data_type()));
                    }
                }
            }
        }

        // Then check joined tables in order
        for join in &self.joins {
            if let Some(store) = self.cache.borrow().get_table(&join.table) {
                let schema = store.schema();
                let ref_name = join.reference_name();
                // Match against both the reference name (alias) and the actual table name
                if target_table.is_none() || target_table == Some(ref_name) || target_table == Some(join.table.as_str()) {
                    if let Some(col) = schema.get_column(target_col) {
                        // Return table-relative index, not absolute offset
                        return Some((ref_name.to_string(), col.index(), col.data_type()));
                    }
                }
            }
        }

        None
    }

    /// Creates a SchemaLayout for projected columns, supporting multi-table column references.
    fn create_projection_layout(&self, column_names: &[String]) -> crate::binary_protocol::SchemaLayout {
        use crate::binary_protocol::{BinaryDataType, ColumnLayout, SchemaLayout};

        // Extract just the column part from qualified names and count occurrences
        let mut name_counts: hashbrown::HashMap<&str, usize> = hashbrown::HashMap::new();
        for col_name in column_names {
            let simple_name = if let Some(dot_pos) = col_name.find('.') {
                &col_name[dot_pos + 1..]
            } else {
                col_name.as_str()
            };
            *name_counts.entry(simple_name).or_insert(0) += 1;
        }

        let mut columns: Vec<ColumnLayout> = Vec::new();
        let mut offset = 0usize;

        for name in column_names {
            // Look up column info from any table
            if let Some((_, _, data_type)) = self.get_column_info_any_table(name) {
                let binary_type = BinaryDataType::from(data_type);
                let fixed_size = binary_type.fixed_size();

                // Determine the final column name - use simple name when unique
                let final_name = if let Some(dot_pos) = name.find('.') {
                    let simple_name = &name[dot_pos + 1..];
                    if name_counts.get(simple_name).copied().unwrap_or(0) > 1 {
                        // Duplicate - keep qualified name
                        name.clone()
                    } else {
                        // Unique - use simple name
                        simple_name.to_string()
                    }
                } else {
                    name.clone()
                };

                columns.push(ColumnLayout {
                    name: final_name,
                    data_type: binary_type,
                    fixed_size,
                    is_nullable: true, // Conservative: assume nullable
                    offset,
                });
                offset += fixed_size;
            }
        }

        let null_mask_size = (columns.len() + 7) / 8;
        let data_size: usize = columns.iter().map(|c| c.fixed_size).sum();
        let row_stride = null_mask_size + data_size;

        SchemaLayout::new(columns, row_stride, null_mask_size)
    }

    /// Gets column info for JOIN conditions, checking both the main table and the join table.
    /// Returns (table_name, column_index, data_type).
    ///
    /// For JOIN conditions like `col('dept_id').eq('id')`:
    /// - The left column (dept_id) should come from the main table
    /// - The right column (id) should come from the join table
    ///
    /// Also supports qualified column names like `col('orders.year')`.
    fn get_column_info_for_join(&self, col_name: &str, join_table: &str) -> Option<(String, usize, DataType)> {
        // Check if column name is qualified (contains '.')
        if let Some(dot_pos) = col_name.find('.') {
            let table_part = &col_name[..dot_pos];
            let col_part = &col_name[dot_pos + 1..];

            // Try to find the table and column
            if let Some(info) = self.cache
                .borrow()
                .get_table(table_part)
                .and_then(|store| {
                    store.schema().get_column(col_part).map(|c| (table_part.to_string(), c.index(), c.data_type()))
                })
            {
                return Some(info);
            }

            // Also check if it matches the main table
            if let Some(table_name) = &self.from_table {
                if table_name == table_part {
                    if let Some(schema) = self.get_schema() {
                        if let Some(col) = schema.get_column(col_part) {
                            return Some((table_name.clone(), col.index(), col.data_type()));
                        }
                    }
                }
            }
        }

        // First try the join table (for the right side of JOIN conditions)
        if let Some(info) = self.cache
            .borrow()
            .get_table(join_table)
            .and_then(|store| {
                store.schema().get_column(col_name).map(|c| (join_table.to_string(), c.index(), c.data_type()))
            })
        {
            return Some(info);
        }

        // Then try the main table
        if let Some(table_name) = &self.from_table {
            if let Some(schema) = self.get_schema() {
                if let Some(col) = schema.get_column(col_name) {
                    return Some((table_name.clone(), col.index(), col.data_type()));
                }
            }
        }

        None
    }

    /// Gets column info for JOIN conditions with pre-computed table offsets and alias support.
    /// This is used for multi-table JOINs where column indices need to account for
    /// the combined row structure and table aliases.
    fn get_column_info_for_join_with_offsets_alias(
        &self,
        col_name: &str,
        current_join: &JoinClause,
        table_offsets: &hashbrown::HashMap<String, usize>,
    ) -> Option<(String, usize, DataType)> {
        let current_ref_name = current_join.reference_name();

        // Check if column name is qualified (contains '.')
        if let Some(dot_pos) = col_name.find('.') {
            let table_part = &col_name[..dot_pos];
            let col_part = &col_name[dot_pos + 1..];

            // Check if table_part matches the current join's reference name (alias or table)
            if table_part == current_ref_name {
                if let Some(store) = self.cache.borrow().get_table(&current_join.table) {
                    if let Some(col) = store.schema().get_column(col_part) {
                        // Current join table uses original index (no offset)
                        // Return actual table name for Relation compatibility
                        return Some((current_join.table.clone(), col.index(), col.data_type()));
                    }
                }
            }

            // Check if table_part matches the main table
            if let Some(main_table) = &self.from_table {
                if table_part == main_table {
                    if let Some(schema) = self.get_schema() {
                        if let Some(col) = schema.get_column(col_part) {
                            let offset = table_offsets.get(main_table).copied().unwrap_or(0);
                            return Some((table_part.to_string(), offset + col.index(), col.data_type()));
                        }
                    }
                }
            }

            // Check if table_part matches any other join's reference name
            for join in &self.joins {
                let ref_name = join.reference_name();
                if table_part == ref_name && ref_name != current_ref_name {
                    if let Some(store) = self.cache.borrow().get_table(&join.table) {
                        if let Some(col) = store.schema().get_column(col_part) {
                            let offset = table_offsets.get(ref_name).copied().unwrap_or(0);
                            return Some((table_part.to_string(), offset + col.index(), col.data_type()));
                        }
                    }
                }
            }

            // Try direct table lookup (for cases without alias)
            if let Some(store) = self.cache.borrow().get_table(table_part) {
                if let Some(col) = store.schema().get_column(col_part) {
                    let idx = if table_part == &current_join.table {
                        col.index()
                    } else {
                        let offset = table_offsets.get(table_part).copied().unwrap_or(0);
                        offset + col.index()
                    };
                    return Some((table_part.to_string(), idx, col.data_type()));
                }
            }
        }

        // For unqualified column names, try current join table first
        if let Some(store) = self.cache.borrow().get_table(&current_join.table) {
            if let Some(col) = store.schema().get_column(col_name) {
                // Current join table uses original index (no offset)
                return Some((current_ref_name.to_string(), col.index(), col.data_type()));
            }
        }

        // Then try the main table
        if let Some(table_name) = &self.from_table {
            let offset = table_offsets.get(table_name).copied().unwrap_or(0);
            if let Some(schema) = self.get_schema() {
                if let Some(col) = schema.get_column(col_name) {
                    return Some((table_name.clone(), offset + col.index(), col.data_type()));
                }
            }
        }

        None
    }
}

#[wasm_bindgen]
impl SelectBuilder {
    /// Sets the FROM table.
    pub fn from(mut self, table: &str) -> Self {
        self.from_table = Some(table.to_string());
        self
    }

    /// Sets or extends the WHERE clause.
    /// Multiple calls to where_() are combined with AND.
    #[wasm_bindgen(js_name = "where")]
    pub fn where_(mut self, predicate: &Expr) -> Self {
        self.where_clause = Some(match self.where_clause {
            Some(existing) => Expr::and(&existing, predicate),
            None => predicate.clone(),
        });
        self
    }

    /// Adds an ORDER BY clause.
    #[wasm_bindgen(js_name = orderBy)]
    pub fn order_by(mut self, column: &str, order: JsSortOrder) -> Self {
        self.order_by.push((column.to_string(), order.into()));
        self
    }

    /// Sets the LIMIT.
    pub fn limit(mut self, n: usize) -> Self {
        self.limit_val = Some(n);
        self
    }

    /// Sets the OFFSET.
    pub fn offset(mut self, n: usize) -> Self {
        self.offset_val = Some(n);
        self
    }

    /// Parses a table specification that may include an alias.
    /// Supports formats: "table_name" or "table_name as alias"
    fn parse_table_spec(table_spec: &str) -> (String, Option<String>) {
        // Check for " as " (case insensitive)
        let lower = table_spec.to_lowercase();
        if let Some(pos) = lower.find(" as ") {
            let table = table_spec[..pos].trim().to_string();
            let alias = table_spec[pos + 4..].trim().to_string();
            (table, Some(alias))
        } else {
            (table_spec.trim().to_string(), None)
        }
    }

    /// Adds an INNER JOIN.
    #[wasm_bindgen(js_name = innerJoin)]
    pub fn inner_join(mut self, table: &str, condition: &Expr) -> Self {
        let (table_name, alias) = Self::parse_table_spec(table);
        self.joins.push(JoinClause {
            table: table_name,
            alias,
            condition: condition.clone(),
            join_type: JoinType::Inner,
        });
        self
    }

    /// Adds a LEFT JOIN.
    #[wasm_bindgen(js_name = leftJoin)]
    pub fn left_join(mut self, table: &str, condition: &Expr) -> Self {
        let (table_name, alias) = Self::parse_table_spec(table);
        self.joins.push(JoinClause {
            table: table_name,
            alias,
            condition: condition.clone(),
            join_type: JoinType::Left,
        });
        self
    }

    /// Adds a GROUP BY clause.
    #[wasm_bindgen(js_name = groupBy)]
    pub fn group_by(mut self, columns: &JsValue) -> Self {
        if let Some(arr) = columns.dyn_ref::<js_sys::Array>() {
            self.group_by_cols = arr.iter().filter_map(|v| v.as_string()).collect();
        } else if let Some(s) = columns.as_string() {
            self.group_by_cols = alloc::vec![s];
        }
        self
    }

    /// Adds a COUNT(*) aggregate.
    #[wasm_bindgen(js_name = count)]
    pub fn count(mut self) -> Self {
        self.aggregates.push((AggregateFunc::Count, None));
        self
    }

    /// Adds a COUNT(column) aggregate.
    #[wasm_bindgen(js_name = countCol)]
    pub fn count_col(mut self, column: &str) -> Self {
        self.aggregates.push((AggregateFunc::Count, Some(column.to_string())));
        self
    }

    /// Adds a SUM(column) aggregate.
    #[wasm_bindgen(js_name = sum)]
    pub fn sum(mut self, column: &str) -> Self {
        self.aggregates.push((AggregateFunc::Sum, Some(column.to_string())));
        self
    }

    /// Adds an AVG(column) aggregate.
    #[wasm_bindgen(js_name = avg)]
    pub fn avg(mut self, column: &str) -> Self {
        self.aggregates.push((AggregateFunc::Avg, Some(column.to_string())));
        self
    }

    /// Adds a MIN(column) aggregate.
    #[wasm_bindgen(js_name = min)]
    pub fn min(mut self, column: &str) -> Self {
        self.aggregates.push((AggregateFunc::Min, Some(column.to_string())));
        self
    }

    /// Adds a MAX(column) aggregate.
    #[wasm_bindgen(js_name = max)]
    pub fn max(mut self, column: &str) -> Self {
        self.aggregates.push((AggregateFunc::Max, Some(column.to_string())));
        self
    }

    /// Adds a STDDEV(column) aggregate.
    #[wasm_bindgen(js_name = stddev)]
    pub fn stddev(mut self, column: &str) -> Self {
        self.aggregates.push((AggregateFunc::StdDev, Some(column.to_string())));
        self
    }

    /// Adds a GEOMEAN(column) aggregate.
    #[wasm_bindgen(js_name = geomean)]
    pub fn geomean(mut self, column: &str) -> Self {
        self.aggregates.push((AggregateFunc::GeoMean, Some(column.to_string())));
        self
    }

    /// Adds a DISTINCT(column) aggregate (returns count of distinct values).
    #[wasm_bindgen(js_name = distinct)]
    pub fn distinct(mut self, column: &str) -> Self {
        self.aggregates.push((AggregateFunc::Distinct, Some(column.to_string())));
        self
    }

    /// Executes the query and returns results.
    pub async fn exec(&self) -> Result<JsValue, JsValue> {
        let table_name = self
            .from_table
            .as_ref()
            .ok_or_else(|| JsValue::from_str("FROM table not specified"))?;

        let cache = self.cache.borrow();
        let store = cache
            .get_table(table_name)
            .ok_or_else(|| JsValue::from_str(&alloc::format!("Table not found: {}", table_name)))?;

        let schema = store.schema().clone();

        // Build logical plan using query engine
        // ORDER BY, LIMIT, and OFFSET are now handled in the logical plan
        let plan = self.build_logical_plan(table_name);

        // Execute using query engine (with index optimization)
        let rows = execute_plan(&cache, table_name, plan)
            .map_err(|e| JsValue::from_str(&alloc::format!("Query execution error: {:?}", e)))?;

        // Convert to JS array
        if !self.aggregates.is_empty() || !self.group_by_cols.is_empty() {
            // For aggregate queries, build column names from group_by + aggregates
            Ok(self.aggregate_rows_to_js_array(&rows))
        } else if let Some(cols) = self.parse_columns() {
            // When we have projection, the rows contain only the projected columns
            // in the order specified by the projection
            Ok(projected_rows_to_js_array(&rows, &cols))
        } else if !self.joins.is_empty() {
            // For JOIN queries without projection, collect all schemas and use joined conversion
            let mut schemas: Vec<&Table> = Vec::with_capacity(1 + self.joins.len());
            schemas.push(store.schema());
            for join in &self.joins {
                let join_store = cache.get_table(&join.table).ok_or_else(|| {
                    JsValue::from_str(&alloc::format!("Join table not found: {}", join.table))
                })?;
                schemas.push(join_store.schema());
            }
            Ok(joined_rows_to_js_array(&rows, &schemas))
        } else {
            Ok(rows_to_js_array(&rows, &schema))
        }
    }

    /// Builds column names for aggregate queries.
    /// Returns: group_by columns + aggregate function names (e.g., "count", "sum_value")
    fn build_aggregate_column_names(&self) -> Vec<String> {
        let mut col_names: Vec<String> = Vec::new();
        for col in &self.group_by_cols {
            // Use simple column name (without table prefix)
            let simple_name = if let Some(dot_pos) = col.find('.') {
                &col[dot_pos + 1..]
            } else {
                col.as_str()
            };
            col_names.push(simple_name.to_string());
        }
        for (func, col_opt) in &self.aggregates {
            let func_name = match func {
                AggregateFunc::Count => "count",
                AggregateFunc::Sum => "sum",
                AggregateFunc::Avg => "avg",
                AggregateFunc::Min => "min",
                AggregateFunc::Max => "max",
                AggregateFunc::Distinct => "distinct",
                AggregateFunc::StdDev => "stddev",
                AggregateFunc::GeoMean => "geomean",
            };
            let col_name = if let Some(col) = col_opt {
                let simple_name = if let Some(dot_pos) = col.find('.') {
                    &col[dot_pos + 1..]
                } else {
                    col.as_str()
                };
                alloc::format!("{}_{}", func_name, simple_name)
            } else {
                func_name.to_string() // COUNT(*)
            };
            col_names.push(col_name);
        }
        col_names
    }

    /// Converts aggregate result rows to JS array.
    fn aggregate_rows_to_js_array(&self, rows: &[Rc<Row>]) -> JsValue {
        let result = js_sys::Array::new();
        let col_names = self.build_aggregate_column_names();

        for row in rows {
            let obj = js_sys::Object::new();
            for (i, name) in col_names.iter().enumerate() {
                if let Some(value) = row.get(i) {
                    let js_val = crate::convert::value_to_js(value);
                    let _ = js_sys::Reflect::set(&obj, &JsValue::from_str(name), &js_val);
                }
            }
            result.push(&obj);
        }

        result.into()
    }

    /// Explains the query plan without executing it.
    ///
    /// Returns an object with:
    /// - `logical`: The original logical plan
    /// - `optimized`: The optimized logical plan (after index selection, etc.)
    /// - `physical`: The final physical execution plan
    pub fn explain(&self) -> Result<JsValue, JsValue> {
        let table_name = self
            .from_table
            .as_ref()
            .ok_or_else(|| JsValue::from_str("FROM table not specified"))?;

        let cache = self.cache.borrow();
        let _ = cache
            .get_table(table_name)
            .ok_or_else(|| JsValue::from_str(&alloc::format!("Table not found: {}", table_name)))?;

        // Build logical plan
        let plan = self.build_logical_plan(table_name);

        // Get explain result
        let result = explain_plan(&cache, table_name, plan);

        // Convert to JS object
        let obj = js_sys::Object::new();
        js_sys::Reflect::set(&obj, &"logical".into(), &result.logical_plan.into())?;
        js_sys::Reflect::set(&obj, &"optimized".into(), &result.optimized_plan.into())?;
        js_sys::Reflect::set(&obj, &"physical".into(), &result.physical_plan.into())?;

        Ok(obj.into())
    }

    /// Creates an observable query using re-query strategy.
    /// When data changes, the cached physical plan is re-executed (no optimization overhead).
    pub fn observe(&self) -> Result<JsObservableQuery, JsValue> {
        let table_name = self
            .from_table
            .as_ref()
            .ok_or_else(|| JsValue::from_str("FROM table not specified"))?;

        let table_id = self
            .table_id_map
            .borrow()
            .get(table_name)
            .copied()
            .ok_or_else(|| JsValue::from_str(&alloc::format!("Table ID not found: {}", table_name)))?;

        let cache_ref = self.cache.clone();
        let cache = cache_ref.borrow();
        let store = cache
            .get_table(table_name)
            .ok_or_else(|| JsValue::from_str(&alloc::format!("Table not found: {}", table_name)))?;

        let schema = store.schema().clone();

        // Build binary layout: merge main table + all joined table schemas
        let binary_layout = if self.joins.is_empty() {
            if let Some(cols) = self.parse_columns() {
                SchemaLayout::from_projection(&schema, &cols)
            } else {
                SchemaLayout::from_schema(&schema)
            }
        } else {
            let mut schemas: Vec<&Table> = Vec::with_capacity(1 + self.joins.len());
            schemas.push(store.schema());
            for join in &self.joins {
                let join_store = cache.get_table(&join.table).ok_or_else(|| {
                    JsValue::from_str(&alloc::format!("Join table not found: {}", join.table))
                })?;
                schemas.push(join_store.schema());
            }
            SchemaLayout::from_schemas(&schemas)
        };

        // Build logical plan and compile to physical plan (cached for re-execution)
        let logical_plan = self.build_logical_plan(table_name);
        let physical_plan = compile_plan(&cache, table_name, logical_plan.clone());

        // Get initial result using the compiled physical plan
        let initial_rows = execute_physical_plan(&cache, &physical_plan)
            .map_err(|e| JsValue::from_str(&alloc::format!("Query execution error: {:?}", e)))?;

        drop(cache); // Release borrow

        // Create re-query observable with cached physical plan
        let observable = ReQueryObservable::new(
            physical_plan,
            cache_ref.clone(),
            initial_rows,
        );
        let observable_rc = Rc::new(RefCell::new(observable));

        // Register with query registry
        self.query_registry
            .borrow_mut()
            .register(observable_rc.clone(), table_id);

        // Return observable with appropriate column info
        if !self.aggregates.is_empty() || !self.group_by_cols.is_empty() {
            // For aggregate queries, build column names from group_by + aggregates
            let aggregate_cols = self.build_aggregate_column_names();
            Ok(JsObservableQuery::new_with_aggregates(observable_rc, schema, aggregate_cols, binary_layout))
        } else if let Some(cols) = self.parse_columns() {
            Ok(JsObservableQuery::new_with_projection(observable_rc, schema, cols, binary_layout))
        } else {
            Ok(JsObservableQuery::new(observable_rc, schema, binary_layout))
        }
    }

    /// Creates a changes stream (initial + incremental).
    pub fn changes(&self) -> Result<JsChangesStream, JsValue> {
        let observable = self.observe()?;
        Ok(JsChangesStream::from_observable(observable))
    }

    /// Creates an IVM-based observable query using DBSP incremental dataflow.
    ///
    /// Unlike `observe()` which re-executes the full query on every change (O(result_set)),
    /// `trace()` compiles the query into a dataflow graph and propagates only deltas (O(delta)).
    ///
    /// Returns an error if the query is not incrementalizable (e.g. contains ORDER BY / LIMIT).
    pub fn trace(&self) -> Result<JsIvmObservableQuery, JsValue> {
        let table_name = self
            .from_table
            .as_ref()
            .ok_or_else(|| JsValue::from_str("FROM table not specified"))?;

        let cache_ref = self.cache.clone();
        let cache = cache_ref.borrow();
        let store = cache
            .get_table(table_name)
            .ok_or_else(|| JsValue::from_str(&alloc::format!("Table not found: {}", table_name)))?;

        let schema = store.schema().clone();

        // Build binary layout
        let binary_layout = if self.joins.is_empty() {
            if let Some(cols) = self.parse_columns() {
                SchemaLayout::from_projection(&schema, &cols)
            } else {
                SchemaLayout::from_schema(&schema)
            }
        } else {
            let mut schemas: Vec<&Table> = Vec::with_capacity(1 + self.joins.len());
            schemas.push(store.schema());
            for join in &self.joins {
                let join_store = cache.get_table(&join.table).ok_or_else(|| {
                    JsValue::from_str(&alloc::format!("Join table not found: {}", join.table))
                })?;
                schemas.push(join_store.schema());
            }
            SchemaLayout::from_schemas(&schemas)
        };

        // Build logical plan and compile to physical plan
        let logical_plan = self.build_logical_plan(table_name);
        let physical_plan = compile_plan(&cache, table_name, logical_plan);

        // Compile physical plan to dataflow — errors if not incrementalizable
        let table_id_map = self.table_id_map.borrow();
        let compile_result = compile_to_dataflow(&physical_plan, &table_id_map)
            .ok_or_else(|| JsValue::from_str(
                "Query is not incrementalizable (contains ORDER BY, LIMIT, or other non-streamable operators). Use observe() instead."
            ))?;

        // Get initial result using the compiled physical plan
        let initial_rows = execute_physical_plan(&cache, &physical_plan)
            .map_err(|e| JsValue::from_str(&alloc::format!("Query execution error: {:?}", e)))?;

        drop(cache);
        drop(table_id_map);

        // Convert Rc<Row> → Row for ObservableQuery
        let initial_owned: Vec<Row> = initial_rows.iter().map(|rc| (**rc).clone()).collect();

        // Create IVM observable with dataflow and initial result
        let observable = ObservableQuery::with_initial(compile_result.dataflow, initial_owned);
        let observable_rc = Rc::new(RefCell::new(observable));

        // Register with query registry for IVM delta propagation
        self.query_registry
            .borrow_mut()
            .register_ivm(observable_rc.clone());

        // Return with appropriate column info
        if !self.aggregates.is_empty() || !self.group_by_cols.is_empty() {
            let aggregate_cols = self.build_aggregate_column_names();
            Ok(JsIvmObservableQuery::new_with_aggregates(observable_rc, schema, aggregate_cols, binary_layout))
        } else if let Some(cols) = self.parse_columns() {
            Ok(JsIvmObservableQuery::new_with_projection(observable_rc, schema, cols, binary_layout))
        } else {
            Ok(JsIvmObservableQuery::new(observable_rc, schema, binary_layout))
        }
    }

    /// Gets the schema layout for binary decoding.
    /// The layout can be cached by JS for repeated queries on the same table.
    #[wasm_bindgen(js_name = getSchemaLayout)]
    pub fn get_schema_layout(&self) -> Result<crate::binary_protocol::SchemaLayout, JsValue> {
        let table_name = self
            .from_table
            .as_ref()
            .ok_or_else(|| JsValue::from_str("FROM table not specified"))?;

        let cache = self.cache.borrow();
        let store = cache
            .get_table(table_name)
            .ok_or_else(|| JsValue::from_str(&alloc::format!("Table not found: {}", table_name)))?;

        let schema = store.schema();

        // Create layout based on projection
        // - Projection queries: create new each time
        // - Full table queries: use cache (get_or_create)
        let layout = if let Some(cols) = self.parse_columns() {
            // Projection query: create layout from projected columns
            // For JOIN queries, we need to look up columns from multiple tables
            self.create_projection_layout(&cols)
        } else {
            self.schema_layout_cache
                .borrow_mut()
                .get_or_create_full(table_name, schema)
                .clone()
        };

        Ok(layout)
    }

    /// Executes the query and returns a binary result buffer.
    /// Use with getSchemaLayout() for zero-copy decoding in JS.
    #[wasm_bindgen(js_name = execBinary)]
    pub async fn exec_binary(&self) -> Result<crate::binary_protocol::BinaryResult, JsValue> {
        let table_name = self
            .from_table
            .as_ref()
            .ok_or_else(|| JsValue::from_str("FROM table not specified"))?;

        let cache = self.cache.borrow();
        let store = cache
            .get_table(table_name)
            .ok_or_else(|| JsValue::from_str(&alloc::format!("Table not found: {}", table_name)))?;

        let schema = store.schema();

        // Build logical plan
        let plan = self.build_logical_plan(table_name);

        // Compute plan fingerprint for caching
        let fingerprint = compute_plan_fingerprint(&plan);

        // Get or compile physical plan (cached)
        let rows = {
            let mut plan_cache = self.plan_cache.borrow_mut();
            let physical_plan = plan_cache.get_or_insert_with(fingerprint, || {
                compile_plan(&cache, table_name, plan)
            });

            // Execute the cached physical plan
            execute_physical_plan(&cache, physical_plan)
                .map_err(|e| JsValue::from_str(&alloc::format!("Query execution error: {:?}", e)))?
        };

        // Create layout based on projection
        // - Projection queries: create new each time (column combinations vary too much)
        // - Full table queries: use cache (get_or_create)
        let layout = if let Some(cols) = self.parse_columns() {
            // Projection query: create layout from projected columns
            // For JOIN queries, we need to look up columns from multiple tables
            self.create_projection_layout(&cols)
        } else {
            // Full table query: use cached layout (clone for encoder)
            self.schema_layout_cache
                .borrow_mut()
                .get_or_create_full(table_name, schema)
                .clone()
        };

        // Encode to binary
        let mut encoder = crate::binary_protocol::BinaryEncoder::new(layout, rows.len());
        encoder.encode_rows(&rows);
        let buffer = encoder.finish();

        Ok(crate::binary_protocol::BinaryResult::new(buffer))
    }
}

/// INSERT query builder.
#[wasm_bindgen]
pub struct InsertBuilder {
    cache: Rc<RefCell<TableCache>>,
    query_registry: Rc<RefCell<QueryRegistry>>,
    table_id_map: Rc<RefCell<hashbrown::HashMap<String, TableId>>>,
    table_name: String,
    values_data: Option<JsValue>,
}

impl InsertBuilder {
    pub(crate) fn new(
        cache: Rc<RefCell<TableCache>>,
        query_registry: Rc<RefCell<QueryRegistry>>,
        table_id_map: Rc<RefCell<hashbrown::HashMap<String, TableId>>>,
        table: &str,
    ) -> Self {
        Self {
            cache,
            query_registry,
            table_id_map,
            table_name: table.to_string(),
            values_data: None,
        }
    }
}

#[wasm_bindgen]
impl InsertBuilder {
    /// Sets the values to insert.
    pub fn values(mut self, data: &JsValue) -> Self {
        self.values_data = Some(data.clone());
        self
    }

    /// Executes the insert operation.
    pub async fn exec(&self) -> Result<JsValue, JsValue> {
        let values = self
            .values_data
            .as_ref()
            .ok_or_else(|| JsValue::from_str("No values specified"))?;

        let mut cache = self.cache.borrow_mut();
        let store = cache
            .get_table_mut(&self.table_name)
            .ok_or_else(|| JsValue::from_str(&alloc::format!("Table not found: {}", self.table_name)))?;

        let schema = store.schema().clone();

        // Get the count of rows to insert first
        let arr = js_sys::Array::from(values);
        let row_count = arr.length() as u64;

        // Reserve row IDs for all rows at once to avoid ID conflicts
        let start_row_id = reserve_row_ids(row_count);

        // Convert JS values to rows
        let rows = js_array_to_rows(values, &schema, start_row_id)?;
        let row_count = rows.len();

        // Build deltas for IVM notification
        let deltas: Vec<Delta<Row>> = rows.iter().map(|r| Delta::insert(r.clone())).collect();

        // Insert rows and collect their IDs
        let mut inserted_ids = hashbrown::HashSet::new();
        for row in rows {
            inserted_ids.insert(row.id());
            store
                .insert(row)
                .map_err(|e| JsValue::from_str(&alloc::format!("{:?}", e)))?;
        }

        // Notify query registry with changed IDs and deltas
        if let Some(table_id) = self.table_id_map.borrow().get(&self.table_name).copied() {
            drop(cache); // Release borrow before notifying
            self.query_registry
                .borrow_mut()
                .on_table_change_ivm(table_id, deltas, &inserted_ids);
        }

        Ok(JsValue::from_f64(row_count as f64))
    }
}

/// UPDATE query builder.
#[wasm_bindgen]
pub struct UpdateBuilder {
    cache: Rc<RefCell<TableCache>>,
    query_registry: Rc<RefCell<QueryRegistry>>,
    table_id_map: Rc<RefCell<hashbrown::HashMap<String, TableId>>>,
    table_name: String,
    set_values: Vec<(String, JsValue)>,
    where_clause: Option<Expr>,
}

impl UpdateBuilder {
    pub(crate) fn new(
        cache: Rc<RefCell<TableCache>>,
        query_registry: Rc<RefCell<QueryRegistry>>,
        table_id_map: Rc<RefCell<hashbrown::HashMap<String, TableId>>>,
        table: &str,
    ) -> Self {
        Self {
            cache,
            query_registry,
            table_id_map,
            table_name: table.to_string(),
            set_values: Vec::new(),
            where_clause: None,
        }
    }
}

#[wasm_bindgen]
impl UpdateBuilder {
    /// Sets column values.
    /// Can be called with either:
    /// - An object: set({ column: value, ... })
    /// - Two arguments: set(column, value)
    pub fn set(mut self, column_or_obj: &JsValue, value: Option<JsValue>) -> Self {
        if let Some(val) = value {
            // Two-argument form: set(column, value)
            if let Some(col_name) = column_or_obj.as_string() {
                self.set_values.push((col_name, val));
            }
        } else if let Some(obj) = column_or_obj.dyn_ref::<js_sys::Object>() {
            // Object form: set({ column: value, ... })
            let keys = js_sys::Object::keys(obj);
            for key in keys.iter() {
                if let Some(col_name) = key.as_string() {
                    let value = js_sys::Reflect::get(obj, &key).unwrap_or(JsValue::NULL);
                    self.set_values.push((col_name, value));
                }
            }
        }
        self
    }

    /// Sets or extends the WHERE clause.
    /// Multiple calls to where_() are combined with AND.
    #[wasm_bindgen(js_name = "where")]
    pub fn where_(mut self, predicate: &Expr) -> Self {
        self.where_clause = Some(match self.where_clause {
            Some(existing) => Expr::and(&existing, predicate),
            None => predicate.clone(),
        });
        self
    }

    /// Executes the update operation.
    pub async fn exec(&self) -> Result<JsValue, JsValue> {
        let schema = {
            let cache = self.cache.borrow();
            let store = cache
                .get_table(&self.table_name)
                .ok_or_else(|| JsValue::from_str(&alloc::format!("Table not found: {}", self.table_name)))?;
            store.schema().clone()
        };

        // Find rows to update using query engine (with index optimization)
        let rows_to_update: Vec<Row> = if let Some(ref predicate) = self.where_clause {
            // Build logical plan: SELECT * FROM table WHERE predicate
            let get_col_info = |name: &str| -> Option<(String, usize, DataType)> {
                schema.get_column(name).map(|col| {
                    (self.table_name.clone(), col.index(), col.data_type())
                })
            };
            let ast_predicate = predicate.to_ast_with_table(&get_col_info);

            let plan = LogicalPlan::Filter {
                input: Box::new(LogicalPlan::Scan {
                    table: self.table_name.clone(),
                }),
                predicate: ast_predicate,
            };

            // Execute using query engine (with index optimization)
            let cache = self.cache.borrow();
            execute_plan(&cache, &self.table_name, plan)
                .map_err(|e| JsValue::from_str(&alloc::format!("Query execution error: {:?}", e)))?
                .into_iter()
                .map(|rc| (*rc).clone())
                .collect()
        } else {
            // No WHERE clause - update all rows (full scan is necessary)
            let cache = self.cache.borrow();
            let store = cache
                .get_table(&self.table_name)
                .ok_or_else(|| JsValue::from_str(&alloc::format!("Table not found: {}", self.table_name)))?;
            store.scan().map(|rc| (*rc).clone()).collect()
        };

        let mut cache = self.cache.borrow_mut();
        let store = cache
            .get_table_mut(&self.table_name)
            .ok_or_else(|| JsValue::from_str(&alloc::format!("Table not found: {}", self.table_name)))?;

        let mut deltas = Vec::new();
        let mut update_count = 0;
        let mut updated_ids = hashbrown::HashSet::new();

        for old_row in rows_to_update {
            // Create new row with updated values
            let mut new_values = old_row.values().to_vec();

            for (col_name, js_val) in &self.set_values {
                if let Some(col) = schema.get_column(col_name) {
                    let idx = col.index();
                    let value = js_to_value(js_val, col.data_type())?;
                    if idx < new_values.len() {
                        new_values[idx] = value;
                    }
                }
            }

            // Create new row with incremented version
            let new_version = old_row.version().wrapping_add(1);
            let new_row = Row::new_with_version(old_row.id(), new_version, new_values);

            // Build deltas
            deltas.push(Delta::delete(old_row.clone()));
            deltas.push(Delta::insert(new_row.clone()));

            // Track updated row ID
            updated_ids.insert(old_row.id());

            // Update in store
            store
                .update(old_row.id(), new_row)
                .map_err(|e| JsValue::from_str(&alloc::format!("{:?}", e)))?;

            update_count += 1;
        }

        // Notify query registry with changed IDs and deltas
        if let Some(table_id) = self.table_id_map.borrow().get(&self.table_name).copied() {
            drop(cache);
            self.query_registry
                .borrow_mut()
                .on_table_change_ivm(table_id, deltas, &updated_ids);
        }

        Ok(JsValue::from_f64(update_count as f64))
    }
}

/// DELETE query builder.
#[wasm_bindgen]
pub struct DeleteBuilder {
    cache: Rc<RefCell<TableCache>>,
    query_registry: Rc<RefCell<QueryRegistry>>,
    table_id_map: Rc<RefCell<hashbrown::HashMap<String, TableId>>>,
    table_name: String,
    where_clause: Option<Expr>,
}

impl DeleteBuilder {
    pub(crate) fn new(
        cache: Rc<RefCell<TableCache>>,
        query_registry: Rc<RefCell<QueryRegistry>>,
        table_id_map: Rc<RefCell<hashbrown::HashMap<String, TableId>>>,
        table: &str,
    ) -> Self {
        Self {
            cache,
            query_registry,
            table_id_map,
            table_name: table.to_string(),
            where_clause: None,
        }
    }
}

#[wasm_bindgen]
impl DeleteBuilder {
    /// Sets or extends the WHERE clause.
    /// Multiple calls to where_() are combined with AND.
    #[wasm_bindgen(js_name = "where")]
    pub fn where_(mut self, predicate: &Expr) -> Self {
        self.where_clause = Some(match self.where_clause {
            Some(existing) => Expr::and(&existing, predicate),
            None => predicate.clone(),
        });
        self
    }

    /// Executes the delete operation.
    pub async fn exec(&self) -> Result<JsValue, JsValue> {
        let schema = {
            let cache = self.cache.borrow();
            let store = cache
                .get_table(&self.table_name)
                .ok_or_else(|| JsValue::from_str(&alloc::format!("Table not found: {}", self.table_name)))?;
            store.schema().clone()
        };

        // Find rows to delete using query engine (with index optimization)
        let rows_to_delete: Vec<Row> = if let Some(ref predicate) = self.where_clause {
            // Build logical plan: SELECT * FROM table WHERE predicate
            let get_col_info = |name: &str| -> Option<(String, usize, DataType)> {
                schema.get_column(name).map(|col| {
                    (self.table_name.clone(), col.index(), col.data_type())
                })
            };
            let ast_predicate = predicate.to_ast_with_table(&get_col_info);

            let plan = LogicalPlan::Filter {
                input: Box::new(LogicalPlan::Scan {
                    table: self.table_name.clone(),
                }),
                predicate: ast_predicate,
            };

            // Execute using query engine (with index optimization)
            let cache = self.cache.borrow();
            execute_plan(&cache, &self.table_name, plan)
                .map_err(|e| JsValue::from_str(&alloc::format!("Query execution error: {:?}", e)))?
                .into_iter()
                .map(|rc| (*rc).clone())
                .collect()
        } else {
            // No WHERE clause - delete all rows (full scan is necessary)
            let cache = self.cache.borrow();
            let store = cache
                .get_table(&self.table_name)
                .ok_or_else(|| JsValue::from_str(&alloc::format!("Table not found: {}", self.table_name)))?;
            store.scan().map(|rc| (*rc).clone()).collect()
        };

        let mut cache = self.cache.borrow_mut();
        let store = cache
            .get_table_mut(&self.table_name)
            .ok_or_else(|| JsValue::from_str(&alloc::format!("Table not found: {}", self.table_name)))?;

        let deltas: Vec<Delta<Row>> = rows_to_delete
            .iter()
            .map(|r| Delta::delete(r.clone()))
            .collect();

        let delete_count = rows_to_delete.len();

        // Delete rows and collect their IDs
        let mut deleted_ids = hashbrown::HashSet::new();
        for row in rows_to_delete {
            deleted_ids.insert(row.id());
            store
                .delete(row.id())
                .map_err(|e| JsValue::from_str(&alloc::format!("{:?}", e)))?;
        }

        // Notify query registry with changed IDs and deltas
        if let Some(table_id) = self.table_id_map.borrow().get(&self.table_name).copied() {
            drop(cache);
            self.query_registry
                .borrow_mut()
                .on_table_change_ivm(table_id, deltas, &deleted_ids);
        }

        Ok(JsValue::from_f64(delete_count as f64))
    }
}

// ---------------------------------------------------------------------------
// JSONB helpers for evaluate_predicate
// ---------------------------------------------------------------------------

/// Minimal JSON text parser producing `cynos_jsonb::JsonbValue`.
///
/// Handles: null, true, false, numbers, quoted strings, arrays, objects.
/// This mirrors the logic in `PhysicalPlanRunner::parse_json_str` so that
/// the re-query filter path and the query executor agree on semantics.
fn parse_json_text(s: &str) -> Option<cynos_jsonb::JsonbValue> {
    let s = s.trim();
    if s == "null" {
        return Some(cynos_jsonb::JsonbValue::Null);
    }
    if s == "true" {
        return Some(cynos_jsonb::JsonbValue::Bool(true));
    }
    if s == "false" {
        return Some(cynos_jsonb::JsonbValue::Bool(false));
    }
    if let Ok(n) = s.parse::<f64>() {
        return Some(cynos_jsonb::JsonbValue::Number(n));
    }
    if s.starts_with('"') && s.ends_with('"') && s.len() >= 2 {
        let inner = &s[1..s.len() - 1];
        return Some(cynos_jsonb::JsonbValue::String(
            unescape_json(inner),
        ));
    }
    if s.starts_with('{') {
        return parse_json_object(s);
    }
    if s.starts_with('[') {
        return parse_json_array(s);
    }
    None
}

fn unescape_json(s: &str) -> String {
    let mut result = String::new();
    let mut chars = s.chars();
    while let Some(c) = chars.next() {
        if c == '\\' {
            match chars.next() {
                Some('n') => result.push('\n'),
                Some('t') => result.push('\t'),
                Some('r') => result.push('\r'),
                Some('"') => result.push('"'),
                Some('\\') => result.push('\\'),
                Some('/') => result.push('/'),
                Some(other) => {
                    result.push('\\');
                    result.push(other);
                }
                None => result.push('\\'),
            }
        } else {
            result.push(c);
        }
    }
    result
}

fn parse_json_object(s: &str) -> Option<cynos_jsonb::JsonbValue> {
    let s = s.trim();
    if !s.starts_with('{') || !s.ends_with('}') {
        return None;
    }
    let inner = s[1..s.len() - 1].trim();
    if inner.is_empty() {
        return Some(cynos_jsonb::JsonbValue::Object(
            cynos_jsonb::JsonbObject::new(),
        ));
    }
    let mut obj = cynos_jsonb::JsonbObject::new();
    for pair in split_json_top_level(inner, ',') {
        let pair = pair.trim();
        // Find the colon separating key from value
        let colon_pos = find_json_colon(pair)?;
        let key_str = pair[..colon_pos].trim();
        let val_str = pair[colon_pos + 1..].trim();
        // Key must be a quoted string
        if key_str.starts_with('"') && key_str.ends_with('"') && key_str.len() >= 2 {
            let key = unescape_json(&key_str[1..key_str.len() - 1]);
            let val = parse_json_text(val_str)?;
            obj.insert(key, val);
        } else {
            return None;
        }
    }
    Some(cynos_jsonb::JsonbValue::Object(obj))
}

fn parse_json_array(s: &str) -> Option<cynos_jsonb::JsonbValue> {
    let s = s.trim();
    if !s.starts_with('[') || !s.ends_with(']') {
        return None;
    }
    let inner = s[1..s.len() - 1].trim();
    if inner.is_empty() {
        return Some(cynos_jsonb::JsonbValue::Array(Vec::new()));
    }
    let mut arr = Vec::new();
    for elem in split_json_top_level(inner, ',') {
        arr.push(parse_json_text(elem.trim())?);
    }
    Some(cynos_jsonb::JsonbValue::Array(arr))
}

/// Split a JSON string at top-level occurrences of `sep`,
/// respecting nested braces, brackets, and quoted strings.
fn split_json_top_level(s: &str, sep: char) -> Vec<&str> {
    let mut parts = Vec::new();
    let mut depth = 0i32;
    let mut in_string = false;
    let mut escape = false;
    let mut start = 0;
    for (i, c) in s.char_indices() {
        if escape {
            escape = false;
            continue;
        }
        if c == '\\' && in_string {
            escape = true;
            continue;
        }
        if c == '"' {
            in_string = !in_string;
            continue;
        }
        if in_string {
            continue;
        }
        if c == '{' || c == '[' {
            depth += 1;
        } else if c == '}' || c == ']' {
            depth -= 1;
        } else if c == sep && depth == 0 {
            parts.push(&s[start..i]);
            start = i + c.len_utf8();
        }
    }
    if start <= s.len() {
        parts.push(&s[start..]);
    }
    parts
}

/// Find the first colon at top level (outside strings and nested structures).
fn find_json_colon(s: &str) -> Option<usize> {
    let mut in_string = false;
    let mut escape = false;
    for (i, c) in s.char_indices() {
        if escape {
            escape = false;
            continue;
        }
        if c == '\\' && in_string {
            escape = true;
            continue;
        }
        if c == '"' {
            in_string = !in_string;
            continue;
        }
        if !in_string && c == ':' {
            return Some(i);
        }
    }
    None
}

/// Compare a `cynos_jsonb::JsonbValue` with a `cynos_core::Value`.
fn compare_jsonb_with_value(jsonb: &cynos_jsonb::JsonbValue, value: &Value) -> bool {
    match (jsonb, value) {
        (cynos_jsonb::JsonbValue::Null, Value::Null) => true,
        (cynos_jsonb::JsonbValue::Bool(a), Value::Boolean(b)) => a == b,
        (cynos_jsonb::JsonbValue::Number(a), Value::Int32(b)) => (*a - *b as f64).abs() < f64::EPSILON,
        (cynos_jsonb::JsonbValue::Number(a), Value::Int64(b)) => (*a - *b as f64).abs() < f64::EPSILON,
        (cynos_jsonb::JsonbValue::Number(a), Value::Float64(b)) => (*a - *b).abs() < f64::EPSILON,
        (cynos_jsonb::JsonbValue::String(a), Value::String(b)) => a == b,
        _ => false,
    }
}

/// Convert a `cynos_jsonb::JsonbValue` to its string representation.
fn jsonb_value_to_string(v: &cynos_jsonb::JsonbValue) -> String {
    match v {
        cynos_jsonb::JsonbValue::Null => String::from("null"),
        cynos_jsonb::JsonbValue::Bool(b) => if *b { String::from("true") } else { String::from("false") },
        cynos_jsonb::JsonbValue::Number(n) => {
            use alloc::format;
            format!("{}", n)
        }
        cynos_jsonb::JsonbValue::String(s) => s.clone(),
        _ => {
            use alloc::format;
            format!("{:?}", v)
        }
    }
}

/// Evaluates a predicate against a row.
pub(crate) fn evaluate_predicate(predicate: &Expr, row: &Row, schema: &Table) -> bool {
    match predicate.inner() {
        ExprInner::Comparison { column, op, value } => {
            let col = schema.get_column(&column.name());
            if col.is_none() {
                return false;
            }
            let col = col.unwrap();
            let idx = col.index();

            let row_val = match row.get(idx) {
                Some(v) => v,
                None => return false,
            };

            let cmp_val = match js_to_value(value, col.data_type()) {
                Ok(v) => v,
                Err(_) => return false,
            };

            use crate::expr::ComparisonOp;
            match op {
                ComparisonOp::Eq => row_val == &cmp_val,
                ComparisonOp::Ne => row_val != &cmp_val,
                ComparisonOp::Gt => row_val > &cmp_val,
                ComparisonOp::Gte => row_val >= &cmp_val,
                ComparisonOp::Lt => row_val < &cmp_val,
                ComparisonOp::Lte => row_val <= &cmp_val,
            }
        }
        ExprInner::Between { column, low, high } => {
            let col = schema.get_column(&column.name());
            if col.is_none() {
                return false;
            }
            let col = col.unwrap();
            let idx = col.index();

            let row_val = match row.get(idx) {
                Some(v) => v,
                None => return false,
            };

            let low_val = match js_to_value(low, col.data_type()) {
                Ok(v) => v,
                Err(_) => return false,
            };
            let high_val = match js_to_value(high, col.data_type()) {
                Ok(v) => v,
                Err(_) => return false,
            };

            row_val >= &low_val && row_val <= &high_val
        }
        ExprInner::NotBetween { column, low, high } => {
            let col = schema.get_column(&column.name());
            if col.is_none() {
                return false;
            }
            let col = col.unwrap();
            let idx = col.index();

            let row_val = match row.get(idx) {
                Some(v) => v,
                None => return false,
            };

            let low_val = match js_to_value(low, col.data_type()) {
                Ok(v) => v,
                Err(_) => return false,
            };
            let high_val = match js_to_value(high, col.data_type()) {
                Ok(v) => v,
                Err(_) => return false,
            };

            row_val < &low_val || row_val > &high_val
        }
        ExprInner::InList { column, values } => {
            let col = schema.get_column(&column.name());
            if col.is_none() {
                return false;
            }
            let col = col.unwrap();
            let idx = col.index();

            let row_val = match row.get(idx) {
                Some(v) => v,
                None => return false,
            };

            let arr = js_sys::Array::from(values);
            arr.iter().any(|v| {
                if let Ok(cmp_val) = js_to_value(&v, col.data_type()) {
                    row_val == &cmp_val
                } else {
                    false
                }
            })
        }
        ExprInner::NotInList { column, values } => {
            let col = schema.get_column(&column.name());
            if col.is_none() {
                return false;
            }
            let col = col.unwrap();
            let idx = col.index();

            let row_val = match row.get(idx) {
                Some(v) => v,
                None => return false,
            };

            let arr = js_sys::Array::from(values);
            !arr.iter().any(|v| {
                if let Ok(cmp_val) = js_to_value(&v, col.data_type()) {
                    row_val == &cmp_val
                } else {
                    false
                }
            })
        }
        ExprInner::Like { column, pattern } => {
            let col = schema.get_column(&column.name());
            if col.is_none() {
                return false;
            }
            let idx = col.unwrap().index();

            match row.get(idx) {
                Some(Value::String(s)) => cynos_core::pattern_match::like(s, pattern),
                _ => false,
            }
        }
        ExprInner::NotLike { column, pattern } => {
            let col = schema.get_column(&column.name());
            if col.is_none() {
                return false;
            }
            let idx = col.unwrap().index();

            match row.get(idx) {
                Some(Value::String(s)) => !cynos_core::pattern_match::like(s, pattern),
                _ => false,
            }
        }
        ExprInner::Match { column, pattern } => {
            let col = schema.get_column(&column.name());
            if col.is_none() {
                return false;
            }
            let idx = col.unwrap().index();

            match row.get(idx) {
                Some(Value::String(s)) => cynos_core::pattern_match::regex(s, pattern),
                _ => false,
            }
        }
        ExprInner::NotMatch { column, pattern } => {
            let col = schema.get_column(&column.name());
            if col.is_none() {
                return false;
            }
            let idx = col.unwrap().index();

            match row.get(idx) {
                Some(Value::String(s)) => !cynos_core::pattern_match::regex(s, pattern),
                _ => false,
            }
        }
        ExprInner::IsNull { column } => {
            let col = schema.get_column(&column.name());
            if col.is_none() {
                return false;
            }
            let idx = col.unwrap().index();

            match row.get(idx) {
                Some(Value::Null) | None => true,
                _ => false,
            }
        }
        ExprInner::IsNotNull { column } => {
            let col = schema.get_column(&column.name());
            if col.is_none() {
                return false;
            }
            let idx = col.unwrap().index();

            match row.get(idx) {
                Some(Value::Null) | None => false,
                _ => true,
            }
        }
        ExprInner::JsonbEq { column, path, value } => {
            let col = schema.get_column(&column.name());
            if col.is_none() {
                return false;
            }
            let idx = col.unwrap().index();

            let jsonb_val = match row.get(idx) {
                Some(Value::Jsonb(j)) => j,
                _ => return false,
            };

            // Parse JSON text bytes → cynos_jsonb::JsonbValue, then query path
            let json_str = match core::str::from_utf8(&jsonb_val.0) {
                Ok(s) => s,
                Err(_) => return false,
            };
            let parsed = match parse_json_text(json_str) {
                Some(v) => v,
                None => return false,
            };
            let json_path = match cynos_jsonb::JsonPath::parse(path) {
                Ok(p) => p,
                Err(_) => return false,
            };
            let results = parsed.query(&json_path);
            if results.is_empty() {
                return false;
            }

            // Compare first result with expected value
            if let Ok(cmp_val) = js_to_value(value, DataType::String) {
                compare_jsonb_with_value(results[0], &cmp_val)
            } else {
                false
            }
        }
        ExprInner::JsonbContains { column, path, value } => {
            let col = schema.get_column(&column.name());
            if col.is_none() {
                return false;
            }
            let idx = col.unwrap().index();

            let jsonb_val = match row.get(idx) {
                Some(Value::Jsonb(j)) => j,
                _ => return false,
            };

            let json_str = match core::str::from_utf8(&jsonb_val.0) {
                Ok(s) => s,
                Err(_) => return false,
            };
            let parsed = match parse_json_text(json_str) {
                Some(v) => v,
                None => return false,
            };
            let json_path = match cynos_jsonb::JsonPath::parse(path) {
                Ok(p) => p,
                Err(_) => return false,
            };
            let results = parsed.query(&json_path);
            if results.is_empty() {
                return false;
            }

            if let Ok(cmp_val) = js_to_value(value, DataType::String) {
                if let Value::String(s) = &cmp_val {
                    // Check if the extracted value's string representation contains the search string
                    let extracted_str = jsonb_value_to_string(results[0]);
                    extracted_str.contains(s.as_str())
                } else {
                    false
                }
            } else {
                false
            }
        }
        ExprInner::JsonbExists { column, path } => {
            let col = schema.get_column(&column.name());
            if col.is_none() {
                return false;
            }
            let idx = col.unwrap().index();

            let jsonb_val = match row.get(idx) {
                Some(Value::Jsonb(j)) => j,
                _ => return false,
            };

            let json_str = match core::str::from_utf8(&jsonb_val.0) {
                Ok(s) => s,
                Err(_) => return false,
            };
            let parsed = match parse_json_text(json_str) {
                Some(v) => v,
                None => return false,
            };
            let json_path = match cynos_jsonb::JsonPath::parse(path) {
                Ok(p) => p,
                Err(_) => return false,
            };
            !parsed.query(&json_path).is_empty()
        }
        ExprInner::And { left, right } => {
            evaluate_predicate(left, row, schema) && evaluate_predicate(right, row, schema)
        }
        ExprInner::Or { left, right } => {
            evaluate_predicate(left, row, schema) || evaluate_predicate(right, row, schema)
        }
        ExprInner::Not { inner } => !evaluate_predicate(inner, row, schema),
        ExprInner::True => true,
        // ColumnRef / Literal are value expressions, not predicates.
        // Treating them as `true` preserves backward compatibility.
        ExprInner::ColumnRef { .. } | ExprInner::Literal { .. } => true,
    }
}

#[cfg(test)]
mod tests {
    use wasm_bindgen_test::*;

    wasm_bindgen_test_configure!(run_in_browser);

    use cynos_core::pattern_match;

    #[wasm_bindgen_test]
    fn test_like_match_exact() {
        assert!(pattern_match::like("hello", "hello"));
        assert!(!pattern_match::like("hello", "world"));
    }

    #[wasm_bindgen_test]
    fn test_like_match_percent() {
        assert!(pattern_match::like("hello", "%"));
        assert!(pattern_match::like("hello", "h%"));
        assert!(pattern_match::like("hello", "%o"));
        assert!(pattern_match::like("hello", "h%o"));
        assert!(pattern_match::like("hello", "%ell%"));
        assert!(!pattern_match::like("hello", "x%"));
    }

    #[wasm_bindgen_test]
    fn test_like_match_underscore() {
        assert!(pattern_match::like("hello", "_ello"));
        assert!(pattern_match::like("hello", "h_llo"));
        assert!(pattern_match::like("hello", "hell_"));
        assert!(pattern_match::like("hello", "_____"));
        assert!(!pattern_match::like("hello", "______"));
    }

    #[wasm_bindgen_test]
    fn test_like_match_combined() {
        assert!(pattern_match::like("hello", "h%_o"));
        assert!(pattern_match::like("hello world", "hello%"));
        assert!(pattern_match::like("hello world", "%world"));
    }
}
