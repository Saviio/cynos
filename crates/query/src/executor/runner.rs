//! Physical plan runner - unified execution framework for physical query plans.
//!
//! This module provides the `PhysicalPlanRunner` which executes physical query plans
//! by recursively evaluating plan nodes and combining results using the appropriate
//! execution operators.

use crate::ast::{AggregateFunc, BinaryOp, ColumnRef, Expr, SortOrder, UnaryOp};
use crate::executor::{
    AggregateExecutor, HashJoin, LimitExecutor, Relation, RelationEntry,
    SharedTables, SortExecutor, SortMergeJoin,
};
use crate::planner::PhysicalPlan;
use alloc::boxed::Box;
use alloc::collections::BTreeMap;
use alloc::rc::Rc;
use alloc::string::String;
use alloc::vec;
use alloc::vec::Vec;
use cynos_core::{Row, Value};
use cynos_jsonb::{JsonbObject, JsonbValue, JsonPath};

/// Context for expression evaluation in JOIN queries.
/// Contains table metadata needed to compute correct column indices at runtime.
#[derive(Clone, Debug)]
pub struct EvalContext<'a> {
    /// Table names in the relation (in order).
    pub tables: &'a [String],
    /// Column counts for each table (used to compute offsets).
    pub table_column_counts: &'a [usize],
}

impl<'a> EvalContext<'a> {
    /// Creates a new evaluation context.
    #[inline]
    pub fn new(tables: &'a [String], table_column_counts: &'a [usize]) -> Self {
        Self { tables, table_column_counts }
    }

    /// Computes the actual column index in the combined row based on table name and table-relative index.
    #[inline]
    pub fn resolve_column_index(&self, table_name: &str, table_relative_index: usize) -> usize {
        let mut offset = 0;
        for (i, t) in self.tables.iter().enumerate() {
            if t == table_name {
                return offset + table_relative_index;
            }
            offset += self.table_column_counts.get(i).copied().unwrap_or(0);
        }
        // Fallback: if table not found, use the original index
        // This can happen for single-table queries or after projection
        table_relative_index
    }
}

/// Error type for plan execution.
#[derive(Clone, Debug)]
pub enum ExecutionError {
    /// Table not found in data source.
    TableNotFound(String),
    /// Index not found.
    IndexNotFound { table: String, index: String },
    /// Column not found.
    ColumnNotFound { table: String, column: String },
    /// Type mismatch during evaluation.
    TypeMismatch(String),
    /// Invalid operation.
    InvalidOperation(String),
}

impl core::fmt::Display for ExecutionError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            ExecutionError::TableNotFound(t) => write!(f, "Table not found: {}", t),
            ExecutionError::IndexNotFound { table, index } => {
                write!(f, "Index {} not found on table {}", index, table)
            }
            ExecutionError::ColumnNotFound { table, column } => {
                write!(f, "Column {}.{} not found", table, column)
            }
            ExecutionError::TypeMismatch(msg) => write!(f, "Type mismatch: {}", msg),
            ExecutionError::InvalidOperation(msg) => write!(f, "Invalid operation: {}", msg),
        }
    }
}

/// Regex operation for lightweight regex matching.
#[derive(Clone, Debug, PartialEq)]
enum RegexOp {
    Start,
    End,
    Char(char),
    Any,
    Digit,
    NonDigit,
    Word,
    NonWord,
    Whitespace,
    NonWhitespace,
    CharClass(Vec<CharClassItem>),
    NegCharClass(Vec<CharClassItem>),
    Star(Box<RegexOp>),
    Plus(Box<RegexOp>),
    Question(Box<RegexOp>),
    GroupStart,
    GroupEnd,
    GroupStar,
    GroupPlus,
    GroupQuestion,
    Alternation,
}

/// Character class item for regex.
#[derive(Clone, Debug, PartialEq)]
enum CharClassItem {
    Char(char),
    Range(char, char),
}

/// Result type for plan execution.
pub type ExecutionResult<T> = Result<T, ExecutionError>;

/// Data source trait for providing table and index data.
///
/// Implementations of this trait provide access to table rows and index lookups
/// for the physical plan runner.
pub trait DataSource {
    /// Returns all rows from a table.
    fn get_table_rows(&self, table: &str) -> ExecutionResult<Vec<Rc<Row>>>;

    /// Returns rows from an index scan with a key range.
    fn get_index_range(
        &self,
        table: &str,
        index: &str,
        range_start: Option<&Value>,
        range_end: Option<&Value>,
        include_start: bool,
        include_end: bool,
    ) -> ExecutionResult<Vec<Rc<Row>>> {
        // Default implementation: call with no limit
        self.get_index_range_with_limit(
            table,
            index,
            range_start,
            range_end,
            include_start,
            include_end,
            None,
            0,
            false,
        )
    }

    /// Returns rows from an index scan with a key range, limit, offset, and reverse option.
    /// This enables true pushdown of LIMIT to the storage layer.
    fn get_index_range_with_limit(
        &self,
        table: &str,
        index: &str,
        range_start: Option<&Value>,
        range_end: Option<&Value>,
        include_start: bool,
        include_end: bool,
        limit: Option<usize>,
        offset: usize,
        reverse: bool,
    ) -> ExecutionResult<Vec<Rc<Row>>>;

    /// Returns rows from an index point lookup.
    fn get_index_point(&self, table: &str, index: &str, key: &Value) -> ExecutionResult<Vec<Rc<Row>>>;

    /// Returns rows from an index point lookup with limit.
    fn get_index_point_with_limit(
        &self,
        table: &str,
        index: &str,
        key: &Value,
        limit: Option<usize>,
    ) -> ExecutionResult<Vec<Rc<Row>>> {
        // Default implementation: get all rows and apply limit in memory
        let rows = self.get_index_point(table, index, key)?;
        Ok(if let Some(limit) = limit {
            rows.into_iter().take(limit).collect()
        } else {
            rows
        })
    }

    /// Returns the column count for a table.
    fn get_column_count(&self, table: &str) -> ExecutionResult<usize>;

    /// Returns rows from a GIN index lookup by key-value pair.
    /// Used for JSONB path equality queries like `$.category = 'Electronics'`.
    fn get_gin_index_rows(
        &self,
        table: &str,
        index: &str,
        key: &str,
        value: &str,
    ) -> ExecutionResult<Vec<Rc<Row>>> {
        // Default implementation: fall back to table scan (no GIN index support)
        let _ = (index, key, value);
        self.get_table_rows(table)
    }

    /// Returns rows from a GIN index lookup by key existence.
    /// Used for JSONB key existence queries.
    fn get_gin_index_rows_by_key(
        &self,
        table: &str,
        index: &str,
        key: &str,
    ) -> ExecutionResult<Vec<Rc<Row>>> {
        // Default implementation: fall back to table scan (no GIN index support)
        let _ = (index, key);
        self.get_table_rows(table)
    }

    /// Returns rows from a GIN index lookup by multiple key-value pairs (AND query).
    /// Used for combined JSONB path equality queries like `$.category = 'A' AND $.status = 'active'`.
    fn get_gin_index_rows_multi(
        &self,
        table: &str,
        index: &str,
        pairs: &[(&str, &str)],
    ) -> ExecutionResult<Vec<Rc<Row>>> {
        // Default implementation: fall back to table scan (no GIN index support)
        let _ = (index, pairs);
        self.get_table_rows(table)
    }
}

/// Physical plan runner - executes physical query plans.
///
/// The runner recursively evaluates plan nodes, executing each operator
/// and combining results according to the plan structure.
pub struct PhysicalPlanRunner<'a, D: DataSource> {
    data_source: &'a D,
}

impl<'a, D: DataSource> PhysicalPlanRunner<'a, D> {
    /// Creates a new physical plan runner with the given data source.
    pub fn new(data_source: &'a D) -> Self {
        Self { data_source }
    }

    /// Executes a physical plan and returns the result relation.
    pub fn execute(&self, plan: &PhysicalPlan) -> ExecutionResult<Relation> {
        match plan {
            PhysicalPlan::TableScan { table } => self.execute_table_scan(table),

            PhysicalPlan::IndexScan {
                table,
                index,
                range_start,
                range_end,
                include_start,
                include_end,
                limit,
                offset,
                reverse,
            } => self.execute_index_scan(
                table,
                index,
                range_start.as_ref(),
                range_end.as_ref(),
                *include_start,
                *include_end,
                *limit,
                *offset,
                *reverse,
            ),

            PhysicalPlan::IndexGet { table, index, key, limit } => {
                self.execute_index_get(table, index, key, *limit)
            }

            PhysicalPlan::IndexInGet { table, index, keys } => {
                self.execute_index_in_get(table, index, keys)
            }

            PhysicalPlan::GinIndexScan {
                table,
                index,
                key,
                value,
                query_type,
            } => self.execute_gin_index_scan(table, index, key, value.as_deref(), query_type),

            PhysicalPlan::GinIndexScanMulti {
                table,
                index,
                pairs,
            } => self.execute_gin_index_scan_multi(table, index, pairs),

            PhysicalPlan::Filter { input, predicate } => {
                let input_rel = self.execute(input)?;
                self.execute_filter(input_rel, predicate)
            }

            PhysicalPlan::Project { input, columns } => {
                let input_rel = self.execute(input)?;
                self.execute_project(input_rel, columns)
            }

            PhysicalPlan::HashJoin {
                left,
                right,
                condition,
                join_type,
            } => {
                let left_rel = self.execute(left)?;
                let right_rel = self.execute(right)?;
                self.execute_hash_join(left_rel, right_rel, condition, *join_type)
            }

            PhysicalPlan::SortMergeJoin {
                left,
                right,
                condition,
                join_type,
            } => {
                let left_rel = self.execute(left)?;
                let right_rel = self.execute(right)?;
                self.execute_sort_merge_join(left_rel, right_rel, condition, *join_type)
            }

            PhysicalPlan::NestedLoopJoin {
                left,
                right,
                condition,
                join_type,
            } => {
                let left_rel = self.execute(left)?;
                let right_rel = self.execute(right)?;
                self.execute_nested_loop_join(left_rel, right_rel, condition, *join_type)
            }

            PhysicalPlan::IndexNestedLoopJoin {
                outer,
                inner_table,
                inner_index,
                condition,
                join_type,
            } => {
                let outer_rel = self.execute(outer)?;
                self.execute_index_nested_loop_join(
                    outer_rel,
                    inner_table,
                    inner_index,
                    condition,
                    *join_type,
                )
            }

            PhysicalPlan::HashAggregate {
                input,
                group_by,
                aggregates,
            } => {
                let input_rel = self.execute(input)?;
                self.execute_hash_aggregate(input_rel, group_by, aggregates)
            }

            PhysicalPlan::Sort { input, order_by } => {
                let input_rel = self.execute(input)?;
                self.execute_sort(input_rel, order_by)
            }

            PhysicalPlan::Limit {
                input,
                limit,
                offset,
            } => {
                let input_rel = self.execute(input)?;
                self.execute_limit(input_rel, *limit, *offset)
            }

            PhysicalPlan::CrossProduct { left, right } => {
                let left_rel = self.execute(left)?;
                let right_rel = self.execute(right)?;
                self.execute_cross_product(left_rel, right_rel)
            }

            PhysicalPlan::NoOp { input } => self.execute(input),

            PhysicalPlan::TopN {
                input,
                order_by,
                limit,
                offset,
            } => {
                // TopN is executed as Sort + Limit for now
                // A more efficient implementation would use a heap
                let input_rel = self.execute(input)?;
                let sorted = self.execute_sort(input_rel, order_by)?;
                self.execute_limit(sorted, *limit, *offset)
            }

            PhysicalPlan::Empty => Ok(Relation::empty()),
        }
    }

    // ========== Scan Operations ==========

    fn execute_table_scan(&self, table: &str) -> ExecutionResult<Relation> {
        let rows = self.data_source.get_table_rows(table)?;
        let column_count = self.data_source.get_column_count(table)?;
        Ok(Relation::from_rows_with_column_count(rows, alloc::vec![table.into()], column_count))
    }

    fn execute_index_scan(
        &self,
        table: &str,
        index: &str,
        range_start: Option<&Value>,
        range_end: Option<&Value>,
        include_start: bool,
        include_end: bool,
        limit: Option<usize>,
        offset: Option<usize>,
        reverse: bool,
    ) -> ExecutionResult<Relation> {
        // Push limit, offset, and reverse down to storage layer for early termination
        let rows = self.data_source.get_index_range_with_limit(
            table,
            index,
            range_start,
            range_end,
            include_start,
            include_end,
            limit,
            offset.unwrap_or(0),
            reverse,
        )?;
        let column_count = self.data_source.get_column_count(table)?;
        Ok(Relation::from_rows_with_column_count(rows, alloc::vec![table.into()], column_count))
    }

    fn execute_index_get(
        &self,
        table: &str,
        index: &str,
        key: &Value,
        limit: Option<usize>,
    ) -> ExecutionResult<Relation> {
        let rows = self.data_source.get_index_point_with_limit(table, index, key, limit)?;
        let column_count = self.data_source.get_column_count(table)?;
        Ok(Relation::from_rows_with_column_count(rows, alloc::vec![table.into()], column_count))
    }

    /// Executes an index multi-point lookup (for IN queries).
    /// Performs multiple index lookups and unions the results.
    fn execute_index_in_get(
        &self,
        table: &str,
        index: &str,
        keys: &[Value],
    ) -> ExecutionResult<Relation> {
        let mut all_rows = Vec::new();
        let mut seen_ids = alloc::collections::BTreeSet::new();

        // Perform index lookup for each key and collect unique rows
        for key in keys {
            let rows = self.data_source.get_index_point(table, index, key)?;
            for row in rows {
                // Deduplicate by row id (in case of non-unique index)
                if seen_ids.insert(row.id()) {
                    all_rows.push(row);
                }
            }
        }

        let column_count = self.data_source.get_column_count(table)?;
        Ok(Relation::from_rows_with_column_count(all_rows, alloc::vec![table.into()], column_count))
    }

    fn execute_gin_index_scan(
        &self,
        table: &str,
        index: &str,
        key: &str,
        value: Option<&str>,
        query_type: &str,
    ) -> ExecutionResult<Relation> {
        let rows = match (query_type, value) {
            ("eq", Some(v)) => self.data_source.get_gin_index_rows(table, index, key, v)?,
            ("contains", _) | ("exists", _) => {
                self.data_source.get_gin_index_rows_by_key(table, index, key)?
            }
            _ => self.data_source.get_table_rows(table)?,
        };
        let column_count = self.data_source.get_column_count(table)?;
        Ok(Relation::from_rows_with_column_count(rows, alloc::vec![table.into()], column_count))
    }

    fn execute_gin_index_scan_multi(
        &self,
        table: &str,
        index: &str,
        pairs: &[(String, String)],
    ) -> ExecutionResult<Relation> {
        // Convert to slice of references for the DataSource trait
        let pair_refs: Vec<(&str, &str)> = pairs
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();
        let rows = self.data_source.get_gin_index_rows_multi(table, index, &pair_refs)?;
        let column_count = self.data_source.get_column_count(table)?;
        Ok(Relation::from_rows_with_column_count(rows, alloc::vec![table.into()], column_count))
    }

    // ========== Filter Operation ==========

    fn execute_filter(&self, input: Relation, predicate: &Expr) -> ExecutionResult<Relation> {
        let tables = input.tables().to_vec();
        let table_column_counts = input.table_column_counts().to_vec();
        let ctx = EvalContext::new(&tables, &table_column_counts);

        let entries: Vec<RelationEntry> = input
            .into_iter()
            .filter(|entry| self.eval_predicate_ctx(predicate, entry, &ctx))
            .collect();

        Ok(Relation { entries, tables, table_column_counts })
    }

    // ========== Project Operation ==========

    fn execute_project(&self, input: Relation, columns: &[Expr]) -> ExecutionResult<Relation> {
        let tables = input.tables().to_vec();
        let table_column_counts = input.table_column_counts().to_vec();
        let shared_tables: SharedTables = tables.clone().into();
        let ctx = EvalContext::new(&tables, &table_column_counts);

        let entries: Vec<RelationEntry> = input
            .into_iter()
            .map(|entry| {
                let values: Vec<Value> = columns
                    .iter()
                    .map(|col| self.eval_expr_ctx(col, &entry, Some(&ctx)))
                    .collect();
                RelationEntry::new_combined(Rc::new(Row::new(entry.id(), values)), shared_tables.clone())
            })
            .collect();

        // After projection, the result has a single "virtual" table with the projected columns
        Ok(Relation {
            entries,
            tables,
            table_column_counts: alloc::vec![columns.len()],
        })
    }

    // ========== Join Operations ==========

    fn execute_hash_join(
        &self,
        left: Relation,
        right: Relation,
        condition: &Expr,
        join_type: crate::ast::JoinType,
    ) -> ExecutionResult<Relation> {
        // Extract join key indices from condition
        let (left_key_idx, right_key_idx) = self.extract_join_keys(condition, &left, &right)?;

        let is_outer = matches!(
            join_type,
            crate::ast::JoinType::LeftOuter | crate::ast::JoinType::FullOuter
        );

        let join = HashJoin::new(left_key_idx, right_key_idx, is_outer);
        Ok(join.execute(left, right))
    }

    fn execute_sort_merge_join(
        &self,
        left: Relation,
        right: Relation,
        condition: &Expr,
        join_type: crate::ast::JoinType,
    ) -> ExecutionResult<Relation> {
        let (left_key_idx, right_key_idx) = self.extract_join_keys(condition, &left, &right)?;
        let is_outer = matches!(
            join_type,
            crate::ast::JoinType::LeftOuter | crate::ast::JoinType::FullOuter
        );

        let join = SortMergeJoin::new(left_key_idx, right_key_idx, is_outer);
        Ok(join.execute_with_sort(left, right))
    }

    fn execute_nested_loop_join(
        &self,
        left: Relation,
        right: Relation,
        condition: &Expr,
        join_type: crate::ast::JoinType,
    ) -> ExecutionResult<Relation> {
        let is_outer = matches!(
            join_type,
            crate::ast::JoinType::LeftOuter | crate::ast::JoinType::FullOuter
        );

        // For nested loop join, we use predicate-based evaluation
        let mut result_entries = Vec::new();
        let left_tables = left.tables().to_vec();
        let right_tables = right.tables().to_vec();
        let left_column_counts = left.table_column_counts().to_vec();
        let right_column_counts = right.table_column_counts().to_vec();

        // Get left column count for adjusting right table column indices
        let left_col_count = left
            .entries
            .first()
            .map(|e| e.row.len())
            .unwrap_or(0);
        let right_col_count = right
            .entries
            .first()
            .map(|e| e.row.len())
            .unwrap_or(0);

        // Adjust condition to use combined row indices
        // Right table columns need to be offset by left table column count
        let adjusted_condition = self.adjust_join_condition_indices(condition, &left_tables, &right_tables, left_col_count);

        for left_entry in left.iter() {
            let mut matched = false;

            for right_entry in right.iter() {
                // Create a combined entry for predicate evaluation
                let combined = RelationEntry::combine(
                    left_entry,
                    &left_tables,
                    right_entry,
                    &right_tables,
                );

                if self.eval_predicate(&adjusted_condition, &combined) {
                    matched = true;
                    result_entries.push(combined);
                }
            }

            if is_outer && !matched {
                let combined = RelationEntry::combine_with_null(
                    left_entry,
                    &left_tables,
                    right_col_count,
                    &right_tables,
                );
                result_entries.push(combined);
            }
        }

        let mut tables = left_tables;
        tables.extend(right_tables);

        // Compute combined table column counts
        let mut table_column_counts = left_column_counts;
        table_column_counts.extend(right_column_counts);

        Ok(Relation {
            entries: result_entries,
            tables,
            table_column_counts,
        })
    }

    fn execute_index_nested_loop_join(
        &self,
        outer: Relation,
        inner_table: &str,
        inner_index: &str,
        condition: &Expr,
        join_type: crate::ast::JoinType,
    ) -> ExecutionResult<Relation> {
        let is_outer = matches!(
            join_type,
            crate::ast::JoinType::LeftOuter | crate::ast::JoinType::FullOuter
        );

        // Extract the outer key column index from condition
        let outer_key_idx = self.extract_outer_key_index(condition, &outer)?;
        let inner_col_count = self.data_source.get_column_count(inner_table)?;

        let mut result_entries = Vec::new();
        let outer_tables = outer.tables().to_vec();
        let outer_column_counts = outer.table_column_counts().to_vec();
        let inner_tables = alloc::vec![inner_table.into()];

        for outer_entry in outer.iter() {
            let key_value = outer_entry.get_field(outer_key_idx);

            let inner_rows = if let Some(key) = key_value {
                if !key.is_null() {
                    self.data_source
                        .get_index_point(inner_table, inner_index, key)?
                } else {
                    Vec::new()
                }
            } else {
                Vec::new()
            };

            if inner_rows.is_empty() {
                if is_outer {
                    let combined = RelationEntry::combine_with_null(
                        outer_entry,
                        &outer_tables,
                        inner_col_count,
                        &inner_tables,
                    );
                    result_entries.push(combined);
                }
            } else {
                for inner_row in inner_rows {
                    let inner_entry = RelationEntry::from_row(inner_row, inner_table);
                    let combined = RelationEntry::combine(
                        outer_entry,
                        &outer_tables,
                        &inner_entry,
                        &inner_tables,
                    );
                    result_entries.push(combined);
                }
            }
        }

        let mut tables = outer_tables;
        tables.extend(inner_tables);

        // Compute combined table column counts
        let mut table_column_counts = outer_column_counts;
        table_column_counts.push(inner_col_count);

        Ok(Relation {
            entries: result_entries,
            tables,
            table_column_counts,
        })
    }

    fn execute_cross_product(&self, left: Relation, right: Relation) -> ExecutionResult<Relation> {
        let mut result_entries = Vec::new();
        let left_tables = left.tables().to_vec();
        let right_tables = right.tables().to_vec();
        let left_column_counts = left.table_column_counts().to_vec();
        let right_column_counts = right.table_column_counts().to_vec();

        for left_entry in left.iter() {
            for right_entry in right.iter() {
                let combined = RelationEntry::combine(
                    left_entry,
                    &left_tables,
                    right_entry,
                    &right_tables,
                );
                result_entries.push(combined);
            }
        }

        let mut tables = left_tables;
        tables.extend(right_tables);

        // Compute combined table column counts
        let mut table_column_counts = left_column_counts;
        table_column_counts.extend(right_column_counts);

        Ok(Relation {
            entries: result_entries,
            tables,
            table_column_counts,
        })
    }

    // ========== Aggregate Operation ==========

    fn execute_hash_aggregate(
        &self,
        input: Relation,
        group_by: &[Expr],
        aggregates: &[(AggregateFunc, Expr)],
    ) -> ExecutionResult<Relation> {
        // Convert Expr group_by to column indices
        let group_by_indices: Vec<usize> = group_by
            .iter()
            .filter_map(|expr| {
                if let Expr::Column(col) = expr {
                    Some(col.index)
                } else {
                    None
                }
            })
            .collect();

        // Convert aggregates to (func, Option<column_index>)
        let agg_specs: Vec<(AggregateFunc, Option<usize>)> = aggregates
            .iter()
            .map(|(func, expr)| {
                let col_idx = if let Expr::Column(col) = expr {
                    Some(col.index)
                } else {
                    None
                };
                (*func, col_idx)
            })
            .collect();

        let executor = AggregateExecutor::new(group_by_indices, agg_specs);
        Ok(executor.execute(input))
    }

    // ========== Sort Operation ==========

    fn execute_sort(
        &self,
        input: Relation,
        order_by: &[(Expr, SortOrder)],
    ) -> ExecutionResult<Relation> {
        let tables = input.tables().to_vec();
        let table_column_counts = input.table_column_counts().to_vec();
        let ctx = EvalContext::new(&tables, &table_column_counts);

        // Convert Expr order_by to column indices using dynamic computation
        let order_by_indices: Vec<(usize, SortOrder)> = order_by
            .iter()
            .filter_map(|(expr, order)| {
                if let Expr::Column(col) = expr {
                    let actual_index = ctx.resolve_column_index(&col.table, col.index);
                    Some((actual_index, *order))
                } else {
                    None
                }
            })
            .collect();

        let executor = SortExecutor::new(order_by_indices);
        Ok(executor.execute(input))
    }

    // ========== Limit Operation ==========

    fn execute_limit(
        &self,
        input: Relation,
        limit: usize,
        offset: usize,
    ) -> ExecutionResult<Relation> {
        let executor = LimitExecutor::new(limit, offset);
        Ok(executor.execute(input))
    }

    // ========== Expression Evaluation ==========

    /// Evaluates an expression against a relation entry.
    /// If `ctx` is provided, column indices are dynamically computed based on table metadata.
    /// This is needed for JOIN queries where the optimizer may have reordered tables.
    fn eval_expr_ctx(&self, expr: &Expr, entry: &RelationEntry, ctx: Option<&EvalContext<'_>>) -> Value {
        match expr {
            Expr::Column(col) => {
                let index = if let Some(c) = ctx {
                    c.resolve_column_index(&col.table, col.index)
                } else {
                    col.index
                };
                entry.get_field(index).cloned().unwrap_or(Value::Null)
            }

            Expr::Literal(value) => value.clone(),

            Expr::BinaryOp { left, op, right } => {
                let left_val = self.eval_expr_ctx(left, entry, ctx);
                let right_val = self.eval_expr_ctx(right, entry, ctx);
                self.eval_binary_op(*op, &left_val, &right_val)
            }

            Expr::UnaryOp { op, expr } => {
                let val = self.eval_expr_ctx(expr, entry, ctx);
                self.eval_unary_op(*op, &val)
            }

            Expr::Aggregate { expr, .. } => {
                if let Some(e) = expr {
                    self.eval_expr_ctx(e, entry, ctx)
                } else {
                    Value::Int64(1) // COUNT(*)
                }
            }

            Expr::Between { expr, low, high } => {
                let val = self.eval_expr_ctx(expr, entry, ctx);
                let low_val = self.eval_expr_ctx(low, entry, ctx);
                let high_val = self.eval_expr_ctx(high, entry, ctx);
                Value::Boolean(val >= low_val && val <= high_val)
            }

            Expr::NotBetween { expr, low, high } => {
                let val = self.eval_expr_ctx(expr, entry, ctx);
                let low_val = self.eval_expr_ctx(low, entry, ctx);
                let high_val = self.eval_expr_ctx(high, entry, ctx);
                Value::Boolean(val < low_val || val > high_val)
            }

            Expr::In { expr, list } => {
                let val = self.eval_expr_ctx(expr, entry, ctx);
                let in_list = list.iter().any(|item| self.eval_expr_ctx(item, entry, ctx) == val);
                Value::Boolean(in_list)
            }

            Expr::NotIn { expr, list } => {
                let val = self.eval_expr_ctx(expr, entry, ctx);
                let in_list = list.iter().any(|item| self.eval_expr_ctx(item, entry, ctx) == val);
                Value::Boolean(!in_list)
            }

            Expr::Like { expr, pattern } => {
                let val = self.eval_expr_ctx(expr, entry, ctx);
                if let Value::String(s) = val {
                    Value::Boolean(self.match_like_pattern(&s, pattern))
                } else {
                    Value::Boolean(false)
                }
            }

            Expr::NotLike { expr, pattern } => {
                let val = self.eval_expr_ctx(expr, entry, ctx);
                if let Value::String(s) = val {
                    Value::Boolean(!self.match_like_pattern(&s, pattern))
                } else {
                    Value::Boolean(true)
                }
            }

            Expr::Match { expr, pattern } => {
                let val = self.eval_expr_ctx(expr, entry, ctx);
                if let Value::String(s) = val {
                    Value::Boolean(self.match_regex_pattern(&s, pattern))
                } else {
                    Value::Boolean(false)
                }
            }

            Expr::NotMatch { expr, pattern } => {
                let val = self.eval_expr_ctx(expr, entry, ctx);
                if let Value::String(s) = val {
                    Value::Boolean(!self.match_regex_pattern(&s, pattern))
                } else {
                    Value::Boolean(true)
                }
            }

            Expr::Function { name, args } => {
                let arg_values: Vec<Value> = args.iter().map(|a| self.eval_expr_ctx(a, entry, ctx)).collect();
                self.eval_function(name, &arg_values)
            }
        }
    }

    /// Evaluates an expression against a relation entry (simple version without context).
    #[inline]
    fn eval_expr(&self, expr: &Expr, entry: &RelationEntry) -> Value {
        self.eval_expr_ctx(expr, entry, None)
    }

    /// Evaluates a predicate expression against a relation entry.
    #[inline]
    fn eval_predicate(&self, expr: &Expr, entry: &RelationEntry) -> bool {
        match self.eval_expr(expr, entry) {
            Value::Boolean(b) => b,
            Value::Null => false,
            _ => false,
        }
    }

    /// Evaluates a predicate expression with context for JOIN queries.
    #[inline]
    fn eval_predicate_ctx(&self, expr: &Expr, entry: &RelationEntry, ctx: &EvalContext<'_>) -> bool {
        match self.eval_expr_ctx(expr, entry, Some(ctx)) {
            Value::Boolean(b) => b,
            Value::Null => false,
            _ => false,
        }
    }

    fn eval_binary_op(&self, op: BinaryOp, left: &Value, right: &Value) -> Value {
        // Handle NULL propagation
        if left.is_null() || right.is_null() {
            return match op {
                BinaryOp::And => {
                    // NULL AND FALSE = FALSE, NULL AND TRUE = NULL
                    if let Value::Boolean(false) = left {
                        return Value::Boolean(false);
                    }
                    if let Value::Boolean(false) = right {
                        return Value::Boolean(false);
                    }
                    Value::Null
                }
                BinaryOp::Or => {
                    // NULL OR TRUE = TRUE, NULL OR FALSE = NULL
                    if let Value::Boolean(true) = left {
                        return Value::Boolean(true);
                    }
                    if let Value::Boolean(true) = right {
                        return Value::Boolean(true);
                    }
                    Value::Null
                }
                _ => Value::Null,
            };
        }

        match op {
            BinaryOp::Eq => Value::Boolean(left == right),
            BinaryOp::Ne => Value::Boolean(left != right),
            BinaryOp::Lt => Value::Boolean(left < right),
            BinaryOp::Le => Value::Boolean(left <= right),
            BinaryOp::Gt => Value::Boolean(left > right),
            BinaryOp::Ge => Value::Boolean(left >= right),
            BinaryOp::And => {
                let l = matches!(left, Value::Boolean(true));
                let r = matches!(right, Value::Boolean(true));
                Value::Boolean(l && r)
            }
            BinaryOp::Or => {
                let l = matches!(left, Value::Boolean(true));
                let r = matches!(right, Value::Boolean(true));
                Value::Boolean(l || r)
            }
            BinaryOp::Add => self.eval_arithmetic(left, right, |a, b| a + b),
            BinaryOp::Sub => self.eval_arithmetic(left, right, |a, b| a - b),
            BinaryOp::Mul => self.eval_arithmetic(left, right, |a, b| a * b),
            BinaryOp::Div => {
                // Check for division by zero
                match right {
                    Value::Int32(0) | Value::Int64(0) => Value::Null,
                    Value::Float64(f) if *f == 0.0 => Value::Null,
                    _ => self.eval_arithmetic(left, right, |a, b| if b != 0.0 { a / b } else { 0.0 }),
                }
            }
            BinaryOp::Mod => {
                match (left, right) {
                    (Value::Int64(a), Value::Int64(b)) if *b != 0 => Value::Int64(a % b),
                    (Value::Int32(a), Value::Int32(b)) if *b != 0 => Value::Int32(a % b),
                    _ => Value::Null,
                }
            }
            BinaryOp::Like | BinaryOp::In | BinaryOp::Between => {
                // These are handled specially in eval_expr
                Value::Null
            }
        }
    }

    fn eval_arithmetic<F>(&self, left: &Value, right: &Value, op: F) -> Value
    where
        F: Fn(f64, f64) -> f64,
    {
        let l = match left {
            Value::Int32(i) => *i as f64,
            Value::Int64(i) => *i as f64,
            Value::Float64(f) => *f,
            _ => return Value::Null,
        };
        let r = match right {
            Value::Int32(i) => *i as f64,
            Value::Int64(i) => *i as f64,
            Value::Float64(f) => *f,
            _ => return Value::Null,
        };

        let result = op(l, r);

        // Preserve integer type if both inputs are integers
        match (left, right) {
            (Value::Int64(_), Value::Int64(_)) => Value::Int64(result as i64),
            (Value::Int32(_), Value::Int32(_)) => Value::Int32(result as i32),
            _ => Value::Float64(result),
        }
    }

    fn eval_unary_op(&self, op: UnaryOp, value: &Value) -> Value {
        match op {
            UnaryOp::Not => match value {
                Value::Boolean(b) => Value::Boolean(!b),
                Value::Null => Value::Null,
                _ => Value::Null,
            },
            UnaryOp::Neg => match value {
                Value::Int32(i) => Value::Int32(-i),
                Value::Int64(i) => Value::Int64(-i),
                Value::Float64(f) => Value::Float64(-f),
                _ => Value::Null,
            },
            UnaryOp::IsNull => Value::Boolean(value.is_null()),
            UnaryOp::IsNotNull => Value::Boolean(!value.is_null()),
        }
    }

    fn eval_function(&self, name: &str, args: &[Value]) -> Value {
        match name.to_uppercase().as_str() {
            "ABS" => {
                if let Some(v) = args.first() {
                    match v {
                        Value::Int32(i) => Value::Int32(i.abs()),
                        Value::Int64(i) => Value::Int64(i.abs()),
                        Value::Float64(f) => Value::Float64(f.abs()),
                        _ => Value::Null,
                    }
                } else {
                    Value::Null
                }
            }
            "UPPER" => {
                if let Some(Value::String(s)) = args.first() {
                    Value::String(s.to_uppercase().into())
                } else {
                    Value::Null
                }
            }
            "LOWER" => {
                if let Some(Value::String(s)) = args.first() {
                    Value::String(s.to_lowercase().into())
                } else {
                    Value::Null
                }
            }
            "LENGTH" => {
                if let Some(Value::String(s)) = args.first() {
                    Value::Int64(s.len() as i64)
                } else {
                    Value::Null
                }
            }
            "COALESCE" => {
                for arg in args {
                    if !arg.is_null() {
                        return arg.clone();
                    }
                }
                Value::Null
            }
            // JSONB path equality: jsonb_path_eq(jsonb_value, path, expected_value)
            "JSONB_PATH_EQ" => {
                if args.len() >= 3 {
                    if let (Value::Jsonb(jsonb), Value::String(path)) = (&args[0], &args[1]) {
                        let expected = &args[2];
                        return self.jsonb_path_eq(jsonb, path, expected);
                    }
                }
                Value::Boolean(false)
            }
            // JSONB contains: jsonb_contains(jsonb_value, path)
            "JSONB_CONTAINS" => {
                if args.len() >= 2 {
                    if let (Value::Jsonb(jsonb), Value::String(path)) = (&args[0], &args[1]) {
                        return self.jsonb_path_exists(jsonb, path);
                    }
                }
                Value::Boolean(false)
            }
            // JSONB exists: jsonb_exists(jsonb_value, path)
            "JSONB_EXISTS" => {
                if args.len() >= 2 {
                    if let (Value::Jsonb(jsonb), Value::String(path)) = (&args[0], &args[1]) {
                        return self.jsonb_path_exists(jsonb, path);
                    }
                }
                Value::Boolean(false)
            }
            _ => Value::Null,
        }
    }

    fn match_like_pattern(&self, value: &str, pattern: &str) -> bool {
        // Simple LIKE pattern matching with % and _ wildcards
        let pattern_chars: Vec<char> = pattern.chars().collect();
        let value_chars: Vec<char> = value.chars().collect();
        self.match_like_recursive(&value_chars, &pattern_chars, 0, 0)
    }

    /// Matches a regex pattern against a string value.
    /// Supports a subset of regex: . * + ? ^ $ [] [^] | () \d \w \s
    fn match_regex_pattern(&self, value: &str, pattern: &str) -> bool {
        let compiled = match self.compile_regex(pattern) {
            Some(c) => c,
            None => return false,
        };
        self.regex_match_compiled(value, &compiled)
    }

    /// Compiled regex instruction
    fn compile_regex(&self, pattern: &str) -> Option<Vec<RegexOp>> {
        let mut ops = Vec::new();
        let mut chars = pattern.chars().peekable();
        let mut in_group = false;

        while let Some(c) = chars.next() {
            match c {
                '^' if ops.is_empty() => ops.push(RegexOp::Start),
                '$' if chars.peek().is_none() => ops.push(RegexOp::End),
                '.' => self.apply_quantifier(&mut chars, &mut ops, RegexOp::Any),
                '*' | '+' | '?' => return None, // Invalid: quantifier without preceding element
                '\\' => {
                    let escaped = chars.next()?;
                    let op = match escaped {
                        'd' => RegexOp::Digit,
                        'D' => RegexOp::NonDigit,
                        'w' => RegexOp::Word,
                        'W' => RegexOp::NonWord,
                        's' => RegexOp::Whitespace,
                        'S' => RegexOp::NonWhitespace,
                        'n' => RegexOp::Char('\n'),
                        't' => RegexOp::Char('\t'),
                        'r' => RegexOp::Char('\r'),
                        _ => RegexOp::Char(escaped),
                    };
                    self.apply_quantifier(&mut chars, &mut ops, op);
                }
                '[' => {
                    let (class_op, negated) = self.parse_char_class(&mut chars)?;
                    let op = if negated {
                        RegexOp::NegCharClass(class_op)
                    } else {
                        RegexOp::CharClass(class_op)
                    };
                    self.apply_quantifier(&mut chars, &mut ops, op);
                }
                '(' => {
                    in_group = true;
                    ops.push(RegexOp::GroupStart);
                }
                ')' => {
                    if !in_group {
                        return None;
                    }
                    in_group = false;
                    ops.push(RegexOp::GroupEnd);
                    // Check for quantifier after group
                    if let Some(&q) = chars.peek() {
                        match q {
                            '*' => {
                                chars.next();
                                ops.push(RegexOp::GroupStar);
                            }
                            '+' => {
                                chars.next();
                                ops.push(RegexOp::GroupPlus);
                            }
                            '?' => {
                                chars.next();
                                ops.push(RegexOp::GroupQuestion);
                            }
                            _ => {}
                        }
                    }
                }
                '|' => ops.push(RegexOp::Alternation),
                _ => self.apply_quantifier(&mut chars, &mut ops, RegexOp::Char(c)),
            }
        }

        if in_group {
            return None; // Unclosed group
        }

        Some(ops)
    }

    fn apply_quantifier(
        &self,
        chars: &mut core::iter::Peekable<core::str::Chars>,
        ops: &mut Vec<RegexOp>,
        base_op: RegexOp,
    ) {
        if let Some(&q) = chars.peek() {
            match q {
                '*' => {
                    chars.next();
                    ops.push(RegexOp::Star(Box::new(base_op)));
                }
                '+' => {
                    chars.next();
                    ops.push(RegexOp::Plus(Box::new(base_op)));
                }
                '?' => {
                    chars.next();
                    ops.push(RegexOp::Question(Box::new(base_op)));
                }
                _ => ops.push(base_op),
            }
        } else {
            ops.push(base_op);
        }
    }

    fn parse_char_class(
        &self,
        chars: &mut core::iter::Peekable<core::str::Chars>,
    ) -> Option<(Vec<CharClassItem>, bool)> {
        let mut items = Vec::new();
        let negated = chars.peek() == Some(&'^');
        if negated {
            chars.next();
        }

        while let Some(c) = chars.next() {
            if c == ']' {
                return Some((items, negated));
            }
            if c == '\\' {
                let escaped = chars.next()?;
                items.push(CharClassItem::Char(match escaped {
                    'n' => '\n',
                    't' => '\t',
                    'r' => '\r',
                    'd' => {
                        items.push(CharClassItem::Range('0', '9'));
                        continue;
                    }
                    'w' => {
                        items.push(CharClassItem::Range('a', 'z'));
                        items.push(CharClassItem::Range('A', 'Z'));
                        items.push(CharClassItem::Range('0', '9'));
                        items.push(CharClassItem::Char('_'));
                        continue;
                    }
                    's' => {
                        items.push(CharClassItem::Char(' '));
                        items.push(CharClassItem::Char('\t'));
                        items.push(CharClassItem::Char('\n'));
                        items.push(CharClassItem::Char('\r'));
                        continue;
                    }
                    _ => escaped,
                }));
            } else if chars.peek() == Some(&'-') {
                chars.next(); // consume '-'
                if let Some(end) = chars.next() {
                    if end == ']' {
                        items.push(CharClassItem::Char(c));
                        items.push(CharClassItem::Char('-'));
                        return Some((items, negated));
                    }
                    items.push(CharClassItem::Range(c, end));
                } else {
                    items.push(CharClassItem::Char(c));
                    items.push(CharClassItem::Char('-'));
                }
            } else {
                items.push(CharClassItem::Char(c));
            }
        }
        None // Unclosed character class
    }

    fn regex_match_compiled(&self, value: &str, ops: &[RegexOp]) -> bool {
        let chars: Vec<char> = value.chars().collect();

        // Handle alternation by splitting into alternatives
        let alternatives = self.split_alternatives(ops);
        if alternatives.len() > 1 {
            return alternatives.iter().any(|alt| self.regex_match_ops(&chars, alt, 0, 0));
        }

        // Check if pattern requires start anchor
        let has_start = ops.first() == Some(&RegexOp::Start);
        let ops_to_match = if has_start { &ops[1..] } else { ops };

        if has_start {
            self.regex_match_ops(&chars, ops_to_match, 0, 0)
        } else {
            // Try matching at each position
            for start in 0..=chars.len() {
                if self.regex_match_ops(&chars, ops_to_match, start, 0) {
                    return true;
                }
            }
            false
        }
    }

    fn split_alternatives(&self, ops: &[RegexOp]) -> Vec<Vec<RegexOp>> {
        let mut alternatives = Vec::new();
        let mut current = Vec::new();
        let mut depth = 0;

        for op in ops {
            match op {
                RegexOp::GroupStart => {
                    depth += 1;
                    current.push(op.clone());
                }
                RegexOp::GroupEnd => {
                    depth -= 1;
                    current.push(op.clone());
                }
                RegexOp::Alternation if depth == 0 => {
                    alternatives.push(core::mem::take(&mut current));
                }
                _ => current.push(op.clone()),
            }
        }
        alternatives.push(current);
        alternatives
    }

    fn regex_match_ops(&self, chars: &[char], ops: &[RegexOp], pos: usize, op_idx: usize) -> bool {
        if op_idx >= ops.len() {
            return true; // All ops matched
        }

        let op = &ops[op_idx];
        match op {
            RegexOp::End => pos == chars.len(),
            RegexOp::Start => self.regex_match_ops(chars, ops, pos, op_idx + 1),
            RegexOp::Char(c) => {
                pos < chars.len()
                    && chars[pos] == *c
                    && self.regex_match_ops(chars, ops, pos + 1, op_idx + 1)
            }
            RegexOp::Any => {
                pos < chars.len() && self.regex_match_ops(chars, ops, pos + 1, op_idx + 1)
            }
            RegexOp::Digit => {
                pos < chars.len()
                    && chars[pos].is_ascii_digit()
                    && self.regex_match_ops(chars, ops, pos + 1, op_idx + 1)
            }
            RegexOp::NonDigit => {
                pos < chars.len()
                    && !chars[pos].is_ascii_digit()
                    && self.regex_match_ops(chars, ops, pos + 1, op_idx + 1)
            }
            RegexOp::Word => {
                pos < chars.len()
                    && (chars[pos].is_ascii_alphanumeric() || chars[pos] == '_')
                    && self.regex_match_ops(chars, ops, pos + 1, op_idx + 1)
            }
            RegexOp::NonWord => {
                pos < chars.len()
                    && !(chars[pos].is_ascii_alphanumeric() || chars[pos] == '_')
                    && self.regex_match_ops(chars, ops, pos + 1, op_idx + 1)
            }
            RegexOp::Whitespace => {
                pos < chars.len()
                    && chars[pos].is_ascii_whitespace()
                    && self.regex_match_ops(chars, ops, pos + 1, op_idx + 1)
            }
            RegexOp::NonWhitespace => {
                pos < chars.len()
                    && !chars[pos].is_ascii_whitespace()
                    && self.regex_match_ops(chars, ops, pos + 1, op_idx + 1)
            }
            RegexOp::CharClass(items) => {
                pos < chars.len()
                    && self.char_matches_class(chars[pos], items)
                    && self.regex_match_ops(chars, ops, pos + 1, op_idx + 1)
            }
            RegexOp::NegCharClass(items) => {
                pos < chars.len()
                    && !self.char_matches_class(chars[pos], items)
                    && self.regex_match_ops(chars, ops, pos + 1, op_idx + 1)
            }
            RegexOp::Star(inner) => {
                // Try matching zero or more times (greedy)
                let mut p = pos;
                let mut positions = vec![p];
                while p < chars.len() && self.single_op_matches(chars[p], inner) {
                    p += 1;
                    positions.push(p);
                }
                // Try from longest match to shortest
                for &try_pos in positions.iter().rev() {
                    if self.regex_match_ops(chars, ops, try_pos, op_idx + 1) {
                        return true;
                    }
                }
                false
            }
            RegexOp::Plus(inner) => {
                // Must match at least once
                if pos >= chars.len() || !self.single_op_matches(chars[pos], inner) {
                    return false;
                }
                let mut p = pos + 1;
                let mut positions = vec![p];
                while p < chars.len() && self.single_op_matches(chars[p], inner) {
                    p += 1;
                    positions.push(p);
                }
                for &try_pos in positions.iter().rev() {
                    if self.regex_match_ops(chars, ops, try_pos, op_idx + 1) {
                        return true;
                    }
                }
                false
            }
            RegexOp::Question(inner) => {
                // Try matching once, then zero times
                if pos < chars.len() && self.single_op_matches(chars[pos], inner) {
                    if self.regex_match_ops(chars, ops, pos + 1, op_idx + 1) {
                        return true;
                    }
                }
                self.regex_match_ops(chars, ops, pos, op_idx + 1)
            }
            RegexOp::GroupStart | RegexOp::GroupEnd => {
                self.regex_match_ops(chars, ops, pos, op_idx + 1)
            }
            RegexOp::GroupStar | RegexOp::GroupPlus | RegexOp::GroupQuestion => {
                // Group quantifiers are handled during compilation
                self.regex_match_ops(chars, ops, pos, op_idx + 1)
            }
            RegexOp::Alternation => {
                // Should be handled by split_alternatives
                self.regex_match_ops(chars, ops, pos, op_idx + 1)
            }
        }
    }

    fn single_op_matches(&self, c: char, op: &RegexOp) -> bool {
        match op {
            RegexOp::Char(expected) => c == *expected,
            RegexOp::Any => true,
            RegexOp::Digit => c.is_ascii_digit(),
            RegexOp::NonDigit => !c.is_ascii_digit(),
            RegexOp::Word => c.is_ascii_alphanumeric() || c == '_',
            RegexOp::NonWord => !(c.is_ascii_alphanumeric() || c == '_'),
            RegexOp::Whitespace => c.is_ascii_whitespace(),
            RegexOp::NonWhitespace => !c.is_ascii_whitespace(),
            RegexOp::CharClass(items) => self.char_matches_class(c, items),
            RegexOp::NegCharClass(items) => !self.char_matches_class(c, items),
            _ => false,
        }
    }

    fn char_matches_class(&self, c: char, items: &[CharClassItem]) -> bool {
        items.iter().any(|item| match item {
            CharClassItem::Char(ch) => c == *ch,
            CharClassItem::Range(start, end) => c >= *start && c <= *end,
        })
    }

    fn match_like_recursive(
        &self,
        value: &[char],
        pattern: &[char],
        vi: usize,
        pi: usize,
    ) -> bool {
        if pi >= pattern.len() {
            return vi >= value.len();
        }

        match pattern[pi] {
            '%' => {
                // % matches zero or more characters
                for i in vi..=value.len() {
                    if self.match_like_recursive(value, pattern, i, pi + 1) {
                        return true;
                    }
                }
                false
            }
            '_' => {
                // _ matches exactly one character
                if vi < value.len() {
                    self.match_like_recursive(value, pattern, vi + 1, pi + 1)
                } else {
                    false
                }
            }
            c => {
                // Literal character match
                if vi < value.len() && value[vi] == c {
                    self.match_like_recursive(value, pattern, vi + 1, pi + 1)
                } else {
                    false
                }
            }
        }
    }

    // ========== JSONB Helper Methods ==========

    /// Parses JSON string bytes to JsonbValue.
    fn parse_json_bytes(&self, bytes: &[u8]) -> Option<JsonbValue> {
        let json_str = core::str::from_utf8(bytes).ok()?;
        self.parse_json_str(json_str)
    }

    /// Parses a JSON string to JsonbValue.
    fn parse_json_str(&self, s: &str) -> Option<JsonbValue> {
        // Simple JSON parser for common types
        let s = s.trim();
        if s == "null" {
            return Some(JsonbValue::Null);
        }
        if s == "true" {
            return Some(JsonbValue::Bool(true));
        }
        if s == "false" {
            return Some(JsonbValue::Bool(false));
        }
        // Try parsing as number
        if let Ok(n) = s.parse::<f64>() {
            return Some(JsonbValue::Number(n));
        }
        // Try parsing as string (quoted)
        if s.starts_with('"') && s.ends_with('"') && s.len() >= 2 {
            let inner = &s[1..s.len() - 1];
            // Handle escape sequences
            let unescaped = self.unescape_json_string(inner);
            return Some(JsonbValue::String(unescaped));
        }
        // Try parsing as object
        if s.starts_with('{') && s.ends_with('}') {
            return self.parse_json_object(s);
        }
        // Try parsing as array
        if s.starts_with('[') && s.ends_with(']') {
            return self.parse_json_array(s);
        }
        None
    }

    /// Unescapes a JSON string.
    fn unescape_json_string(&self, s: &str) -> String {
        let mut result = String::new();
        let mut chars = s.chars().peekable();
        while let Some(c) = chars.next() {
            if c == '\\' {
                if let Some(&next) = chars.peek() {
                    match next {
                        '"' | '\\' | '/' => {
                            result.push(next);
                            chars.next();
                        }
                        'n' => {
                            result.push('\n');
                            chars.next();
                        }
                        'r' => {
                            result.push('\r');
                            chars.next();
                        }
                        't' => {
                            result.push('\t');
                            chars.next();
                        }
                        _ => result.push(c),
                    }
                } else {
                    result.push(c);
                }
            } else {
                result.push(c);
            }
        }
        result
    }

    /// Parses a JSON object string.
    fn parse_json_object(&self, s: &str) -> Option<JsonbValue> {
        let inner = s[1..s.len() - 1].trim();
        if inner.is_empty() {
            return Some(JsonbValue::Object(JsonbObject::new()));
        }

        let mut obj = JsonbObject::new();
        let mut depth = 0;
        let mut in_string = false;
        let mut escape = false;
        let mut start = 0;

        for (i, c) in inner.char_indices() {
            if escape {
                escape = false;
                continue;
            }
            match c {
                '\\' if in_string => escape = true,
                '"' => in_string = !in_string,
                '{' | '[' if !in_string => depth += 1,
                '}' | ']' if !in_string => depth -= 1,
                ',' if !in_string && depth == 0 => {
                    if let Some((k, v)) = self.parse_json_kv(&inner[start..i]) {
                        obj.insert(k, v);
                    }
                    start = i + 1;
                }
                _ => {}
            }
        }
        // Parse last entry
        if start < inner.len() {
            if let Some((k, v)) = self.parse_json_kv(&inner[start..]) {
                obj.insert(k, v);
            }
        }

        Some(JsonbValue::Object(obj))
    }

    /// Parses a JSON key-value pair.
    fn parse_json_kv(&self, s: &str) -> Option<(String, JsonbValue)> {
        let s = s.trim();
        let colon_pos = s.find(':')?;
        let key_part = s[..colon_pos].trim();
        let value_part = s[colon_pos + 1..].trim();

        // Key should be a quoted string
        if key_part.starts_with('"') && key_part.ends_with('"') && key_part.len() >= 2 {
            let key = self.unescape_json_string(&key_part[1..key_part.len() - 1]);
            let value = self.parse_json_str(value_part)?;
            Some((key, value))
        } else {
            None
        }
    }

    /// Parses a JSON array string.
    fn parse_json_array(&self, s: &str) -> Option<JsonbValue> {
        let inner = s[1..s.len() - 1].trim();
        if inner.is_empty() {
            return Some(JsonbValue::Array(Vec::new()));
        }

        let mut items = Vec::new();
        let mut depth = 0;
        let mut in_string = false;
        let mut escape = false;
        let mut start = 0;

        for (i, c) in inner.char_indices() {
            if escape {
                escape = false;
                continue;
            }
            match c {
                '\\' if in_string => escape = true,
                '"' => in_string = !in_string,
                '{' | '[' if !in_string => depth += 1,
                '}' | ']' if !in_string => depth -= 1,
                ',' if !in_string && depth == 0 => {
                    if let Some(v) = self.parse_json_str(inner[start..i].trim()) {
                        items.push(v);
                    }
                    start = i + 1;
                }
                _ => {}
            }
        }
        // Parse last item
        if start < inner.len() {
            if let Some(v) = self.parse_json_str(inner[start..].trim()) {
                items.push(v);
            }
        }

        Some(JsonbValue::Array(items))
    }

    /// Evaluates a JSONB path equality expression.
    fn jsonb_path_eq(&self, jsonb: &cynos_core::JsonbValue, path: &str, expected: &Value) -> Value {
        // Parse the JSON string bytes to JsonbValue
        let json_value = match self.parse_json_bytes(&jsonb.0) {
            Some(v) => v,
            None => return Value::Boolean(false),
        };

        // Parse the JSONPath
        let json_path = match JsonPath::parse(path) {
            Ok(p) => p,
            Err(_) => return Value::Boolean(false),
        };

        // Query the path
        let results = json_value.query(&json_path);
        if results.is_empty() {
            return Value::Boolean(false);
        }

        // Compare the first result with the expected value
        let actual = results[0];
        Value::Boolean(self.compare_jsonb_value(actual, expected))
    }

    /// Checks if a JSONB path exists.
    fn jsonb_path_exists(&self, jsonb: &cynos_core::JsonbValue, path: &str) -> Value {
        // Parse the JSON string bytes to JsonbValue
        let json_value = match self.parse_json_bytes(&jsonb.0) {
            Some(v) => v,
            None => return Value::Boolean(false),
        };

        // Parse the JSONPath
        let json_path = match JsonPath::parse(path) {
            Ok(p) => p,
            Err(_) => return Value::Boolean(false),
        };

        // Query the path
        let results = json_value.query(&json_path);
        Value::Boolean(!results.is_empty())
    }

    /// Compares a JsonbValue with a Value.
    fn compare_jsonb_value(&self, jsonb: &JsonbValue, value: &Value) -> bool {
        match (jsonb, value) {
            (JsonbValue::Null, Value::Null) => true,
            (JsonbValue::Bool(a), Value::Boolean(b)) => a == b,
            (JsonbValue::Number(a), Value::Int32(b)) => (*a - *b as f64).abs() < f64::EPSILON,
            (JsonbValue::Number(a), Value::Int64(b)) => (*a - *b as f64).abs() < f64::EPSILON,
            (JsonbValue::Number(a), Value::Float64(b)) => (*a - *b).abs() < f64::EPSILON,
            (JsonbValue::String(a), Value::String(b)) => a == b,
            _ => false,
        }
    }

    // ========== Helper Methods ==========

    /// Extracts join key column indices from a join condition.
    fn extract_join_keys(
        &self,
        condition: &Expr,
        left: &Relation,
        right: &Relation,
    ) -> ExecutionResult<(usize, usize)> {
        if let Expr::BinaryOp {
            left: left_expr,
            op: BinaryOp::Eq,
            right: right_expr,
        } = condition
        {
            let left_col = self.extract_column_ref(left_expr)?;
            let right_col = self.extract_column_ref(right_expr)?;

            // Determine which column belongs to which relation
            let left_tables = left.tables();
            let right_tables = right.tables();
            let left_ctx = EvalContext::new(left_tables, left.table_column_counts());
            let right_ctx = EvalContext::new(right_tables, right.table_column_counts());

            if left_tables.contains(&left_col.table) && right_tables.contains(&right_col.table) {
                // left_col belongs to left relation, right_col belongs to right relation
                let left_idx = left_ctx.resolve_column_index(&left_col.table, left_col.index);
                let right_idx = right_ctx.resolve_column_index(&right_col.table, right_col.index);
                Ok((left_idx, right_idx))
            } else if left_tables.contains(&right_col.table)
                && right_tables.contains(&left_col.table)
            {
                // right_col belongs to left relation, left_col belongs to right relation
                let left_idx = left_ctx.resolve_column_index(&right_col.table, right_col.index);
                let right_idx = right_ctx.resolve_column_index(&left_col.table, left_col.index);
                Ok((left_idx, right_idx))
            } else {
                Err(ExecutionError::InvalidOperation(
                    "Join columns do not match relation tables".into(),
                ))
            }
        } else {
            Err(ExecutionError::InvalidOperation(
                "Expected equi-join condition".into(),
            ))
        }
    }

    fn extract_outer_key_index(
        &self,
        condition: &Expr,
        outer: &Relation,
    ) -> ExecutionResult<usize> {
        if let Expr::BinaryOp {
            left: left_expr,
            op: BinaryOp::Eq,
            right: right_expr,
        } = condition
        {
            let left_col = self.extract_column_ref(left_expr)?;
            let right_col = self.extract_column_ref(right_expr)?;

            let outer_tables = outer.tables();

            if outer_tables.contains(&left_col.table) {
                Ok(left_col.index)
            } else if outer_tables.contains(&right_col.table) {
                Ok(right_col.index)
            } else {
                Err(ExecutionError::InvalidOperation(
                    "Outer key column not found in outer relation".into(),
                ))
            }
        } else {
            Err(ExecutionError::InvalidOperation(
                "Expected equi-join condition".into(),
            ))
        }
    }

    fn extract_column_ref<'b>(&self, expr: &'b Expr) -> ExecutionResult<&'b ColumnRef> {
        if let Expr::Column(col) = expr {
            Ok(col)
        } else {
            Err(ExecutionError::InvalidOperation(
                "Expected column reference".into(),
            ))
        }
    }

    /// Adjusts column indices in a join condition for nested loop join evaluation.
    /// Right table columns need to be offset by the left table's column count
    /// since the combined row has left columns first, then right columns.
    fn adjust_join_condition_indices(
        &self,
        condition: &Expr,
        left_tables: &[String],
        right_tables: &[String],
        left_col_count: usize,
    ) -> Expr {
        match condition {
            Expr::Column(col) => {
                // Check if this column belongs to the right table
                if right_tables.contains(&col.table) {
                    // Adjust index by adding left table column count
                    Expr::Column(ColumnRef {
                        table: col.table.clone(),
                        column: col.column.clone(),
                        index: col.index + left_col_count,
                    })
                } else {
                    // Left table column, keep as is
                    condition.clone()
                }
            }
            Expr::BinaryOp { left, op, right } => {
                let adjusted_left = self.adjust_join_condition_indices(left, left_tables, right_tables, left_col_count);
                let adjusted_right = self.adjust_join_condition_indices(right, left_tables, right_tables, left_col_count);
                Expr::BinaryOp {
                    left: Box::new(adjusted_left),
                    op: *op,
                    right: Box::new(adjusted_right),
                }
            }
            Expr::UnaryOp { op, expr } => {
                let adjusted_expr = self.adjust_join_condition_indices(expr, left_tables, right_tables, left_col_count);
                Expr::UnaryOp {
                    op: *op,
                    expr: Box::new(adjusted_expr),
                }
            }
            // For other expression types, return as is
            _ => condition.clone(),
        }
    }
}

/// A simple in-memory data source for testing and simple use cases.
#[derive(Default)]
pub struct InMemoryDataSource {
    tables: BTreeMap<String, TableData>,
}

/// Data for a single table.
#[derive(Default)]
struct TableData {
    rows: Vec<Rc<Row>>,
    indexes: BTreeMap<String, IndexData>,
    column_count: usize,
}

/// Data for a single index.
struct IndexData {
    /// Maps key values to row indices.
    key_to_rows: BTreeMap<Value, Vec<usize>>,
}

impl InMemoryDataSource {
    /// Creates a new empty in-memory data source.
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds a table with the given rows.
    pub fn add_table(&mut self, name: impl Into<String>, rows: Vec<Row>, column_count: usize) {
        self.tables.insert(
            name.into(),
            TableData {
                rows: rows.into_iter().map(Rc::new).collect(),
                indexes: BTreeMap::new(),
                column_count,
            },
        );
    }

    /// Creates an index on a table column.
    pub fn create_index(
        &mut self,
        table: &str,
        index_name: impl Into<String>,
        column_index: usize,
    ) -> ExecutionResult<()> {
        let table_data = self
            .tables
            .get_mut(table)
            .ok_or_else(|| ExecutionError::TableNotFound(table.into()))?;

        let mut key_to_rows: BTreeMap<Value, Vec<usize>> = BTreeMap::new();

        for (row_idx, row) in table_data.rows.iter().enumerate() {
            if let Some(key) = row.get(column_index) {
                key_to_rows.entry(key.clone()).or_default().push(row_idx);
            }
        }

        table_data
            .indexes
            .insert(index_name.into(), IndexData { key_to_rows });

        Ok(())
    }
}

impl DataSource for InMemoryDataSource {
    fn get_table_rows(&self, table: &str) -> ExecutionResult<Vec<Rc<Row>>> {
        self.tables
            .get(table)
            .map(|t| t.rows.clone())
            .ok_or_else(|| ExecutionError::TableNotFound(table.into()))
    }

    fn get_index_range(
        &self,
        table: &str,
        index: &str,
        range_start: Option<&Value>,
        range_end: Option<&Value>,
        include_start: bool,
        include_end: bool,
    ) -> ExecutionResult<Vec<Rc<Row>>> {
        let table_data = self
            .tables
            .get(table)
            .ok_or_else(|| ExecutionError::TableNotFound(table.into()))?;

        let index_data = table_data.indexes.get(index).ok_or_else(|| {
            ExecutionError::IndexNotFound {
                table: table.into(),
                index: index.into(),
            }
        })?;

        let mut result = Vec::new();

        for (key, row_indices) in &index_data.key_to_rows {
            let in_range = match (range_start, range_end) {
                (Some(start), Some(end)) => {
                    let start_ok = if include_start {
                        key >= start
                    } else {
                        key > start
                    };
                    let end_ok = if include_end { key <= end } else { key < end };
                    start_ok && end_ok
                }
                (Some(start), None) => {
                    if include_start {
                        key >= start
                    } else {
                        key > start
                    }
                }
                (None, Some(end)) => {
                    if include_end {
                        key <= end
                    } else {
                        key < end
                    }
                }
                (None, None) => true,
            };

            if in_range {
                for &idx in row_indices {
                    result.push(Rc::clone(&table_data.rows[idx]));
                }
            }
        }

        Ok(result)
    }

    fn get_index_range_with_limit(
        &self,
        table: &str,
        index: &str,
        range_start: Option<&Value>,
        range_end: Option<&Value>,
        include_start: bool,
        include_end: bool,
        limit: Option<usize>,
        offset: usize,
        reverse: bool,
    ) -> ExecutionResult<Vec<Rc<Row>>> {
        let table_data = self
            .tables
            .get(table)
            .ok_or_else(|| ExecutionError::TableNotFound(table.into()))?;

        let index_data = table_data.indexes.get(index).ok_or_else(|| {
            ExecutionError::IndexNotFound {
                table: table.into(),
                index: index.into(),
            }
        })?;

        // Collect keys in range first
        let keys_in_range: Vec<&Value> = index_data
            .key_to_rows
            .keys()
            .filter(|key| {
                match (range_start, range_end) {
                    (Some(start), Some(end)) => {
                        let start_ok = if include_start {
                            *key >= start
                        } else {
                            *key > start
                        };
                        let end_ok = if include_end { *key <= end } else { *key < end };
                        start_ok && end_ok
                    }
                    (Some(start), None) => {
                        if include_start {
                            *key >= start
                        } else {
                            *key > start
                        }
                    }
                    (None, Some(end)) => {
                        if include_end {
                            *key <= end
                        } else {
                            *key < end
                        }
                    }
                    (None, None) => true,
                }
            })
            .collect();

        let mut result = Vec::new();
        let mut skipped = 0;
        let mut collected = 0;

        // Iterate in forward or reverse order based on the reverse flag
        let iter: Box<dyn Iterator<Item = &&Value>> = if reverse {
            Box::new(keys_in_range.iter().rev())
        } else {
            Box::new(keys_in_range.iter())
        };

        for key in iter {
            if let Some(row_indices) = index_data.key_to_rows.get(*key) {
                for &idx in row_indices {
                    // Apply offset
                    if skipped < offset {
                        skipped += 1;
                        continue;
                    }
                    // Apply limit
                    if let Some(lim) = limit {
                        if collected >= lim {
                            return Ok(result);
                        }
                    }
                    result.push(Rc::clone(&table_data.rows[idx]));
                    collected += 1;
                }
            }
        }

        Ok(result)
    }

    fn get_index_point(&self, table: &str, index: &str, key: &Value) -> ExecutionResult<Vec<Rc<Row>>> {
        let table_data = self
            .tables
            .get(table)
            .ok_or_else(|| ExecutionError::TableNotFound(table.into()))?;

        let index_data = table_data.indexes.get(index).ok_or_else(|| {
            ExecutionError::IndexNotFound {
                table: table.into(),
                index: index.into(),
            }
        })?;

        let result = index_data
            .key_to_rows
            .get(key)
            .map(|indices| indices.iter().map(|&i| Rc::clone(&table_data.rows[i])).collect())
            .unwrap_or_default();

        Ok(result)
    }

    fn get_column_count(&self, table: &str) -> ExecutionResult<usize> {
        self.tables
            .get(table)
            .map(|t| t.column_count)
            .ok_or_else(|| ExecutionError::TableNotFound(table.into()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::JoinType;
    use alloc::boxed::Box;
    use alloc::vec;

    fn create_test_data_source() -> InMemoryDataSource {
        let mut ds = InMemoryDataSource::new();

        // Users table: id, name, dept_id
        let users = vec![
            Row::new(1, vec![Value::Int64(1), Value::String("Alice".into()), Value::Int64(10)]),
            Row::new(2, vec![Value::Int64(2), Value::String("Bob".into()), Value::Int64(20)]),
            Row::new(3, vec![Value::Int64(3), Value::String("Charlie".into()), Value::Int64(10)]),
        ];
        ds.add_table("users", users, 3);
        ds.create_index("users", "idx_id", 0).unwrap();
        ds.create_index("users", "idx_dept", 2).unwrap();

        // Departments table: id, name
        let depts = vec![
            Row::new(10, vec![Value::Int64(10), Value::String("Engineering".into())]),
            Row::new(20, vec![Value::Int64(20), Value::String("Sales".into())]),
            Row::new(30, vec![Value::Int64(30), Value::String("Marketing".into())]),
        ];
        ds.add_table("departments", depts, 2);
        ds.create_index("departments", "idx_id", 0).unwrap();

        ds
    }

    #[test]
    fn test_table_scan() {
        let ds = create_test_data_source();
        let runner = PhysicalPlanRunner::new(&ds);

        let plan = PhysicalPlan::table_scan("users");
        let result = runner.execute(&plan).unwrap();

        assert_eq!(result.len(), 3);
        assert_eq!(result.tables(), &["users"]);
    }

    #[test]
    fn test_index_scan() {
        let ds = create_test_data_source();
        let runner = PhysicalPlanRunner::new(&ds);

        let plan = PhysicalPlan::IndexScan {
            table: "users".into(),
            index: "idx_id".into(),
            range_start: Some(Value::Int64(1)),
            range_end: Some(Value::Int64(2)),
            include_start: true,
            include_end: true,
            limit: None,
            offset: None,
            reverse: false,
        };
        let result = runner.execute(&plan).unwrap();

        assert_eq!(result.len(), 2);
    }

    #[test]
    fn test_index_get() {
        let ds = create_test_data_source();
        let runner = PhysicalPlanRunner::new(&ds);

        let plan = PhysicalPlan::index_get("users", "idx_id", Value::Int64(2));
        let result = runner.execute(&plan).unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(
            result.entries[0].get_field(1),
            Some(&Value::String("Bob".into()))
        );
    }

    #[test]
    fn test_filter() {
        let ds = create_test_data_source();
        let runner = PhysicalPlanRunner::new(&ds);

        // Filter: dept_id = 10
        let plan = PhysicalPlan::filter(
            PhysicalPlan::table_scan("users"),
            Expr::eq(
                Expr::column("users", "dept_id", 2),
                Expr::literal(Value::Int64(10)),
            ),
        );
        let result = runner.execute(&plan).unwrap();

        assert_eq!(result.len(), 2); // Alice and Charlie
    }

    #[test]
    fn test_project() {
        let ds = create_test_data_source();
        let runner = PhysicalPlanRunner::new(&ds);

        // Project: id, name
        let plan = PhysicalPlan::project(
            PhysicalPlan::table_scan("users"),
            vec![
                Expr::column("users", "id", 0),
                Expr::column("users", "name", 1),
            ],
        );
        let result = runner.execute(&plan).unwrap();

        assert_eq!(result.len(), 3);
        assert_eq!(result.entries[0].row.len(), 2);
    }

    #[test]
    fn test_hash_join() {
        let ds = create_test_data_source();
        let runner = PhysicalPlanRunner::new(&ds);

        // Join users and departments on dept_id = id
        let plan = PhysicalPlan::hash_join(
            PhysicalPlan::table_scan("users"),
            PhysicalPlan::table_scan("departments"),
            Expr::eq(
                Expr::column("users", "dept_id", 2),
                Expr::column("departments", "id", 0),
            ),
            JoinType::Inner,
        );
        let result = runner.execute(&plan).unwrap();

        // Alice and Charlie match Engineering (10), Bob matches Sales (20)
        assert_eq!(result.len(), 3);
        assert_eq!(result.tables(), &["users", "departments"]);
    }

    #[test]
    fn test_left_outer_join() {
        let mut ds = InMemoryDataSource::new();

        let left = vec![
            Row::new(1, vec![Value::Int64(1)]),
            Row::new(2, vec![Value::Int64(2)]),
            Row::new(3, vec![Value::Int64(3)]),
        ];
        ds.add_table("left", left, 1);

        let right = vec![Row::new(10, vec![Value::Int64(1)])];
        ds.add_table("right", right, 1);

        let runner = PhysicalPlanRunner::new(&ds);

        let plan = PhysicalPlan::hash_join(
            PhysicalPlan::table_scan("left"),
            PhysicalPlan::table_scan("right"),
            Expr::eq(
                Expr::column("left", "id", 0),
                Expr::column("right", "id", 0),
            ),
            JoinType::LeftOuter,
        );
        let result = runner.execute(&plan).unwrap();

        // 1 match + 2 unmatched with nulls
        assert_eq!(result.len(), 3);
    }

    #[test]
    fn test_nested_loop_join_with_predicate() {
        let mut ds = InMemoryDataSource::new();

        let left = vec![
            Row::new(1, vec![Value::Int64(10)]),
            Row::new(2, vec![Value::Int64(20)]),
        ];
        ds.add_table("left", left, 1);

        let right = vec![
            Row::new(10, vec![Value::Int64(5)]),
            Row::new(11, vec![Value::Int64(15)]),
            Row::new(12, vec![Value::Int64(25)]),
        ];
        ds.add_table("right", right, 1);

        let runner = PhysicalPlanRunner::new(&ds);

        // left.value > right.value
        let plan = PhysicalPlan::nested_loop_join(
            PhysicalPlan::table_scan("left"),
            PhysicalPlan::table_scan("right"),
            Expr::gt(
                Expr::column("left", "value", 0),
                Expr::column("right", "value", 0),
            ),
            JoinType::Inner,
        );
        let result = runner.execute(&plan).unwrap();

        // 10 > 5, 20 > 5, 20 > 15 = 3 matches
        assert_eq!(result.len(), 3);
    }

    #[test]
    fn test_cross_product() {
        let mut ds = InMemoryDataSource::new();

        let left = vec![
            Row::new(1, vec![Value::Int64(1)]),
            Row::new(2, vec![Value::Int64(2)]),
        ];
        ds.add_table("left", left, 1);

        let right = vec![
            Row::new(10, vec![Value::String("A".into())]),
            Row::new(11, vec![Value::String("B".into())]),
            Row::new(12, vec![Value::String("C".into())]),
        ];
        ds.add_table("right", right, 1);

        let runner = PhysicalPlanRunner::new(&ds);

        let plan = PhysicalPlan::CrossProduct {
            left: Box::new(PhysicalPlan::table_scan("left")),
            right: Box::new(PhysicalPlan::table_scan("right")),
        };
        let result = runner.execute(&plan).unwrap();

        // 2 * 3 = 6
        assert_eq!(result.len(), 6);
    }

    #[test]
    fn test_sort() {
        let ds = create_test_data_source();
        let runner = PhysicalPlanRunner::new(&ds);

        let plan = PhysicalPlan::sort(
            PhysicalPlan::table_scan("users"),
            vec![(Expr::column("users", "name", 1), SortOrder::Asc)],
        );
        let result = runner.execute(&plan).unwrap();

        assert_eq!(result.len(), 3);
        // Should be sorted: Alice, Bob, Charlie
        assert_eq!(
            result.entries[0].get_field(1),
            Some(&Value::String("Alice".into()))
        );
        assert_eq!(
            result.entries[1].get_field(1),
            Some(&Value::String("Bob".into()))
        );
        assert_eq!(
            result.entries[2].get_field(1),
            Some(&Value::String("Charlie".into()))
        );
    }

    #[test]
    fn test_limit() {
        let ds = create_test_data_source();
        let runner = PhysicalPlanRunner::new(&ds);

        let plan = PhysicalPlan::limit(PhysicalPlan::table_scan("users"), 2, 1);
        let result = runner.execute(&plan).unwrap();

        assert_eq!(result.len(), 2);
    }

    #[test]
    fn test_aggregate_count() {
        let ds = create_test_data_source();
        let runner = PhysicalPlanRunner::new(&ds);

        let plan = PhysicalPlan::hash_aggregate(
            PhysicalPlan::table_scan("users"),
            vec![],
            vec![(AggregateFunc::Count, Expr::column("users", "id", 0))],
        );
        let result = runner.execute(&plan).unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result.entries[0].get_field(0), Some(&Value::Int64(3)));
    }

    #[test]
    fn test_aggregate_group_by() {
        let ds = create_test_data_source();
        let runner = PhysicalPlanRunner::new(&ds);

        // COUNT(*) GROUP BY dept_id
        let plan = PhysicalPlan::hash_aggregate(
            PhysicalPlan::table_scan("users"),
            vec![Expr::column("users", "dept_id", 2)],
            vec![(AggregateFunc::Count, Expr::column("users", "id", 0))],
        );
        let result = runner.execute(&plan).unwrap();

        // Two groups: dept_id=10 (2 users), dept_id=20 (1 user)
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn test_complex_query() {
        let ds = create_test_data_source();
        let runner = PhysicalPlanRunner::new(&ds);

        // SELECT name FROM users WHERE dept_id = 10 ORDER BY name LIMIT 1
        let plan = PhysicalPlan::limit(
            PhysicalPlan::sort(
                PhysicalPlan::project(
                    PhysicalPlan::filter(
                        PhysicalPlan::table_scan("users"),
                        Expr::eq(
                            Expr::column("users", "dept_id", 2),
                            Expr::literal(Value::Int64(10)),
                        ),
                    ),
                    vec![Expr::column("users", "name", 1)],
                ),
                vec![(Expr::column("users", "name", 0), SortOrder::Asc)],
            ),
            1,
            0,
        );
        let result = runner.execute(&plan).unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(
            result.entries[0].get_field(0),
            Some(&Value::String("Alice".into()))
        );
    }

    #[test]
    fn test_empty_result() {
        let ds = create_test_data_source();
        let runner = PhysicalPlanRunner::new(&ds);

        let plan = PhysicalPlan::Empty;
        let result = runner.execute(&plan).unwrap();

        assert!(result.is_empty());
    }

    #[test]
    fn test_noop() {
        let ds = create_test_data_source();
        let runner = PhysicalPlanRunner::new(&ds);

        let plan = PhysicalPlan::NoOp {
            input: Box::new(PhysicalPlan::table_scan("users")),
        };
        let result = runner.execute(&plan).unwrap();

        assert_eq!(result.len(), 3);
    }

    #[test]
    fn test_expression_evaluation() {
        let ds = create_test_data_source();
        let runner = PhysicalPlanRunner::new(&ds);

        // Test arithmetic in projection
        let plan = PhysicalPlan::project(
            PhysicalPlan::table_scan("users"),
            vec![Expr::BinaryOp {
                left: Box::new(Expr::column("users", "id", 0)),
                op: BinaryOp::Mul,
                right: Box::new(Expr::literal(Value::Int64(10))),
            }],
        );
        let result = runner.execute(&plan).unwrap();

        assert_eq!(result.entries[0].get_field(0), Some(&Value::Int64(10)));
        assert_eq!(result.entries[1].get_field(0), Some(&Value::Int64(20)));
        assert_eq!(result.entries[2].get_field(0), Some(&Value::Int64(30)));
    }

    #[test]
    fn test_like_pattern() {
        let mut ds = InMemoryDataSource::new();

        let data = vec![
            Row::new(1, vec![Value::String("Alice".into())]),
            Row::new(2, vec![Value::String("Bob".into())]),
            Row::new(3, vec![Value::String("Charlie".into())]),
            Row::new(4, vec![Value::String("Alex".into())]),
        ];
        ds.add_table("names", data, 1);

        let runner = PhysicalPlanRunner::new(&ds);

        // Filter: name LIKE 'Al%'
        let plan = PhysicalPlan::filter(
            PhysicalPlan::table_scan("names"),
            Expr::Like {
                expr: Box::new(Expr::column("names", "name", 0)),
                pattern: "Al%".into(),
            },
        );
        let result = runner.execute(&plan).unwrap();

        assert_eq!(result.len(), 2); // Alice and Alex
    }

    #[test]
    fn test_between() {
        let mut ds = InMemoryDataSource::new();

        let data = vec![
            Row::new(1, vec![Value::Int64(5)]),
            Row::new(2, vec![Value::Int64(10)]),
            Row::new(3, vec![Value::Int64(15)]),
            Row::new(4, vec![Value::Int64(20)]),
        ];
        ds.add_table("numbers", data, 1);

        let runner = PhysicalPlanRunner::new(&ds);

        // Filter: value BETWEEN 10 AND 15
        let plan = PhysicalPlan::filter(
            PhysicalPlan::table_scan("numbers"),
            Expr::Between {
                expr: Box::new(Expr::column("numbers", "value", 0)),
                low: Box::new(Expr::literal(Value::Int64(10))),
                high: Box::new(Expr::literal(Value::Int64(15))),
            },
        );
        let result = runner.execute(&plan).unwrap();

        assert_eq!(result.len(), 2); // 10 and 15
    }

    #[test]
    fn test_in_list() {
        let ds = create_test_data_source();
        let runner = PhysicalPlanRunner::new(&ds);

        // Filter: id IN (1, 3)
        let plan = PhysicalPlan::filter(
            PhysicalPlan::table_scan("users"),
            Expr::In {
                expr: Box::new(Expr::column("users", "id", 0)),
                list: vec![
                    Expr::literal(Value::Int64(1)),
                    Expr::literal(Value::Int64(3)),
                ],
            },
        );
        let result = runner.execute(&plan).unwrap();

        assert_eq!(result.len(), 2); // Alice and Charlie
    }

    #[test]
    fn test_not_in_list() {
        let ds = create_test_data_source();
        let runner = PhysicalPlanRunner::new(&ds);

        // Filter: id NOT IN (1, 3)
        let plan = PhysicalPlan::filter(
            PhysicalPlan::table_scan("users"),
            Expr::NotIn {
                expr: Box::new(Expr::column("users", "id", 0)),
                list: vec![
                    Expr::literal(Value::Int64(1)),
                    Expr::literal(Value::Int64(3)),
                ],
            },
        );
        let result = runner.execute(&plan).unwrap();

        assert_eq!(result.len(), 1); // Only Bob (id=2)
    }

    #[test]
    fn test_not_between() {
        let ds = create_test_data_source();
        let runner = PhysicalPlanRunner::new(&ds);

        // Filter: dept_id NOT BETWEEN 15 AND 25 (only dept_id=10 passes)
        let plan = PhysicalPlan::filter(
            PhysicalPlan::table_scan("users"),
            Expr::NotBetween {
                expr: Box::new(Expr::column("users", "dept_id", 2)),
                low: Box::new(Expr::literal(Value::Int64(15))),
                high: Box::new(Expr::literal(Value::Int64(25))),
            },
        );
        let result = runner.execute(&plan).unwrap();

        assert_eq!(result.len(), 2); // Alice and Charlie (dept_id=10)
    }

    #[test]
    fn test_not_like() {
        let ds = create_test_data_source();
        let runner = PhysicalPlanRunner::new(&ds);

        // Filter: name NOT LIKE 'A%'
        let plan = PhysicalPlan::filter(
            PhysicalPlan::table_scan("users"),
            Expr::NotLike {
                expr: Box::new(Expr::column("users", "name", 1)),
                pattern: "A%".into(),
            },
        );
        let result = runner.execute(&plan).unwrap();

        assert_eq!(result.len(), 2); // Bob and Charlie
    }

    #[test]
    fn test_regex_match() {
        let ds = create_test_data_source();
        let runner = PhysicalPlanRunner::new(&ds);

        // Filter: name MATCH '^[AB].*'
        let plan = PhysicalPlan::filter(
            PhysicalPlan::table_scan("users"),
            Expr::Match {
                expr: Box::new(Expr::column("users", "name", 1)),
                pattern: "^[AB].*".into(),
            },
        );
        let result = runner.execute(&plan).unwrap();

        assert_eq!(result.len(), 2); // Alice and Bob
    }

    #[test]
    fn test_regex_match_digit() {
        let mut ds = InMemoryDataSource::new();
        let data = vec![
            Row::new(1, vec![Value::String("abc123".into())]),
            Row::new(2, vec![Value::String("xyz".into())]),
            Row::new(3, vec![Value::String("test456def".into())]),
        ];
        ds.add_table("data", data, 1);

        let runner = PhysicalPlanRunner::new(&ds);

        // Filter: col MATCH '\d+'
        let plan = PhysicalPlan::filter(
            PhysicalPlan::table_scan("data"),
            Expr::Match {
                expr: Box::new(Expr::column("data", "col", 0)),
                pattern: "\\d+".into(),
            },
        );
        let result = runner.execute(&plan).unwrap();

        assert_eq!(result.len(), 2); // abc123 and test456def
    }

    #[test]
    fn test_not_regex_match() {
        let ds = create_test_data_source();
        let runner = PhysicalPlanRunner::new(&ds);

        // Filter: name NOT MATCH '^[AB].*'
        let plan = PhysicalPlan::filter(
            PhysicalPlan::table_scan("users"),
            Expr::NotMatch {
                expr: Box::new(Expr::column("users", "name", 1)),
                pattern: "^[AB].*".into(),
            },
        );
        let result = runner.execute(&plan).unwrap();

        assert_eq!(result.len(), 1); // Only Charlie
    }

    #[test]
    fn test_null_handling() {
        let mut ds = InMemoryDataSource::new();

        let data = vec![
            Row::new(1, vec![Value::Int64(1), Value::String("A".into())]),
            Row::new(2, vec![Value::Int64(2), Value::Null]),
            Row::new(3, vec![Value::Null, Value::String("C".into())]),
        ];
        ds.add_table("data", data, 2);

        let runner = PhysicalPlanRunner::new(&ds);

        // Filter: col1 IS NOT NULL
        let plan = PhysicalPlan::filter(
            PhysicalPlan::table_scan("data"),
            Expr::is_not_null(Expr::column("data", "col1", 0)),
        );
        let result = runner.execute(&plan).unwrap();

        assert_eq!(result.len(), 2);
    }

    #[test]
    fn test_index_scan_reverse() {
        let mut ds = InMemoryDataSource::new();

        // Create table with scores: 10, 20, 30, 40, 50
        let data = vec![
            Row::new(1, vec![Value::Int64(10)]),
            Row::new(2, vec![Value::Int64(20)]),
            Row::new(3, vec![Value::Int64(30)]),
            Row::new(4, vec![Value::Int64(40)]),
            Row::new(5, vec![Value::Int64(50)]),
        ];
        ds.add_table("scores", data, 1);
        ds.create_index("scores", "idx_score", 0).unwrap();

        let runner = PhysicalPlanRunner::new(&ds);

        // Forward scan with limit 3: should get 10, 20, 30
        let plan_forward = PhysicalPlan::IndexScan {
            table: "scores".into(),
            index: "idx_score".into(),
            range_start: None,
            range_end: None,
            include_start: true,
            include_end: true,
            limit: Some(3),
            offset: None,
            reverse: false,
        };
        let result_forward = runner.execute(&plan_forward).unwrap();
        assert_eq!(result_forward.len(), 3);
        assert_eq!(result_forward.entries[0].get_field(0), Some(&Value::Int64(10)));
        assert_eq!(result_forward.entries[1].get_field(0), Some(&Value::Int64(20)));
        assert_eq!(result_forward.entries[2].get_field(0), Some(&Value::Int64(30)));

        // Reverse scan with limit 3: should get 50, 40, 30
        let plan_reverse = PhysicalPlan::IndexScan {
            table: "scores".into(),
            index: "idx_score".into(),
            range_start: None,
            range_end: None,
            include_start: true,
            include_end: true,
            limit: Some(3),
            offset: None,
            reverse: true,
        };
        let result_reverse = runner.execute(&plan_reverse).unwrap();
        assert_eq!(result_reverse.len(), 3);
        assert_eq!(result_reverse.entries[0].get_field(0), Some(&Value::Int64(50)));
        assert_eq!(result_reverse.entries[1].get_field(0), Some(&Value::Int64(40)));
        assert_eq!(result_reverse.entries[2].get_field(0), Some(&Value::Int64(30)));
    }

    #[test]
    fn test_index_scan_reverse_with_offset() {
        let mut ds = InMemoryDataSource::new();

        // Create table with scores: 10, 20, 30, 40, 50
        let data = vec![
            Row::new(1, vec![Value::Int64(10)]),
            Row::new(2, vec![Value::Int64(20)]),
            Row::new(3, vec![Value::Int64(30)]),
            Row::new(4, vec![Value::Int64(40)]),
            Row::new(5, vec![Value::Int64(50)]),
        ];
        ds.add_table("scores", data, 1);
        ds.create_index("scores", "idx_score", 0).unwrap();

        let runner = PhysicalPlanRunner::new(&ds);

        // Reverse scan with offset 1, limit 2: skip 50, get 40, 30
        let plan = PhysicalPlan::IndexScan {
            table: "scores".into(),
            index: "idx_score".into(),
            range_start: None,
            range_end: None,
            include_start: true,
            include_end: true,
            limit: Some(2),
            offset: Some(1),
            reverse: true,
        };
        let result = runner.execute(&plan).unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result.entries[0].get_field(0), Some(&Value::Int64(40)));
        assert_eq!(result.entries[1].get_field(0), Some(&Value::Int64(30)));
    }
}
