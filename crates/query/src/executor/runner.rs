//! Physical plan runner - unified execution framework for physical query plans.
//!
//! This module provides the `PhysicalPlanRunner` which executes physical query plans
//! by recursively evaluating plan nodes and combining results using the appropriate
//! execution operators.

use crate::ast::{AggregateFunc, BinaryOp, ColumnRef, Expr, SortOrder, UnaryOp};
use crate::executor::{
    AggregateExecutor, LimitExecutor, Relation, RelationEntry, SharedTables, SortExecutor,
};
use crate::planner::{IndexBounds, PhysicalPlan};
use alloc::boxed::Box;
use alloc::collections::BTreeMap;
use alloc::rc::Rc;
use alloc::string::String;
use alloc::vec;
use alloc::vec::Vec;
use core::cmp::Ordering;
use cynos_core::{Row, Value};
use cynos_index::KeyRange;
use cynos_jsonb::{JsonPath, JsonbObject, JsonbValue};

// ========== TopN Heap Entry ==========

const NULL_VALUE: Value = Value::Null;

/// Entry wrapper for binary heap-based TopN execution.
/// Stores a reference to the order_by indices to avoid cloning per entry.
struct TopNHeapEntry<'a> {
    entry: RelationEntry,
    order_by: &'a [(usize, SortOrder)],
}

impl<'a> PartialEq for TopNHeapEntry<'a> {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl<'a> Eq for TopNHeapEntry<'a> {}

impl<'a> PartialOrd for TopNHeapEntry<'a> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<'a> Ord for TopNHeapEntry<'a> {
    fn cmp(&self, other: &Self) -> Ordering {
        // For TopN with ASC order, we want to keep the k smallest elements.
        // BinaryHeap is a max-heap, so the largest element is at the top.
        // We need the heap to keep the k smallest, with the largest of those at top.
        // So for ASC: normal comparison (larger values have higher priority, stay at top)
        // For DESC: reversed comparison (smaller values have higher priority, stay at top)
        for (idx, order) in self.order_by {
            let a = self.entry.get_field(*idx);
            let b = other.entry.get_field(*idx);
            let cmp = match (a, b) {
                (Some(va), Some(vb)) => va.partial_cmp(vb).unwrap_or(Ordering::Equal),
                (Some(_), None) => Ordering::Greater,
                (None, Some(_)) => Ordering::Less,
                (None, None) => Ordering::Equal,
            };
            if cmp != Ordering::Equal {
                // For ASC: keep smallest k, so larger values should be at heap top (normal order)
                // For DESC: keep largest k, so smaller values should be at heap top (reversed)
                let final_cmp = match order {
                    SortOrder::Asc => cmp,
                    SortOrder::Desc => cmp.reverse(),
                };
                return final_cmp;
            }
        }
        Ordering::Equal
    }
}

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
        Self {
            tables,
            table_column_counts,
        }
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

trait RowAccessor {
    fn get_value(&self, index: usize) -> Option<&Value>;
}

impl RowAccessor for Row {
    #[inline]
    fn get_value(&self, index: usize) -> Option<&Value> {
        self.get(index)
    }
}

impl RowAccessor for RelationEntry {
    #[inline]
    fn get_value(&self, index: usize) -> Option<&Value> {
        self.get_field(index)
    }
}

impl RowAccessor for &[Value] {
    #[inline]
    fn get_value(&self, index: usize) -> Option<&Value> {
        self.get(index)
    }
}

struct JoinedRowView<'a> {
    left: Option<&'a RelationEntry>,
    right: Option<&'a RelationEntry>,
    left_width: usize,
    right_width: usize,
}

impl<'a> JoinedRowView<'a> {
    #[inline]
    fn new(
        left: Option<&'a RelationEntry>,
        right: Option<&'a RelationEntry>,
        left_width: usize,
        right_width: usize,
    ) -> Self {
        Self {
            left,
            right,
            left_width,
            right_width,
        }
    }

    #[inline]
    fn version(&self) -> u64 {
        match (self.left, self.right) {
            (Some(left), Some(right)) => left.row.version().wrapping_add(right.row.version()),
            (Some(left), None) => left.row.version(),
            (None, Some(right)) => right.row.version(),
            (None, None) => 0,
        }
    }

    fn materialize_row(&self) -> Rc<Row> {
        let mut values = Vec::with_capacity(self.left_width + self.right_width);
        if let Some(left) = self.left {
            values.extend(left.row.values().iter().cloned());
        } else {
            values.resize(self.left_width, Value::Null);
        }

        if let Some(right) = self.right {
            values.extend(right.row.values().iter().cloned());
        } else {
            values.resize(self.left_width + self.right_width, Value::Null);
        }

        Rc::new(Row::dummy_with_version(self.version(), values))
    }

    #[inline]
    fn materialize_entry(&self, shared_tables: SharedTables) -> RelationEntry {
        RelationEntry::new_combined(self.materialize_row(), shared_tables)
    }
}

impl RowAccessor for JoinedRowView<'_> {
    #[inline]
    fn get_value(&self, index: usize) -> Option<&Value> {
        if index < self.left_width {
            return Some(match self.left {
                Some(left) => left.get_field(index).unwrap_or(&NULL_VALUE),
                None => &NULL_VALUE,
            });
        }

        let right_index = index - self.left_width;
        if right_index < self.right_width {
            return Some(match self.right {
                Some(right) => right.get_field(right_index).unwrap_or(&NULL_VALUE),
                None => &NULL_VALUE,
            });
        }

        None
    }
}

#[derive(Clone, Debug)]
struct SimpleBinaryPredicate {
    column_index: usize,
    op: BinaryOp,
    literal: Value,
    column_on_left: bool,
    kernel: SimplePredicateKernel,
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum ComparisonOp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum NumericLiteral {
    Int32(i32),
    Int64(i64),
    Float64(f64),
}

#[derive(Clone, Debug, PartialEq)]
enum SimplePredicateKernel {
    Generic,
    Numeric {
        op: ComparisonOp,
        literal: NumericLiteral,
    },
    Boolean {
        op: ComparisonOp,
        literal: bool,
    },
    String {
        op: ComparisonOp,
        literal: String,
    },
    DateTime {
        op: ComparisonOp,
        literal: i64,
    },
    Bytes {
        op: ComparisonOp,
        literal: Vec<u8>,
    },
    Jsonb {
        op: ComparisonOp,
        literal: Vec<u8>,
    },
}

#[derive(Clone, Debug, PartialEq)]
enum BetweenPredicateKernel {
    Generic {
        low: Value,
        high: Value,
    },
    Numeric {
        low: NumericLiteral,
        high: NumericLiteral,
    },
    Boolean {
        low: bool,
        high: bool,
    },
    String {
        low: String,
        high: String,
    },
    DateTime {
        low: i64,
        high: i64,
    },
    Bytes {
        low: Vec<u8>,
        high: Vec<u8>,
    },
    Jsonb {
        low: Vec<u8>,
        high: Vec<u8>,
    },
}

#[derive(Clone, Debug, PartialEq)]
enum InListPredicateKernel {
    Empty,
    NullOnly,
    Numeric {
        int32_literals: Vec<i32>,
        int64_literals: Vec<i64>,
        float64_literals: Vec<f64>,
        contains_null: bool,
    },
    Boolean {
        literals: Vec<bool>,
        contains_null: bool,
    },
    String {
        literals: Vec<String>,
        contains_null: bool,
    },
    DateTime {
        literals: Vec<i64>,
        contains_null: bool,
    },
    Bytes {
        literals: Vec<Vec<u8>>,
        contains_null: bool,
    },
    Jsonb {
        literals: Vec<Vec<u8>>,
        contains_null: bool,
    },
    Generic(Vec<Value>),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum InListKernelFamily {
    Numeric,
    Boolean,
    String,
    DateTime,
    Bytes,
    Jsonb,
}

#[derive(Clone)]
struct SingleTableFilter {
    predicate: CompiledRowPredicate,
}

#[derive(Clone, Debug)]
enum CompiledRowPredicate {
    Literal(PredicateValueState),
    Comparison(SimpleBinaryPredicate),
    And(Vec<CompiledRowPredicate>),
    Or(Vec<CompiledRowPredicate>),
    Not(Box<CompiledRowPredicate>),
    IsNull {
        column_index: usize,
    },
    IsNotNull {
        column_index: usize,
    },
    Between {
        column_index: usize,
        kernel: BetweenPredicateKernel,
        negated: bool,
    },
    InList {
        column_index: usize,
        kernel: InListPredicateKernel,
        negated: bool,
    },
    Like {
        column_index: usize,
        pattern: LikePatternKernel,
        negated: bool,
    },
    Match {
        column_index: usize,
        pattern: String,
        negated: bool,
    },
    Generic(Expr),
}

#[derive(Clone, Debug)]
enum LikePatternKernel {
    Exact(String),
    Prefix(String),
    Suffix(String),
    Contains(String),
    Generic(String),
}

#[derive(Clone, Copy, Debug)]
enum PredicateValueState {
    Null,
    Boolean(bool),
    Other,
}

#[derive(Clone)]
enum CachedSingleTableProjection {
    Identity,
    Columns(Vec<usize>),
    Exprs(Vec<Expr>),
}

#[derive(Clone)]
struct CachedSingleTablePipeline {
    table: String,
    filters: Vec<SingleTableFilter>,
    projection: CachedSingleTableProjection,
    limit: Option<(usize, usize)>,
}

#[derive(Clone)]
struct CompiledExecMeta {
    tables: Vec<String>,
    table_column_counts: Vec<usize>,
    shared_tables: SharedTables,
}

impl CompiledExecMeta {
    fn new(tables: Vec<String>, table_column_counts: Vec<usize>) -> Self {
        let shared_tables: SharedTables = alloc::sync::Arc::from(tables.as_slice());
        Self {
            tables,
            table_column_counts,
            shared_tables,
        }
    }

    fn empty() -> Self {
        Self::new(Vec::new(), Vec::new())
    }

    #[inline]
    fn resolve_column_index(&self, table_name: &str, table_relative_index: usize) -> usize {
        if table_name.is_empty() {
            return table_relative_index;
        }

        let mut offset = 0usize;
        for (index, table) in self.tables.iter().enumerate() {
            if table == table_name {
                return offset + table_relative_index;
            }
            offset += self.table_column_counts.get(index).copied().unwrap_or(0);
        }

        table_relative_index
    }

    fn into_relation(self, entries: Vec<RelationEntry>) -> Relation {
        Relation::from_entries(entries, self.tables, self.table_column_counts)
    }
}

#[derive(Clone)]
enum CompiledSourcePlan {
    TableScan {
        table: String,
    },
    IndexScan {
        table: String,
        index: String,
        bounds: IndexBounds,
        limit: Option<usize>,
        offset: usize,
        reverse: bool,
    },
    IndexGet {
        table: String,
        index: String,
        key: Value,
        limit: Option<usize>,
    },
    IndexInGet {
        table: String,
        index: String,
        keys: Vec<Value>,
    },
    GinIndexScan {
        table: String,
        index: String,
        key: String,
        value: Option<String>,
        query_type: String,
    },
    GinIndexScanMulti {
        table: String,
        index: String,
        pairs: Vec<(String, String)>,
    },
}

#[derive(Clone)]
enum CompiledProjection {
    Columns(Vec<usize>),
    Exprs(Vec<Expr>),
}

#[derive(Clone, Copy)]
struct CompiledEquiJoinKeys {
    left_key_idx: usize,
    right_key_idx: usize,
}

#[derive(Clone)]
enum CompiledExecPlanKind {
    Empty,
    Source(CompiledSourcePlan),
    Filter {
        input: Box<CompiledExecPlan>,
        predicate: CompiledRowPredicate,
    },
    Project {
        input: Box<CompiledExecPlan>,
        projection: CompiledProjection,
    },
    Limit {
        input: Box<CompiledExecPlan>,
        limit: usize,
        offset: usize,
    },
    UnionAll {
        left: Box<CompiledExecPlan>,
        right: Box<CompiledExecPlan>,
    },
    UnionDistinct {
        left: Box<CompiledExecPlan>,
        right: Box<CompiledExecPlan>,
    },
    HashJoin {
        left: Box<CompiledExecPlan>,
        right: Box<CompiledExecPlan>,
        keys: CompiledEquiJoinKeys,
        join_type: crate::ast::JoinType,
    },
    SortMergeJoin {
        left: Box<CompiledExecPlan>,
        right: Box<CompiledExecPlan>,
        keys: CompiledEquiJoinKeys,
        join_type: crate::ast::JoinType,
    },
    NestedLoopJoin {
        left: Box<CompiledExecPlan>,
        right: Box<CompiledExecPlan>,
        predicate: CompiledRowPredicate,
        join_type: crate::ast::JoinType,
    },
    IndexNestedLoopJoin {
        outer: Box<CompiledExecPlan>,
        inner_table: String,
        inner_index: String,
        outer_key_idx: usize,
        join_type: crate::ast::JoinType,
    },
    HashAggregate {
        input: Box<CompiledExecPlan>,
        group_by: Vec<Expr>,
        aggregates: Vec<(AggregateFunc, Expr)>,
    },
    Sort {
        input: Box<CompiledExecPlan>,
        order_by: Vec<(Expr, SortOrder)>,
    },
    TopN {
        input: Box<CompiledExecPlan>,
        order_by: Vec<(Expr, SortOrder)>,
        limit: usize,
        offset: usize,
    },
    CrossProduct {
        left: Box<CompiledExecPlan>,
        right: Box<CompiledExecPlan>,
    },
}

#[derive(Clone)]
struct CompiledExecPlan {
    meta: CompiledExecMeta,
    estimated_rows: Option<usize>,
    kind: CompiledExecPlanKind,
}

#[derive(Clone)]
enum PlanExecutionArtifactKind {
    None,
    SingleTablePipeline(CachedSingleTablePipeline),
    FilteredTableScan {
        table: String,
        predicate: CompiledRowPredicate,
    },
    CompiledExecPlan(CompiledExecPlan),
}

#[derive(Clone)]
pub struct PlanExecutionArtifact {
    kind: PlanExecutionArtifactKind,
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

    /// Visits rows from a table in storage order.
    /// Return `false` from the visitor to stop early.
    fn visit_table_rows<F>(&self, table: &str, mut visitor: F) -> ExecutionResult<()>
    where
        F: FnMut(&Rc<Row>) -> bool,
    {
        for row in self.get_table_rows(table)? {
            if !visitor(&row) {
                break;
            }
        }
        Ok(())
    }

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

    /// Returns rows from a composite index scan with tuple bounds.
    fn get_index_range_composite(
        &self,
        table: &str,
        index: &str,
        range: Option<&KeyRange<Vec<Value>>>,
    ) -> ExecutionResult<Vec<Rc<Row>>> {
        self.get_index_range_composite_with_limit(table, index, range, None, 0, false)
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

    /// Returns rows from a composite index scan with tuple bounds, limit, offset, and order.
    fn get_index_range_composite_with_limit(
        &self,
        table: &str,
        index: &str,
        range: Option<&KeyRange<Vec<Value>>>,
        limit: Option<usize>,
        offset: usize,
        reverse: bool,
    ) -> ExecutionResult<Vec<Rc<Row>>>;

    /// Visits rows from an index scan with a key range, limit, offset, and order.
    /// Return `false` from the visitor to stop early.
    fn visit_index_range_with_limit<F>(
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
        mut visitor: F,
    ) -> ExecutionResult<()>
    where
        F: FnMut(&Rc<Row>) -> bool,
    {
        for row in self.get_index_range_with_limit(
            table,
            index,
            range_start,
            range_end,
            include_start,
            include_end,
            limit,
            offset,
            reverse,
        )? {
            if !visitor(&row) {
                break;
            }
        }
        Ok(())
    }

    /// Visits rows from a composite index scan with tuple bounds, limit, offset, and order.
    /// Return `false` from the visitor to stop early.
    fn visit_index_range_composite_with_limit<F>(
        &self,
        table: &str,
        index: &str,
        range: Option<&KeyRange<Vec<Value>>>,
        limit: Option<usize>,
        offset: usize,
        reverse: bool,
        mut visitor: F,
    ) -> ExecutionResult<()>
    where
        F: FnMut(&Rc<Row>) -> bool,
    {
        for row in
            self.get_index_range_composite_with_limit(table, index, range, limit, offset, reverse)?
        {
            if !visitor(&row) {
                break;
            }
        }
        Ok(())
    }

    /// Returns rows from an index point lookup.
    fn get_index_point(
        &self,
        table: &str,
        index: &str,
        key: &Value,
    ) -> ExecutionResult<Vec<Rc<Row>>>;

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

    /// Visits rows from an index point lookup with optional limit.
    /// Return `false` from the visitor to stop early.
    fn visit_index_point_with_limit<F>(
        &self,
        table: &str,
        index: &str,
        key: &Value,
        limit: Option<usize>,
        mut visitor: F,
    ) -> ExecutionResult<()>
    where
        F: FnMut(&Rc<Row>) -> bool,
    {
        for row in self.get_index_point_with_limit(table, index, key, limit)? {
            if !visitor(&row) {
                break;
            }
        }
        Ok(())
    }

    /// Returns the column count for a table.
    fn get_column_count(&self, table: &str) -> ExecutionResult<usize>;

    /// Returns the number of rows in a table.
    fn get_table_row_count(&self, table: &str) -> ExecutionResult<usize> {
        Ok(self.get_table_rows(table)?.len())
    }

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

    /// Visits rows from a GIN index lookup by key-value pair.
    /// Return `false` from the visitor to stop early.
    fn visit_gin_index_rows<F>(
        &self,
        table: &str,
        index: &str,
        key: &str,
        value: &str,
        mut visitor: F,
    ) -> ExecutionResult<()>
    where
        F: FnMut(&Rc<Row>) -> bool,
    {
        for row in self.get_gin_index_rows(table, index, key, value)? {
            if !visitor(&row) {
                break;
            }
        }
        Ok(())
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

    /// Visits rows from a GIN index lookup by key existence.
    /// Return `false` from the visitor to stop early.
    fn visit_gin_index_rows_by_key<F>(
        &self,
        table: &str,
        index: &str,
        key: &str,
        mut visitor: F,
    ) -> ExecutionResult<()>
    where
        F: FnMut(&Rc<Row>) -> bool,
    {
        for row in self.get_gin_index_rows_by_key(table, index, key)? {
            if !visitor(&row) {
                break;
            }
        }
        Ok(())
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

    /// Visits rows from a multi-predicate GIN index lookup.
    /// Return `false` from the visitor to stop early.
    fn visit_gin_index_rows_multi<F>(
        &self,
        table: &str,
        index: &str,
        pairs: &[(&str, &str)],
        mut visitor: F,
    ) -> ExecutionResult<()>
    where
        F: FnMut(&Rc<Row>) -> bool,
    {
        for row in self.get_gin_index_rows_multi(table, index, pairs)? {
            if !visitor(&row) {
                break;
            }
        }
        Ok(())
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

    /// Compiles an execution artifact for repeated execution of a physical plan.
    /// The artifact caches lowered single-table predicates/pipelines without
    /// changing logical or physical plan semantics.
    pub fn compile_execution_artifact(plan: &PhysicalPlan) -> PlanExecutionArtifact {
        let kind = if let Some(pipeline) = Self::compile_single_table_pipeline_artifact(plan) {
            PlanExecutionArtifactKind::SingleTablePipeline(pipeline)
        } else if let PhysicalPlan::Filter { input, predicate } = plan {
            if let PhysicalPlan::TableScan { table } = input.as_ref() {
                PlanExecutionArtifactKind::FilteredTableScan {
                    table: table.clone(),
                    predicate: Self::compile_row_predicate(predicate),
                }
            } else {
                PlanExecutionArtifactKind::None
            }
        } else {
            PlanExecutionArtifactKind::None
        };

        PlanExecutionArtifact { kind }
    }

    /// Compiles an execution artifact using runtime table metadata from the data source.
    /// This lowers the full physical plan into a reusable fused executable whenever
    /// metadata is available, while falling back to the lightweight legacy artifact.
    pub fn compile_execution_artifact_with_data_source(
        &self,
        plan: &PhysicalPlan,
    ) -> PlanExecutionArtifact {
        if let Some(pipeline) = Self::compile_single_table_pipeline_artifact(plan) {
            return PlanExecutionArtifact {
                kind: PlanExecutionArtifactKind::SingleTablePipeline(pipeline),
            };
        }

        if let PhysicalPlan::Filter { input, predicate } = Self::strip_noop(plan) {
            if let PhysicalPlan::TableScan { table } = Self::strip_noop(input) {
                return PlanExecutionArtifact {
                    kind: PlanExecutionArtifactKind::FilteredTableScan {
                        table: table.clone(),
                        predicate: Self::compile_row_predicate(predicate),
                    },
                };
            }
        }

        match self.compile_exec_plan(plan) {
            Ok(plan) => PlanExecutionArtifact {
                kind: PlanExecutionArtifactKind::CompiledExecPlan(plan),
            },
            Err(_) => Self::compile_execution_artifact(plan),
        }
    }

    /// Executes a physical plan using a precompiled execution artifact.
    pub fn execute_with_artifact(
        &self,
        plan: &PhysicalPlan,
        artifact: &PlanExecutionArtifact,
    ) -> ExecutionResult<Relation> {
        match &artifact.kind {
            PlanExecutionArtifactKind::SingleTablePipeline(pipeline) => {
                self.execute_cached_single_table_pipeline(pipeline)
            }
            PlanExecutionArtifactKind::FilteredTableScan { table, predicate } => {
                self.execute_compiled_filtered_table_scan(table, predicate)
            }
            PlanExecutionArtifactKind::CompiledExecPlan(exec_plan) => {
                self.execute_compiled_exec_plan(exec_plan)
            }
            PlanExecutionArtifactKind::None => self.execute(plan),
        }
    }

    /// Executes a physical plan using a precompiled execution artifact and emits
    /// output rows directly, avoiding top-level `RelationEntry` materialization.
    pub fn execute_with_artifact_rows<F>(
        &self,
        plan: &PhysicalPlan,
        artifact: &PlanExecutionArtifact,
        mut emit: F,
    ) -> ExecutionResult<()>
    where
        F: FnMut(Rc<Row>) -> ExecutionResult<bool>,
    {
        match &artifact.kind {
            PlanExecutionArtifactKind::SingleTablePipeline(pipeline) => {
                let _ = self.execute_cached_single_table_pipeline_rows(pipeline, &mut emit)?;
                Ok(())
            }
            PlanExecutionArtifactKind::FilteredTableScan { table, predicate } => {
                let _ =
                    self.execute_compiled_filtered_table_scan_rows(table, predicate, &mut emit)?;
                Ok(())
            }
            PlanExecutionArtifactKind::CompiledExecPlan(exec_plan) => {
                let mut row_emit = |entry: RelationEntry| emit(entry.row);
                let _ = self.execute_compiled_into_dyn(exec_plan, &mut row_emit)?;
                Ok(())
            }
            PlanExecutionArtifactKind::None => {
                let relation = self.execute(plan)?;
                for entry in relation.entries {
                    if !emit(entry.row)? {
                        break;
                    }
                }
                Ok(())
            }
        }
    }

    /// Executes a physical plan using a precompiled execution artifact and
    /// collects the output rows directly.
    pub fn execute_with_artifact_row_vec(
        &self,
        plan: &PhysicalPlan,
        artifact: &PlanExecutionArtifact,
    ) -> ExecutionResult<Vec<Rc<Row>>> {
        match &artifact.kind {
            PlanExecutionArtifactKind::SingleTablePipeline(pipeline) => {
                self.collect_cached_single_table_pipeline_rows(pipeline)
            }
            PlanExecutionArtifactKind::FilteredTableScan { table, predicate } => {
                self.collect_compiled_filtered_table_scan_rows(table, predicate)
            }
            PlanExecutionArtifactKind::CompiledExecPlan(exec_plan) => {
                let mut rows = match exec_plan.estimated_rows {
                    Some(rows) => Vec::with_capacity(rows),
                    None => Vec::new(),
                };
                let mut emit = |entry: RelationEntry| {
                    rows.push(entry.row);
                    Ok(true)
                };
                self.execute_compiled_into_dyn(exec_plan, &mut emit)?;
                Ok(rows)
            }
            PlanExecutionArtifactKind::None => Ok(self
                .execute(plan)?
                .entries
                .into_iter()
                .map(|entry| entry.row)
                .collect()),
        }
    }

    fn compile_exec_plan(&self, plan: &PhysicalPlan) -> ExecutionResult<CompiledExecPlan> {
        let plan = Self::strip_noop(plan);
        match plan {
            PhysicalPlan::Empty => Ok(CompiledExecPlan {
                meta: CompiledExecMeta::empty(),
                estimated_rows: Some(0),
                kind: CompiledExecPlanKind::Empty,
            }),
            PhysicalPlan::TableScan { table } => Ok(CompiledExecPlan {
                meta: self.compile_single_table_meta(table)?,
                estimated_rows: Some(self.data_source.get_table_row_count(table)?),
                kind: CompiledExecPlanKind::Source(CompiledSourcePlan::TableScan {
                    table: table.clone(),
                }),
            }),
            PhysicalPlan::IndexScan {
                table,
                index,
                bounds,
                limit,
                offset,
                reverse,
            } => {
                Ok(CompiledExecPlan {
                    meta: self.compile_single_table_meta(table)?,
                    estimated_rows: Some(limit.unwrap_or_else(|| {
                        self.data_source.get_table_row_count(table).unwrap_or(0)
                    })),
                    kind: CompiledExecPlanKind::Source(CompiledSourcePlan::IndexScan {
                        table: table.clone(),
                        index: index.clone(),
                        bounds: bounds.clone(),
                        limit: *limit,
                        offset: offset.unwrap_or(0),
                        reverse: *reverse,
                    }),
                })
            }
            PhysicalPlan::IndexGet {
                table,
                index,
                key,
                limit,
            } => Ok(CompiledExecPlan {
                meta: self.compile_single_table_meta(table)?,
                estimated_rows: *limit,
                kind: CompiledExecPlanKind::Source(CompiledSourcePlan::IndexGet {
                    table: table.clone(),
                    index: index.clone(),
                    key: key.clone(),
                    limit: *limit,
                }),
            }),
            PhysicalPlan::IndexInGet { table, index, keys } => Ok(CompiledExecPlan {
                meta: self.compile_single_table_meta(table)?,
                estimated_rows: None,
                kind: CompiledExecPlanKind::Source(CompiledSourcePlan::IndexInGet {
                    table: table.clone(),
                    index: index.clone(),
                    keys: keys.clone(),
                }),
            }),
            PhysicalPlan::GinIndexScan {
                table,
                index,
                key,
                value,
                query_type,
            } => Ok(CompiledExecPlan {
                meta: self.compile_single_table_meta(table)?,
                estimated_rows: None,
                kind: CompiledExecPlanKind::Source(CompiledSourcePlan::GinIndexScan {
                    table: table.clone(),
                    index: index.clone(),
                    key: key.clone(),
                    value: value.clone(),
                    query_type: query_type.clone(),
                }),
            }),
            PhysicalPlan::GinIndexScanMulti {
                table,
                index,
                pairs,
            } => Ok(CompiledExecPlan {
                meta: self.compile_single_table_meta(table)?,
                estimated_rows: None,
                kind: CompiledExecPlanKind::Source(CompiledSourcePlan::GinIndexScanMulti {
                    table: table.clone(),
                    index: index.clone(),
                    pairs: pairs.clone(),
                }),
            }),
            PhysicalPlan::Filter { input, predicate } => {
                let input = self.compile_exec_plan(input)?;
                let bound_predicate = Self::bind_expr_to_meta(predicate, &input.meta);
                Ok(CompiledExecPlan {
                    meta: input.meta.clone(),
                    estimated_rows: input.estimated_rows,
                    kind: CompiledExecPlanKind::Filter {
                        input: Box::new(input),
                        predicate: Self::compile_row_predicate(&bound_predicate),
                    },
                })
            }
            PhysicalPlan::Project { input, columns } => {
                let input = self.compile_exec_plan(input)?;
                let meta = Self::compiled_project_meta(&input.meta, columns.len());

                let bound_columns = Self::bind_exprs_to_meta(columns, &input.meta);
                let projection = if bound_columns
                    .iter()
                    .all(|expr| matches!(expr, Expr::Column(_)))
                {
                    CompiledProjection::Columns(
                        bound_columns
                            .iter()
                            .filter_map(|expr| match expr {
                                Expr::Column(column) => Some(column.index),
                                _ => None,
                            })
                            .collect(),
                    )
                } else {
                    CompiledProjection::Exprs(bound_columns)
                };

                Ok(CompiledExecPlan {
                    meta,
                    estimated_rows: input.estimated_rows,
                    kind: CompiledExecPlanKind::Project {
                        input: Box::new(input),
                        projection,
                    },
                })
            }
            PhysicalPlan::Limit {
                input,
                limit,
                offset,
            } => {
                let input = self.compile_exec_plan(input)?;
                let estimated_rows = Some(match input.estimated_rows {
                    Some(rows) => rows.saturating_sub(*offset).min(*limit),
                    None => *limit,
                });
                Ok(CompiledExecPlan {
                    meta: input.meta.clone(),
                    estimated_rows,
                    kind: CompiledExecPlanKind::Limit {
                        input: Box::new(input),
                        limit: *limit,
                        offset: *offset,
                    },
                })
            }
            PhysicalPlan::Union { left, right, all } => {
                let left = self.compile_exec_plan(left)?;
                let right = self.compile_exec_plan(right)?;
                let estimated_rows = match (left.estimated_rows, right.estimated_rows) {
                    (Some(left), Some(right)) => Some(left.saturating_add(right)),
                    _ => None,
                };
                Ok(CompiledExecPlan {
                    meta: left.meta.clone(),
                    estimated_rows,
                    kind: if *all {
                        CompiledExecPlanKind::UnionAll {
                            left: Box::new(left),
                            right: Box::new(right),
                        }
                    } else {
                        CompiledExecPlanKind::UnionDistinct {
                            left: Box::new(left),
                            right: Box::new(right),
                        }
                    },
                })
            }
            PhysicalPlan::HashJoin {
                left,
                right,
                condition,
                join_type,
            } => {
                let left = self.compile_exec_plan(left)?;
                let right = self.compile_exec_plan(right)?;
                let keys = Self::extract_join_keys_from_meta(condition, &left.meta, &right.meta)?;
                Ok(CompiledExecPlan {
                    meta: Self::compiled_join_meta(&left.meta, &right.meta),
                    estimated_rows: Self::estimate_join_output_rows(
                        left.estimated_rows,
                        right.estimated_rows,
                        *join_type,
                    ),
                    kind: CompiledExecPlanKind::HashJoin {
                        left: Box::new(left),
                        right: Box::new(right),
                        keys,
                        join_type: *join_type,
                    },
                })
            }
            PhysicalPlan::SortMergeJoin {
                left,
                right,
                condition,
                join_type,
            } => {
                let left = self.compile_exec_plan(left)?;
                let right = self.compile_exec_plan(right)?;
                let keys = Self::extract_join_keys_from_meta(condition, &left.meta, &right.meta)?;
                Ok(CompiledExecPlan {
                    meta: Self::compiled_join_meta(&left.meta, &right.meta),
                    estimated_rows: Self::estimate_join_output_rows(
                        left.estimated_rows,
                        right.estimated_rows,
                        *join_type,
                    ),
                    kind: CompiledExecPlanKind::SortMergeJoin {
                        left: Box::new(left),
                        right: Box::new(right),
                        keys,
                        join_type: *join_type,
                    },
                })
            }
            PhysicalPlan::NestedLoopJoin {
                left,
                right,
                condition,
                join_type,
            } => {
                let left = self.compile_exec_plan(left)?;
                let right = self.compile_exec_plan(right)?;
                let join_meta = Self::compiled_join_meta(&left.meta, &right.meta);
                let bound_condition = Self::bind_expr_to_meta(condition, &join_meta);
                Ok(CompiledExecPlan {
                    meta: join_meta,
                    estimated_rows: Self::estimate_join_output_rows(
                        left.estimated_rows,
                        right.estimated_rows,
                        *join_type,
                    ),
                    kind: CompiledExecPlanKind::NestedLoopJoin {
                        left: Box::new(left),
                        right: Box::new(right),
                        predicate: Self::compile_row_predicate(&bound_condition),
                        join_type: *join_type,
                    },
                })
            }
            PhysicalPlan::IndexNestedLoopJoin {
                outer,
                inner_table,
                inner_index,
                condition,
                join_type,
            } => {
                let outer = self.compile_exec_plan(outer)?;
                let inner_meta = self.compile_single_table_meta(inner_table)?;
                let outer_key_idx =
                    Self::extract_outer_key_index_from_meta(condition, &outer.meta)?;
                Ok(CompiledExecPlan {
                    meta: Self::compiled_join_meta(&outer.meta, &inner_meta),
                    estimated_rows: Self::estimate_index_join_output_rows(
                        outer.estimated_rows,
                        *join_type,
                    ),
                    kind: CompiledExecPlanKind::IndexNestedLoopJoin {
                        outer: Box::new(outer),
                        inner_table: inner_table.clone(),
                        inner_index: inner_index.clone(),
                        outer_key_idx,
                        join_type: *join_type,
                    },
                })
            }
            PhysicalPlan::HashAggregate {
                input,
                group_by,
                aggregates,
            } => {
                let input = self.compile_exec_plan(input)?;
                Ok(CompiledExecPlan {
                    meta: Self::compiled_project_meta(
                        &input.meta,
                        group_by.len().saturating_add(aggregates.len()),
                    ),
                    estimated_rows: None,
                    kind: CompiledExecPlanKind::HashAggregate {
                        input: Box::new(input),
                        group_by: group_by.clone(),
                        aggregates: aggregates.clone(),
                    },
                })
            }
            PhysicalPlan::Sort { input, order_by } => {
                let input = self.compile_exec_plan(input)?;
                Ok(CompiledExecPlan {
                    meta: input.meta.clone(),
                    estimated_rows: input.estimated_rows,
                    kind: CompiledExecPlanKind::Sort {
                        input: Box::new(input),
                        order_by: order_by.clone(),
                    },
                })
            }
            PhysicalPlan::TopN {
                input,
                order_by,
                limit,
                offset,
            } => {
                let input = self.compile_exec_plan(input)?;
                Ok(CompiledExecPlan {
                    meta: input.meta.clone(),
                    estimated_rows: Some(*limit),
                    kind: CompiledExecPlanKind::TopN {
                        input: Box::new(input),
                        order_by: order_by.clone(),
                        limit: *limit,
                        offset: *offset,
                    },
                })
            }
            PhysicalPlan::CrossProduct { left, right } => {
                let left = self.compile_exec_plan(left)?;
                let right = self.compile_exec_plan(right)?;
                Ok(CompiledExecPlan {
                    meta: Self::compiled_join_meta(&left.meta, &right.meta),
                    estimated_rows: match (left.estimated_rows, right.estimated_rows) {
                        (Some(left_rows), Some(right_rows)) => {
                            Some(left_rows.saturating_mul(right_rows))
                        }
                        _ => None,
                    },
                    kind: CompiledExecPlanKind::CrossProduct {
                        left: Box::new(left),
                        right: Box::new(right),
                    },
                })
            }
            PhysicalPlan::NoOp { input } => self.compile_exec_plan(input),
        }
    }

    fn compile_single_table_meta(&self, table: &str) -> ExecutionResult<CompiledExecMeta> {
        Ok(CompiledExecMeta::new(
            alloc::vec![table.into()],
            alloc::vec![self.data_source.get_column_count(table)?],
        ))
    }

    fn compiled_project_meta(
        input: &CompiledExecMeta,
        output_column_count: usize,
    ) -> CompiledExecMeta {
        CompiledExecMeta::new(input.tables.clone(), alloc::vec![output_column_count])
    }

    fn compiled_join_meta(left: &CompiledExecMeta, right: &CompiledExecMeta) -> CompiledExecMeta {
        let mut tables = left.tables.clone();
        tables.extend(right.tables.iter().cloned());

        let mut table_column_counts = left.table_column_counts.clone();
        table_column_counts.extend(right.table_column_counts.iter().copied());

        CompiledExecMeta::new(tables, table_column_counts)
    }

    fn estimate_join_output_rows(
        left_rows: Option<usize>,
        right_rows: Option<usize>,
        join_type: crate::ast::JoinType,
    ) -> Option<usize> {
        match join_type {
            crate::ast::JoinType::LeftOuter => left_rows,
            crate::ast::JoinType::RightOuter => right_rows,
            crate::ast::JoinType::FullOuter => match (left_rows, right_rows) {
                (Some(left_rows), Some(right_rows)) => Some(left_rows.saturating_add(right_rows)),
                _ => None,
            },
            crate::ast::JoinType::Cross => match (left_rows, right_rows) {
                (Some(left_rows), Some(right_rows)) => Some(left_rows.saturating_mul(right_rows)),
                _ => None,
            },
            crate::ast::JoinType::Inner => match (left_rows, right_rows) {
                (Some(left_rows), Some(right_rows)) => Some(left_rows.max(right_rows)),
                (Some(left_rows), None) => Some(left_rows),
                (None, Some(right_rows)) => Some(right_rows),
                (None, None) => None,
            },
        }
    }

    fn estimate_index_join_output_rows(
        outer_rows: Option<usize>,
        join_type: crate::ast::JoinType,
    ) -> Option<usize> {
        match join_type {
            crate::ast::JoinType::LeftOuter | crate::ast::JoinType::Inner => outer_rows,
            crate::ast::JoinType::RightOuter
            | crate::ast::JoinType::FullOuter
            | crate::ast::JoinType::Cross => None,
        }
    }

    fn extract_join_keys_from_meta(
        condition: &Expr,
        left: &CompiledExecMeta,
        right: &CompiledExecMeta,
    ) -> ExecutionResult<CompiledEquiJoinKeys> {
        if let Expr::BinaryOp {
            left: left_expr,
            op: BinaryOp::Eq,
            right: right_expr,
        } = condition
        {
            let left_col = Self::extract_column_ref_static(left_expr)?;
            let right_col = Self::extract_column_ref_static(right_expr)?;

            if left.tables.contains(&left_col.table) && right.tables.contains(&right_col.table) {
                return Ok(CompiledEquiJoinKeys {
                    left_key_idx: left.resolve_column_index(&left_col.table, left_col.index),
                    right_key_idx: right.resolve_column_index(&right_col.table, right_col.index),
                });
            }

            if left.tables.contains(&right_col.table) && right.tables.contains(&left_col.table) {
                return Ok(CompiledEquiJoinKeys {
                    left_key_idx: left.resolve_column_index(&right_col.table, right_col.index),
                    right_key_idx: right.resolve_column_index(&left_col.table, left_col.index),
                });
            }
        }

        Err(ExecutionError::InvalidOperation(
            "Expected equi-join condition".into(),
        ))
    }

    fn extract_outer_key_index_from_meta(
        condition: &Expr,
        outer: &CompiledExecMeta,
    ) -> ExecutionResult<usize> {
        if let Expr::BinaryOp {
            left: left_expr,
            op: BinaryOp::Eq,
            right: right_expr,
        } = condition
        {
            let left_col = Self::extract_column_ref_static(left_expr)?;
            let right_col = Self::extract_column_ref_static(right_expr)?;

            if outer.tables.contains(&left_col.table) {
                return Ok(outer.resolve_column_index(&left_col.table, left_col.index));
            }
            if outer.tables.contains(&right_col.table) {
                return Ok(outer.resolve_column_index(&right_col.table, right_col.index));
            }
        }

        Err(ExecutionError::InvalidOperation(
            "Expected equi-join condition".into(),
        ))
    }

    fn bind_exprs_to_meta(exprs: &[Expr], meta: &CompiledExecMeta) -> Vec<Expr> {
        exprs
            .iter()
            .map(|expr| Self::bind_expr_to_meta(expr, meta))
            .collect()
    }

    fn bind_expr_to_meta(expr: &Expr, meta: &CompiledExecMeta) -> Expr {
        match expr {
            Expr::Column(column) => Expr::Column(ColumnRef {
                table: column.table.clone(),
                column: column.column.clone(),
                index: meta.resolve_column_index(&column.table, column.index),
            }),
            Expr::Literal(value) => Expr::Literal(value.clone()),
            Expr::BinaryOp { left, op, right } => Expr::BinaryOp {
                left: Box::new(Self::bind_expr_to_meta(left, meta)),
                op: *op,
                right: Box::new(Self::bind_expr_to_meta(right, meta)),
            },
            Expr::UnaryOp { op, expr } => Expr::UnaryOp {
                op: *op,
                expr: Box::new(Self::bind_expr_to_meta(expr, meta)),
            },
            Expr::Function { name, args } => Expr::Function {
                name: name.clone(),
                args: args
                    .iter()
                    .map(|arg| Self::bind_expr_to_meta(arg, meta))
                    .collect(),
            },
            Expr::Aggregate {
                func,
                expr,
                distinct,
            } => Expr::Aggregate {
                func: *func,
                expr: expr
                    .as_ref()
                    .map(|expr| Box::new(Self::bind_expr_to_meta(expr, meta))),
                distinct: *distinct,
            },
            Expr::Between { expr, low, high } => Expr::Between {
                expr: Box::new(Self::bind_expr_to_meta(expr, meta)),
                low: Box::new(Self::bind_expr_to_meta(low, meta)),
                high: Box::new(Self::bind_expr_to_meta(high, meta)),
            },
            Expr::NotBetween { expr, low, high } => Expr::NotBetween {
                expr: Box::new(Self::bind_expr_to_meta(expr, meta)),
                low: Box::new(Self::bind_expr_to_meta(low, meta)),
                high: Box::new(Self::bind_expr_to_meta(high, meta)),
            },
            Expr::In { expr, list } => Expr::In {
                expr: Box::new(Self::bind_expr_to_meta(expr, meta)),
                list: list
                    .iter()
                    .map(|expr| Self::bind_expr_to_meta(expr, meta))
                    .collect(),
            },
            Expr::NotIn { expr, list } => Expr::NotIn {
                expr: Box::new(Self::bind_expr_to_meta(expr, meta)),
                list: list
                    .iter()
                    .map(|expr| Self::bind_expr_to_meta(expr, meta))
                    .collect(),
            },
            Expr::Like { expr, pattern } => Expr::Like {
                expr: Box::new(Self::bind_expr_to_meta(expr, meta)),
                pattern: pattern.clone(),
            },
            Expr::NotLike { expr, pattern } => Expr::NotLike {
                expr: Box::new(Self::bind_expr_to_meta(expr, meta)),
                pattern: pattern.clone(),
            },
            Expr::Match { expr, pattern } => Expr::Match {
                expr: Box::new(Self::bind_expr_to_meta(expr, meta)),
                pattern: pattern.clone(),
            },
            Expr::NotMatch { expr, pattern } => Expr::NotMatch {
                expr: Box::new(Self::bind_expr_to_meta(expr, meta)),
                pattern: pattern.clone(),
            },
        }
    }

    /// Executes a physical plan and returns the result relation.
    pub fn execute(&self, plan: &PhysicalPlan) -> ExecutionResult<Relation> {
        let artifact = Self::compile_execution_artifact(plan);
        if !matches!(artifact.kind, PlanExecutionArtifactKind::None) {
            return self.execute_with_artifact(plan, &artifact);
        }

        match plan {
            PhysicalPlan::TableScan { table } => self.execute_table_scan(table),

            PhysicalPlan::IndexScan {
                table,
                index,
                bounds,
                limit,
                offset,
                reverse,
            } => self.execute_index_scan(table, index, bounds, *limit, *offset, *reverse),

            PhysicalPlan::IndexGet {
                table,
                index,
                key,
                limit,
            } => self.execute_index_get(table, index, key, *limit),

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

            PhysicalPlan::Union { left, right, all } => {
                let left_rel = self.execute(left)?;
                let right_rel = self.execute(right)?;
                self.execute_union(left_rel, right_rel, *all)
            }

            PhysicalPlan::NoOp { input } => self.execute(input),

            PhysicalPlan::TopN {
                input,
                order_by,
                limit,
                offset,
            } => {
                let input_rel = self.execute(input)?;
                self.execute_topn(input_rel, order_by, *limit, *offset)
            }

            PhysicalPlan::Empty => Ok(Relation::empty()),
        }
    }

    fn execute_compiled_exec_plan(&self, plan: &CompiledExecPlan) -> ExecutionResult<Relation> {
        let mut entries = match plan.estimated_rows {
            Some(rows) => Vec::with_capacity(rows),
            None => Vec::new(),
        };

        self.execute_compiled_into(plan, &mut |entry| {
            entries.push(entry);
            Ok(true)
        })?;

        Ok(plan.meta.clone().into_relation(entries))
    }

    fn execute_compiled_into<F>(
        &self,
        plan: &CompiledExecPlan,
        emit: &mut F,
    ) -> ExecutionResult<bool>
    where
        F: FnMut(RelationEntry) -> ExecutionResult<bool>,
    {
        let emit: &mut dyn FnMut(RelationEntry) -> ExecutionResult<bool> = emit;
        self.execute_compiled_into_dyn(plan, emit)
    }

    fn execute_compiled_into_dyn(
        &self,
        plan: &CompiledExecPlan,
        emit: &mut dyn FnMut(RelationEntry) -> ExecutionResult<bool>,
    ) -> ExecutionResult<bool> {
        match &plan.kind {
            CompiledExecPlanKind::Empty => Ok(true),
            CompiledExecPlanKind::Source(source) => {
                self.execute_compiled_source_into(source, &plan.meta, emit)
            }
            CompiledExecPlanKind::Filter { input, predicate } => {
                self.execute_compiled_into_dyn(input, &mut |entry| {
                    if matches!(
                        self.eval_compiled_row_predicate(entry.row.as_ref(), predicate),
                        PredicateValueState::Boolean(true)
                    ) {
                        emit(entry)
                    } else {
                        Ok(true)
                    }
                })
            }
            CompiledExecPlanKind::Project { input, projection } => {
                self.execute_compiled_into_dyn(input, &mut |entry| {
                    let values = match projection {
                        CompiledProjection::Columns(indices) => indices
                            .iter()
                            .map(|&index| entry.row.get(index).cloned().unwrap_or(Value::Null))
                            .collect(),
                        CompiledProjection::Exprs(exprs) => exprs
                            .iter()
                            .map(|expr| self.eval_row_expr(expr, entry.row.as_ref()))
                            .collect(),
                    };

                    let projected = Rc::new(Row::new_with_version(
                        entry.id(),
                        entry.row.version(),
                        values,
                    ));
                    emit(Self::entry_from_meta(projected, &plan.meta))
                })
            }
            CompiledExecPlanKind::Limit {
                input,
                limit,
                offset,
            } => {
                let mut skipped = 0usize;
                let mut taken = 0usize;
                self.execute_compiled_into_dyn(input, &mut |entry| {
                    if skipped < *offset {
                        skipped += 1;
                        return Ok(true);
                    }
                    if taken >= *limit {
                        return Ok(false);
                    }
                    taken += 1;
                    if !emit(entry)? {
                        return Ok(false);
                    }
                    Ok(taken < *limit)
                })
            }
            CompiledExecPlanKind::UnionAll { left, right } => {
                if !self.execute_compiled_into_dyn(left, emit)? {
                    return Ok(false);
                }
                self.execute_compiled_into_dyn(right, emit)
            }
            CompiledExecPlanKind::UnionDistinct { left, right } => {
                let mut seen = alloc::collections::BTreeSet::new();
                if !self.execute_compiled_into_dyn(left, &mut |entry| {
                    let key = entry.row.values().to_vec();
                    if seen.insert(key) {
                        emit(entry)
                    } else {
                        Ok(true)
                    }
                })? {
                    return Ok(false);
                }

                self.execute_compiled_into_dyn(right, &mut |entry| {
                    let key = entry.row.values().to_vec();
                    if seen.insert(key) {
                        emit(entry)
                    } else {
                        Ok(true)
                    }
                })
            }
            CompiledExecPlanKind::HashJoin {
                left,
                right,
                keys,
                join_type,
            } => {
                let left = self.execute_compiled_exec_plan(left)?;
                let right = self.execute_compiled_exec_plan(right)?;
                self.emit_hash_join_entries(
                    &left,
                    &right,
                    keys.left_key_idx,
                    keys.right_key_idx,
                    *join_type,
                    emit,
                )
            }
            CompiledExecPlanKind::SortMergeJoin {
                left,
                right,
                keys,
                join_type,
            } => self.emit_sort_merge_join_entries(
                self.execute_compiled_exec_plan(left)?,
                self.execute_compiled_exec_plan(right)?,
                keys.left_key_idx,
                keys.right_key_idx,
                *join_type,
                emit,
            ),
            CompiledExecPlanKind::NestedLoopJoin {
                left,
                right,
                predicate,
                join_type,
            } => {
                let left = self.execute_compiled_exec_plan(left)?;
                let right = self.execute_compiled_exec_plan(right)?;
                self.emit_nested_loop_join_entries_compiled(
                    &left, &right, predicate, *join_type, emit,
                )
            }
            CompiledExecPlanKind::IndexNestedLoopJoin {
                outer,
                inner_table,
                inner_index,
                outer_key_idx,
                join_type,
            } => {
                let outer = self.execute_compiled_exec_plan(outer)?;
                self.emit_index_nested_loop_join_entries_compiled(
                    &outer,
                    inner_table,
                    inner_index,
                    *outer_key_idx,
                    *join_type,
                    emit,
                )
            }
            CompiledExecPlanKind::HashAggregate {
                input,
                group_by,
                aggregates,
            } => self.emit_relation(
                self.execute_hash_aggregate(
                    self.execute_compiled_exec_plan(input)?,
                    group_by,
                    aggregates,
                )?,
                emit,
            ),
            CompiledExecPlanKind::Sort { input, order_by } => self.emit_relation(
                self.execute_sort(self.execute_compiled_exec_plan(input)?, order_by)?,
                emit,
            ),
            CompiledExecPlanKind::TopN {
                input,
                order_by,
                limit,
                offset,
            } => self.emit_relation(
                self.execute_topn(
                    self.execute_compiled_exec_plan(input)?,
                    order_by,
                    *limit,
                    *offset,
                )?,
                emit,
            ),
            CompiledExecPlanKind::CrossProduct { left, right } => {
                let left = self.execute_compiled_exec_plan(left)?;
                let right = self.execute_compiled_exec_plan(right)?;
                self.emit_cross_product_entries(&left, &right, emit)
            }
        }
    }

    fn execute_compiled_source_into(
        &self,
        source: &CompiledSourcePlan,
        meta: &CompiledExecMeta,
        emit: &mut dyn FnMut(RelationEntry) -> ExecutionResult<bool>,
    ) -> ExecutionResult<bool> {
        match source {
            CompiledSourcePlan::TableScan { table } => {
                self.visit_compiled_source_rows(meta, emit, |visit| {
                    self.data_source.visit_table_rows(table, visit)
                })
            }
            CompiledSourcePlan::IndexScan {
                table,
                index,
                bounds,
                limit,
                offset,
                reverse,
            } => match bounds {
                IndexBounds::Unbounded => self.visit_compiled_source_rows(meta, emit, |visit| {
                    self.data_source.visit_index_range_with_limit(
                        table, index, None, None, true, true, *limit, *offset, *reverse, visit,
                    )
                }),
                IndexBounds::Scalar(range) => match range {
                    KeyRange::All => self.visit_compiled_source_rows(meta, emit, |visit| {
                        self.data_source.visit_index_range_with_limit(
                            table, index, None, None, true, true, *limit, *offset, *reverse, visit,
                        )
                    }),
                    KeyRange::Only(value) => self.visit_compiled_source_rows(meta, emit, |visit| {
                        self.data_source.visit_index_range_with_limit(
                            table,
                            index,
                            Some(value),
                            Some(value),
                            true,
                            true,
                            *limit,
                            *offset,
                            *reverse,
                            visit,
                        )
                    }),
                    KeyRange::LowerBound { value, exclusive } => {
                        self.visit_compiled_source_rows(meta, emit, |visit| {
                            self.data_source.visit_index_range_with_limit(
                                table,
                                index,
                                Some(value),
                                None,
                                !exclusive,
                                true,
                                *limit,
                                *offset,
                                *reverse,
                                visit,
                            )
                        })
                    }
                    KeyRange::UpperBound { value, exclusive } => {
                        self.visit_compiled_source_rows(meta, emit, |visit| {
                            self.data_source.visit_index_range_with_limit(
                                table,
                                index,
                                None,
                                Some(value),
                                true,
                                !exclusive,
                                *limit,
                                *offset,
                                *reverse,
                                visit,
                            )
                        })
                    }
                    KeyRange::Bound {
                        lower,
                        upper,
                        lower_exclusive,
                        upper_exclusive,
                    } => self.visit_compiled_source_rows(meta, emit, |visit| {
                        self.data_source.visit_index_range_with_limit(
                            table,
                            index,
                            Some(lower),
                            Some(upper),
                            !lower_exclusive,
                            !upper_exclusive,
                            *limit,
                            *offset,
                            *reverse,
                            visit,
                        )
                    }),
                },
                IndexBounds::Composite(range) => {
                    self.visit_compiled_source_rows(meta, emit, |visit| {
                        self.data_source.visit_index_range_composite_with_limit(
                            table,
                            index,
                            Some(range),
                            *limit,
                            *offset,
                            *reverse,
                            visit,
                        )
                    })
                }
            },
            CompiledSourcePlan::IndexGet {
                table,
                index,
                key,
                limit,
            } => self.visit_compiled_source_rows(meta, emit, |visit| {
                self.data_source
                    .visit_index_point_with_limit(table, index, key, *limit, visit)
            }),
            CompiledSourcePlan::IndexInGet { table, index, keys } => {
                let mut seen_ids = alloc::collections::BTreeSet::new();
                for key in keys {
                    let mut error = None;
                    let mut continue_scan = true;
                    self.data_source.visit_index_point_with_limit(
                        table,
                        index,
                        key,
                        None,
                        |row| {
                            if !seen_ids.insert(row.id()) {
                                return true;
                            }

                            match Self::emit_source_row(row, meta, emit) {
                                Ok(next) => {
                                    continue_scan = next;
                                    next
                                }
                                Err(err) => {
                                    error = Some(err);
                                    false
                                }
                            }
                        },
                    )?;
                    if let Some(err) = error {
                        return Err(err);
                    }
                    if !continue_scan {
                        return Ok(false);
                    }
                }
                Ok(true)
            }
            CompiledSourcePlan::GinIndexScan {
                table,
                index,
                key,
                value,
                query_type,
            } => match (query_type.as_str(), value.as_deref()) {
                ("eq", Some(value)) => self.visit_compiled_source_rows(meta, emit, |visit| {
                    self.data_source
                        .visit_gin_index_rows(table, index, key, value, visit)
                }),
                ("contains", _) | ("exists", _) => {
                    self.visit_compiled_source_rows(meta, emit, |visit| {
                        self.data_source
                            .visit_gin_index_rows_by_key(table, index, key, visit)
                    })
                }
                _ => self.visit_compiled_source_rows(meta, emit, |visit| {
                    self.data_source.visit_table_rows(table, visit)
                }),
            },
            CompiledSourcePlan::GinIndexScanMulti {
                table,
                index,
                pairs,
            } => {
                let pair_refs: Vec<(&str, &str)> = pairs
                    .iter()
                    .map(|(key, value)| (key.as_str(), value.as_str()))
                    .collect();
                self.visit_compiled_source_rows(meta, emit, |visit| {
                    self.data_source
                        .visit_gin_index_rows_multi(table, index, &pair_refs, visit)
                })
            }
        }
    }

    fn visit_compiled_source_rows<V>(
        &self,
        meta: &CompiledExecMeta,
        emit: &mut dyn FnMut(RelationEntry) -> ExecutionResult<bool>,
        visit: V,
    ) -> ExecutionResult<bool>
    where
        V: FnOnce(&mut dyn FnMut(&Rc<Row>) -> bool) -> ExecutionResult<()>,
    {
        let mut error = None;
        let mut continue_scan = true;
        let mut visitor = |row: &Rc<Row>| match Self::emit_source_row(row, meta, emit) {
            Ok(next) => {
                continue_scan = next;
                next
            }
            Err(err) => {
                error = Some(err);
                false
            }
        };

        visit(&mut visitor)?;

        if let Some(err) = error {
            return Err(err);
        }
        Ok(continue_scan)
    }

    fn emit_source_row(
        row: &Rc<Row>,
        meta: &CompiledExecMeta,
        emit: &mut dyn FnMut(RelationEntry) -> ExecutionResult<bool>,
    ) -> ExecutionResult<bool> {
        emit(RelationEntry::new_shared(
            Rc::clone(row),
            meta.shared_tables.clone(),
        ))
    }

    fn entry_from_meta(row: Rc<Row>, meta: &CompiledExecMeta) -> RelationEntry {
        if meta.tables.len() <= 1 {
            RelationEntry::new_shared(row, meta.shared_tables.clone())
        } else {
            RelationEntry::new_combined(row, meta.shared_tables.clone())
        }
    }

    fn emit_relation(
        &self,
        relation: Relation,
        emit: &mut dyn FnMut(RelationEntry) -> ExecutionResult<bool>,
    ) -> ExecutionResult<bool> {
        for entry in relation.entries {
            if !emit(entry)? {
                return Ok(false);
            }
        }
        Ok(true)
    }

    #[inline]
    fn relation_row_width(relation: &Relation) -> usize {
        relation
            .entries
            .first()
            .map(|entry| entry.row.len())
            .unwrap_or_else(|| relation.table_column_counts().iter().sum())
    }

    fn combined_relation_metadata(left: &Relation, right: &Relation) -> (Vec<String>, Vec<usize>) {
        let mut tables = left.tables().to_vec();
        tables.extend(right.tables().iter().cloned());

        let mut table_column_counts = left.table_column_counts().to_vec();
        table_column_counts.extend(right.table_column_counts().iter().copied());

        (tables, table_column_counts)
    }

    #[inline]
    fn combined_shared_tables(left: &Relation, right: &Relation) -> SharedTables {
        let (tables, _) = Self::combined_relation_metadata(left, right);
        tables.into()
    }

    #[inline]
    fn emit_join_view(
        &self,
        view: &JoinedRowView<'_>,
        shared_tables: &SharedTables,
        emit: &mut dyn FnMut(RelationEntry) -> ExecutionResult<bool>,
    ) -> ExecutionResult<bool> {
        emit(view.materialize_entry(shared_tables.clone()))
    }

    fn compile_single_table_pipeline_artifact(
        plan: &PhysicalPlan,
    ) -> Option<CachedSingleTablePipeline> {
        let mut current = Self::strip_noop(plan);
        let mut limit = None;
        let mut projection = CachedSingleTableProjection::Identity;

        match current {
            PhysicalPlan::Limit {
                input,
                limit: plan_limit,
                offset,
            } => {
                limit = Some((*plan_limit, *offset));
                current = Self::strip_noop(input);
            }
            PhysicalPlan::Project { .. } => {}
            _ => return None,
        }

        if let PhysicalPlan::Project { input, columns } = current {
            projection = if columns.iter().all(|expr| matches!(expr, Expr::Column(_))) {
                CachedSingleTableProjection::Columns(
                    columns
                        .iter()
                        .filter_map(|expr| match expr {
                            Expr::Column(column) => Some(column.index),
                            _ => None,
                        })
                        .collect(),
                )
            } else {
                CachedSingleTableProjection::Exprs(columns.clone())
            };
            current = Self::strip_noop(input);
        }

        let mut filters = Vec::new();
        while let PhysicalPlan::Filter { input, predicate } = current {
            filters.push(SingleTableFilter {
                predicate: Self::compile_row_predicate(predicate),
            });
            current = Self::strip_noop(input);
        }

        match current {
            PhysicalPlan::TableScan { table } => Some(CachedSingleTablePipeline {
                table: table.clone(),
                filters,
                projection,
                limit,
            }),
            _ => None,
        }
    }

    #[inline]
    fn strip_noop<'b>(mut plan: &'b PhysicalPlan) -> &'b PhysicalPlan {
        while let PhysicalPlan::NoOp { input } = plan {
            plan = input;
        }
        plan
    }

    fn execute_cached_single_table_pipeline(
        &self,
        pipeline: &CachedSingleTablePipeline,
    ) -> ExecutionResult<Relation> {
        let source_column_count = self.data_source.get_column_count(&pipeline.table)?;
        let output_column_count = match &pipeline.projection {
            CachedSingleTableProjection::Identity => source_column_count,
            CachedSingleTableProjection::Columns(indices) => indices.len(),
            CachedSingleTableProjection::Exprs(exprs) => exprs.len(),
        };
        let (tables, shared_tables) = Self::single_table_context(&pipeline.table);
        let row_capacity = match pipeline.limit {
            Some((limit, _)) => limit,
            None => self.data_source.get_table_row_count(&pipeline.table)?,
        };

        if let Some((limit, _)) = pipeline.limit {
            if limit == 0 {
                return Ok(Relation::from_entries(
                    Vec::new(),
                    tables,
                    alloc::vec![output_column_count],
                ));
            }
        }

        let mut skipped = 0usize;
        let mut taken = 0usize;
        let limit = pipeline.limit.map(|(limit, _)| limit);
        let offset = pipeline.limit.map(|(_, offset)| offset).unwrap_or(0);

        match &pipeline.projection {
            CachedSingleTableProjection::Identity => {
                let mut entries = Vec::with_capacity(row_capacity);
                self.data_source.visit_table_rows(&pipeline.table, |row| {
                    if !self.row_matches_filters(row.as_ref(), &pipeline.filters) {
                        return true;
                    }
                    if skipped < offset {
                        skipped += 1;
                        return true;
                    }
                    if limit.is_some_and(|limit| taken >= limit) {
                        return false;
                    }
                    entries.push(RelationEntry::new_shared(
                        Rc::clone(row),
                        shared_tables.clone(),
                    ));
                    taken += 1;
                    !limit.is_some_and(|limit| taken >= limit)
                })?;

                Ok(Relation::from_entries(
                    entries,
                    tables,
                    alloc::vec![source_column_count],
                ))
            }
            CachedSingleTableProjection::Columns(indices) => {
                let mut entries = Vec::with_capacity(row_capacity);
                self.data_source.visit_table_rows(&pipeline.table, |row| {
                    if !self.row_matches_filters(row.as_ref(), &pipeline.filters) {
                        return true;
                    }
                    if skipped < offset {
                        skipped += 1;
                        return true;
                    }
                    if limit.is_some_and(|limit| taken >= limit) {
                        return false;
                    }

                    let values = indices
                        .iter()
                        .map(|&index| row.get(index).cloned().unwrap_or(Value::Null))
                        .collect();
                    entries.push(RelationEntry::new_shared(
                        Rc::new(Row::new_with_version(row.id(), row.version(), values)),
                        shared_tables.clone(),
                    ));
                    taken += 1;
                    !limit.is_some_and(|limit| taken >= limit)
                })?;

                Ok(Relation::from_entries(
                    entries,
                    tables,
                    alloc::vec![output_column_count],
                ))
            }
            CachedSingleTableProjection::Exprs(exprs) => {
                let mut entries = Vec::with_capacity(row_capacity);
                self.data_source.visit_table_rows(&pipeline.table, |row| {
                    if !self.row_matches_filters(row.as_ref(), &pipeline.filters) {
                        return true;
                    }
                    if skipped < offset {
                        skipped += 1;
                        return true;
                    }
                    if limit.is_some_and(|limit| taken >= limit) {
                        return false;
                    }

                    let values = exprs
                        .iter()
                        .map(|expr| self.eval_row_expr(expr, row.as_ref()))
                        .collect();
                    entries.push(RelationEntry::new_shared(
                        Rc::new(Row::new_with_version(row.id(), row.version(), values)),
                        shared_tables.clone(),
                    ));
                    taken += 1;
                    !limit.is_some_and(|limit| taken >= limit)
                })?;

                Ok(Relation::from_entries(
                    entries,
                    tables,
                    alloc::vec![output_column_count],
                ))
            }
        }
    }

    fn execute_cached_single_table_pipeline_rows<F>(
        &self,
        pipeline: &CachedSingleTablePipeline,
        emit: &mut F,
    ) -> ExecutionResult<bool>
    where
        F: FnMut(Rc<Row>) -> ExecutionResult<bool>,
    {
        if let Some((limit, _)) = pipeline.limit {
            if limit == 0 {
                return Ok(true);
            }
        }

        let mut skipped = 0usize;
        let mut taken = 0usize;
        let limit = pipeline.limit.map(|(limit, _)| limit);
        let offset = pipeline.limit.map(|(_, offset)| offset).unwrap_or(0);

        match &pipeline.projection {
            CachedSingleTableProjection::Identity => {
                let mut error = None;
                let mut continue_scan = true;
                self.data_source.visit_table_rows(&pipeline.table, |row| {
                    if !self.row_matches_filters(row.as_ref(), &pipeline.filters) {
                        return true;
                    }
                    if skipped < offset {
                        skipped += 1;
                        return true;
                    }
                    if limit.is_some_and(|limit| taken >= limit) {
                        continue_scan = false;
                        return false;
                    }

                    match emit(Rc::clone(row)) {
                        Ok(next) => {
                            taken += 1;
                            continue_scan = next && !limit.is_some_and(|limit| taken >= limit);
                            continue_scan
                        }
                        Err(err) => {
                            error = Some(err);
                            false
                        }
                    }
                })?;

                if let Some(err) = error {
                    return Err(err);
                }
                Ok(continue_scan)
            }
            CachedSingleTableProjection::Columns(indices) => {
                let mut error = None;
                let mut continue_scan = true;
                self.data_source.visit_table_rows(&pipeline.table, |row| {
                    if !self.row_matches_filters(row.as_ref(), &pipeline.filters) {
                        return true;
                    }
                    if skipped < offset {
                        skipped += 1;
                        return true;
                    }
                    if limit.is_some_and(|limit| taken >= limit) {
                        continue_scan = false;
                        return false;
                    }

                    let values = indices
                        .iter()
                        .map(|&index| row.get(index).cloned().unwrap_or(Value::Null))
                        .collect();
                    let projected = Rc::new(Row::new_with_version(row.id(), row.version(), values));
                    match emit(projected) {
                        Ok(next) => {
                            taken += 1;
                            continue_scan = next && !limit.is_some_and(|limit| taken >= limit);
                            continue_scan
                        }
                        Err(err) => {
                            error = Some(err);
                            false
                        }
                    }
                })?;

                if let Some(err) = error {
                    return Err(err);
                }
                Ok(continue_scan)
            }
            CachedSingleTableProjection::Exprs(exprs) => {
                let mut error = None;
                let mut continue_scan = true;
                self.data_source.visit_table_rows(&pipeline.table, |row| {
                    if !self.row_matches_filters(row.as_ref(), &pipeline.filters) {
                        return true;
                    }
                    if skipped < offset {
                        skipped += 1;
                        return true;
                    }
                    if limit.is_some_and(|limit| taken >= limit) {
                        continue_scan = false;
                        return false;
                    }

                    let values = exprs
                        .iter()
                        .map(|expr| self.eval_row_expr(expr, row.as_ref()))
                        .collect();
                    let projected = Rc::new(Row::new_with_version(row.id(), row.version(), values));
                    match emit(projected) {
                        Ok(next) => {
                            taken += 1;
                            continue_scan = next && !limit.is_some_and(|limit| taken >= limit);
                            continue_scan
                        }
                        Err(err) => {
                            error = Some(err);
                            false
                        }
                    }
                })?;

                if let Some(err) = error {
                    return Err(err);
                }
                Ok(continue_scan)
            }
        }
    }

    fn collect_cached_single_table_pipeline_rows(
        &self,
        pipeline: &CachedSingleTablePipeline,
    ) -> ExecutionResult<Vec<Rc<Row>>> {
        let mut rows = match pipeline.limit {
            Some((limit, _)) => Vec::with_capacity(limit),
            None => Vec::new(),
        };
        self.execute_cached_single_table_pipeline_rows(pipeline, &mut |row| {
            rows.push(row);
            Ok(true)
        })?;
        Ok(rows)
    }

    #[inline]
    fn row_matches_filters(&self, row: &Row, filters: &[SingleTableFilter]) -> bool {
        filters.iter().all(|filter| {
            matches!(
                self.eval_compiled_predicate_accessor(row, &filter.predicate),
                PredicateValueState::Boolean(true)
            )
        })
    }

    #[inline]
    fn single_table_context(table: &str) -> (Vec<String>, SharedTables) {
        let tables = alloc::vec![String::from(table)];
        let shared_tables: SharedTables = alloc::sync::Arc::from(tables.as_slice());
        (tables, shared_tables)
    }

    // ========== Scan Operations ==========

    fn execute_table_scan(&self, table: &str) -> ExecutionResult<Relation> {
        let column_count = self.data_source.get_column_count(table)?;
        let row_count = self.data_source.get_table_row_count(table)?;
        let (tables, shared_tables) = Self::single_table_context(table);
        let mut entries = Vec::with_capacity(row_count);
        self.data_source.visit_table_rows(table, |row| {
            entries.push(RelationEntry::new_shared(
                Rc::clone(row),
                shared_tables.clone(),
            ));
            true
        })?;
        Ok(Relation::from_entries(
            entries,
            tables,
            alloc::vec![column_count],
        ))
    }

    fn execute_compiled_filtered_table_scan(
        &self,
        table: &str,
        predicate: &CompiledRowPredicate,
    ) -> ExecutionResult<Relation> {
        let column_count = self.data_source.get_column_count(table)?;
        let row_count = self.data_source.get_table_row_count(table)?;
        let (tables, shared_tables) = Self::single_table_context(table);
        let mut entries = Vec::with_capacity(row_count);
        self.data_source.visit_table_rows(table, |row| {
            if matches!(
                self.eval_compiled_row_predicate(row.as_ref(), predicate),
                PredicateValueState::Boolean(true)
            ) {
                entries.push(RelationEntry::new_shared(
                    Rc::clone(row),
                    shared_tables.clone(),
                ));
            }
            true
        })?;

        Ok(Relation::from_entries(
            entries,
            tables,
            alloc::vec![column_count],
        ))
    }

    fn execute_compiled_filtered_table_scan_rows<F>(
        &self,
        table: &str,
        predicate: &CompiledRowPredicate,
        emit: &mut F,
    ) -> ExecutionResult<bool>
    where
        F: FnMut(Rc<Row>) -> ExecutionResult<bool>,
    {
        let mut error = None;
        let mut continue_scan = true;
        self.data_source.visit_table_rows(table, |row| {
            if !matches!(
                self.eval_compiled_row_predicate(row.as_ref(), predicate),
                PredicateValueState::Boolean(true)
            ) {
                return true;
            }

            match emit(Rc::clone(row)) {
                Ok(next) => {
                    continue_scan = next;
                    next
                }
                Err(err) => {
                    error = Some(err);
                    false
                }
            }
        })?;

        if let Some(err) = error {
            return Err(err);
        }
        Ok(continue_scan)
    }

    fn collect_compiled_filtered_table_scan_rows(
        &self,
        table: &str,
        predicate: &CompiledRowPredicate,
    ) -> ExecutionResult<Vec<Rc<Row>>> {
        let mut rows = Vec::with_capacity(self.data_source.get_table_row_count(table)?);
        self.execute_compiled_filtered_table_scan_rows(table, predicate, &mut |row| {
            rows.push(row);
            Ok(true)
        })?;
        Ok(rows)
    }

    fn execute_index_scan(
        &self,
        table: &str,
        index: &str,
        bounds: &IndexBounds,
        limit: Option<usize>,
        offset: Option<usize>,
        reverse: bool,
    ) -> ExecutionResult<Relation> {
        // Push limit, offset, and reverse down to storage layer for early termination
        let rows = match bounds {
            IndexBounds::Unbounded => self.data_source.get_index_range_with_limit(
                table,
                index,
                None,
                None,
                true,
                true,
                limit,
                offset.unwrap_or(0),
                reverse,
            )?,
            IndexBounds::Scalar(range) => match range {
                KeyRange::All => self.data_source.get_index_range_with_limit(
                    table,
                    index,
                    None,
                    None,
                    true,
                    true,
                    limit,
                    offset.unwrap_or(0),
                    reverse,
                )?,
                KeyRange::Only(value) => self.data_source.get_index_range_with_limit(
                    table,
                    index,
                    Some(value),
                    Some(value),
                    true,
                    true,
                    limit,
                    offset.unwrap_or(0),
                    reverse,
                )?,
                KeyRange::LowerBound { value, exclusive } => {
                    self.data_source.get_index_range_with_limit(
                        table,
                        index,
                        Some(value),
                        None,
                        !exclusive,
                        true,
                        limit,
                        offset.unwrap_or(0),
                        reverse,
                    )?
                }
                KeyRange::UpperBound { value, exclusive } => {
                    self.data_source.get_index_range_with_limit(
                        table,
                        index,
                        None,
                        Some(value),
                        true,
                        !exclusive,
                        limit,
                        offset.unwrap_or(0),
                        reverse,
                    )?
                }
                KeyRange::Bound {
                    lower,
                    upper,
                    lower_exclusive,
                    upper_exclusive,
                } => self.data_source.get_index_range_with_limit(
                    table,
                    index,
                    Some(lower),
                    Some(upper),
                    !lower_exclusive,
                    !upper_exclusive,
                    limit,
                    offset.unwrap_or(0),
                    reverse,
                )?,
            },
            IndexBounds::Composite(range) => {
                self.data_source.get_index_range_composite_with_limit(
                    table,
                    index,
                    Some(range),
                    limit,
                    offset.unwrap_or(0),
                    reverse,
                )?
            }
        };
        let column_count = self.data_source.get_column_count(table)?;
        Ok(Relation::from_rows_with_column_count(
            rows,
            alloc::vec![table.into()],
            column_count,
        ))
    }

    fn execute_index_get(
        &self,
        table: &str,
        index: &str,
        key: &Value,
        limit: Option<usize>,
    ) -> ExecutionResult<Relation> {
        let rows = self
            .data_source
            .get_index_point_with_limit(table, index, key, limit)?;
        let column_count = self.data_source.get_column_count(table)?;
        Ok(Relation::from_rows_with_column_count(
            rows,
            alloc::vec![table.into()],
            column_count,
        ))
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
        Ok(Relation::from_rows_with_column_count(
            all_rows,
            alloc::vec![table.into()],
            column_count,
        ))
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
            ("contains", _) | ("exists", _) => self
                .data_source
                .get_gin_index_rows_by_key(table, index, key)?,
            _ => self.data_source.get_table_rows(table)?,
        };
        let column_count = self.data_source.get_column_count(table)?;
        Ok(Relation::from_rows_with_column_count(
            rows,
            alloc::vec![table.into()],
            column_count,
        ))
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
        let rows = self
            .data_source
            .get_gin_index_rows_multi(table, index, &pair_refs)?;
        let column_count = self.data_source.get_column_count(table)?;
        Ok(Relation::from_rows_with_column_count(
            rows,
            alloc::vec![table.into()],
            column_count,
        ))
    }

    // ========== Filter Operation ==========

    fn execute_filter(&self, input: Relation, predicate: &Expr) -> ExecutionResult<Relation> {
        let tables = input.tables().to_vec();
        let table_column_counts = input.table_column_counts().to_vec();

        if tables.len() <= 1 {
            let simple_predicate = Self::simple_binary_predicate(predicate);
            let entries: Vec<RelationEntry> = input
                .into_iter()
                .filter(|entry| {
                    if let Some(simple_predicate) = &simple_predicate {
                        self.eval_simple_binary_predicate(entry, simple_predicate)
                    } else {
                        self.eval_predicate(predicate, entry)
                    }
                })
                .collect();

            return Ok(Relation {
                entries,
                tables,
                table_column_counts,
            });
        }

        let ctx = EvalContext::new(&tables, &table_column_counts);

        let entries: Vec<RelationEntry> = input
            .into_iter()
            .filter(|entry| self.eval_predicate_ctx(predicate, entry, &ctx))
            .collect();

        Ok(Relation {
            entries,
            tables,
            table_column_counts,
        })
    }

    fn simple_binary_predicate(predicate: &Expr) -> Option<SimpleBinaryPredicate> {
        match predicate {
            Expr::BinaryOp { left, op, right } => match (left.as_ref(), right.as_ref()) {
                (Expr::Column(column), Expr::Literal(literal)) => Some(SimpleBinaryPredicate {
                    column_index: column.index,
                    op: *op,
                    literal: literal.clone(),
                    column_on_left: true,
                    kernel: Self::simple_predicate_kernel(*op, literal, true),
                }),
                (Expr::Literal(literal), Expr::Column(column)) => Some(SimpleBinaryPredicate {
                    column_index: column.index,
                    op: *op,
                    literal: literal.clone(),
                    column_on_left: false,
                    kernel: Self::simple_predicate_kernel(*op, literal, false),
                }),
                _ => None,
            },
            _ => None,
        }
    }

    fn compile_row_predicate(predicate: &Expr) -> CompiledRowPredicate {
        match predicate {
            Expr::BinaryOp {
                left,
                op: BinaryOp::And,
                right,
            } => {
                let mut predicates = Vec::new();
                Self::push_compiled_logical_terms(left, BinaryOp::And, &mut predicates);
                Self::push_compiled_logical_terms(right, BinaryOp::And, &mut predicates);
                CompiledRowPredicate::And(predicates)
            }
            Expr::BinaryOp {
                left,
                op: BinaryOp::Or,
                right,
            } => {
                let mut predicates = Vec::new();
                Self::push_compiled_logical_terms(left, BinaryOp::Or, &mut predicates);
                Self::push_compiled_logical_terms(right, BinaryOp::Or, &mut predicates);
                CompiledRowPredicate::Or(predicates)
            }
            Expr::UnaryOp {
                op: UnaryOp::Not,
                expr,
            } => CompiledRowPredicate::Not(Box::new(Self::compile_row_predicate(expr))),
            Expr::UnaryOp {
                op: UnaryOp::IsNull,
                expr,
            } => match expr.as_ref() {
                Expr::Column(column) => CompiledRowPredicate::IsNull {
                    column_index: column.index,
                },
                _ => CompiledRowPredicate::Generic(predicate.clone()),
            },
            Expr::UnaryOp {
                op: UnaryOp::IsNotNull,
                expr,
            } => match expr.as_ref() {
                Expr::Column(column) => CompiledRowPredicate::IsNotNull {
                    column_index: column.index,
                },
                _ => CompiledRowPredicate::Generic(predicate.clone()),
            },
            Expr::Between { expr, low, high } => match (expr.as_ref(), low.as_ref(), high.as_ref())
            {
                (Expr::Column(column), Expr::Literal(low), Expr::Literal(high)) => {
                    CompiledRowPredicate::Between {
                        column_index: column.index,
                        kernel: Self::compile_between_predicate_kernel(low, high),
                        negated: false,
                    }
                }
                _ => CompiledRowPredicate::Generic(predicate.clone()),
            },
            Expr::NotBetween { expr, low, high } => {
                match (expr.as_ref(), low.as_ref(), high.as_ref()) {
                    (Expr::Column(column), Expr::Literal(low), Expr::Literal(high)) => {
                        CompiledRowPredicate::Between {
                            column_index: column.index,
                            kernel: Self::compile_between_predicate_kernel(low, high),
                            negated: true,
                        }
                    }
                    _ => CompiledRowPredicate::Generic(predicate.clone()),
                }
            }
            Expr::In { expr, list } => Self::compile_row_in_list(predicate, expr, list, false),
            Expr::NotIn { expr, list } => Self::compile_row_in_list(predicate, expr, list, true),
            Expr::Like { expr, pattern } => match expr.as_ref() {
                Expr::Column(column) => CompiledRowPredicate::Like {
                    column_index: column.index,
                    pattern: Self::compile_like_pattern(pattern),
                    negated: false,
                },
                _ => CompiledRowPredicate::Generic(predicate.clone()),
            },
            Expr::NotLike { expr, pattern } => match expr.as_ref() {
                Expr::Column(column) => CompiledRowPredicate::Like {
                    column_index: column.index,
                    pattern: Self::compile_like_pattern(pattern),
                    negated: true,
                },
                _ => CompiledRowPredicate::Generic(predicate.clone()),
            },
            Expr::Match { expr, pattern } => match expr.as_ref() {
                Expr::Column(column) => CompiledRowPredicate::Match {
                    column_index: column.index,
                    pattern: pattern.clone(),
                    negated: false,
                },
                _ => CompiledRowPredicate::Generic(predicate.clone()),
            },
            Expr::NotMatch { expr, pattern } => match expr.as_ref() {
                Expr::Column(column) => CompiledRowPredicate::Match {
                    column_index: column.index,
                    pattern: pattern.clone(),
                    negated: true,
                },
                _ => CompiledRowPredicate::Generic(predicate.clone()),
            },
            Expr::Literal(value) => {
                CompiledRowPredicate::Literal(Self::predicate_value_state_from_value_ref(value))
            }
            _ => Self::simple_binary_predicate(predicate)
                .map(CompiledRowPredicate::Comparison)
                .unwrap_or(CompiledRowPredicate::Generic(predicate.clone())),
        }
    }

    fn compile_row_in_list(
        predicate: &Expr,
        expr: &Expr,
        list: &[Expr],
        negated: bool,
    ) -> CompiledRowPredicate {
        match expr {
            Expr::Column(column) => {
                let mut literals = Vec::with_capacity(list.len());
                for item in list {
                    if let Expr::Literal(value) = item {
                        literals.push(value.clone());
                    } else {
                        return CompiledRowPredicate::Generic(predicate.clone());
                    }
                }
                CompiledRowPredicate::InList {
                    column_index: column.index,
                    kernel: Self::compile_in_list_predicate_kernel(literals),
                    negated,
                }
            }
            _ => CompiledRowPredicate::Generic(predicate.clone()),
        }
    }

    fn push_compiled_logical_terms(
        predicate: &Expr,
        op: BinaryOp,
        predicates: &mut Vec<CompiledRowPredicate>,
    ) {
        if let Expr::BinaryOp {
            left,
            op: inner_op,
            right,
        } = predicate
        {
            if *inner_op == op {
                Self::push_compiled_logical_terms(left, op, predicates);
                Self::push_compiled_logical_terms(right, op, predicates);
                return;
            }
        }

        predicates.push(Self::compile_row_predicate(predicate));
    }

    #[inline]
    fn compile_like_pattern(pattern: &str) -> LikePatternKernel {
        if pattern.contains('_') {
            return LikePatternKernel::Generic(pattern.into());
        }

        let percent_count = pattern.bytes().filter(|byte| *byte == b'%').count();
        match percent_count {
            0 => LikePatternKernel::Exact(pattern.into()),
            1 if pattern.ends_with('%') => {
                LikePatternKernel::Prefix(pattern[..pattern.len().saturating_sub(1)].into())
            }
            1 if pattern.starts_with('%') => LikePatternKernel::Suffix(pattern[1..].into()),
            2 if pattern.starts_with('%') && pattern.ends_with('%') => {
                LikePatternKernel::Contains(pattern[1..pattern.len().saturating_sub(1)].into())
            }
            _ => LikePatternKernel::Generic(pattern.into()),
        }
    }

    #[inline]
    fn predicate_value_state_from_value_ref(value: &Value) -> PredicateValueState {
        match value {
            Value::Null => PredicateValueState::Null,
            Value::Boolean(value) => PredicateValueState::Boolean(*value),
            _ => PredicateValueState::Other,
        }
    }

    #[inline]
    fn numeric_literal_from_value_ref(value: &Value) -> Option<NumericLiteral> {
        match value {
            Value::Int32(value) => Some(NumericLiteral::Int32(*value)),
            Value::Int64(value) => Some(NumericLiteral::Int64(*value)),
            Value::Float64(value) => Some(NumericLiteral::Float64(*value)),
            _ => None,
        }
    }

    #[inline]
    fn compile_between_predicate_kernel(low: &Value, high: &Value) -> BetweenPredicateKernel {
        if let (Some(low), Some(high)) = (
            Self::numeric_literal_from_value_ref(low),
            Self::numeric_literal_from_value_ref(high),
        ) {
            return BetweenPredicateKernel::Numeric { low, high };
        }

        match (low, high) {
            (Value::Boolean(low), Value::Boolean(high)) => BetweenPredicateKernel::Boolean {
                low: *low,
                high: *high,
            },
            (Value::String(low), Value::String(high)) => BetweenPredicateKernel::String {
                low: low.clone(),
                high: high.clone(),
            },
            (Value::DateTime(low), Value::DateTime(high)) => BetweenPredicateKernel::DateTime {
                low: *low,
                high: *high,
            },
            (Value::Bytes(low), Value::Bytes(high)) => BetweenPredicateKernel::Bytes {
                low: low.clone(),
                high: high.clone(),
            },
            (Value::Jsonb(low), Value::Jsonb(high)) => BetweenPredicateKernel::Jsonb {
                low: low.0.clone(),
                high: high.0.clone(),
            },
            _ => BetweenPredicateKernel::Generic {
                low: low.clone(),
                high: high.clone(),
            },
        }
    }

    #[inline]
    fn compile_in_list_predicate_kernel(literals: Vec<Value>) -> InListPredicateKernel {
        if literals.is_empty() {
            return InListPredicateKernel::Empty;
        }

        let mut family = None;
        let mut contains_null = false;
        for literal in &literals {
            let next_family = match literal {
                Value::Null => {
                    contains_null = true;
                    continue;
                }
                Value::Int32(_) | Value::Int64(_) | Value::Float64(_) => {
                    InListKernelFamily::Numeric
                }
                Value::Boolean(_) => InListKernelFamily::Boolean,
                Value::String(_) => InListKernelFamily::String,
                Value::DateTime(_) => InListKernelFamily::DateTime,
                Value::Bytes(_) => InListKernelFamily::Bytes,
                Value::Jsonb(_) => InListKernelFamily::Jsonb,
            };

            match family {
                Some(current) if current != next_family => {
                    return InListPredicateKernel::Generic(literals);
                }
                Some(_) => {}
                None => family = Some(next_family),
            }
        }

        match family {
            None => InListPredicateKernel::NullOnly,
            Some(InListKernelFamily::Numeric) => {
                let mut int32_literals = Vec::new();
                let mut int64_literals = Vec::new();
                let mut float64_literals = Vec::new();

                for literal in &literals {
                    match literal {
                        Value::Int32(value) => int32_literals.push(*value),
                        Value::Int64(value) => int64_literals.push(*value),
                        Value::Float64(value) => float64_literals.push(*value),
                        Value::Null => {}
                        _ => unreachable!("validated homogeneous numeric IN list"),
                    }
                }

                InListPredicateKernel::Numeric {
                    int32_literals,
                    int64_literals,
                    float64_literals,
                    contains_null,
                }
            }
            Some(InListKernelFamily::Boolean) => {
                let literals = literals
                    .iter()
                    .filter_map(|literal| match literal {
                        Value::Boolean(value) => Some(*value),
                        Value::Null => None,
                        _ => unreachable!("validated homogeneous boolean IN list"),
                    })
                    .collect();
                InListPredicateKernel::Boolean {
                    literals,
                    contains_null,
                }
            }
            Some(InListKernelFamily::String) => {
                let literals = literals
                    .iter()
                    .filter_map(|literal| match literal {
                        Value::String(value) => Some(value.clone()),
                        Value::Null => None,
                        _ => unreachable!("validated homogeneous string IN list"),
                    })
                    .collect();
                InListPredicateKernel::String {
                    literals,
                    contains_null,
                }
            }
            Some(InListKernelFamily::DateTime) => {
                let literals = literals
                    .iter()
                    .filter_map(|literal| match literal {
                        Value::DateTime(value) => Some(*value),
                        Value::Null => None,
                        _ => unreachable!("validated homogeneous datetime IN list"),
                    })
                    .collect();
                InListPredicateKernel::DateTime {
                    literals,
                    contains_null,
                }
            }
            Some(InListKernelFamily::Bytes) => {
                let literals = literals
                    .iter()
                    .filter_map(|literal| match literal {
                        Value::Bytes(value) => Some(value.clone()),
                        Value::Null => None,
                        _ => unreachable!("validated homogeneous bytes IN list"),
                    })
                    .collect();
                InListPredicateKernel::Bytes {
                    literals,
                    contains_null,
                }
            }
            Some(InListKernelFamily::Jsonb) => {
                let literals = literals
                    .iter()
                    .filter_map(|literal| match literal {
                        Value::Jsonb(value) => Some(value.0.clone()),
                        Value::Null => None,
                        _ => unreachable!("validated homogeneous jsonb IN list"),
                    })
                    .collect();
                InListPredicateKernel::Jsonb {
                    literals,
                    contains_null,
                }
            }
        }
    }

    #[inline]
    fn simple_predicate_kernel(
        op: BinaryOp,
        literal: &Value,
        column_on_left: bool,
    ) -> SimplePredicateKernel {
        let Some(op) = Self::normalize_comparison_op(op, column_on_left) else {
            return SimplePredicateKernel::Generic;
        };

        if let Some(literal) = Self::numeric_literal_from_value_ref(literal) {
            return SimplePredicateKernel::Numeric { op, literal };
        }

        match literal {
            Value::Int32(_) | Value::Int64(_) | Value::Float64(_) => {
                unreachable!("numeric literals return earlier")
            }
            Value::Boolean(value) => SimplePredicateKernel::Boolean {
                op,
                literal: *value,
            },
            Value::String(value) => SimplePredicateKernel::String {
                op,
                literal: value.clone(),
            },
            Value::DateTime(value) => SimplePredicateKernel::DateTime {
                op,
                literal: *value,
            },
            Value::Bytes(value) => SimplePredicateKernel::Bytes {
                op,
                literal: value.clone(),
            },
            Value::Jsonb(value) => SimplePredicateKernel::Jsonb {
                op,
                literal: value.0.clone(),
            },
            Value::Null => SimplePredicateKernel::Generic,
        }
    }

    #[inline]
    fn normalize_comparison_op(op: BinaryOp, column_on_left: bool) -> Option<ComparisonOp> {
        let normalized = if column_on_left {
            op
        } else {
            match op {
                BinaryOp::Eq => BinaryOp::Eq,
                BinaryOp::Ne => BinaryOp::Ne,
                BinaryOp::Lt => BinaryOp::Gt,
                BinaryOp::Le => BinaryOp::Ge,
                BinaryOp::Gt => BinaryOp::Lt,
                BinaryOp::Ge => BinaryOp::Le,
                _ => return None,
            }
        };

        match normalized {
            BinaryOp::Eq => Some(ComparisonOp::Eq),
            BinaryOp::Ne => Some(ComparisonOp::Ne),
            BinaryOp::Lt => Some(ComparisonOp::Lt),
            BinaryOp::Le => Some(ComparisonOp::Le),
            BinaryOp::Gt => Some(ComparisonOp::Gt),
            BinaryOp::Ge => Some(ComparisonOp::Ge),
            _ => None,
        }
    }

    #[inline]
    fn eval_simple_binary_predicate(
        &self,
        entry: &RelationEntry,
        predicate: &SimpleBinaryPredicate,
    ) -> bool {
        let null = Value::Null;
        let value = entry.get_field(predicate.column_index).unwrap_or(&null);
        self.eval_simple_binary_predicate_value(value, predicate)
    }

    #[inline]
    fn eval_simple_binary_predicate_value(
        &self,
        value: &Value,
        predicate: &SimpleBinaryPredicate,
    ) -> bool {
        if value.is_null() || predicate.literal.is_null() {
            return false;
        }

        match &predicate.kernel {
            SimplePredicateKernel::Numeric { op, literal } => {
                if let Some(ordering) = Self::compare_numeric_value(value, *literal) {
                    return Self::eval_comparison_op(*op, ordering);
                }
            }
            SimplePredicateKernel::Boolean { op, literal } => {
                if let Value::Boolean(value) = value {
                    return Self::eval_comparison_op(*op, value.cmp(literal));
                }
            }
            SimplePredicateKernel::String { op, literal } => {
                if let Value::String(value) = value {
                    return Self::eval_comparison_op(*op, value.as_str().cmp(literal));
                }
            }
            SimplePredicateKernel::DateTime { op, literal } => {
                if let Value::DateTime(value) = value {
                    return Self::eval_comparison_op(*op, value.cmp(literal));
                }
            }
            SimplePredicateKernel::Bytes { op, literal } => {
                if let Value::Bytes(value) = value {
                    return Self::eval_comparison_op(*op, value.as_slice().cmp(literal));
                }
            }
            SimplePredicateKernel::Jsonb { op, literal } => {
                if let Value::Jsonb(value) = value {
                    return Self::eval_comparison_op(*op, value.0.as_slice().cmp(literal));
                }
            }
            SimplePredicateKernel::Generic => {}
        }

        if predicate.column_on_left {
            self.eval_binary_op_bool(predicate.op, value, &predicate.literal)
        } else {
            self.eval_binary_op_bool(predicate.op, &predicate.literal, value)
        }
    }

    fn eval_compiled_row_predicate(
        &self,
        row: &Row,
        predicate: &CompiledRowPredicate,
    ) -> PredicateValueState {
        self.eval_compiled_predicate_accessor(row, predicate)
    }

    fn eval_compiled_predicate_accessor<A: RowAccessor>(
        &self,
        accessor: &A,
        predicate: &CompiledRowPredicate,
    ) -> PredicateValueState {
        match predicate {
            CompiledRowPredicate::Literal(value) => *value,
            CompiledRowPredicate::Comparison(predicate) => {
                self.eval_compiled_row_comparison_accessor(accessor, predicate)
            }
            CompiledRowPredicate::And(predicates) => {
                let mut state = PredicateValueState::Boolean(true);
                for predicate in predicates {
                    state = Self::combine_predicate_states(
                        BinaryOp::And,
                        state,
                        self.eval_compiled_predicate_accessor(accessor, predicate),
                    );
                    if matches!(state, PredicateValueState::Boolean(false)) {
                        break;
                    }
                }
                state
            }
            CompiledRowPredicate::Or(predicates) => {
                let mut state = PredicateValueState::Boolean(false);
                for predicate in predicates {
                    state = Self::combine_predicate_states(
                        BinaryOp::Or,
                        state,
                        self.eval_compiled_predicate_accessor(accessor, predicate),
                    );
                    if matches!(state, PredicateValueState::Boolean(true)) {
                        break;
                    }
                }
                state
            }
            CompiledRowPredicate::Not(predicate) => {
                match self.eval_compiled_predicate_accessor(accessor, predicate) {
                    PredicateValueState::Boolean(value) => PredicateValueState::Boolean(!value),
                    PredicateValueState::Null | PredicateValueState::Other => {
                        PredicateValueState::Null
                    }
                }
            }
            CompiledRowPredicate::IsNull { column_index } => PredicateValueState::Boolean(
                accessor
                    .get_value(*column_index)
                    .map(Value::is_null)
                    .unwrap_or(true),
            ),
            CompiledRowPredicate::IsNotNull { column_index } => PredicateValueState::Boolean(
                accessor
                    .get_value(*column_index)
                    .map(|value| !value.is_null())
                    .unwrap_or(false),
            ),
            CompiledRowPredicate::Between {
                column_index,
                kernel,
                negated,
            } => {
                let value = accessor.get_value(*column_index).unwrap_or(&NULL_VALUE);
                let in_range = Self::eval_between_predicate_value(value, kernel);
                PredicateValueState::Boolean(if *negated { !in_range } else { in_range })
            }
            CompiledRowPredicate::InList {
                column_index,
                kernel,
                negated,
            } => {
                let value = accessor.get_value(*column_index).unwrap_or(&NULL_VALUE);
                let contains = Self::eval_in_list_predicate_value(value, kernel);
                PredicateValueState::Boolean(if *negated { !contains } else { contains })
            }
            CompiledRowPredicate::Like {
                column_index,
                pattern,
                negated,
            } => match accessor.get_value(*column_index) {
                Some(Value::String(value)) => {
                    let matched = self.eval_like_pattern(value.as_str(), pattern);
                    PredicateValueState::Boolean(if *negated { !matched } else { matched })
                }
                _ => PredicateValueState::Boolean(*negated),
            },
            CompiledRowPredicate::Match {
                column_index,
                pattern,
                negated,
            } => match accessor.get_value(*column_index) {
                Some(Value::String(value)) => {
                    let matched = self.match_regex_pattern(value.as_str(), pattern);
                    PredicateValueState::Boolean(if *negated { !matched } else { matched })
                }
                _ => PredicateValueState::Boolean(*negated),
            },
            CompiledRowPredicate::Generic(expr) => Self::predicate_value_state_from_value(
                self.eval_accessor_expr(expr, accessor, None),
            ),
        }
    }

    #[inline]
    fn eval_compiled_row_comparison_accessor<A: RowAccessor>(
        &self,
        accessor: &A,
        predicate: &SimpleBinaryPredicate,
    ) -> PredicateValueState {
        let value = accessor
            .get_value(predicate.column_index)
            .unwrap_or(&NULL_VALUE);
        self.eval_compiled_row_comparison_value(value, predicate)
    }

    #[inline]
    fn eval_compiled_row_comparison_value(
        &self,
        value: &Value,
        predicate: &SimpleBinaryPredicate,
    ) -> PredicateValueState {
        if value.is_null() || predicate.literal.is_null() {
            return PredicateValueState::Null;
        }

        match &predicate.kernel {
            SimplePredicateKernel::Numeric { op, literal } => {
                if let Some(ordering) = Self::compare_numeric_value(value, *literal) {
                    return PredicateValueState::Boolean(Self::eval_comparison_op(*op, ordering));
                }
            }
            SimplePredicateKernel::Boolean { op, literal } => {
                if let Value::Boolean(value) = value {
                    return PredicateValueState::Boolean(Self::eval_comparison_op(
                        *op,
                        value.cmp(literal),
                    ));
                }
            }
            SimplePredicateKernel::String { op, literal } => {
                if let Value::String(value) = value {
                    return PredicateValueState::Boolean(Self::eval_comparison_op(
                        *op,
                        value.as_str().cmp(literal),
                    ));
                }
            }
            SimplePredicateKernel::DateTime { op, literal } => {
                if let Value::DateTime(value) = value {
                    return PredicateValueState::Boolean(Self::eval_comparison_op(
                        *op,
                        value.cmp(literal),
                    ));
                }
            }
            SimplePredicateKernel::Bytes { op, literal } => {
                if let Value::Bytes(value) = value {
                    return PredicateValueState::Boolean(Self::eval_comparison_op(
                        *op,
                        value.as_slice().cmp(literal),
                    ));
                }
            }
            SimplePredicateKernel::Jsonb { op, literal } => {
                if let Value::Jsonb(value) = value {
                    return PredicateValueState::Boolean(Self::eval_comparison_op(
                        *op,
                        value.0.as_slice().cmp(literal),
                    ));
                }
            }
            SimplePredicateKernel::Generic => {}
        }

        PredicateValueState::Boolean(if predicate.column_on_left {
            self.eval_binary_op_bool(predicate.op, value, &predicate.literal)
        } else {
            self.eval_binary_op_bool(predicate.op, &predicate.literal, value)
        })
    }

    #[inline]
    fn eval_between_predicate_value(value: &Value, kernel: &BetweenPredicateKernel) -> bool {
        match kernel {
            BetweenPredicateKernel::Generic { low, high } => value >= low && value <= high,
            BetweenPredicateKernel::Numeric { low, high } => {
                let Some(low_ordering) = Self::compare_numeric_value(value, *low) else {
                    return false;
                };
                if low_ordering == Ordering::Less {
                    return false;
                }

                let Some(high_ordering) = Self::compare_numeric_value(value, *high) else {
                    return false;
                };
                high_ordering != Ordering::Greater
            }
            BetweenPredicateKernel::Boolean { low, high } => match value {
                Value::Boolean(value) => *value >= *low && *value <= *high,
                _ => false,
            },
            BetweenPredicateKernel::String { low, high } => match value {
                Value::String(value) => {
                    value.as_str() >= low.as_str() && value.as_str() <= high.as_str()
                }
                _ => false,
            },
            BetweenPredicateKernel::DateTime { low, high } => match value {
                Value::DateTime(value) => *value >= *low && *value <= *high,
                _ => false,
            },
            BetweenPredicateKernel::Bytes { low, high } => match value {
                Value::Bytes(value) => {
                    value.as_slice() >= low.as_slice() && value.as_slice() <= high.as_slice()
                }
                _ => false,
            },
            BetweenPredicateKernel::Jsonb { low, high } => match value {
                Value::Jsonb(value) => {
                    value.0.as_slice() >= low.as_slice() && value.0.as_slice() <= high.as_slice()
                }
                _ => false,
            },
        }
    }

    #[inline]
    fn eval_in_list_predicate_value(value: &Value, kernel: &InListPredicateKernel) -> bool {
        match kernel {
            InListPredicateKernel::Empty => false,
            InListPredicateKernel::NullOnly => value.is_null(),
            InListPredicateKernel::Numeric {
                int32_literals,
                int64_literals,
                float64_literals,
                contains_null,
            } => match value {
                Value::Null => *contains_null,
                Value::Int32(value) => int32_literals.contains(value),
                Value::Int64(value) => int64_literals.contains(value),
                Value::Float64(value) => float64_literals
                    .iter()
                    .any(|literal| Self::float_literal_eq(*value, *literal)),
                _ => false,
            },
            InListPredicateKernel::Boolean {
                literals,
                contains_null,
            } => match value {
                Value::Null => *contains_null,
                Value::Boolean(value) => literals.contains(value),
                _ => false,
            },
            InListPredicateKernel::String {
                literals,
                contains_null,
            } => match value {
                Value::Null => *contains_null,
                Value::String(value) => literals.iter().any(|literal| literal == value),
                _ => false,
            },
            InListPredicateKernel::DateTime {
                literals,
                contains_null,
            } => match value {
                Value::Null => *contains_null,
                Value::DateTime(value) => literals.contains(value),
                _ => false,
            },
            InListPredicateKernel::Bytes {
                literals,
                contains_null,
            } => match value {
                Value::Null => *contains_null,
                Value::Bytes(value) => literals.iter().any(|literal| literal == value),
                _ => false,
            },
            InListPredicateKernel::Jsonb {
                literals,
                contains_null,
            } => match value {
                Value::Null => *contains_null,
                Value::Jsonb(value) => literals.iter().any(|literal| literal == &value.0),
                _ => false,
            },
            InListPredicateKernel::Generic(literals) => {
                literals.iter().any(|literal| value == literal)
            }
        }
    }

    #[inline]
    fn float_literal_eq(value: f64, literal: f64) -> bool {
        if value.is_nan() && literal.is_nan() {
            true
        } else {
            value == literal
        }
    }

    #[inline]
    fn combine_predicate_states(
        op: BinaryOp,
        left: PredicateValueState,
        right: PredicateValueState,
    ) -> PredicateValueState {
        debug_assert!(matches!(op, BinaryOp::And | BinaryOp::Or));

        if matches!(left, PredicateValueState::Null) || matches!(right, PredicateValueState::Null) {
            match op {
                BinaryOp::And => {
                    if matches!(left, PredicateValueState::Boolean(false))
                        || matches!(right, PredicateValueState::Boolean(false))
                    {
                        return PredicateValueState::Boolean(false);
                    }
                    PredicateValueState::Null
                }
                BinaryOp::Or => {
                    if matches!(left, PredicateValueState::Boolean(true))
                        || matches!(right, PredicateValueState::Boolean(true))
                    {
                        return PredicateValueState::Boolean(true);
                    }
                    PredicateValueState::Null
                }
                _ => PredicateValueState::Null,
            }
        } else {
            let left_true = matches!(left, PredicateValueState::Boolean(true));
            let right_true = matches!(right, PredicateValueState::Boolean(true));
            PredicateValueState::Boolean(match op {
                BinaryOp::And => left_true && right_true,
                BinaryOp::Or => left_true || right_true,
                _ => false,
            })
        }
    }

    #[inline]
    fn predicate_value_state_from_value(value: Value) -> PredicateValueState {
        match value {
            Value::Null => PredicateValueState::Null,
            Value::Boolean(value) => PredicateValueState::Boolean(value),
            _ => PredicateValueState::Other,
        }
    }

    #[inline]
    fn eval_like_pattern(&self, value: &str, pattern: &LikePatternKernel) -> bool {
        match pattern {
            LikePatternKernel::Exact(pattern) => value == pattern,
            LikePatternKernel::Prefix(pattern) => value.starts_with(pattern),
            LikePatternKernel::Suffix(pattern) => value.ends_with(pattern),
            LikePatternKernel::Contains(pattern) => value.contains(pattern),
            LikePatternKernel::Generic(pattern) => self.match_like_pattern(value, pattern),
        }
    }

    #[inline]
    fn eval_comparison_op(op: ComparisonOp, ordering: Ordering) -> bool {
        match op {
            ComparisonOp::Eq => ordering == Ordering::Equal,
            ComparisonOp::Ne => ordering != Ordering::Equal,
            ComparisonOp::Lt => ordering == Ordering::Less,
            ComparisonOp::Le => ordering != Ordering::Greater,
            ComparisonOp::Gt => ordering == Ordering::Greater,
            ComparisonOp::Ge => ordering != Ordering::Less,
        }
    }

    #[inline]
    fn compare_numeric_value(value: &Value, literal: NumericLiteral) -> Option<Ordering> {
        match value {
            Value::Int32(value) => Some(match literal {
                NumericLiteral::Int32(literal) => value.cmp(&literal),
                NumericLiteral::Int64(literal) => (*value as i64).cmp(&literal),
                NumericLiteral::Float64(literal) => {
                    if literal.is_nan() {
                        Ordering::Less
                    } else {
                        (*value as f64)
                            .partial_cmp(&literal)
                            .unwrap_or(Ordering::Equal)
                    }
                }
            }),
            Value::Int64(value) => Some(match literal {
                NumericLiteral::Int32(literal) => value.cmp(&(literal as i64)),
                NumericLiteral::Int64(literal) => value.cmp(&literal),
                NumericLiteral::Float64(literal) => {
                    if literal.is_nan() {
                        Ordering::Less
                    } else {
                        (*value as f64)
                            .partial_cmp(&literal)
                            .unwrap_or(Ordering::Equal)
                    }
                }
            }),
            Value::Float64(value) => Some(match literal {
                NumericLiteral::Int32(literal) => Self::compare_f64(*value, literal as f64),
                NumericLiteral::Int64(literal) => Self::compare_f64(*value, literal as f64),
                NumericLiteral::Float64(literal) => Self::compare_f64(*value, literal),
            }),
            _ => None,
        }
    }

    #[inline]
    fn compare_f64(left: f64, right: f64) -> Ordering {
        match (left.is_nan(), right.is_nan()) {
            (true, true) => Ordering::Equal,
            (true, false) => Ordering::Greater,
            (false, true) => Ordering::Less,
            (false, false) => left.partial_cmp(&right).unwrap_or(Ordering::Equal),
        }
    }

    #[inline]
    fn eval_binary_op_bool(&self, op: BinaryOp, left: &Value, right: &Value) -> bool {
        match op {
            BinaryOp::Eq => left == right,
            BinaryOp::Ne => left != right,
            BinaryOp::Lt => left < right,
            BinaryOp::Le => left <= right,
            BinaryOp::Gt => left > right,
            BinaryOp::Ge => left >= right,
            BinaryOp::And => {
                matches!(left, Value::Boolean(true)) && matches!(right, Value::Boolean(true))
            }
            BinaryOp::Or => {
                matches!(left, Value::Boolean(true)) || matches!(right, Value::Boolean(true))
            }
            _ => matches!(self.eval_binary_op(op, left, right), Value::Boolean(true)),
        }
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
                RelationEntry::new_combined(
                    Rc::new(Row::new_with_version(
                        entry.id(),
                        entry.row.version(),
                        values,
                    )),
                    shared_tables.clone(),
                )
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

    fn emit_hash_join_entries(
        &self,
        left: &Relation,
        right: &Relation,
        left_key_idx: usize,
        right_key_idx: usize,
        join_type: crate::ast::JoinType,
        emit: &mut dyn FnMut(RelationEntry) -> ExecutionResult<bool>,
    ) -> ExecutionResult<bool> {
        let is_outer = matches!(
            join_type,
            crate::ast::JoinType::LeftOuter | crate::ast::JoinType::FullOuter
        );
        let (build_rel, probe_rel, build_key_idx, probe_key_idx, build_is_left) = if is_outer {
            (right, left, right_key_idx, left_key_idx, false)
        } else if left.len() <= right.len() {
            (left, right, left_key_idx, right_key_idx, true)
        } else {
            (right, left, right_key_idx, left_key_idx, false)
        };

        let shared_tables = Self::combined_shared_tables(left, right);
        let left_width = Self::relation_row_width(left);
        let right_width = Self::relation_row_width(right);

        let mut hash_table: hashbrown::HashMap<&Value, Vec<u32>> =
            hashbrown::HashMap::with_capacity(build_rel.len());

        for (index, entry) in build_rel.entries.iter().enumerate() {
            if let Some(key_value) = entry.get_field(build_key_idx) {
                if !key_value.is_null() {
                    hash_table.entry(key_value).or_default().push(index as u32);
                }
            }
        }

        for probe_entry in probe_rel.entries.iter() {
            let mut matched = false;
            if let Some(key_value) = probe_entry.get_field(probe_key_idx) {
                if !key_value.is_null() {
                    if let Some(build_indices) = hash_table.get(key_value) {
                        matched = true;
                        for &build_index in build_indices {
                            let build_entry = &build_rel.entries[build_index as usize];
                            let view = if build_is_left {
                                JoinedRowView::new(
                                    Some(build_entry),
                                    Some(probe_entry),
                                    left_width,
                                    right_width,
                                )
                            } else {
                                JoinedRowView::new(
                                    Some(probe_entry),
                                    Some(build_entry),
                                    left_width,
                                    right_width,
                                )
                            };
                            if !self.emit_join_view(&view, &shared_tables, emit)? {
                                return Ok(false);
                            }
                        }
                    }
                }
            }

            if is_outer && !matched {
                let view = JoinedRowView::new(Some(probe_entry), None, left_width, right_width);
                if !self.emit_join_view(&view, &shared_tables, emit)? {
                    return Ok(false);
                }
            }
        }

        Ok(true)
    }

    fn emit_sort_merge_join_entries(
        &self,
        mut left: Relation,
        mut right: Relation,
        left_key_idx: usize,
        right_key_idx: usize,
        join_type: crate::ast::JoinType,
        emit: &mut dyn FnMut(RelationEntry) -> ExecutionResult<bool>,
    ) -> ExecutionResult<bool> {
        left.entries.sort_by(|a, b| {
            let a_val = a.get_field(left_key_idx);
            let b_val = b.get_field(left_key_idx);
            match (a_val, b_val) {
                (None, None) => Ordering::Equal,
                (None, Some(_)) => Ordering::Less,
                (Some(_), None) => Ordering::Greater,
                (Some(a_val), Some(b_val)) => a_val.cmp(b_val),
            }
        });
        right.entries.sort_by(|a, b| {
            let a_val = a.get_field(right_key_idx);
            let b_val = b.get_field(right_key_idx);
            match (a_val, b_val) {
                (None, None) => Ordering::Equal,
                (None, Some(_)) => Ordering::Less,
                (Some(_), None) => Ordering::Greater,
                (Some(a_val), Some(b_val)) => a_val.cmp(b_val),
            }
        });

        let is_outer = matches!(
            join_type,
            crate::ast::JoinType::LeftOuter | crate::ast::JoinType::FullOuter
        );
        let shared_tables = Self::combined_shared_tables(&left, &right);
        let left_width = Self::relation_row_width(&left);
        let right_width = Self::relation_row_width(&right);

        let mut left_index = 0usize;
        let mut right_index = 0usize;

        while left_index < left.entries.len() {
            let left_entry = &left.entries[left_index];
            let left_value = left_entry.get_field(left_key_idx);
            if left_value.map(|value| value.is_null()).unwrap_or(true) {
                if is_outer {
                    let view = JoinedRowView::new(Some(left_entry), None, left_width, right_width);
                    if !self.emit_join_view(&view, &shared_tables, emit)? {
                        return Ok(false);
                    }
                }
                left_index += 1;
                continue;
            }

            let left_value = left_value.unwrap();
            while right_index < right.entries.len() {
                let right_value = right.entries[right_index].get_field(right_key_idx);
                if right_value.map(|value| value.is_null()).unwrap_or(true) {
                    right_index += 1;
                    continue;
                }
                if right_value.unwrap() < left_value {
                    right_index += 1;
                } else {
                    break;
                }
            }

            let mut matched = false;
            let mut right_scan = right_index;
            while right_scan < right.entries.len() {
                let right_entry = &right.entries[right_scan];
                let right_value = right_entry.get_field(right_key_idx);
                if right_value.map(|value| value.is_null()).unwrap_or(true) {
                    right_scan += 1;
                    continue;
                }

                match left_value.cmp(right_value.unwrap()) {
                    Ordering::Equal => {
                        matched = true;
                        let view = JoinedRowView::new(
                            Some(left_entry),
                            Some(right_entry),
                            left_width,
                            right_width,
                        );
                        if !self.emit_join_view(&view, &shared_tables, emit)? {
                            return Ok(false);
                        }
                        right_scan += 1;
                    }
                    Ordering::Less => break,
                    Ordering::Greater => right_scan += 1,
                }
            }

            if is_outer && !matched {
                let view = JoinedRowView::new(Some(left_entry), None, left_width, right_width);
                if !self.emit_join_view(&view, &shared_tables, emit)? {
                    return Ok(false);
                }
            }

            left_index += 1;
        }

        Ok(true)
    }

    fn emit_nested_loop_join_entries(
        &self,
        left: &Relation,
        right: &Relation,
        condition: &Expr,
        join_type: crate::ast::JoinType,
        emit: &mut dyn FnMut(RelationEntry) -> ExecutionResult<bool>,
    ) -> ExecutionResult<bool> {
        let shared_tables = Self::combined_shared_tables(left, right);
        let (tables, table_column_counts) = Self::combined_relation_metadata(left, right);
        let ctx = EvalContext::new(&tables, &table_column_counts);
        let left_width = Self::relation_row_width(left);
        let right_width = Self::relation_row_width(right);

        let emit_unmatched_left = matches!(
            join_type,
            crate::ast::JoinType::LeftOuter | crate::ast::JoinType::FullOuter
        );
        let track_right_matches = matches!(
            join_type,
            crate::ast::JoinType::RightOuter | crate::ast::JoinType::FullOuter
        );
        let mut right_matched = if track_right_matches {
            vec![false; right.entries.len()]
        } else {
            Vec::new()
        };

        for left_entry in left.iter() {
            let mut matched = false;
            for (right_index, right_entry) in right.entries.iter().enumerate() {
                let view = JoinedRowView::new(
                    Some(left_entry),
                    Some(right_entry),
                    left_width,
                    right_width,
                );
                if self.eval_predicate_accessor_ctx(condition, &view, Some(&ctx)) {
                    matched = true;
                    if track_right_matches {
                        right_matched[right_index] = true;
                    }
                    if !self.emit_join_view(&view, &shared_tables, emit)? {
                        return Ok(false);
                    }
                }
            }

            if emit_unmatched_left && !matched {
                let view = JoinedRowView::new(Some(left_entry), None, left_width, right_width);
                if !self.emit_join_view(&view, &shared_tables, emit)? {
                    return Ok(false);
                }
            }
        }

        if track_right_matches {
            for (right_index, right_entry) in right.entries.iter().enumerate() {
                if !right_matched[right_index] {
                    let view = JoinedRowView::new(None, Some(right_entry), left_width, right_width);
                    if !self.emit_join_view(&view, &shared_tables, emit)? {
                        return Ok(false);
                    }
                }
            }
        }

        Ok(true)
    }

    fn emit_nested_loop_join_entries_compiled(
        &self,
        left: &Relation,
        right: &Relation,
        predicate: &CompiledRowPredicate,
        join_type: crate::ast::JoinType,
        emit: &mut dyn FnMut(RelationEntry) -> ExecutionResult<bool>,
    ) -> ExecutionResult<bool> {
        let shared_tables = Self::combined_shared_tables(left, right);
        let left_width = Self::relation_row_width(left);
        let right_width = Self::relation_row_width(right);

        let emit_unmatched_left = matches!(
            join_type,
            crate::ast::JoinType::LeftOuter | crate::ast::JoinType::FullOuter
        );
        let track_right_matches = matches!(
            join_type,
            crate::ast::JoinType::RightOuter | crate::ast::JoinType::FullOuter
        );
        let mut right_matched = if track_right_matches {
            vec![false; right.entries.len()]
        } else {
            Vec::new()
        };

        for left_entry in left.iter() {
            let mut matched = false;
            for (right_index, right_entry) in right.entries.iter().enumerate() {
                let view = JoinedRowView::new(
                    Some(left_entry),
                    Some(right_entry),
                    left_width,
                    right_width,
                );
                if matches!(
                    self.eval_compiled_predicate_accessor(&view, predicate),
                    PredicateValueState::Boolean(true)
                ) {
                    matched = true;
                    if track_right_matches {
                        right_matched[right_index] = true;
                    }
                    if !self.emit_join_view(&view, &shared_tables, emit)? {
                        return Ok(false);
                    }
                }
            }

            if emit_unmatched_left && !matched {
                let view = JoinedRowView::new(Some(left_entry), None, left_width, right_width);
                if !self.emit_join_view(&view, &shared_tables, emit)? {
                    return Ok(false);
                }
            }
        }

        if track_right_matches {
            for (right_index, right_entry) in right.entries.iter().enumerate() {
                if !right_matched[right_index] {
                    let view = JoinedRowView::new(None, Some(right_entry), left_width, right_width);
                    if !self.emit_join_view(&view, &shared_tables, emit)? {
                        return Ok(false);
                    }
                }
            }
        }

        Ok(true)
    }

    fn emit_index_nested_loop_join_entries(
        &self,
        outer: &Relation,
        inner_table: &str,
        inner_index: &str,
        condition: &Expr,
        join_type: crate::ast::JoinType,
        emit: &mut dyn FnMut(RelationEntry) -> ExecutionResult<bool>,
    ) -> ExecutionResult<bool> {
        let is_outer = matches!(
            join_type,
            crate::ast::JoinType::LeftOuter | crate::ast::JoinType::FullOuter
        );
        let outer_key_idx = self.extract_outer_key_index(condition, outer)?;
        let inner_col_count = self.data_source.get_column_count(inner_table)?;

        let mut tables = outer.tables().to_vec();
        tables.push(inner_table.into());
        let shared_tables: SharedTables = tables.into();
        let outer_width = Self::relation_row_width(outer);

        for outer_entry in outer.iter() {
            let mut matched = false;
            let mut visit_error = None;
            let mut continue_scan = true;

            if let Some(key) = outer_entry.get_field(outer_key_idx) {
                if !key.is_null() {
                    self.data_source.visit_index_point_with_limit(
                        inner_table,
                        inner_index,
                        key,
                        None,
                        |inner_row| {
                            let inner_entry =
                                RelationEntry::from_row(Rc::clone(inner_row), inner_table);
                            let view = JoinedRowView::new(
                                Some(outer_entry),
                                Some(&inner_entry),
                                outer_width,
                                inner_col_count,
                            );
                            matched = true;
                            match self.emit_join_view(&view, &shared_tables, emit) {
                                Ok(next) => {
                                    continue_scan = next;
                                    next
                                }
                                Err(err) => {
                                    visit_error = Some(err);
                                    false
                                }
                            }
                        },
                    )?;
                }
            }

            if let Some(err) = visit_error {
                return Err(err);
            }
            if !continue_scan {
                return Ok(false);
            }

            if is_outer && !matched {
                let view =
                    JoinedRowView::new(Some(outer_entry), None, outer_width, inner_col_count);
                if !self.emit_join_view(&view, &shared_tables, emit)? {
                    return Ok(false);
                }
            }
        }

        Ok(true)
    }

    fn emit_index_nested_loop_join_entries_compiled(
        &self,
        outer: &Relation,
        inner_table: &str,
        inner_index: &str,
        outer_key_idx: usize,
        join_type: crate::ast::JoinType,
        emit: &mut dyn FnMut(RelationEntry) -> ExecutionResult<bool>,
    ) -> ExecutionResult<bool> {
        let is_outer = matches!(
            join_type,
            crate::ast::JoinType::LeftOuter | crate::ast::JoinType::FullOuter
        );
        let inner_col_count = self.data_source.get_column_count(inner_table)?;

        let mut tables = outer.tables().to_vec();
        tables.push(inner_table.into());
        let shared_tables: SharedTables = tables.into();
        let outer_width = Self::relation_row_width(outer);

        for outer_entry in outer.iter() {
            let mut matched = false;
            let mut visit_error = None;
            let mut continue_scan = true;

            if let Some(key) = outer_entry.get_field(outer_key_idx) {
                if !key.is_null() {
                    self.data_source.visit_index_point_with_limit(
                        inner_table,
                        inner_index,
                        key,
                        None,
                        |inner_row| {
                            let inner_entry =
                                RelationEntry::from_row(Rc::clone(inner_row), inner_table);
                            let view = JoinedRowView::new(
                                Some(outer_entry),
                                Some(&inner_entry),
                                outer_width,
                                inner_col_count,
                            );
                            matched = true;
                            match self.emit_join_view(&view, &shared_tables, emit) {
                                Ok(next) => {
                                    continue_scan = next;
                                    next
                                }
                                Err(err) => {
                                    visit_error = Some(err);
                                    false
                                }
                            }
                        },
                    )?;
                }
            }

            if let Some(err) = visit_error {
                return Err(err);
            }
            if !continue_scan {
                return Ok(false);
            }

            if is_outer && !matched {
                let view =
                    JoinedRowView::new(Some(outer_entry), None, outer_width, inner_col_count);
                if !self.emit_join_view(&view, &shared_tables, emit)? {
                    return Ok(false);
                }
            }
        }

        Ok(true)
    }

    fn emit_cross_product_entries(
        &self,
        left: &Relation,
        right: &Relation,
        emit: &mut dyn FnMut(RelationEntry) -> ExecutionResult<bool>,
    ) -> ExecutionResult<bool> {
        let shared_tables = Self::combined_shared_tables(left, right);
        let left_width = Self::relation_row_width(left);
        let right_width = Self::relation_row_width(right);

        for left_entry in left.iter() {
            for right_entry in right.iter() {
                let view = JoinedRowView::new(
                    Some(left_entry),
                    Some(right_entry),
                    left_width,
                    right_width,
                );
                if !self.emit_join_view(&view, &shared_tables, emit)? {
                    return Ok(false);
                }
            }
        }

        Ok(true)
    }

    fn execute_hash_join(
        &self,
        left: Relation,
        right: Relation,
        condition: &Expr,
        join_type: crate::ast::JoinType,
    ) -> ExecutionResult<Relation> {
        let (left_key_idx, right_key_idx) = self.extract_join_keys(condition, &left, &right)?;
        let (tables, table_column_counts) = Self::combined_relation_metadata(&left, &right);
        let mut entries = Vec::new();
        self.emit_hash_join_entries(
            &left,
            &right,
            left_key_idx,
            right_key_idx,
            join_type,
            &mut |entry| {
                entries.push(entry);
                Ok(true)
            },
        )?;
        Ok(Relation::from_entries(entries, tables, table_column_counts))
    }

    fn execute_sort_merge_join(
        &self,
        left: Relation,
        right: Relation,
        condition: &Expr,
        join_type: crate::ast::JoinType,
    ) -> ExecutionResult<Relation> {
        let (left_key_idx, right_key_idx) = self.extract_join_keys(condition, &left, &right)?;
        let (tables, table_column_counts) = Self::combined_relation_metadata(&left, &right);
        let mut entries = Vec::new();
        self.emit_sort_merge_join_entries(
            left,
            right,
            left_key_idx,
            right_key_idx,
            join_type,
            &mut |entry| {
                entries.push(entry);
                Ok(true)
            },
        )?;
        Ok(Relation::from_entries(entries, tables, table_column_counts))
    }

    fn execute_nested_loop_join(
        &self,
        left: Relation,
        right: Relation,
        condition: &Expr,
        join_type: crate::ast::JoinType,
    ) -> ExecutionResult<Relation> {
        let (tables, table_column_counts) = Self::combined_relation_metadata(&left, &right);
        let mut entries = Vec::new();
        self.emit_nested_loop_join_entries(&left, &right, condition, join_type, &mut |entry| {
            entries.push(entry);
            Ok(true)
        })?;
        Ok(Relation::from_entries(entries, tables, table_column_counts))
    }

    fn execute_index_nested_loop_join(
        &self,
        outer: Relation,
        inner_table: &str,
        inner_index: &str,
        condition: &Expr,
        join_type: crate::ast::JoinType,
    ) -> ExecutionResult<Relation> {
        let inner_col_count = self.data_source.get_column_count(inner_table)?;
        let mut tables = outer.tables().to_vec();
        tables.push(inner_table.into());
        let mut table_column_counts = outer.table_column_counts().to_vec();
        table_column_counts.push(inner_col_count);

        let mut entries = Vec::new();
        self.emit_index_nested_loop_join_entries(
            &outer,
            inner_table,
            inner_index,
            condition,
            join_type,
            &mut |entry| {
                entries.push(entry);
                Ok(true)
            },
        )?;

        Ok(Relation::from_entries(entries, tables, table_column_counts))
    }

    fn execute_cross_product(&self, left: Relation, right: Relation) -> ExecutionResult<Relation> {
        let (tables, table_column_counts) = Self::combined_relation_metadata(&left, &right);
        let mut entries = Vec::new();
        self.emit_cross_product_entries(&left, &right, &mut |entry| {
            entries.push(entry);
            Ok(true)
        })?;
        Ok(Relation::from_entries(entries, tables, table_column_counts))
    }

    fn execute_union(
        &self,
        left: Relation,
        right: Relation,
        all: bool,
    ) -> ExecutionResult<Relation> {
        let left_width: usize = left.table_column_counts().iter().sum();
        let right_width: usize = right.table_column_counts().iter().sum();
        if left_width != right_width {
            return Err(ExecutionError::InvalidOperation(
                "UNION inputs must have the same column count".into(),
            ));
        }

        let tables = left.tables().to_vec();
        let table_column_counts = left.table_column_counts().to_vec();

        if all {
            let entries = left.into_iter().chain(right).collect();
            return Ok(Relation::from_entries(entries, tables, table_column_counts));
        }

        let mut seen = alloc::collections::BTreeSet::new();
        let mut entries = Vec::new();

        for entry in left.into_iter().chain(right) {
            let key = entry.row.values().to_vec();
            if seen.insert(key) {
                entries.push(entry);
            }
        }

        Ok(Relation::from_entries(entries, tables, table_column_counts))
    }

    // ========== Aggregate Operation ==========

    fn execute_hash_aggregate(
        &self,
        input: Relation,
        group_by: &[Expr],
        aggregates: &[(AggregateFunc, Expr)],
    ) -> ExecutionResult<Relation> {
        let tables = input.tables().to_vec();
        let table_column_counts = input.table_column_counts().to_vec();
        let ctx = EvalContext::new(&tables, &table_column_counts);

        // Convert Expr group_by to column indices
        let group_by_indices: Vec<usize> = group_by
            .iter()
            .filter_map(|expr| {
                if let Expr::Column(col) = expr {
                    Some(ctx.resolve_column_index(&col.table, col.index))
                } else {
                    None
                }
            })
            .collect();

        // Convert aggregates to (func, Option<column_index>)
        let agg_specs: Vec<(AggregateFunc, Option<usize>)> = aggregates
            .iter()
            .map(|(func, expr)| {
                let col_idx = match expr {
                    Expr::Column(col) => Some(ctx.resolve_column_index(&col.table, col.index)),
                    Expr::Aggregate {
                        expr: Some(inner), ..
                    } => {
                        if let Expr::Column(col) = inner.as_ref() {
                            Some(ctx.resolve_column_index(&col.table, col.index))
                        } else {
                            None
                        }
                    }
                    _ => None,
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

    // ========== TopN Operation ==========

    /// Executes TopN using a binary heap for O(n log k) performance.
    /// This is more efficient than Sort + Limit when k << n.
    fn execute_topn(
        &self,
        input: Relation,
        order_by: &[(Expr, SortOrder)],
        limit: usize,
        offset: usize,
    ) -> ExecutionResult<Relation> {
        use alloc::collections::BinaryHeap;

        let tables = input.tables().to_vec();
        let table_column_counts = input.table_column_counts().to_vec();
        let ctx = EvalContext::new(&tables, &table_column_counts);

        // Convert order_by to column indices
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

        let k = limit + offset;

        // For small k or small input, just use sort
        if k == 0 || input.len() <= k * 2 {
            let executor = SortExecutor::new(order_by_indices);
            let sorted = executor.execute(input);
            let limit_executor = LimitExecutor::new(limit, offset);
            return Ok(limit_executor.execute(sorted));
        }

        // Build heap with capacity k (using reference to avoid cloning order_by per entry)
        let mut heap: BinaryHeap<TopNHeapEntry> = BinaryHeap::with_capacity(k + 1);

        for entry in input.into_iter() {
            let heap_entry = TopNHeapEntry {
                entry,
                order_by: &order_by_indices,
            };

            if heap.len() < k {
                heap.push(heap_entry);
            } else if let Some(top) = heap.peek() {
                // Replace if new entry should come before the worst (top) in final result.
                // For ASC: heap keeps k smallest, top is largest of those. Replace if new < top.
                // For DESC: heap keeps k largest, top is smallest of those. Replace if new > top.
                // In our Ord impl: for ASC, larger values are Greater; for DESC, smaller are Greater.
                // So we replace when heap_entry < top (Less), meaning heap_entry is better.
                if heap_entry.cmp(top) == Ordering::Less {
                    heap.pop();
                    heap.push(heap_entry);
                }
            }
        }

        // Extract and sort the k elements
        let mut result: Vec<RelationEntry> = heap.into_iter().map(|e| e.entry).collect();

        // Sort the result (heap doesn't maintain full order)
        result.sort_by(|a, b| {
            for (idx, order) in &order_by_indices {
                let va = a.get_field(*idx);
                let vb = b.get_field(*idx);
                let cmp = match (va, vb) {
                    (Some(va), Some(vb)) => va.partial_cmp(vb).unwrap_or(Ordering::Equal),
                    (Some(_), None) => Ordering::Greater,
                    (None, Some(_)) => Ordering::Less,
                    (None, None) => Ordering::Equal,
                };
                if cmp != Ordering::Equal {
                    return match order {
                        SortOrder::Asc => cmp,
                        SortOrder::Desc => cmp.reverse(),
                    };
                }
            }
            Ordering::Equal
        });

        // Apply offset and limit
        let final_result: Vec<RelationEntry> =
            result.into_iter().skip(offset).take(limit).collect();

        Ok(Relation::from_entries(
            final_result,
            tables,
            table_column_counts,
        ))
    }

    // ========== Expression Evaluation ==========

    fn eval_row_expr(&self, expr: &Expr, row: &Row) -> Value {
        self.eval_accessor_expr(expr, row, None)
    }

    fn eval_accessor_expr<A: RowAccessor>(
        &self,
        expr: &Expr,
        accessor: &A,
        ctx: Option<&EvalContext<'_>>,
    ) -> Value {
        match expr {
            Expr::Column(col) => {
                let index = if let Some(c) = ctx {
                    c.resolve_column_index(&col.table, col.index)
                } else {
                    col.index
                };
                accessor.get_value(index).cloned().unwrap_or(Value::Null)
            }

            Expr::Literal(value) => value.clone(),

            Expr::BinaryOp { left, op, right } => {
                let left_val = self.eval_accessor_expr(left, accessor, ctx);
                let right_val = self.eval_accessor_expr(right, accessor, ctx);
                self.eval_binary_op(*op, &left_val, &right_val)
            }

            Expr::UnaryOp { op, expr } => {
                let val = self.eval_accessor_expr(expr, accessor, ctx);
                self.eval_unary_op(*op, &val)
            }

            Expr::Aggregate { expr, .. } => {
                if let Some(e) = expr {
                    self.eval_accessor_expr(e, accessor, ctx)
                } else {
                    Value::Int64(1) // COUNT(*)
                }
            }

            Expr::Between { expr, low, high } => {
                let val = self.eval_accessor_expr(expr, accessor, ctx);
                let low_val = self.eval_accessor_expr(low, accessor, ctx);
                let high_val = self.eval_accessor_expr(high, accessor, ctx);
                Value::Boolean(val >= low_val && val <= high_val)
            }

            Expr::NotBetween { expr, low, high } => {
                let val = self.eval_accessor_expr(expr, accessor, ctx);
                let low_val = self.eval_accessor_expr(low, accessor, ctx);
                let high_val = self.eval_accessor_expr(high, accessor, ctx);
                Value::Boolean(val < low_val || val > high_val)
            }

            Expr::In { expr, list } => {
                let val = self.eval_accessor_expr(expr, accessor, ctx);
                let in_list = list
                    .iter()
                    .any(|item| self.eval_accessor_expr(item, accessor, ctx) == val);
                Value::Boolean(in_list)
            }

            Expr::NotIn { expr, list } => {
                let val = self.eval_accessor_expr(expr, accessor, ctx);
                let in_list = list
                    .iter()
                    .any(|item| self.eval_accessor_expr(item, accessor, ctx) == val);
                Value::Boolean(!in_list)
            }

            Expr::Like { expr, pattern } => {
                let val = self.eval_accessor_expr(expr, accessor, ctx);
                if let Value::String(s) = val {
                    Value::Boolean(self.match_like_pattern(&s, pattern))
                } else {
                    Value::Boolean(false)
                }
            }

            Expr::NotLike { expr, pattern } => {
                let val = self.eval_accessor_expr(expr, accessor, ctx);
                if let Value::String(s) = val {
                    Value::Boolean(!self.match_like_pattern(&s, pattern))
                } else {
                    Value::Boolean(true)
                }
            }

            Expr::Match { expr, pattern } => {
                let val = self.eval_accessor_expr(expr, accessor, ctx);
                if let Value::String(s) = val {
                    Value::Boolean(self.match_regex_pattern(&s, pattern))
                } else {
                    Value::Boolean(false)
                }
            }

            Expr::NotMatch { expr, pattern } => {
                let val = self.eval_accessor_expr(expr, accessor, ctx);
                if let Value::String(s) = val {
                    Value::Boolean(!self.match_regex_pattern(&s, pattern))
                } else {
                    Value::Boolean(true)
                }
            }

            Expr::Function { name, args } => {
                let arg_values: Vec<Value> = args
                    .iter()
                    .map(|a| self.eval_accessor_expr(a, accessor, ctx))
                    .collect();
                self.eval_function(name, &arg_values)
            }
        }
    }

    /// Evaluates an expression against a relation entry.
    /// If `ctx` is provided, column indices are dynamically computed based on table metadata.
    /// This is needed for JOIN queries where the optimizer may have reordered tables.
    fn eval_expr_ctx(
        &self,
        expr: &Expr,
        entry: &RelationEntry,
        ctx: Option<&EvalContext<'_>>,
    ) -> Value {
        self.eval_accessor_expr(expr, entry, ctx)
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
    fn eval_predicate_ctx(
        &self,
        expr: &Expr,
        entry: &RelationEntry,
        ctx: &EvalContext<'_>,
    ) -> bool {
        self.eval_predicate_accessor_ctx(expr, entry, Some(ctx))
    }

    #[inline]
    fn eval_predicate_accessor_ctx<A: RowAccessor>(
        &self,
        expr: &Expr,
        accessor: &A,
        ctx: Option<&EvalContext<'_>>,
    ) -> bool {
        match self.eval_accessor_expr(expr, accessor, ctx) {
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
                    _ => {
                        self.eval_arithmetic(left, right, |a, b| if b != 0.0 { a / b } else { 0.0 })
                    }
                }
            }
            BinaryOp::Mod => match (left, right) {
                (Value::Int64(a), Value::Int64(b)) if *b != 0 => Value::Int64(a % b),
                (Value::Int32(a), Value::Int32(b)) if *b != 0 => Value::Int32(a % b),
                _ => Value::Null,
            },
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
            // JSONB contains: jsonb_contains(jsonb_value, path, expected_value)
            "JSONB_CONTAINS" => {
                if args.len() >= 3 {
                    if let (Value::Jsonb(jsonb), Value::String(path)) = (&args[0], &args[1]) {
                        return self.jsonb_contains(jsonb, path, &args[2]);
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
            return alternatives
                .iter()
                .any(|alt| self.regex_match_ops(&chars, alt, 0, 0));
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

    fn match_like_recursive(&self, value: &[char], pattern: &[char], vi: usize, pi: usize) -> bool {
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

    /// Evaluates a JSONB contains expression using the extracted path value.
    fn jsonb_contains(
        &self,
        jsonb: &cynos_core::JsonbValue,
        path: &str,
        expected: &Value,
    ) -> Value {
        let json_value = match self.parse_json_bytes(&jsonb.0) {
            Some(v) => v,
            None => return Value::Boolean(false),
        };

        let json_path = match JsonPath::parse(path) {
            Ok(p) => p,
            Err(_) => return Value::Boolean(false),
        };

        let results = json_value.query(&json_path);
        if results.is_empty() {
            return Value::Boolean(false);
        }

        let actual = results[0];
        let contains = match expected {
            Value::String(expected_str) => self
                .jsonb_value_to_string(actual)
                .contains(expected_str.as_str()),
            _ => self.compare_jsonb_value(actual, expected),
        };

        Value::Boolean(contains)
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

    fn jsonb_value_to_string(&self, value: &JsonbValue) -> String {
        value.stringify_for_contains()
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
        Self::extract_column_ref_static(expr)
    }

    fn extract_column_ref_static<'b>(expr: &'b Expr) -> ExecutionResult<&'b ColumnRef> {
        if let Expr::Column(col) = expr {
            Ok(col)
        } else {
            Err(ExecutionError::InvalidOperation(
                "Expected column reference".into(),
            ))
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

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum InMemoryIndexKey {
    Scalar(Value),
    Composite(Vec<Value>),
}

/// Data for a single index.
struct IndexData {
    /// Maps key values to row indices.
    key_to_rows: BTreeMap<InMemoryIndexKey, Vec<usize>>,
}

impl InMemoryIndexKey {
    fn scalar(value: Value) -> Self {
        Self::Scalar(value)
    }

    fn composite(values: Vec<Value>) -> Self {
        if values.len() == 1 {
            Self::Scalar(values.into_iter().next().unwrap_or(Value::Null))
        } else {
            Self::Composite(values)
        }
    }
}

fn in_memory_scalar_range(
    range_start: Option<&Value>,
    range_end: Option<&Value>,
    include_start: bool,
    include_end: bool,
) -> Option<KeyRange<InMemoryIndexKey>> {
    match (range_start, range_end) {
        (Some(start), Some(end)) => Some(KeyRange::bound(
            InMemoryIndexKey::scalar(start.clone()),
            InMemoryIndexKey::scalar(end.clone()),
            !include_start,
            !include_end,
        )),
        (Some(start), None) => Some(KeyRange::lower_bound(
            InMemoryIndexKey::scalar(start.clone()),
            !include_start,
        )),
        (None, Some(end)) => Some(KeyRange::upper_bound(
            InMemoryIndexKey::scalar(end.clone()),
            !include_end,
        )),
        (None, None) => None,
    }
}

fn in_memory_composite_range(
    range: Option<&KeyRange<Vec<Value>>>,
) -> Option<KeyRange<InMemoryIndexKey>> {
    range.cloned().map(|range| match range {
        KeyRange::All => KeyRange::All,
        KeyRange::Only(values) => KeyRange::Only(InMemoryIndexKey::composite(values)),
        KeyRange::LowerBound { value, exclusive } => KeyRange::LowerBound {
            value: InMemoryIndexKey::composite(value),
            exclusive,
        },
        KeyRange::UpperBound { value, exclusive } => KeyRange::UpperBound {
            value: InMemoryIndexKey::composite(value),
            exclusive,
        },
        KeyRange::Bound {
            lower,
            upper,
            lower_exclusive,
            upper_exclusive,
        } => KeyRange::Bound {
            lower: InMemoryIndexKey::composite(lower),
            upper: InMemoryIndexKey::composite(upper),
            lower_exclusive,
            upper_exclusive,
        },
    })
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

        let mut key_to_rows: BTreeMap<InMemoryIndexKey, Vec<usize>> = BTreeMap::new();

        for (row_idx, row) in table_data.rows.iter().enumerate() {
            if let Some(key) = row.get(column_index) {
                key_to_rows
                    .entry(InMemoryIndexKey::scalar(key.clone()))
                    .or_default()
                    .push(row_idx);
            }
        }

        table_data
            .indexes
            .insert(index_name.into(), IndexData { key_to_rows });

        Ok(())
    }

    /// Creates a composite index on multiple table columns.
    pub fn create_composite_index(
        &mut self,
        table: &str,
        index_name: impl Into<String>,
        column_indices: &[usize],
    ) -> ExecutionResult<()> {
        let table_data = self
            .tables
            .get_mut(table)
            .ok_or_else(|| ExecutionError::TableNotFound(table.into()))?;

        let mut key_to_rows: BTreeMap<InMemoryIndexKey, Vec<usize>> = BTreeMap::new();

        for (row_idx, row) in table_data.rows.iter().enumerate() {
            let key = column_indices
                .iter()
                .map(|&idx| row.get(idx).cloned().unwrap_or(Value::Null))
                .collect();
            key_to_rows
                .entry(InMemoryIndexKey::composite(key))
                .or_default()
                .push(row_idx);
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

    fn visit_table_rows<F>(&self, table: &str, mut visitor: F) -> ExecutionResult<()>
    where
        F: FnMut(&Rc<Row>) -> bool,
    {
        let table_data = self
            .tables
            .get(table)
            .ok_or_else(|| ExecutionError::TableNotFound(table.into()))?;

        for row in &table_data.rows {
            if !visitor(row) {
                break;
            }
        }
        Ok(())
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

        let index_data =
            table_data
                .indexes
                .get(index)
                .ok_or_else(|| ExecutionError::IndexNotFound {
                    table: table.into(),
                    index: index.into(),
                })?;

        let mut result = Vec::new();
        let range = in_memory_scalar_range(range_start, range_end, include_start, include_end);

        for (key, row_indices) in &index_data.key_to_rows {
            if range
                .as_ref()
                .map(|range| range.contains(key))
                .unwrap_or(true)
            {
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

        let index_data =
            table_data
                .indexes
                .get(index)
                .ok_or_else(|| ExecutionError::IndexNotFound {
                    table: table.into(),
                    index: index.into(),
                })?;

        // Collect keys in range first
        let range = in_memory_scalar_range(range_start, range_end, include_start, include_end);
        let keys_in_range: Vec<&InMemoryIndexKey> = index_data
            .key_to_rows
            .keys()
            .filter(|key| {
                range
                    .as_ref()
                    .map(|range| range.contains(key))
                    .unwrap_or(true)
            })
            .collect();

        let mut result = Vec::new();
        let mut skipped = 0;
        let mut collected = 0;

        // Iterate in forward or reverse order based on the reverse flag
        let iter: Box<dyn Iterator<Item = &&InMemoryIndexKey>> = if reverse {
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

    fn get_index_point(
        &self,
        table: &str,
        index: &str,
        key: &Value,
    ) -> ExecutionResult<Vec<Rc<Row>>> {
        let table_data = self
            .tables
            .get(table)
            .ok_or_else(|| ExecutionError::TableNotFound(table.into()))?;

        let index_data =
            table_data
                .indexes
                .get(index)
                .ok_or_else(|| ExecutionError::IndexNotFound {
                    table: table.into(),
                    index: index.into(),
                })?;

        let result = index_data
            .key_to_rows
            .get(&InMemoryIndexKey::scalar(key.clone()))
            .map(|indices| {
                indices
                    .iter()
                    .map(|&i| Rc::clone(&table_data.rows[i]))
                    .collect()
            })
            .unwrap_or_default();

        Ok(result)
    }

    fn get_index_range_composite_with_limit(
        &self,
        table: &str,
        index: &str,
        range: Option<&KeyRange<Vec<Value>>>,
        limit: Option<usize>,
        offset: usize,
        reverse: bool,
    ) -> ExecutionResult<Vec<Rc<Row>>> {
        let table_data = self
            .tables
            .get(table)
            .ok_or_else(|| ExecutionError::TableNotFound(table.into()))?;

        let index_data =
            table_data
                .indexes
                .get(index)
                .ok_or_else(|| ExecutionError::IndexNotFound {
                    table: table.into(),
                    index: index.into(),
                })?;

        let range = in_memory_composite_range(range);
        let keys_in_range: Vec<&InMemoryIndexKey> = index_data
            .key_to_rows
            .keys()
            .filter(|key| {
                range
                    .as_ref()
                    .map(|range| range.contains(key))
                    .unwrap_or(true)
            })
            .collect();

        let mut result = Vec::new();
        let mut skipped = 0;
        let mut collected = 0;

        let iter: Box<dyn Iterator<Item = &&InMemoryIndexKey>> = if reverse {
            Box::new(keys_in_range.iter().rev())
        } else {
            Box::new(keys_in_range.iter())
        };

        for key in iter {
            if let Some(row_indices) = index_data.key_to_rows.get(*key) {
                for &idx in row_indices {
                    if skipped < offset {
                        skipped += 1;
                        continue;
                    }
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

    fn get_column_count(&self, table: &str) -> ExecutionResult<usize> {
        self.tables
            .get(table)
            .map(|t| t.column_count)
            .ok_or_else(|| ExecutionError::TableNotFound(table.into()))
    }

    fn get_table_row_count(&self, table: &str) -> ExecutionResult<usize> {
        self.tables
            .get(table)
            .map(|t| t.rows.len())
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
            Row::new(
                1,
                vec![
                    Value::Int64(1),
                    Value::String("Alice".into()),
                    Value::Int64(10),
                ],
            ),
            Row::new(
                2,
                vec![
                    Value::Int64(2),
                    Value::String("Bob".into()),
                    Value::Int64(20),
                ],
            ),
            Row::new(
                3,
                vec![
                    Value::Int64(3),
                    Value::String("Charlie".into()),
                    Value::Int64(10),
                ],
            ),
        ];
        ds.add_table("users", users, 3);
        ds.create_index("users", "idx_id", 0).unwrap();
        ds.create_index("users", "idx_dept", 2).unwrap();

        // Departments table: id, name
        let depts = vec![
            Row::new(
                10,
                vec![Value::Int64(10), Value::String("Engineering".into())],
            ),
            Row::new(20, vec![Value::Int64(20), Value::String("Sales".into())]),
            Row::new(
                30,
                vec![Value::Int64(30), Value::String("Marketing".into())],
            ),
        ];
        ds.add_table("departments", depts, 2);
        ds.create_index("departments", "idx_id", 0).unwrap();

        ds
    }

    fn relation_snapshot(relation: Relation) -> Vec<(u64, u64, Vec<Value>)> {
        relation
            .entries
            .into_iter()
            .map(|entry| {
                (
                    entry.row.id(),
                    entry.row.version(),
                    entry.row.values().to_vec(),
                )
            })
            .collect()
    }

    fn assert_execution_artifact_matches(plan: &PhysicalPlan) {
        let ds = create_test_data_source();
        let runner = PhysicalPlanRunner::new(&ds);
        let expected = relation_snapshot(runner.execute(plan).unwrap());
        let artifact = PhysicalPlanRunner::<InMemoryDataSource>::compile_execution_artifact(plan);
        let actual = relation_snapshot(runner.execute_with_artifact(plan, &artifact).unwrap());
        assert_eq!(actual, expected);
    }

    fn assert_full_execution_artifact_matches(plan: &PhysicalPlan) {
        let ds = create_test_data_source();
        let runner = PhysicalPlanRunner::new(&ds);
        let expected = relation_snapshot(runner.execute(plan).unwrap());
        let artifact = runner.compile_execution_artifact_with_data_source(plan);
        let actual = relation_snapshot(runner.execute_with_artifact(plan, &artifact).unwrap());
        assert_eq!(actual, expected);
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
            bounds: IndexBounds::Scalar(KeyRange::bound(
                Value::Int64(1),
                Value::Int64(2),
                false,
                false,
            )),
            limit: None,
            offset: None,
            reverse: false,
        };
        let result = runner.execute(&plan).unwrap();

        assert_eq!(result.len(), 2);
    }

    #[test]
    fn test_composite_index_scan_with_tuple_bounds() {
        let mut ds = InMemoryDataSource::new();
        ds.add_table(
            "scores",
            vec![
                Row::new(1, vec![Value::String("apac".into()), Value::Int64(5)]),
                Row::new(2, vec![Value::String("apac".into()), Value::Int64(10)]),
                Row::new(3, vec![Value::String("apac".into()), Value::Int64(20)]),
                Row::new(4, vec![Value::String("emea".into()), Value::Int64(10)]),
            ],
            2,
        );
        ds.create_composite_index("scores", "idx_region_score", &[0, 1])
            .unwrap();

        let runner = PhysicalPlanRunner::new(&ds);
        let plan = PhysicalPlan::IndexScan {
            table: "scores".into(),
            index: "idx_region_score".into(),
            bounds: IndexBounds::Composite(KeyRange::bound(
                alloc::vec![Value::String("apac".into()), Value::Int64(10)],
                alloc::vec![Value::String("apac".into()), Value::Int64(20)],
                false,
                false,
            )),
            limit: None,
            offset: None,
            reverse: false,
        };

        let result = runner.execute(&plan).unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(
            result.entries[0].get_field(0),
            Some(&Value::String("apac".into()))
        );
        assert_eq!(result.entries[0].get_field(1), Some(&Value::Int64(10)));
        assert_eq!(result.entries[1].get_field(1), Some(&Value::Int64(20)));
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
    fn test_filter_compiled_compound_predicate() {
        let ds = create_test_data_source();
        let runner = PhysicalPlanRunner::new(&ds);

        let plan = PhysicalPlan::filter(
            PhysicalPlan::table_scan("users"),
            Expr::and(
                Expr::gte(
                    Expr::column("users", "id", 0),
                    Expr::literal(Value::Int64(2)),
                ),
                Expr::or(
                    Expr::in_list(
                        Expr::column("users", "dept_id", 2),
                        alloc::vec![Value::Int64(10)],
                    ),
                    Expr::not(Expr::eq(
                        Expr::column("users", "name", 1),
                        Expr::literal(Value::String("Bob".into())),
                    )),
                ),
            ),
        );
        let result = runner.execute(&plan).unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(
            result.entries[0].get_field(1),
            Some(&Value::String("Charlie".into()))
        );
    }

    #[test]
    fn test_execution_artifact_matches_simple_filter() {
        let plan = PhysicalPlan::filter(
            PhysicalPlan::table_scan("users"),
            Expr::eq(
                Expr::column("users", "dept_id", 2),
                Expr::literal(Value::Int64(10)),
            ),
        );

        assert_execution_artifact_matches(&plan);
    }

    #[test]
    fn test_execution_artifact_matches_compound_filter() {
        let plan = PhysicalPlan::filter(
            PhysicalPlan::table_scan("users"),
            Expr::and(
                Expr::gte(
                    Expr::column("users", "id", 0),
                    Expr::literal(Value::Int64(2)),
                ),
                Expr::or(
                    Expr::in_list(
                        Expr::column("users", "dept_id", 2),
                        alloc::vec![Value::Int64(10)],
                    ),
                    Expr::not(Expr::eq(
                        Expr::column("users", "name", 1),
                        Expr::literal(Value::String("Bob".into())),
                    )),
                ),
            ),
        );

        assert_execution_artifact_matches(&plan);
    }

    #[test]
    fn test_compile_row_predicate_uses_typed_between_kernel() {
        let predicate = Expr::Between {
            expr: Box::new(Expr::column("users", "id", 0)),
            low: Box::new(Expr::literal(Value::Int32(1))),
            high: Box::new(Expr::literal(Value::Float64(3.0))),
        };

        let compiled = PhysicalPlanRunner::<InMemoryDataSource>::compile_row_predicate(&predicate);
        match compiled {
            CompiledRowPredicate::Between {
                kernel:
                    BetweenPredicateKernel::Numeric {
                        low: NumericLiteral::Int32(1),
                        high: NumericLiteral::Float64(high),
                    },
                negated: false,
                ..
            } => assert_eq!(high, 3.0),
            other => panic!("expected numeric BETWEEN kernel, got {other:?}"),
        }
    }

    #[test]
    fn test_compile_row_predicate_uses_typed_in_list_kernel() {
        let predicate = Expr::In {
            expr: Box::new(Expr::column("users", "name", 1)),
            list: vec![
                Expr::literal(Value::String("Alice".into())),
                Expr::literal(Value::String("Bob".into())),
                Expr::literal(Value::Null),
            ],
        };

        let compiled = PhysicalPlanRunner::<InMemoryDataSource>::compile_row_predicate(&predicate);
        match compiled {
            CompiledRowPredicate::InList {
                kernel:
                    InListPredicateKernel::String {
                        literals,
                        contains_null,
                    },
                negated: false,
                ..
            } => {
                assert_eq!(literals, vec![String::from("Alice"), String::from("Bob")]);
                assert!(contains_null);
            }
            other => panic!("expected string IN-list kernel, got {other:?}"),
        }
    }

    #[test]
    fn test_execution_artifact_matches_typed_kernels_with_nulls() {
        let mut ds = InMemoryDataSource::new();
        ds.add_table(
            "data",
            vec![
                Row::new(
                    1,
                    vec![Value::Int64(5), Value::String("Engineering".into())],
                ),
                Row::new(2, vec![Value::Int64(10), Value::Null]),
                Row::new(3, vec![Value::Null, Value::String("Sales".into())]),
                Row::new(4, vec![Value::Int64(15), Value::String("HR".into())]),
            ],
            2,
        );

        let runner = PhysicalPlanRunner::new(&ds);
        let plans = vec![
            PhysicalPlan::filter(
                PhysicalPlan::table_scan("data"),
                Expr::Between {
                    expr: Box::new(Expr::column("data", "value", 0)),
                    low: Box::new(Expr::literal(Value::Int32(5))),
                    high: Box::new(Expr::literal(Value::Float64(10.0))),
                },
            ),
            PhysicalPlan::filter(
                PhysicalPlan::table_scan("data"),
                Expr::In {
                    expr: Box::new(Expr::column("data", "tag", 1)),
                    list: vec![
                        Expr::literal(Value::String("Engineering".into())),
                        Expr::literal(Value::Null),
                    ],
                },
            ),
        ];

        for plan in plans {
            let expected = relation_snapshot(runner.execute(&plan).unwrap());

            let compiled_artifact =
                PhysicalPlanRunner::<InMemoryDataSource>::compile_execution_artifact(&plan);
            let compiled_actual = relation_snapshot(
                runner
                    .execute_with_artifact(&plan, &compiled_artifact)
                    .unwrap(),
            );
            assert_eq!(compiled_actual, expected);

            let full_artifact = runner.compile_execution_artifact_with_data_source(&plan);
            let full_actual =
                relation_snapshot(runner.execute_with_artifact(&plan, &full_artifact).unwrap());
            assert_eq!(full_actual, expected);
        }
    }

    #[test]
    fn test_filter_compiled_predicate_preserves_null_semantics_under_not() {
        let mut ds = InMemoryDataSource::new();
        ds.add_table(
            "data",
            vec![
                Row::new(1, vec![Value::Int64(1)]),
                Row::new(2, vec![Value::Int64(2)]),
                Row::new(3, vec![Value::Null]),
            ],
            1,
        );

        let runner = PhysicalPlanRunner::new(&ds);
        let plan = PhysicalPlan::filter(
            PhysicalPlan::table_scan("data"),
            Expr::not(Expr::eq(
                Expr::column("data", "col", 0),
                Expr::literal(Value::Int64(1)),
            )),
        );
        let result = runner.execute(&plan).unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result.entries[0].get_field(0), Some(&Value::Int64(2)));
    }

    #[test]
    fn test_simple_filter_preserves_null_semantics() {
        let mut ds = InMemoryDataSource::new();
        ds.add_table(
            "data",
            vec![
                Row::new(1, vec![Value::Int64(1)]),
                Row::new(2, vec![Value::Int64(2)]),
                Row::new(3, vec![Value::Null]),
            ],
            1,
        );

        let runner = PhysicalPlanRunner::new(&ds);
        let plan = PhysicalPlan::filter(
            PhysicalPlan::table_scan("data"),
            Expr::ne(
                Expr::column("data", "col", 0),
                Expr::literal(Value::Int64(1)),
            ),
        );

        let result = runner.execute(&plan).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result.entries[0].get_field(0), Some(&Value::Int64(2)));

        let artifact = runner.compile_execution_artifact_with_data_source(&plan);
        let compiled = runner.execute_with_artifact(&plan, &artifact).unwrap();
        assert_eq!(compiled.len(), 1);
        assert_eq!(compiled.entries[0].get_field(0), Some(&Value::Int64(2)));
    }

    #[test]
    fn test_filter_literal_on_left() {
        let ds = create_test_data_source();
        let runner = PhysicalPlanRunner::new(&ds);

        let plan = PhysicalPlan::filter(
            PhysicalPlan::table_scan("users"),
            Expr::lt(
                Expr::literal(Value::Int64(10)),
                Expr::column("users", "dept_id", 2),
            ),
        );
        let result = runner.execute(&plan).unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(
            result.entries[0].get_field(1),
            Some(&Value::String("Bob".into()))
        );
    }

    #[test]
    fn test_filter_cross_type_numeric_literal() {
        let ds = create_test_data_source();
        let runner = PhysicalPlanRunner::new(&ds);

        let plan = PhysicalPlan::filter(
            PhysicalPlan::table_scan("users"),
            Expr::gt(
                Expr::column("users", "id", 0),
                Expr::literal(Value::Float64(1.5)),
            ),
        );
        let result = runner.execute(&plan).unwrap();

        assert_eq!(result.len(), 2);
        assert_eq!(
            result.entries[0].get_field(1),
            Some(&Value::String("Bob".into()))
        );
        assert_eq!(
            result.entries[1].get_field(1),
            Some(&Value::String("Charlie".into()))
        );
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
    fn test_project_preserves_row_version() {
        let mut ds = InMemoryDataSource::new();
        ds.add_table(
            "users",
            vec![Row::new_with_version(
                1,
                7,
                vec![
                    Value::Int64(1),
                    Value::String("Alice".into()),
                    Value::Int64(10),
                ],
            )],
            3,
        );

        let runner = PhysicalPlanRunner::new(&ds);
        let plan = PhysicalPlan::project(
            PhysicalPlan::table_scan("users"),
            vec![Expr::column("users", "name", 1)],
        );
        let result = runner.execute(&plan).unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result.entries[0].row.version(), 7);
        assert_eq!(result.entries[0].row.id(), 1);
        assert_eq!(
            result.entries[0].get_field(0),
            Some(&Value::String("Alice".into()))
        );
    }

    #[test]
    fn test_single_table_pipeline_filter_project_limit() {
        let ds = create_test_data_source();
        let runner = PhysicalPlanRunner::new(&ds);

        let plan = PhysicalPlan::limit(
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
            1,
            1,
        );
        let result = runner.execute(&plan).unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(
            result.entries[0].get_field(0),
            Some(&Value::String("Charlie".into()))
        );
    }

    #[test]
    fn test_single_table_pipeline_expression_projection() {
        let ds = create_test_data_source();
        let runner = PhysicalPlanRunner::new(&ds);

        let plan = PhysicalPlan::project(
            PhysicalPlan::filter(
                PhysicalPlan::table_scan("users"),
                Expr::eq(
                    Expr::column("users", "dept_id", 2),
                    Expr::literal(Value::Int64(10)),
                ),
            ),
            vec![
                Expr::column("users", "name", 1),
                Expr::BinaryOp {
                    left: Box::new(Expr::column("users", "id", 0)),
                    op: BinaryOp::Mul,
                    right: Box::new(Expr::literal(Value::Int64(10))),
                },
            ],
        );
        let result = runner.execute(&plan).unwrap();

        assert_eq!(result.len(), 2);
        assert_eq!(
            result.entries[0].get_field(0),
            Some(&Value::String("Alice".into()))
        );
        assert_eq!(result.entries[0].get_field(1), Some(&Value::Int64(10)));
        assert_eq!(
            result.entries[1].get_field(0),
            Some(&Value::String("Charlie".into()))
        );
        assert_eq!(result.entries[1].get_field(1), Some(&Value::Int64(30)));
    }

    #[test]
    fn test_execution_artifact_matches_single_table_pipeline() {
        let plan = PhysicalPlan::limit(
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
            1,
            1,
        );

        assert_execution_artifact_matches(&plan);
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
    fn test_full_execution_artifact_matches_hash_join() {
        let plan = PhysicalPlan::hash_join(
            PhysicalPlan::table_scan("users"),
            PhysicalPlan::table_scan("departments"),
            Expr::eq(
                Expr::column("users", "dept_id", 2),
                Expr::column("departments", "id", 0),
            ),
            JoinType::Inner,
        );

        assert_full_execution_artifact_matches(&plan);
    }

    #[test]
    fn test_full_execution_artifact_matches_hash_join_project_limit() {
        let plan = PhysicalPlan::limit(
            PhysicalPlan::project(
                PhysicalPlan::hash_join(
                    PhysicalPlan::table_scan("users"),
                    PhysicalPlan::table_scan("departments"),
                    Expr::eq(
                        Expr::column("users", "dept_id", 2),
                        Expr::column("departments", "id", 0),
                    ),
                    JoinType::Inner,
                ),
                vec![
                    Expr::column("users", "name", 1),
                    Expr::column("departments", "name", 1),
                ],
            ),
            2,
            0,
        );

        assert_full_execution_artifact_matches(&plan);
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
    fn test_full_execution_artifact_matches_nested_loop_join_with_predicate() {
        let plan = PhysicalPlan::nested_loop_join(
            PhysicalPlan::table_scan("left"),
            PhysicalPlan::table_scan("right"),
            Expr::gt(
                Expr::column("left", "value", 0),
                Expr::column("right", "value", 0),
            ),
            JoinType::Inner,
        );

        let mut ds = InMemoryDataSource::new();
        ds.add_table(
            "left",
            vec![
                Row::new(1, vec![Value::Int64(10)]),
                Row::new(2, vec![Value::Int64(20)]),
            ],
            1,
        );
        ds.add_table(
            "right",
            vec![
                Row::new(10, vec![Value::Int64(5)]),
                Row::new(11, vec![Value::Int64(15)]),
                Row::new(12, vec![Value::Int64(25)]),
            ],
            1,
        );

        let runner = PhysicalPlanRunner::new(&ds);
        let expected = relation_snapshot(runner.execute(&plan).unwrap());
        let artifact = runner.compile_execution_artifact_with_data_source(&plan);
        let actual = relation_snapshot(runner.execute_with_artifact(&plan, &artifact).unwrap());
        assert_eq!(actual, expected);
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
    fn test_union_distinct() {
        let ds = create_test_data_source();
        let runner = PhysicalPlanRunner::new(&ds);

        let left = PhysicalPlan::project(
            PhysicalPlan::filter(
                PhysicalPlan::table_scan("users"),
                Expr::eq(
                    Expr::column("users", "dept_id", 2),
                    Expr::literal(Value::Int64(10)),
                ),
            ),
            vec![Expr::column("users", "name", 1)],
        );
        let right = PhysicalPlan::project(
            PhysicalPlan::filter(
                PhysicalPlan::table_scan("users"),
                Expr::In {
                    expr: Box::new(Expr::column("users", "name", 1)),
                    list: vec![
                        Expr::literal(Value::String("Alice".into())),
                        Expr::literal(Value::String("Bob".into())),
                    ],
                },
            ),
            vec![Expr::column("users", "name", 1)],
        );

        let result = runner
            .execute(&PhysicalPlan::union(left, right, false))
            .unwrap();
        let names: Vec<String> = result
            .entries
            .iter()
            .map(|entry| match entry.get_field(0) {
                Some(Value::String(name)) => name.clone(),
                other => panic!("expected name string, got {:?}", other),
            })
            .collect();

        assert_eq!(result.len(), 3);
        assert!(names.contains(&String::from("Alice")));
        assert!(names.contains(&String::from("Bob")));
        assert!(names.contains(&String::from("Charlie")));
    }

    #[test]
    fn test_full_execution_artifact_matches_union_distinct() {
        let left = PhysicalPlan::project(
            PhysicalPlan::filter(
                PhysicalPlan::table_scan("users"),
                Expr::eq(
                    Expr::column("users", "dept_id", 2),
                    Expr::literal(Value::Int64(10)),
                ),
            ),
            vec![Expr::column("users", "name", 1)],
        );
        let right = PhysicalPlan::project(
            PhysicalPlan::filter(
                PhysicalPlan::table_scan("users"),
                Expr::In {
                    expr: Box::new(Expr::column("users", "name", 1)),
                    list: vec![
                        Expr::literal(Value::String("Alice".into())),
                        Expr::literal(Value::String("Bob".into())),
                    ],
                },
            ),
            vec![Expr::column("users", "name", 1)],
        );

        assert_full_execution_artifact_matches(&PhysicalPlan::union(left, right, false));
    }

    #[test]
    fn test_full_execution_artifact_matches_sort_then_limit() {
        let plan = PhysicalPlan::limit(
            PhysicalPlan::sort(
                PhysicalPlan::table_scan("users"),
                vec![(Expr::column("users", "id", 0), SortOrder::Desc)],
            ),
            2,
            0,
        );

        assert_full_execution_artifact_matches(&plan);
    }

    #[test]
    fn test_union_all_preserves_duplicates() {
        let ds = create_test_data_source();
        let runner = PhysicalPlanRunner::new(&ds);

        let left = PhysicalPlan::project(
            PhysicalPlan::filter(
                PhysicalPlan::table_scan("users"),
                Expr::eq(
                    Expr::column("users", "dept_id", 2),
                    Expr::literal(Value::Int64(10)),
                ),
            ),
            vec![Expr::column("users", "name", 1)],
        );
        let right = PhysicalPlan::project(
            PhysicalPlan::filter(
                PhysicalPlan::table_scan("users"),
                Expr::In {
                    expr: Box::new(Expr::column("users", "name", 1)),
                    list: vec![
                        Expr::literal(Value::String("Alice".into())),
                        Expr::literal(Value::String("Bob".into())),
                    ],
                },
            ),
            vec![Expr::column("users", "name", 1)],
        );

        let result = runner
            .execute(&PhysicalPlan::union(left, right, true))
            .unwrap();
        assert_eq!(result.len(), 4);
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
    fn test_full_execution_artifact_matches_hash_aggregate() {
        let plan = PhysicalPlan::hash_aggregate(
            PhysicalPlan::table_scan("users"),
            vec![Expr::column("users", "dept_id", 2)],
            vec![
                (AggregateFunc::Count, Expr::column("users", "id", 0)),
                (AggregateFunc::Sum, Expr::column("users", "id", 0)),
                (AggregateFunc::Avg, Expr::column("users", "id", 0)),
            ],
        );

        assert_full_execution_artifact_matches(&plan);
    }

    #[test]
    fn test_aggregate_after_join_group_by_right_table_column() {
        let ds = create_test_data_source();
        let runner = PhysicalPlanRunner::new(&ds);

        let join_plan = PhysicalPlan::hash_join(
            PhysicalPlan::table_scan("users"),
            PhysicalPlan::table_scan("departments"),
            Expr::eq(
                Expr::column("users", "dept_id", 2),
                Expr::column("departments", "id", 0),
            ),
            JoinType::Inner,
        );

        let plan = PhysicalPlan::hash_aggregate(
            join_plan,
            vec![Expr::column("departments", "name", 1)],
            vec![(AggregateFunc::Count, Expr::column("users", "id", 0))],
        );
        let result = runner.execute(&plan).unwrap();

        let groups: Vec<(String, i64)> = result
            .entries
            .iter()
            .map(|entry| {
                let group_name = match entry.get_field(0) {
                    Some(Value::String(name)) => name.clone(),
                    other => panic!("Expected group name string, got {:?}", other),
                };
                let count = match entry.get_field(1) {
                    Some(Value::Int64(count)) => *count,
                    other => panic!("Expected count int64, got {:?}", other),
                };
                (group_name, count)
            })
            .collect();

        assert_eq!(
            groups,
            vec![(String::from("Engineering"), 2), (String::from("Sales"), 1),]
        );
    }

    #[test]
    fn test_aggregate_after_join_sum_right_table_column() {
        let ds = create_test_data_source();
        let runner = PhysicalPlanRunner::new(&ds);

        let join_plan = PhysicalPlan::hash_join(
            PhysicalPlan::table_scan("users"),
            PhysicalPlan::table_scan("departments"),
            Expr::eq(
                Expr::column("users", "dept_id", 2),
                Expr::column("departments", "id", 0),
            ),
            JoinType::Inner,
        );

        let plan = PhysicalPlan::hash_aggregate(
            join_plan,
            vec![],
            vec![(AggregateFunc::Sum, Expr::column("departments", "id", 0))],
        );
        let result = runner.execute(&plan).unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(
            result.entries[0].get_field(0),
            Some(&Value::Int64(40)),
            "SUM over right-table join column should use the joined-row offset, not the left-table column with the same relative index",
        );
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
            bounds: IndexBounds::all(),
            limit: Some(3),
            offset: None,
            reverse: false,
        };
        let result_forward = runner.execute(&plan_forward).unwrap();
        assert_eq!(result_forward.len(), 3);
        assert_eq!(
            result_forward.entries[0].get_field(0),
            Some(&Value::Int64(10))
        );
        assert_eq!(
            result_forward.entries[1].get_field(0),
            Some(&Value::Int64(20))
        );
        assert_eq!(
            result_forward.entries[2].get_field(0),
            Some(&Value::Int64(30))
        );

        // Reverse scan with limit 3: should get 50, 40, 30
        let plan_reverse = PhysicalPlan::IndexScan {
            table: "scores".into(),
            index: "idx_score".into(),
            bounds: IndexBounds::all(),
            limit: Some(3),
            offset: None,
            reverse: true,
        };
        let result_reverse = runner.execute(&plan_reverse).unwrap();
        assert_eq!(result_reverse.len(), 3);
        assert_eq!(
            result_reverse.entries[0].get_field(0),
            Some(&Value::Int64(50))
        );
        assert_eq!(
            result_reverse.entries[1].get_field(0),
            Some(&Value::Int64(40))
        );
        assert_eq!(
            result_reverse.entries[2].get_field(0),
            Some(&Value::Int64(30))
        );
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
            bounds: IndexBounds::all(),
            limit: Some(2),
            offset: Some(1),
            reverse: true,
        };
        let result = runner.execute(&plan).unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result.entries[0].get_field(0), Some(&Value::Int64(40)));
        assert_eq!(result.entries[1].get_field(0), Some(&Value::Int64(30)));
    }

    // ========== TopN Tests ==========

    #[test]
    fn test_topn_basic_asc() {
        let mut ds = InMemoryDataSource::new();

        let data = vec![
            Row::new(1, vec![Value::Int64(50)]),
            Row::new(2, vec![Value::Int64(20)]),
            Row::new(3, vec![Value::Int64(40)]),
            Row::new(4, vec![Value::Int64(10)]),
            Row::new(5, vec![Value::Int64(30)]),
        ];
        ds.add_table("numbers", data, 1);

        let runner = PhysicalPlanRunner::new(&ds);

        // TopN: ORDER BY value ASC LIMIT 3
        let plan = PhysicalPlan::TopN {
            input: Box::new(PhysicalPlan::table_scan("numbers")),
            order_by: vec![(Expr::column("numbers", "value", 0), SortOrder::Asc)],
            limit: 3,
            offset: 0,
        };
        let result = runner.execute(&plan).unwrap();

        assert_eq!(result.len(), 3);
        assert_eq!(result.entries[0].get_field(0), Some(&Value::Int64(10)));
        assert_eq!(result.entries[1].get_field(0), Some(&Value::Int64(20)));
        assert_eq!(result.entries[2].get_field(0), Some(&Value::Int64(30)));
    }

    #[test]
    fn test_topn_basic_desc() {
        let mut ds = InMemoryDataSource::new();

        let data = vec![
            Row::new(1, vec![Value::Int64(50)]),
            Row::new(2, vec![Value::Int64(20)]),
            Row::new(3, vec![Value::Int64(40)]),
            Row::new(4, vec![Value::Int64(10)]),
            Row::new(5, vec![Value::Int64(30)]),
        ];
        ds.add_table("numbers", data, 1);

        let runner = PhysicalPlanRunner::new(&ds);

        // TopN: ORDER BY value DESC LIMIT 3
        let plan = PhysicalPlan::TopN {
            input: Box::new(PhysicalPlan::table_scan("numbers")),
            order_by: vec![(Expr::column("numbers", "value", 0), SortOrder::Desc)],
            limit: 3,
            offset: 0,
        };
        let result = runner.execute(&plan).unwrap();

        assert_eq!(result.len(), 3);
        assert_eq!(result.entries[0].get_field(0), Some(&Value::Int64(50)));
        assert_eq!(result.entries[1].get_field(0), Some(&Value::Int64(40)));
        assert_eq!(result.entries[2].get_field(0), Some(&Value::Int64(30)));
    }

    #[test]
    fn test_topn_with_offset() {
        let mut ds = InMemoryDataSource::new();

        let data = vec![
            Row::new(1, vec![Value::Int64(50)]),
            Row::new(2, vec![Value::Int64(20)]),
            Row::new(3, vec![Value::Int64(40)]),
            Row::new(4, vec![Value::Int64(10)]),
            Row::new(5, vec![Value::Int64(30)]),
        ];
        ds.add_table("numbers", data, 1);

        let runner = PhysicalPlanRunner::new(&ds);

        // TopN: ORDER BY value ASC LIMIT 2 OFFSET 1 (skip 10, get 20, 30)
        let plan = PhysicalPlan::TopN {
            input: Box::new(PhysicalPlan::table_scan("numbers")),
            order_by: vec![(Expr::column("numbers", "value", 0), SortOrder::Asc)],
            limit: 2,
            offset: 1,
        };
        let result = runner.execute(&plan).unwrap();

        assert_eq!(result.len(), 2);
        assert_eq!(result.entries[0].get_field(0), Some(&Value::Int64(20)));
        assert_eq!(result.entries[1].get_field(0), Some(&Value::Int64(30)));
    }

    #[test]
    fn test_topn_large_dataset_uses_heap() {
        let mut ds = InMemoryDataSource::new();

        // Create 100 rows to ensure heap path is taken (k * 2 < n)
        let data: Vec<Row> = (0..100)
            .map(|i| Row::new(i as u64, vec![Value::Int64(i as i64)]))
            .collect();
        ds.add_table("numbers", data, 1);

        let runner = PhysicalPlanRunner::new(&ds);

        // TopN: ORDER BY value DESC LIMIT 5 (should use heap since 5*2 < 100)
        let plan = PhysicalPlan::TopN {
            input: Box::new(PhysicalPlan::table_scan("numbers")),
            order_by: vec![(Expr::column("numbers", "value", 0), SortOrder::Desc)],
            limit: 5,
            offset: 0,
        };
        let result = runner.execute(&plan).unwrap();

        assert_eq!(result.len(), 5);
        assert_eq!(result.entries[0].get_field(0), Some(&Value::Int64(99)));
        assert_eq!(result.entries[1].get_field(0), Some(&Value::Int64(98)));
        assert_eq!(result.entries[2].get_field(0), Some(&Value::Int64(97)));
        assert_eq!(result.entries[3].get_field(0), Some(&Value::Int64(96)));
        assert_eq!(result.entries[4].get_field(0), Some(&Value::Int64(95)));
    }

    #[test]
    fn test_topn_small_dataset_uses_sort() {
        let mut ds = InMemoryDataSource::new();

        // Create 5 rows - too small for heap (k * 2 >= n)
        let data: Vec<Row> = (0..5)
            .map(|i| Row::new(i as u64, vec![Value::Int64(i as i64)]))
            .collect();
        ds.add_table("numbers", data, 1);

        let runner = PhysicalPlanRunner::new(&ds);

        // TopN: ORDER BY value DESC LIMIT 3 (should use sort since 3*2 >= 5)
        let plan = PhysicalPlan::TopN {
            input: Box::new(PhysicalPlan::table_scan("numbers")),
            order_by: vec![(Expr::column("numbers", "value", 0), SortOrder::Desc)],
            limit: 3,
            offset: 0,
        };
        let result = runner.execute(&plan).unwrap();

        assert_eq!(result.len(), 3);
        assert_eq!(result.entries[0].get_field(0), Some(&Value::Int64(4)));
        assert_eq!(result.entries[1].get_field(0), Some(&Value::Int64(3)));
        assert_eq!(result.entries[2].get_field(0), Some(&Value::Int64(2)));
    }

    #[test]
    fn test_topn_matches_sort_limit() {
        let mut ds = InMemoryDataSource::new();

        // Create 50 rows with random-ish values
        let data: Vec<Row> = vec![
            Row::new(0, vec![Value::Int64(42)]),
            Row::new(1, vec![Value::Int64(17)]),
            Row::new(2, vec![Value::Int64(89)]),
            Row::new(3, vec![Value::Int64(3)]),
            Row::new(4, vec![Value::Int64(56)]),
            Row::new(5, vec![Value::Int64(71)]),
            Row::new(6, vec![Value::Int64(28)]),
            Row::new(7, vec![Value::Int64(94)]),
            Row::new(8, vec![Value::Int64(12)]),
            Row::new(9, vec![Value::Int64(65)]),
            Row::new(10, vec![Value::Int64(33)]),
            Row::new(11, vec![Value::Int64(81)]),
            Row::new(12, vec![Value::Int64(7)]),
            Row::new(13, vec![Value::Int64(49)]),
            Row::new(14, vec![Value::Int64(22)]),
        ];
        ds.add_table("numbers", data, 1);

        let runner = PhysicalPlanRunner::new(&ds);

        // TopN result
        let topn_plan = PhysicalPlan::TopN {
            input: Box::new(PhysicalPlan::table_scan("numbers")),
            order_by: vec![(Expr::column("numbers", "value", 0), SortOrder::Asc)],
            limit: 5,
            offset: 0,
        };
        let topn_result = runner.execute(&topn_plan).unwrap();

        // Sort + Limit result
        let sort_limit_plan = PhysicalPlan::limit(
            PhysicalPlan::sort(
                PhysicalPlan::table_scan("numbers"),
                vec![(Expr::column("numbers", "value", 0), SortOrder::Asc)],
            ),
            5,
            0,
        );
        let sort_limit_result = runner.execute(&sort_limit_plan).unwrap();

        // Results should match
        assert_eq!(topn_result.len(), sort_limit_result.len());
        for i in 0..topn_result.len() {
            assert_eq!(
                topn_result.entries[i].get_field(0),
                sort_limit_result.entries[i].get_field(0)
            );
        }
    }

    #[test]
    fn test_topn_with_strings() {
        let mut ds = InMemoryDataSource::new();

        let data = vec![
            Row::new(1, vec![Value::String("Charlie".into())]),
            Row::new(2, vec![Value::String("Alice".into())]),
            Row::new(3, vec![Value::String("Bob".into())]),
            Row::new(4, vec![Value::String("David".into())]),
            Row::new(5, vec![Value::String("Eve".into())]),
        ];
        ds.add_table("names", data, 1);

        let runner = PhysicalPlanRunner::new(&ds);

        let plan = PhysicalPlan::TopN {
            input: Box::new(PhysicalPlan::table_scan("names")),
            order_by: vec![(Expr::column("names", "name", 0), SortOrder::Asc)],
            limit: 3,
            offset: 0,
        };
        let result = runner.execute(&plan).unwrap();

        assert_eq!(result.len(), 3);
        assert_eq!(
            result.entries[0].get_field(0),
            Some(&Value::String("Alice".into()))
        );
        assert_eq!(
            result.entries[1].get_field(0),
            Some(&Value::String("Bob".into()))
        );
        assert_eq!(
            result.entries[2].get_field(0),
            Some(&Value::String("Charlie".into()))
        );
    }

    #[test]
    fn test_topn_limit_exceeds_input() {
        let mut ds = InMemoryDataSource::new();

        let data = vec![
            Row::new(1, vec![Value::Int64(30)]),
            Row::new(2, vec![Value::Int64(10)]),
            Row::new(3, vec![Value::Int64(20)]),
        ];
        ds.add_table("numbers", data, 1);

        let runner = PhysicalPlanRunner::new(&ds);

        // TopN: LIMIT 10 but only 3 rows
        let plan = PhysicalPlan::TopN {
            input: Box::new(PhysicalPlan::table_scan("numbers")),
            order_by: vec![(Expr::column("numbers", "value", 0), SortOrder::Asc)],
            limit: 10,
            offset: 0,
        };
        let result = runner.execute(&plan).unwrap();

        assert_eq!(result.len(), 3);
        assert_eq!(result.entries[0].get_field(0), Some(&Value::Int64(10)));
        assert_eq!(result.entries[1].get_field(0), Some(&Value::Int64(20)));
        assert_eq!(result.entries[2].get_field(0), Some(&Value::Int64(30)));
    }

    #[test]
    fn test_jsonb_contains_uses_provided_value() {
        let mut ds = InMemoryDataSource::new();
        ds.add_table(
            "products",
            vec![
                Row::new(
                    1,
                    vec![
                        Value::Int64(1),
                        Value::String("Laptop".into()),
                        Value::Jsonb(cynos_core::JsonbValue(
                            br#"{"tags":["computer","portable"]}"#.to_vec(),
                        )),
                    ],
                ),
                Row::new(
                    2,
                    vec![
                        Value::Int64(2),
                        Value::String("Phone".into()),
                        Value::Jsonb(cynos_core::JsonbValue(br#"{"tags":["mobile"]}"#.to_vec())),
                    ],
                ),
                Row::new(
                    3,
                    vec![
                        Value::Int64(3),
                        Value::String("Tablet".into()),
                        Value::Jsonb(cynos_core::JsonbValue(
                            br#"{"tags":["portable","touch"]}"#.to_vec(),
                        )),
                    ],
                ),
            ],
            3,
        );

        let runner = PhysicalPlanRunner::new(&ds);
        let plan = PhysicalPlan::filter(
            PhysicalPlan::table_scan("products"),
            Expr::Function {
                name: "JSONB_CONTAINS".into(),
                args: vec![
                    Expr::column("products", "metadata", 2),
                    Expr::literal(Value::String("$.tags".into())),
                    Expr::literal(Value::String("portable".into())),
                ],
            },
        );

        let result = runner.execute(&plan).unwrap();
        let matched_names: Vec<String> = result
            .entries
            .iter()
            .filter_map(|entry| match entry.get_field(1) {
                Some(Value::String(name)) => Some(name.clone()),
                _ => None,
            })
            .collect();

        assert_eq!(
            matched_names,
            vec![String::from("Laptop"), String::from("Tablet")]
        );
    }
}
