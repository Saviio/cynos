//! Index selection optimization pass.

use crate::ast::{BinaryOp, Expr};
use crate::context::{ExecutionContext, IndexInfo};
use crate::optimizer::OptimizerPass;
use crate::planner::LogicalPlan;
use alloc::boxed::Box;
use alloc::format;
use alloc::string::String;
use alloc::vec::Vec;
use cynos_core::Value;

/// Index selection optimization.
///
/// Analyzes predicates and selects appropriate indexes for scans.
/// This pass identifies Filter(Scan) patterns where the filter predicate
/// can be satisfied using an index, converting them to IndexScan operations.
///
/// Supports:
/// - Point lookups: `col = value` → IndexGet
/// - Range scans: `col > value`, `col < value`, etc. → IndexScan
/// - IN queries: `col IN (v1, v2, v3)` → IndexInGet
/// - JSONB queries with GIN indexes → GinIndexScan
pub struct IndexSelection {
    /// Execution context with table statistics and index information.
    context: Option<ExecutionContext>,
}

impl Default for IndexSelection {
    fn default() -> Self {
        Self::new()
    }
}

impl IndexSelection {
    /// Creates a new index selection pass without context.
    pub fn new() -> Self {
        Self { context: None }
    }

    /// Creates a new index selection pass with execution context.
    pub fn with_context(context: ExecutionContext) -> Self {
        Self {
            context: Some(context),
        }
    }
}

impl OptimizerPass for IndexSelection {
    fn optimize(&self, plan: LogicalPlan) -> LogicalPlan {
        self.select_indexes(plan)
    }

    fn name(&self) -> &'static str {
        "index_selection"
    }
}

/// Information extracted from a predicate for index selection.
#[derive(Debug, Clone)]
pub struct PredicateInfo {
    /// Table name referenced by the predicate.
    pub table: String,
    /// Column name referenced by the predicate.
    pub column: String,
    /// The comparison operator.
    pub op: BinaryOp,
    /// The literal value being compared (if any).
    pub value: Option<Value>,
    /// Whether this is a range predicate (can use index range scan).
    pub is_range: bool,
    /// Whether this is a point lookup (can use index get).
    pub is_point_lookup: bool,
}

/// Merged range bounds for a single column.
/// Used when multiple range predicates on the same column can be combined.
#[derive(Debug, Clone)]
struct MergedRange {
    /// Lower bound value (None means -∞)
    lower_bound: Option<Value>,
    /// Whether the lower bound is inclusive (>=) or exclusive (>)
    lower_inclusive: bool,
    /// Upper bound value (None means +∞)
    upper_bound: Option<Value>,
    /// Whether the upper bound is inclusive (<=) or exclusive (<)
    upper_inclusive: bool,
}

impl MergedRange {
    /// Creates a new unbounded range (-∞, +∞)
    fn new() -> Self {
        Self {
            lower_bound: None,
            lower_inclusive: true,
            upper_bound: None,
            upper_inclusive: true,
        }
    }

    /// Updates the lower bound with a new constraint.
    /// Takes the more restrictive (larger) lower bound.
    fn update_lower(&mut self, value: Value, inclusive: bool) {
        match &self.lower_bound {
            None => {
                self.lower_bound = Some(value);
                self.lower_inclusive = inclusive;
            }
            Some(existing) => {
                use core::cmp::Ordering;
                match value.cmp(existing) {
                    Ordering::Greater => {
                        // New value is larger, use it
                        self.lower_bound = Some(value);
                        self.lower_inclusive = inclusive;
                    }
                    Ordering::Equal => {
                        // Same value: exclusive (>) is more restrictive than inclusive (>=)
                        if !inclusive {
                            self.lower_inclusive = false;
                        }
                    }
                    Ordering::Less => {
                        // Existing is larger, keep it
                    }
                }
            }
        }
    }

    /// Updates the upper bound with a new constraint.
    /// Takes the more restrictive (smaller) upper bound.
    fn update_upper(&mut self, value: Value, inclusive: bool) {
        match &self.upper_bound {
            None => {
                self.upper_bound = Some(value);
                self.upper_inclusive = inclusive;
            }
            Some(existing) => {
                use core::cmp::Ordering;
                match value.cmp(existing) {
                    Ordering::Less => {
                        // New value is smaller, use it
                        self.upper_bound = Some(value);
                        self.upper_inclusive = inclusive;
                    }
                    Ordering::Equal => {
                        // Same value: exclusive (<) is more restrictive than inclusive (<=)
                        if !inclusive {
                            self.upper_inclusive = false;
                        }
                    }
                    Ordering::Greater => {
                        // Existing is smaller, keep it
                    }
                }
            }
        }
    }

    /// Converts to IndexScan range parameters.
    fn to_range_params(self) -> (Option<Value>, Option<Value>, bool, bool) {
        (
            self.lower_bound,
            self.upper_bound,
            self.lower_inclusive,
            self.upper_inclusive,
        )
    }
}

/// Information extracted from an IN predicate for index selection.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct InPredicateInfo {
    /// Table name referenced by the predicate.
    pub table: String,
    /// Column name referenced by the predicate.
    pub column: String,
    /// The literal values in the IN list.
    pub values: Vec<Value>,
}

/// Information about a GIN-indexable predicate.
#[derive(Debug, Clone)]
struct GinPredicateInfo {
    index: String,
    column: String,
    column_index: usize,
    path: String,
    value: Option<Value>,
    query_type: String,
}

impl IndexSelection {
    fn select_indexes(&self, plan: LogicalPlan) -> LogicalPlan {
        match plan {
            // Look for Filter on Scan patterns that could use an index
            LogicalPlan::Filter { input, predicate } => {
                let optimized_input = self.select_indexes(*input);

                // Try to convert Filter(Scan) to IndexScan
                if let LogicalPlan::Scan { ref table } = optimized_input {
                    if let Some(index_plan) =
                        self.try_use_index(table, &predicate, optimized_input.clone())
                    {
                        return index_plan;
                    }
                }

                LogicalPlan::Filter {
                    input: Box::new(optimized_input),
                    predicate,
                }
            }

            LogicalPlan::Project { input, columns } => LogicalPlan::Project {
                input: Box::new(self.select_indexes(*input)),
                columns,
            },

            LogicalPlan::Join {
                left,
                right,
                condition,
                join_type,
            } => LogicalPlan::Join {
                left: Box::new(self.select_indexes(*left)),
                right: Box::new(self.select_indexes(*right)),
                condition,
                join_type,
            },

            LogicalPlan::Aggregate {
                input,
                group_by,
                aggregates,
            } => LogicalPlan::Aggregate {
                input: Box::new(self.select_indexes(*input)),
                group_by,
                aggregates,
            },

            LogicalPlan::Sort { input, order_by } => LogicalPlan::Sort {
                input: Box::new(self.select_indexes(*input)),
                order_by,
            },

            LogicalPlan::Limit {
                input,
                limit,
                offset,
            } => LogicalPlan::Limit {
                input: Box::new(self.select_indexes(*input)),
                limit,
                offset,
            },

            LogicalPlan::CrossProduct { left, right } => LogicalPlan::CrossProduct {
                left: Box::new(self.select_indexes(*left)),
                right: Box::new(self.select_indexes(*right)),
            },

            LogicalPlan::Union { left, right, all } => LogicalPlan::Union {
                left: Box::new(self.select_indexes(*left)),
                right: Box::new(self.select_indexes(*right)),
                all,
            },

            // Leaf nodes
            LogicalPlan::Scan { .. }
            | LogicalPlan::IndexScan { .. }
            | LogicalPlan::IndexGet { .. }
            | LogicalPlan::IndexInGet { .. }
            | LogicalPlan::GinIndexScan { .. }
            | LogicalPlan::GinIndexScanMulti { .. }
            | LogicalPlan::Empty => plan,
        }
    }

    /// Attempts to use an index for the given predicate.
    fn try_use_index(
        &self,
        table: &str,
        predicate: &Expr,
        _original: LogicalPlan,
    ) -> Option<LogicalPlan> {
        // Check if we have context with index information
        let ctx = self.context.as_ref()?;

        // First, try to handle IN predicates
        if let Some(in_info) = self.analyze_in_predicate(predicate) {
            // Find an index that covers the IN column
            if let Some(index) = ctx.find_index(table, &[in_info.column.as_str()]) {
                // Use IndexInGet for IN queries with indexed columns
                return Some(LogicalPlan::IndexInGet {
                    table: table.into(),
                    index: index.name.clone(),
                    keys: in_info.values,
                });
            }
        }

        // Try to handle BETWEEN predicates
        if let Some(between_plan) = self.try_use_between_index(table, predicate, ctx) {
            return Some(between_plan);
        }

        // Then, try to use GIN index for JSONB function queries
        if let Some(gin_plan) = self.try_use_gin_index(table, predicate, ctx) {
            return Some(gin_plan);
        }

        // Try to handle AND compound predicates for B-Tree indexes
        if let Some(btree_plan) = self.try_use_btree_with_and(table, predicate, ctx) {
            return Some(btree_plan);
        }

        // Extract predicate information for B-Tree index (simple predicate)
        let pred_info = self.analyze_predicate(predicate)?;

        // Find an index that covers the predicate column
        let index = ctx.find_index(table, &[pred_info.column.as_str()])?;

        // Skip GIN indexes for regular predicates
        if index.is_gin() {
            return None;
        }

        // Decide whether to use IndexScan or IndexGet based on predicate type
        if pred_info.is_point_lookup {
            // Use IndexGet for equality lookups
            if let Some(value) = pred_info.value {
                return Some(LogicalPlan::IndexGet {
                    table: table.into(),
                    index: index.name.clone(),
                    key: value,
                });
            }
        } else if pred_info.is_range {
            // Use IndexScan for range predicates
            let (range_start, range_end, include_start, include_end) =
                self.compute_range(&pred_info);
            return Some(LogicalPlan::IndexScan {
                table: table.into(),
                index: index.name.clone(),
                range_start,
                range_end,
                include_start,
                include_end,
            });
        }

        None
    }

    /// Attempts to use an index for BETWEEN predicates.
    fn try_use_between_index(
        &self,
        table: &str,
        predicate: &Expr,
        ctx: &ExecutionContext,
    ) -> Option<LogicalPlan> {
        if let Expr::Between { expr, low, high } = predicate {
            // Check if expr is a column reference
            if let Expr::Column(col) = expr.as_ref() {
                // Check if low and high are literals
                if let (Expr::Literal(low_val), Expr::Literal(high_val)) = (low.as_ref(), high.as_ref()) {
                    // Find an index for this column
                    if let Some(index) = ctx.find_index(table, &[col.column.as_str()]) {
                        // Skip GIN indexes
                        if index.is_gin() {
                            return None;
                        }
                        // Use IndexScan for BETWEEN
                        return Some(LogicalPlan::IndexScan {
                            table: table.into(),
                            index: index.name.clone(),
                            range_start: Some(low_val.clone()),
                            range_end: Some(high_val.clone()),
                            include_start: true,
                            include_end: true,
                        });
                    }
                }
            }
        }
        None
    }

    /// Attempts to use a B-Tree index for AND compound predicates.
    /// Extracts sub-predicates from AND, merges range predicates on the same column,
    /// converts to IndexScan/IndexGet, and keeps remaining predicates as Filter.
    fn try_use_btree_with_and(
        &self,
        table: &str,
        predicate: &Expr,
        ctx: &ExecutionContext,
    ) -> Option<LogicalPlan> {
        // Only handle AND compound predicates
        if !matches!(predicate, Expr::BinaryOp { op: BinaryOp::And, .. }) {
            return None;
        }

        // Extract all sub-predicates from AND
        let (indexable, remaining) = self.extract_btree_and_remaining_predicates(predicate, table, ctx);

        if indexable.is_empty() {
            return None;
        }

        // Try to find a point lookup first (highest priority)
        for (pred, info, index) in &indexable {
            if info.is_point_lookup && info.value.is_some() {
                // Build IndexGet plan
                let index_plan = LogicalPlan::IndexGet {
                    table: table.into(),
                    index: index.name.clone(),
                    key: info.value.clone()?,
                };

                // Collect remaining predicates (non-indexable + other indexable)
                let mut all_remaining: Vec<Expr> = remaining;
                for (other_pred, _, _) in &indexable {
                    if !Self::expr_eq(other_pred, pred) {
                        all_remaining.push(other_pred.clone());
                    }
                }

                return Some(self.wrap_with_filter_if_needed(index_plan, all_remaining));
            }
        }

        // No point lookup found, try to merge range predicates on the same column
        // Group range predicates by (column, index)
        let merged = self.merge_range_predicates_by_column(&indexable);

        if merged.is_empty() {
            return None;
        }

        // Select the best merged range (prefer ranges with both bounds)
        let (best_column, best_index, best_range, used_predicates) =
            self.select_best_merged_range(&merged)?;

        // Build IndexScan with merged range
        let (range_start, range_end, include_start, include_end) = best_range.to_range_params();
        let index_plan = LogicalPlan::IndexScan {
            table: table.into(),
            index: best_index.name.clone(),
            range_start,
            range_end,
            include_start,
            include_end,
        };

        // Collect remaining predicates:
        // - All non-indexable predicates
        // - Indexable predicates on other columns
        // - Indexable predicates on the same column but not used in the merge
        let mut all_remaining: Vec<Expr> = remaining;
        for (pred, info, _) in &indexable {
            // Skip predicates that were used in the merged range
            if info.column == best_column && used_predicates.iter().any(|p| Self::expr_eq(p, pred)) {
                continue;
            }
            all_remaining.push(pred.clone());
        }

        Some(self.wrap_with_filter_if_needed(index_plan, all_remaining))
    }

    /// Wraps an index plan with a Filter if there are remaining predicates.
    fn wrap_with_filter_if_needed(
        &self,
        index_plan: LogicalPlan,
        remaining: Vec<Expr>,
    ) -> LogicalPlan {
        if remaining.is_empty() {
            index_plan
        } else {
            let combined_predicate = remaining
                .into_iter()
                .reduce(|acc, pred| Expr::and(acc, pred))
                .unwrap();

            LogicalPlan::Filter {
                input: Box::new(index_plan),
                predicate: combined_predicate,
            }
        }
    }

    /// Groups range predicates by column and merges them into combined ranges.
    /// Returns a map of (column_name, index, merged_range, used_predicates).
    fn merge_range_predicates_by_column(
        &self,
        indexable: &[(Expr, PredicateInfo, IndexInfo)],
    ) -> Vec<(String, IndexInfo, MergedRange, Vec<Expr>)> {
        use hashbrown::HashMap;

        // Group by column name
        let mut by_column: HashMap<String, Vec<(Expr, PredicateInfo, IndexInfo)>> = HashMap::new();
        for (pred, info, index) in indexable {
            if info.is_range && info.value.is_some() {
                by_column
                    .entry(info.column.clone())
                    .or_default()
                    .push((pred.clone(), info.clone(), index.clone()));
            }
        }

        // Merge ranges for each column
        let mut result = Vec::new();
        for (column, predicates) in by_column {
            if predicates.is_empty() {
                continue;
            }

            // All predicates on the same column should use the same index
            let index = predicates[0].2.clone();

            let mut merged = MergedRange::new();
            let mut used_preds = Vec::new();

            for (pred, info, _) in &predicates {
                if let Some(value) = &info.value {
                    match info.op {
                        BinaryOp::Gt => {
                            merged.update_lower(value.clone(), false);
                            used_preds.push(pred.clone());
                        }
                        BinaryOp::Ge => {
                            merged.update_lower(value.clone(), true);
                            used_preds.push(pred.clone());
                        }
                        BinaryOp::Lt => {
                            merged.update_upper(value.clone(), false);
                            used_preds.push(pred.clone());
                        }
                        BinaryOp::Le => {
                            merged.update_upper(value.clone(), true);
                            used_preds.push(pred.clone());
                        }
                        _ => {}
                    }
                }
            }

            if !used_preds.is_empty() {
                result.push((column, index, merged, used_preds));
            }
        }

        result
    }

    /// Selects the best merged range for index usage.
    /// Priority: ranges with both bounds > ranges with only lower bound > ranges with only upper bound
    fn select_best_merged_range(
        &self,
        merged: &[(String, IndexInfo, MergedRange, Vec<Expr>)],
    ) -> Option<(String, IndexInfo, MergedRange, Vec<Expr>)> {
        // Priority 1: Ranges with both bounds (most selective)
        for (col, idx, range, preds) in merged {
            if range.lower_bound.is_some() && range.upper_bound.is_some() {
                return Some((col.clone(), idx.clone(), range.clone(), preds.clone()));
            }
        }

        // Priority 2: Ranges with lower bound only (> or >=)
        for (col, idx, range, preds) in merged {
            if range.lower_bound.is_some() {
                return Some((col.clone(), idx.clone(), range.clone(), preds.clone()));
            }
        }

        // Priority 3: Ranges with upper bound only (< or <=)
        for (col, idx, range, preds) in merged {
            if range.upper_bound.is_some() {
                return Some((col.clone(), idx.clone(), range.clone(), preds.clone()));
            }
        }

        None
    }

    /// Extracts B-Tree indexable predicates and remaining predicates from an AND expression.
    fn extract_btree_and_remaining_predicates(
        &self,
        predicate: &Expr,
        table: &str,
        ctx: &ExecutionContext,
    ) -> (Vec<(Expr, PredicateInfo, IndexInfo)>, Vec<Expr>) {
        let mut indexable = Vec::new();
        let mut remaining = Vec::new();
        self.extract_btree_and_remaining_recursive(predicate, table, ctx, &mut indexable, &mut remaining);
        (indexable, remaining)
    }

    fn extract_btree_and_remaining_recursive(
        &self,
        predicate: &Expr,
        table: &str,
        ctx: &ExecutionContext,
        indexable: &mut Vec<(Expr, PredicateInfo, IndexInfo)>,
        remaining: &mut Vec<Expr>,
    ) {
        match predicate {
            Expr::BinaryOp {
                left,
                op: BinaryOp::And,
                right,
            } => {
                self.extract_btree_and_remaining_recursive(left, table, ctx, indexable, remaining);
                self.extract_btree_and_remaining_recursive(right, table, ctx, indexable, remaining);
            }
            _ => {
                // Try to analyze as a simple predicate
                if let Some(pred_info) = self.analyze_predicate(predicate) {
                    // Check if there's a B-Tree index for this column
                    if let Some(index) = ctx.find_index(table, &[pred_info.column.as_str()]) {
                        if !index.is_gin() && (pred_info.is_point_lookup || pred_info.is_range) {
                            indexable.push((predicate.clone(), pred_info, index.clone()));
                            return;
                        }
                    }
                }
                // Not indexable - add to remaining
                remaining.push(predicate.clone());
            }
        }
    }

    /// Selects the best B-Tree predicate for index usage.
    /// Priority: point lookup (IndexGet) > range scan (IndexScan)
    ///
    /// Note: This function is kept for potential future use but is currently
    /// superseded by `merge_range_predicates_by_column` + `select_best_merged_range`
    /// which can merge multiple range predicates on the same column.
    #[allow(dead_code)]
    fn select_best_btree_predicate(
        &self,
        indexable: &[(Expr, PredicateInfo, IndexInfo)],
    ) -> Option<(Expr, PredicateInfo, IndexInfo)> {
        // First, try to find a point lookup (equality)
        for (pred, info, index) in indexable {
            if info.is_point_lookup && info.value.is_some() {
                return Some((pred.clone(), info.clone(), index.clone()));
            }
        }

        // Fall back to range scan
        for (pred, info, index) in indexable {
            if info.is_range && info.value.is_some() {
                return Some((pred.clone(), info.clone(), index.clone()));
            }
        }

        None
    }

    /// Simple expression equality check for filtering out the selected predicate.
    fn expr_eq(a: &Expr, b: &Expr) -> bool {
        // Use debug representation for simple equality check
        format!("{:?}", a) == format!("{:?}", b)
    }

    /// Analyzes an IN predicate to extract index-relevant information.
    fn analyze_in_predicate(&self, predicate: &Expr) -> Option<InPredicateInfo> {
        match predicate {
            Expr::In { expr, list } => {
                // Check if expr is a column reference
                if let Expr::Column(col) = expr.as_ref() {
                    // Extract all literal values from the list
                    let values: Vec<Value> = list
                        .iter()
                        .filter_map(|item| {
                            if let Expr::Literal(val) = item {
                                Some(val.clone())
                            } else {
                                None
                            }
                        })
                        .collect();

                    // Only use index if all values are literals
                    if values.len() == list.len() && !values.is_empty() {
                        return Some(InPredicateInfo {
                            table: col.table.clone(),
                            column: col.column.clone(),
                            values,
                        });
                    }
                }
                None
            }
            _ => None,
        }
    }


    /// Attempts to use a GIN index for JSONB function queries.
    /// Supports both single predicates and AND combinations of multiple predicates.
    ///
    /// Returns a tuple of (GIN plan, remaining predicates that couldn't be handled by GIN).
    /// The remaining predicates should be wrapped as a Filter around the GIN plan.
    fn try_use_gin_index(
        &self,
        table: &str,
        predicate: &Expr,
        ctx: &ExecutionContext,
    ) -> Option<LogicalPlan> {
        // Extract GIN predicates and collect remaining non-GIN predicates
        let (gin_predicates, remaining_predicates) = self.extract_gin_and_remaining_predicates(predicate, table, ctx);

        if gin_predicates.is_empty() {
            return None;
        }

        // Build the GIN plan
        let gin_plan = if gin_predicates.len() > 1 {
            // Multiple GIN predicates - try to use GinIndexScanMulti for better performance
            let first_index = gin_predicates[0].index.clone();
            let first_column = gin_predicates[0].column.clone();
            let all_same_index = gin_predicates.iter().all(|p| p.index == first_index && p.column == first_column);
            let all_have_values = gin_predicates.iter().all(|p| p.value.is_some());

            if all_same_index && all_have_values {
                let pairs: Vec<(String, Value)> = gin_predicates
                    .into_iter()
                    .filter_map(|p| p.value.map(|v| (p.path, v)))
                    .collect();

                LogicalPlan::GinIndexScanMulti {
                    table: table.into(),
                    index: first_index,
                    column: first_column,
                    pairs,
                }
            } else {
                // Fall back to single predicate if multi-predicate optimization fails
                let info = gin_predicates.into_iter().next()?;
                LogicalPlan::GinIndexScan {
                    table: table.into(),
                    index: info.index,
                    column: info.column,
                    column_index: info.column_index,
                    path: info.path,
                    value: info.value,
                    query_type: info.query_type,
                }
            }
        } else {
            // Single GIN predicate
            let info = gin_predicates.into_iter().next()?;
            LogicalPlan::GinIndexScan {
                table: table.into(),
                index: info.index,
                column: info.column,
                column_index: info.column_index,
                path: info.path,
                value: info.value,
                query_type: info.query_type,
            }
        };

        // If there are remaining predicates, wrap the GIN plan with a Filter
        if remaining_predicates.is_empty() {
            Some(gin_plan)
        } else {
            // Combine remaining predicates with AND
            let combined_predicate = remaining_predicates
                .into_iter()
                .reduce(|acc, pred| Expr::and(acc, pred))
                .unwrap();

            Some(LogicalPlan::Filter {
                input: Box::new(gin_plan),
                predicate: combined_predicate,
            })
        }
    }

    /// Extracts GIN-indexable predicates and remaining non-GIN predicates from an expression.
    /// Returns (gin_predicates, remaining_predicates).
    fn extract_gin_and_remaining_predicates(
        &self,
        predicate: &Expr,
        table: &str,
        ctx: &ExecutionContext,
    ) -> (Vec<GinPredicateInfo>, Vec<Expr>) {
        let mut gin_predicates = Vec::new();
        let mut remaining_predicates = Vec::new();
        self.extract_gin_and_remaining_recursive(predicate, table, ctx, &mut gin_predicates, &mut remaining_predicates);
        (gin_predicates, remaining_predicates)
    }

    fn extract_gin_and_remaining_recursive(
        &self,
        predicate: &Expr,
        table: &str,
        ctx: &ExecutionContext,
        gin_result: &mut Vec<GinPredicateInfo>,
        remaining_result: &mut Vec<Expr>,
    ) {
        match predicate {
            // Handle AND combinations - recursively process both sides
            Expr::BinaryOp {
                left,
                op: BinaryOp::And,
                right,
            } => {
                self.extract_gin_and_remaining_recursive(left, table, ctx, gin_result, remaining_result);
                self.extract_gin_and_remaining_recursive(right, table, ctx, gin_result, remaining_result);
            }
            // Handle JSONB function calls - these can potentially use GIN index
            Expr::Function { name, args } => {
                if let Some(info) = self.analyze_gin_function(name, args, table, ctx) {
                    gin_result.push(info);
                } else {
                    // Function that doesn't match GIN pattern - keep as remaining
                    remaining_result.push(predicate.clone());
                }
            }
            // All other predicates (BinaryOp with Eq/Lt/Gt, etc.) are non-GIN
            _ => {
                remaining_result.push(predicate.clone());
            }
        }
    }

    /// Analyzes a JSONB function call to extract GIN predicate information.
    fn analyze_gin_function(
        &self,
        name: &str,
        args: &[Expr],
        table: &str,
        ctx: &ExecutionContext,
    ) -> Option<GinPredicateInfo> {
        let func_name = name.to_uppercase();
        match func_name.as_str() {
            "JSONB_PATH_EQ" if args.len() >= 3 => {
                if let Expr::Column(col) = &args[0] {
                    let column_name = &col.column;
                    let column_index = col.index;
                    if let Some(index) = ctx.find_gin_index(table, column_name) {
                        let path = self.extract_string_literal(&args[1])?;
                        let value = self.extract_literal(&args[2]);
                        return Some(GinPredicateInfo {
                            index: index.name.clone(),
                            column: column_name.clone(),
                            column_index,
                            path,
                            value,
                            query_type: "eq".into(),
                        });
                    }
                }
            }
            "JSONB_CONTAINS" if args.len() >= 2 => {
                if let Expr::Column(col) = &args[0] {
                    let column_name = &col.column;
                    let column_index = col.index;
                    if let Some(index) = ctx.find_gin_index(table, column_name) {
                        let path = self.extract_string_literal(&args[1])?;
                        return Some(GinPredicateInfo {
                            index: index.name.clone(),
                            column: column_name.clone(),
                            column_index,
                            path,
                            value: None,
                            query_type: "contains".into(),
                        });
                    }
                }
            }
            "JSONB_EXISTS" if args.len() >= 2 => {
                if let Expr::Column(col) = &args[0] {
                    let column_name = &col.column;
                    let column_index = col.index;
                    if let Some(index) = ctx.find_gin_index(table, column_name) {
                        let path = self.extract_string_literal(&args[1])?;
                        return Some(GinPredicateInfo {
                            index: index.name.clone(),
                            column: column_name.clone(),
                            column_index,
                            path,
                            value: None,
                            query_type: "exists".into(),
                        });
                    }
                }
            }
            _ => {}
        }
        None
    }

    /// Extracts a string literal from an expression.
    fn extract_string_literal(&self, expr: &Expr) -> Option<String> {
        if let Expr::Literal(Value::String(s)) = expr {
            Some(s.clone())
        } else {
            None
        }
    }

    /// Extracts a literal value from an expression.
    fn extract_literal(&self, expr: &Expr) -> Option<Value> {
        if let Expr::Literal(v) = expr {
            Some(v.clone())
        } else {
            None
        }
    }

    /// Analyzes a predicate to extract index-relevant information.
    fn analyze_predicate(&self, predicate: &Expr) -> Option<PredicateInfo> {
        match predicate {
            Expr::BinaryOp { left, op, right } => {
                // Check for column = literal pattern
                if let (Expr::Column(col), Expr::Literal(val)) = (left.as_ref(), right.as_ref()) {
                    let is_point_lookup = *op == BinaryOp::Eq;
                    let is_range = matches!(
                        op,
                        BinaryOp::Lt | BinaryOp::Le | BinaryOp::Gt | BinaryOp::Ge
                    );

                    return Some(PredicateInfo {
                        table: col.table.clone(),
                        column: col.column.clone(),
                        op: *op,
                        value: Some(val.clone()),
                        is_range,
                        is_point_lookup,
                    });
                }

                // Check for literal = column pattern (reversed)
                if let (Expr::Literal(val), Expr::Column(col)) = (left.as_ref(), right.as_ref()) {
                    let reversed_op = match op {
                        BinaryOp::Lt => BinaryOp::Gt,
                        BinaryOp::Le => BinaryOp::Ge,
                        BinaryOp::Gt => BinaryOp::Lt,
                        BinaryOp::Ge => BinaryOp::Le,
                        other => *other,
                    };
                    let is_point_lookup = *op == BinaryOp::Eq;
                    let is_range = matches!(
                        op,
                        BinaryOp::Lt | BinaryOp::Le | BinaryOp::Gt | BinaryOp::Ge
                    );

                    return Some(PredicateInfo {
                        table: col.table.clone(),
                        column: col.column.clone(),
                        op: reversed_op,
                        value: Some(val.clone()),
                        is_range,
                        is_point_lookup,
                    });
                }

                None
            }
            _ => None,
        }
    }

    /// Computes the range bounds for an index scan.
    fn compute_range(
        &self,
        pred_info: &PredicateInfo,
    ) -> (Option<Value>, Option<Value>, bool, bool) {
        let value = pred_info.value.clone();

        match pred_info.op {
            BinaryOp::Eq => (value.clone(), value, true, true),
            BinaryOp::Lt => (None, value, true, false),
            BinaryOp::Le => (None, value, true, true),
            BinaryOp::Gt => (value, None, false, true),
            BinaryOp::Ge => (value, None, true, true),
            _ => (None, None, true, true),
        }
    }

    /// Extracts all simple predicates from a compound predicate.
    pub fn extract_predicates(&self, predicate: &Expr) -> Vec<PredicateInfo> {
        let mut result = Vec::new();
        self.extract_predicates_recursive(predicate, &mut result);
        result
    }

    fn extract_predicates_recursive(&self, predicate: &Expr, result: &mut Vec<PredicateInfo>) {
        match predicate {
            Expr::BinaryOp {
                left,
                op: BinaryOp::And,
                right,
            } => {
                self.extract_predicates_recursive(left, result);
                self.extract_predicates_recursive(right, result);
            }
            _ => {
                if let Some(info) = self.analyze_predicate(predicate) {
                    result.push(info);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::{IndexInfo, TableStats};

    #[test]
    fn test_index_selection_basic() {
        let pass = IndexSelection::new();

        let plan = LogicalPlan::filter(
            LogicalPlan::scan("users"),
            Expr::eq(Expr::column("users", "id", 0), Expr::literal(1i64)),
        );

        let optimized = pass.optimize(plan);
        // Without context, should remain unchanged
        assert!(matches!(optimized, LogicalPlan::Filter { .. }));
    }

    #[test]
    fn test_index_selection_with_context() {
        let mut ctx = ExecutionContext::new();
        ctx.register_table(
            "users",
            TableStats {
                row_count: 1000,
                is_sorted: false,
                indexes: alloc::vec![IndexInfo::new(
                    "idx_id",
                    alloc::vec!["id".into()],
                    true
                )],
            },
        );

        let pass = IndexSelection::with_context(ctx);

        let plan = LogicalPlan::filter(
            LogicalPlan::scan("users"),
            Expr::eq(Expr::column("users", "id", 0), Expr::literal(1i64)),
        );

        let optimized = pass.optimize(plan);
        // With context and matching index, should convert to IndexGet
        assert!(matches!(optimized, LogicalPlan::IndexGet { .. }));
    }

    #[test]
    fn test_index_selection_range_scan() {
        let mut ctx = ExecutionContext::new();
        ctx.register_table(
            "orders",
            TableStats {
                row_count: 10000,
                is_sorted: false,
                indexes: alloc::vec![IndexInfo::new(
                    "idx_amount",
                    alloc::vec!["amount".into()],
                    false
                )],
            },
        );

        let pass = IndexSelection::with_context(ctx);

        let plan = LogicalPlan::filter(
            LogicalPlan::scan("orders"),
            Expr::gt(Expr::column("orders", "amount", 0), Expr::literal(100i64)),
        );

        let optimized = pass.optimize(plan);
        // Should convert to IndexScan for range predicate
        assert!(matches!(optimized, LogicalPlan::IndexScan { .. }));
    }

    #[test]
    fn test_analyze_predicate() {
        let pass = IndexSelection::new();

        let pred = Expr::eq(Expr::column("users", "id", 0), Expr::literal(42i64));
        let info = pass.analyze_predicate(&pred).unwrap();

        assert_eq!(info.table, "users");
        assert_eq!(info.column, "id");
        assert!(info.is_point_lookup);
        assert!(!info.is_range);
    }

    #[test]
    fn test_analyze_range_predicate() {
        let pass = IndexSelection::new();

        let pred = Expr::gt(Expr::column("orders", "amount", 0), Expr::literal(100i64));
        let info = pass.analyze_predicate(&pred).unwrap();

        assert_eq!(info.table, "orders");
        assert_eq!(info.column, "amount");
        assert!(!info.is_point_lookup);
        assert!(info.is_range);
        assert_eq!(info.op, BinaryOp::Gt);
    }

    #[test]
    fn test_extract_compound_predicates() {
        let pass = IndexSelection::new();

        let pred = Expr::and(
            Expr::eq(Expr::column("users", "id", 0), Expr::literal(1i64)),
            Expr::gt(Expr::column("users", "age", 1), Expr::literal(18i64)),
        );

        let predicates = pass.extract_predicates(&pred);
        assert_eq!(predicates.len(), 2);
        assert_eq!(predicates[0].column, "id");
        assert_eq!(predicates[1].column, "age");
    }

    #[test]
    fn test_no_index_available() {
        let mut ctx = ExecutionContext::new();
        ctx.register_table(
            "users",
            TableStats {
                row_count: 1000,
                is_sorted: false,
                indexes: alloc::vec![], // No indexes
            },
        );

        let pass = IndexSelection::with_context(ctx);

        let plan = LogicalPlan::filter(
            LogicalPlan::scan("users"),
            Expr::eq(Expr::column("users", "id", 0), Expr::literal(1i64)),
        );

        let optimized = pass.optimize(plan);
        // Without matching index, should remain as Filter
        assert!(matches!(optimized, LogicalPlan::Filter { .. }));
    }

    #[test]
    fn test_in_query_index_selection() {
        let mut ctx = ExecutionContext::new();
        ctx.register_table(
            "users",
            TableStats {
                row_count: 1000,
                is_sorted: false,
                indexes: alloc::vec![IndexInfo::new(
                    "idx_id",
                    alloc::vec!["id".into()],
                    true
                )],
            },
        );

        let pass = IndexSelection::with_context(ctx);

        // Filter: id IN (1, 2, 3)
        let plan = LogicalPlan::filter(
            LogicalPlan::scan("users"),
            Expr::In {
                expr: Box::new(Expr::column("users", "id", 0)),
                list: alloc::vec![
                    Expr::literal(Value::Int64(1)),
                    Expr::literal(Value::Int64(2)),
                    Expr::literal(Value::Int64(3)),
                ],
            },
        );

        let optimized = pass.optimize(plan);
        // Should convert to IndexInGet for IN query with indexed column
        assert!(matches!(optimized, LogicalPlan::IndexInGet { .. }));

        if let LogicalPlan::IndexInGet { table, index, keys } = optimized {
            assert_eq!(table, "users");
            assert_eq!(index, "idx_id");
            assert_eq!(keys.len(), 3);
        }
    }

    #[test]
    fn test_in_query_no_index() {
        let mut ctx = ExecutionContext::new();
        ctx.register_table(
            "users",
            TableStats {
                row_count: 1000,
                is_sorted: false,
                indexes: alloc::vec![], // No indexes
            },
        );

        let pass = IndexSelection::with_context(ctx);

        // Filter: id IN (1, 2, 3) but no index available
        let plan = LogicalPlan::filter(
            LogicalPlan::scan("users"),
            Expr::In {
                expr: Box::new(Expr::column("users", "id", 0)),
                list: alloc::vec![
                    Expr::literal(Value::Int64(1)),
                    Expr::literal(Value::Int64(2)),
                ],
            },
        );

        let optimized = pass.optimize(plan);
        // Without index, should remain as Filter
        assert!(matches!(optimized, LogicalPlan::Filter { .. }));
    }

    #[test]
    fn test_analyze_in_predicate() {
        let pass = IndexSelection::new();

        let pred = Expr::In {
            expr: Box::new(Expr::column("users", "id", 0)),
            list: alloc::vec![
                Expr::literal(Value::Int64(1)),
                Expr::literal(Value::Int64(2)),
                Expr::literal(Value::Int64(3)),
            ],
        };

        let info = pass.analyze_in_predicate(&pred).unwrap();
        assert_eq!(info.table, "users");
        assert_eq!(info.column, "id");
        assert_eq!(info.values.len(), 3);
        assert_eq!(info.values[0], Value::Int64(1));
        assert_eq!(info.values[1], Value::Int64(2));
        assert_eq!(info.values[2], Value::Int64(3));
    }

    #[test]
    fn test_analyze_in_predicate_with_non_literals() {
        let pass = IndexSelection::new();

        // IN list with non-literal expression should not be optimized
        let pred = Expr::In {
            expr: Box::new(Expr::column("users", "id", 0)),
            list: alloc::vec![
                Expr::literal(Value::Int64(1)),
                Expr::column("other", "val", 0), // Non-literal
            ],
        };

        let info = pass.analyze_in_predicate(&pred);
        assert!(info.is_none());
    }

    /// Test case for bug: mixed GIN + non-GIN predicates should preserve non-GIN predicates
    ///
    /// Query: col('status').eq('published') AND col('tags').get('$.primary').eq('tech')
    ///
    /// Expected behcynos:
    /// - The GIN predicate (tags.primary = 'tech') should use GinIndexScan
    /// - The non-GIN predicate (status = 'published') should be preserved as a Filter
    ///
    /// Bug: Currently the non-GIN predicate is completely dropped!
    #[test]
    fn test_mixed_gin_and_non_gin_predicates_bug() {
        let mut ctx = ExecutionContext::new();
        ctx.register_table(
            "documents",
            TableStats {
                row_count: 1000,
                is_sorted: false,
                indexes: alloc::vec![
                    // GIN index on 'tags' column (JSONB)
                    IndexInfo::new_gin("idx_tags", alloc::vec!["tags".into()]),
                    // B-Tree index on 'status' column
                    IndexInfo::new("idx_status", alloc::vec!["status".into()], false),
                ],
            },
        );

        let pass = IndexSelection::with_context(ctx);

        // Build predicate: status = 'published' AND JSONB_PATH_EQ(tags, '$.primary', 'tech')
        // This simulates: col('status').eq('published').and(col('tags').get('$.primary').eq('tech'))
        let predicate = Expr::and(
            // Non-GIN predicate: status = 'published'
            Expr::eq(
                Expr::column("documents", "status", 1),
                Expr::literal(Value::String("published".into())),
            ),
            // GIN predicate: JSONB_PATH_EQ(tags, '$.primary', 'tech')
            Expr::Function {
                name: "JSONB_PATH_EQ".into(),
                args: alloc::vec![
                    Expr::column("documents", "tags", 2),
                    Expr::literal(Value::String("$.primary".into())),
                    Expr::literal(Value::String("tech".into())),
                ],
            },
        );

        let plan = LogicalPlan::filter(LogicalPlan::scan("documents"), predicate);

        let optimized = pass.optimize(plan);

        // BUG: Currently this returns just GinIndexScan, dropping the status = 'published' predicate!
        // The correct behcynos should be:
        // Filter(status = 'published', GinIndexScan(tags, '$.primary', 'tech'))

        // This assertion currently FAILS - demonstrating the bug
        // The optimized plan should be a Filter wrapping a GinIndexScan
        match &optimized {
            LogicalPlan::Filter { input, predicate } => {
                // Good: we have a Filter
                // Check that the input is a GinIndexScan
                assert!(
                    matches!(input.as_ref(), LogicalPlan::GinIndexScan { .. }),
                    "Expected GinIndexScan as input to Filter, got: {:?}",
                    input
                );
                // Check that the predicate is the non-GIN predicate (status = 'published')
                if let Expr::BinaryOp { left, op, right } = predicate {
                    assert_eq!(*op, BinaryOp::Eq);
                    if let Expr::Column(col) = left.as_ref() {
                        assert_eq!(col.column, "status");
                    } else {
                        panic!("Expected column reference in predicate left side");
                    }
                } else {
                    panic!("Expected BinaryOp predicate");
                }
            }
            LogicalPlan::GinIndexScan { .. } => {
                // BUG: This is what currently happens - the status predicate is dropped!
                panic!(
                    "BUG CONFIRMED: Non-GIN predicate (status = 'published') was dropped! \
                     The query will return incorrect results - all rows matching \
                     tags.primary = 'tech' regardless of status."
                );
            }
            other => {
                panic!("Unexpected plan type: {:?}", other);
            }
        }
    }

    /// Test case for bug: B-Tree index should work with AND predicates
    ///
    /// Query: price > 100 AND category = 'Electronics'
    ///
    /// Expected behcynos:
    /// - The indexed predicate (price > 100) should use IndexScan
    /// - The non-indexed predicate (category = 'Electronics') should be preserved as a Filter
    ///
    /// Bug: Currently the entire AND predicate fails to use any index because
    /// analyze_predicate only handles simple predicates, not AND combinations.
    #[test]
    fn test_btree_index_with_and_predicates_bug() {
        let mut ctx = ExecutionContext::new();
        ctx.register_table(
            "products",
            TableStats {
                row_count: 10000,
                is_sorted: false,
                indexes: alloc::vec![
                    // B-Tree index on 'price' column
                    IndexInfo::new("idx_price", alloc::vec!["price".into()], false),
                ],
            },
        );

        let pass = IndexSelection::with_context(ctx);

        // Build predicate: price > 100 AND category = 'Electronics'
        let predicate = Expr::and(
            // Indexed predicate: price > 100
            Expr::gt(
                Expr::column("products", "price", 1),
                Expr::literal(Value::Int64(100)),
            ),
            // Non-indexed predicate: category = 'Electronics'
            Expr::eq(
                Expr::column("products", "category", 2),
                Expr::literal(Value::String("Electronics".into())),
            ),
        );

        let plan = LogicalPlan::filter(LogicalPlan::scan("products"), predicate);

        let optimized = pass.optimize(plan);

        // The correct behcynos should be:
        // Filter(category = 'Electronics', IndexScan(price > 100))
        //
        // But currently it returns:
        // Filter(price > 100 AND category = 'Electronics', Scan(products))
        // because analyze_predicate returns None for AND expressions

        match &optimized {
            LogicalPlan::Filter { input, predicate: _ } => {
                match input.as_ref() {
                    LogicalPlan::IndexScan { index, .. } => {
                        // Good: we're using IndexScan
                        assert_eq!(index, "idx_price");
                    }
                    LogicalPlan::Scan { .. } => {
                        // BUG: Index is not being used!
                        panic!(
                            "BUG CONFIRMED: B-Tree index (idx_price) is not used for AND predicates! \
                             The query falls back to full table scan even though price > 100 \
                             could use the index."
                        );
                    }
                    other => {
                        panic!("Unexpected input plan type: {:?}", other);
                    }
                }
            }
            LogicalPlan::IndexScan { .. } => {
                // This would be acceptable too (if the other predicate is somehow handled)
            }
            other => {
                panic!("Unexpected plan type: {:?}", other);
            }
        }
    }

    /// Test that point lookups (IndexGet) are prioritized over range scans (IndexScan)
    /// when both are available in an AND predicate.
    #[test]
    fn test_btree_and_prioritizes_point_lookup() {
        let mut ctx = ExecutionContext::new();
        ctx.register_table(
            "products",
            TableStats {
                row_count: 10000,
                is_sorted: false,
                indexes: alloc::vec![
                    // B-Tree index on 'price' column (for range scan)
                    IndexInfo::new("idx_price", alloc::vec!["price".into()], false),
                    // B-Tree index on 'id' column (for point lookup)
                    IndexInfo::new("idx_id", alloc::vec!["id".into()], true),
                ],
            },
        );

        let pass = IndexSelection::with_context(ctx);

        // Build predicate: price > 100 AND id = 42
        // Both predicates are indexable, but id = 42 should be preferred (point lookup)
        let predicate = Expr::and(
            // Range predicate: price > 100
            Expr::gt(
                Expr::column("products", "price", 1),
                Expr::literal(Value::Int64(100)),
            ),
            // Point lookup predicate: id = 42
            Expr::eq(
                Expr::column("products", "id", 0),
                Expr::literal(Value::Int64(42)),
            ),
        );

        let plan = LogicalPlan::filter(LogicalPlan::scan("products"), predicate);

        let optimized = pass.optimize(plan);

        // Should use IndexGet for id = 42 (point lookup) and Filter for price > 100
        match &optimized {
            LogicalPlan::Filter { input, predicate } => {
                // Check that we're using IndexGet (point lookup)
                match input.as_ref() {
                    LogicalPlan::IndexGet { index, key, .. } => {
                        assert_eq!(index, "idx_id");
                        assert_eq!(*key, Value::Int64(42));
                    }
                    other => {
                        panic!("Expected IndexGet, got: {:?}", other);
                    }
                }
                // Check that the remaining predicate is price > 100
                if let Expr::BinaryOp { left, op, right } = predicate {
                    assert_eq!(*op, BinaryOp::Gt);
                    if let Expr::Column(col) = left.as_ref() {
                        assert_eq!(col.column, "price");
                    }
                    if let Expr::Literal(Value::Int64(v)) = right.as_ref() {
                        assert_eq!(*v, 100);
                    }
                }
            }
            LogicalPlan::IndexGet { .. } => {
                // Also acceptable if there's no remaining predicate
            }
            other => {
                panic!("Unexpected plan type: {:?}", other);
            }
        }
    }

    /// Test: Multiple range predicates on the same column should be merged.
    ///
    /// Query: price > 10 AND price < 150
    ///
    /// Expected: IndexScan with range (10, 150) - no Filter needed
    #[test]
    fn test_range_merge_same_column() {
        let mut ctx = ExecutionContext::new();
        ctx.register_table(
            "stocks",
            TableStats {
                row_count: 10000,
                is_sorted: false,
                indexes: alloc::vec![IndexInfo::new(
                    "idx_price",
                    alloc::vec!["price".into()],
                    false
                )],
            },
        );

        let pass = IndexSelection::with_context(ctx);

        // Build predicate: price > 10 AND price < 150
        let predicate = Expr::and(
            Expr::gt(
                Expr::column("stocks", "price", 1),
                Expr::literal(Value::Float64(10.0)),
            ),
            Expr::lt(
                Expr::column("stocks", "price", 1),
                Expr::literal(Value::Float64(150.0)),
            ),
        );

        let plan = LogicalPlan::filter(LogicalPlan::scan("stocks"), predicate);
        let optimized = pass.optimize(plan);

        // Should be IndexScan with merged range (10, 150) - no Filter
        match optimized {
            LogicalPlan::IndexScan {
                index,
                range_start,
                range_end,
                include_start,
                include_end,
                ..
            } => {
                assert_eq!(index, "idx_price");
                assert_eq!(range_start, Some(Value::Float64(10.0)));
                assert_eq!(range_end, Some(Value::Float64(150.0)));
                assert!(!include_start, "Lower bound should be exclusive (>)");
                assert!(!include_end, "Upper bound should be exclusive (<)");
            }
            other => {
                panic!(
                    "Expected IndexScan with merged range, got: {:?}",
                    other
                );
            }
        }
    }

    /// Test: Range merge with inclusive bounds (>= and <=)
    #[test]
    fn test_range_merge_inclusive_bounds() {
        let mut ctx = ExecutionContext::new();
        ctx.register_table(
            "products",
            TableStats {
                row_count: 10000,
                is_sorted: false,
                indexes: alloc::vec![IndexInfo::new(
                    "idx_price",
                    alloc::vec!["price".into()],
                    false
                )],
            },
        );

        let pass = IndexSelection::with_context(ctx);

        // Build predicate: price >= 100 AND price <= 500
        let predicate = Expr::and(
            Expr::ge(
                Expr::column("products", "price", 1),
                Expr::literal(Value::Int64(100)),
            ),
            Expr::le(
                Expr::column("products", "price", 1),
                Expr::literal(Value::Int64(500)),
            ),
        );

        let plan = LogicalPlan::filter(LogicalPlan::scan("products"), predicate);
        let optimized = pass.optimize(plan);

        match optimized {
            LogicalPlan::IndexScan {
                range_start,
                range_end,
                include_start,
                include_end,
                ..
            } => {
                assert_eq!(range_start, Some(Value::Int64(100)));
                assert_eq!(range_end, Some(Value::Int64(500)));
                assert!(include_start, "Lower bound should be inclusive (>=)");
                assert!(include_end, "Upper bound should be inclusive (<=)");
            }
            other => {
                panic!("Expected IndexScan, got: {:?}", other);
            }
        }
    }

    /// Test: Range merge takes the more restrictive bound when same value
    /// price > 10 AND price >= 10 should result in price > 10 (exclusive)
    #[test]
    fn test_range_merge_same_value_takes_exclusive() {
        let mut ctx = ExecutionContext::new();
        ctx.register_table(
            "products",
            TableStats {
                row_count: 10000,
                is_sorted: false,
                indexes: alloc::vec![IndexInfo::new(
                    "idx_price",
                    alloc::vec!["price".into()],
                    false
                )],
            },
        );

        let pass = IndexSelection::with_context(ctx);

        // Build predicate: price > 10 AND price >= 10
        let predicate = Expr::and(
            Expr::gt(
                Expr::column("products", "price", 1),
                Expr::literal(Value::Int64(10)),
            ),
            Expr::ge(
                Expr::column("products", "price", 1),
                Expr::literal(Value::Int64(10)),
            ),
        );

        let plan = LogicalPlan::filter(LogicalPlan::scan("products"), predicate);
        let optimized = pass.optimize(plan);

        match optimized {
            LogicalPlan::IndexScan {
                range_start,
                include_start,
                ..
            } => {
                assert_eq!(range_start, Some(Value::Int64(10)));
                assert!(!include_start, "Should take exclusive bound (>)");
            }
            other => {
                panic!("Expected IndexScan, got: {:?}", other);
            }
        }
    }

    /// Test: Range merge takes the larger lower bound
    /// price > 10 AND price > 5 should result in price > 10
    #[test]
    fn test_range_merge_takes_larger_lower_bound() {
        let mut ctx = ExecutionContext::new();
        ctx.register_table(
            "products",
            TableStats {
                row_count: 10000,
                is_sorted: false,
                indexes: alloc::vec![IndexInfo::new(
                    "idx_price",
                    alloc::vec!["price".into()],
                    false
                )],
            },
        );

        let pass = IndexSelection::with_context(ctx);

        // Build predicate: price > 10 AND price > 5
        let predicate = Expr::and(
            Expr::gt(
                Expr::column("products", "price", 1),
                Expr::literal(Value::Int64(10)),
            ),
            Expr::gt(
                Expr::column("products", "price", 1),
                Expr::literal(Value::Int64(5)),
            ),
        );

        let plan = LogicalPlan::filter(LogicalPlan::scan("products"), predicate);
        let optimized = pass.optimize(plan);

        match optimized {
            LogicalPlan::IndexScan { range_start, .. } => {
                assert_eq!(range_start, Some(Value::Int64(10)));
            }
            other => {
                panic!("Expected IndexScan, got: {:?}", other);
            }
        }
    }

    /// Test: Range merge takes the smaller upper bound
    /// price < 150 AND price < 200 should result in price < 150
    #[test]
    fn test_range_merge_takes_smaller_upper_bound() {
        let mut ctx = ExecutionContext::new();
        ctx.register_table(
            "products",
            TableStats {
                row_count: 10000,
                is_sorted: false,
                indexes: alloc::vec![IndexInfo::new(
                    "idx_price",
                    alloc::vec!["price".into()],
                    false
                )],
            },
        );

        let pass = IndexSelection::with_context(ctx);

        // Build predicate: price < 150 AND price < 200
        let predicate = Expr::and(
            Expr::lt(
                Expr::column("products", "price", 1),
                Expr::literal(Value::Int64(150)),
            ),
            Expr::lt(
                Expr::column("products", "price", 1),
                Expr::literal(Value::Int64(200)),
            ),
        );

        let plan = LogicalPlan::filter(LogicalPlan::scan("products"), predicate);
        let optimized = pass.optimize(plan);

        match optimized {
            LogicalPlan::IndexScan { range_end, .. } => {
                assert_eq!(range_end, Some(Value::Int64(150)));
            }
            other => {
                panic!("Expected IndexScan, got: {:?}", other);
            }
        }
    }

    /// Test: Range merge with additional non-indexed predicate
    /// price > 10 AND price < 150 AND status = 'active'
    /// Should produce: Filter(status = 'active', IndexScan(10, 150))
    #[test]
    fn test_range_merge_with_non_indexed_predicate() {
        let mut ctx = ExecutionContext::new();
        ctx.register_table(
            "stocks",
            TableStats {
                row_count: 10000,
                is_sorted: false,
                indexes: alloc::vec![IndexInfo::new(
                    "idx_price",
                    alloc::vec!["price".into()],
                    false
                )],
            },
        );

        let pass = IndexSelection::with_context(ctx);

        // Build predicate: price > 10 AND price < 150 AND status = 'active'
        let predicate = Expr::and(
            Expr::and(
                Expr::gt(
                    Expr::column("stocks", "price", 1),
                    Expr::literal(Value::Float64(10.0)),
                ),
                Expr::lt(
                    Expr::column("stocks", "price", 1),
                    Expr::literal(Value::Float64(150.0)),
                ),
            ),
            Expr::eq(
                Expr::column("stocks", "status", 2),
                Expr::literal(Value::String("active".into())),
            ),
        );

        let plan = LogicalPlan::filter(LogicalPlan::scan("stocks"), predicate);
        let optimized = pass.optimize(plan);

        // Should be Filter(status = 'active', IndexScan(10, 150))
        match optimized {
            LogicalPlan::Filter { input, predicate } => {
                // Check the Filter predicate is status = 'active'
                if let Expr::BinaryOp { left, op, .. } = &predicate {
                    assert_eq!(*op, BinaryOp::Eq);
                    if let Expr::Column(col) = left.as_ref() {
                        assert_eq!(col.column, "status");
                    }
                }

                // Check the input is IndexScan with merged range
                match input.as_ref() {
                    LogicalPlan::IndexScan {
                        range_start,
                        range_end,
                        include_start,
                        include_end,
                        ..
                    } => {
                        assert_eq!(*range_start, Some(Value::Float64(10.0)));
                        assert_eq!(*range_end, Some(Value::Float64(150.0)));
                        assert!(!include_start);
                        assert!(!include_end);
                    }
                    other => {
                        panic!("Expected IndexScan inside Filter, got: {:?}", other);
                    }
                }
            }
            other => {
                panic!("Expected Filter wrapping IndexScan, got: {:?}", other);
            }
        }
    }
}
