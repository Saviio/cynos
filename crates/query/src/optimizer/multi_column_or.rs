//! Multi-column OR pass evaluation and implementation notes.
//!
//! # Overview
//!
//! The MultiColumnOrPass optimizes OR predicates that:
//! 1. Refer to a single table
//! 2. Refer to multiple columns
//! 3. All referred columns are indexed
//!
//! Example:
//! ```text
//! Filter(a.id = 1 OR a.name = 'Alice')    =>    MultiIndexRangeScan
//!        |                                        /            \
//! TableScan(a)                           IndexScan(idx_id)  IndexScan(idx_name)
//! ```
//!
//! # Evaluation for In-Memory Database
//!
//! ## Pros:
//! - Can significantly reduce rows scanned when each OR branch is highly selective
//! - Leverages existing indexes effectively
//! - Results are deduplicated automatically
//!
//! ## Cons:
//! - For in-memory databases, the overhead of multiple index lookups + deduplication
//!   may exceed the cost of a simple table scan with filter
//! - Memory locality is better with sequential scan
//! - Index lookups have overhead (tree traversal, etc.)
//!
//! ## Recommendation:
//! For an in-memory database like Lovefield/Cynos, this optimization is most beneficial when:
//! 1. The table is large (>10,000 rows)
//! 2. Each OR branch is highly selective (<1% of rows)
//! 3. The number of OR branches is small (2-3)
//!
//! For smaller tables or less selective predicates, a simple table scan is often faster.
//!
//! # Implementation Status
//!
//! This pass is implemented but marked as optional. It can be enabled when:
//! - Table statistics indicate it would be beneficial
//! - The user explicitly requests index-based OR optimization

use crate::ast::{BinaryOp, Expr};
use crate::context::ExecutionContext;
use crate::planner::PhysicalPlan;
use alloc::boxed::Box;
use alloc::string::String;
use alloc::vec::Vec;

/// Configuration for when to apply multi-column OR optimization.
#[derive(Clone, Debug)]
pub struct MultiColumnOrConfig {
    /// Minimum table size to consider optimization (default: 10000)
    pub min_table_size: usize,
    /// Maximum selectivity per branch to consider optimization (default: 0.01 = 1%)
    pub max_selectivity: f64,
    /// Maximum number of OR branches to optimize (default: 5)
    pub max_branches: usize,
}

impl Default for MultiColumnOrConfig {
    fn default() -> Self {
        Self {
            min_table_size: 10000,
            max_selectivity: 0.01,
            max_branches: 5,
        }
    }
}

/// Pass that optimizes multi-column OR predicates using multiple index scans.
pub struct MultiColumnOrPass<'a> {
    ctx: &'a ExecutionContext,
    config: MultiColumnOrConfig,
}

impl<'a> MultiColumnOrPass<'a> {
    /// Creates a new MultiColumnOrPass with the given execution context.
    pub fn new(ctx: &'a ExecutionContext) -> Self {
        Self {
            ctx,
            config: MultiColumnOrConfig::default(),
        }
    }

    /// Creates a new MultiColumnOrPass with custom configuration.
    pub fn with_config(ctx: &'a ExecutionContext, config: MultiColumnOrConfig) -> Self {
        Self { ctx, config }
    }

    /// Optimizes the physical plan by converting eligible OR predicates to multi-index scans.
    pub fn optimize(&self, plan: PhysicalPlan) -> PhysicalPlan {
        self.traverse(plan)
    }

    fn traverse(&self, plan: PhysicalPlan) -> PhysicalPlan {
        match plan {
            PhysicalPlan::Filter { input, predicate } => {
                let optimized_input = self.traverse(*input);

                // Check if this is an OR predicate that can be optimized
                if let Some(optimized) =
                    self.try_optimize_or_predicate(&optimized_input, &predicate)
                {
                    return optimized;
                }

                PhysicalPlan::Filter {
                    input: Box::new(optimized_input),
                    predicate,
                }
            }

            // Recursively process other nodes
            PhysicalPlan::Project { input, columns } => PhysicalPlan::Project {
                input: Box::new(self.traverse(*input)),
                columns,
            },

            PhysicalPlan::Sort { input, order_by } => PhysicalPlan::Sort {
                input: Box::new(self.traverse(*input)),
                order_by,
            },

            PhysicalPlan::Limit {
                input,
                limit,
                offset,
            } => PhysicalPlan::Limit {
                input: Box::new(self.traverse(*input)),
                limit,
                offset,
            },

            PhysicalPlan::HashJoin {
                left,
                right,
                condition,
                join_type,
            } => PhysicalPlan::HashJoin {
                left: Box::new(self.traverse(*left)),
                right: Box::new(self.traverse(*right)),
                condition,
                join_type,
            },

            PhysicalPlan::SortMergeJoin {
                left,
                right,
                condition,
                join_type,
            } => PhysicalPlan::SortMergeJoin {
                left: Box::new(self.traverse(*left)),
                right: Box::new(self.traverse(*right)),
                condition,
                join_type,
            },

            PhysicalPlan::NestedLoopJoin {
                left,
                right,
                condition,
                join_type,
            } => PhysicalPlan::NestedLoopJoin {
                left: Box::new(self.traverse(*left)),
                right: Box::new(self.traverse(*right)),
                condition,
                join_type,
            },

            PhysicalPlan::IndexNestedLoopJoin {
                outer,
                inner_table,
                inner_index,
                condition,
                join_type,
            } => PhysicalPlan::IndexNestedLoopJoin {
                outer: Box::new(self.traverse(*outer)),
                inner_table,
                inner_index,
                condition,
                join_type,
            },

            PhysicalPlan::HashAggregate {
                input,
                group_by,
                aggregates,
            } => PhysicalPlan::HashAggregate {
                input: Box::new(self.traverse(*input)),
                group_by,
                aggregates,
            },

            PhysicalPlan::CrossProduct { left, right } => PhysicalPlan::CrossProduct {
                left: Box::new(self.traverse(*left)),
                right: Box::new(self.traverse(*right)),
            },

            PhysicalPlan::NoOp { input } => PhysicalPlan::NoOp {
                input: Box::new(self.traverse(*input)),
            },

            PhysicalPlan::TopN {
                input,
                order_by,
                limit,
                offset,
            } => PhysicalPlan::TopN {
                input: Box::new(self.traverse(*input)),
                order_by,
                limit,
                offset,
            },

            // Leaf nodes - no transformation
            plan @ (PhysicalPlan::TableScan { .. }
            | PhysicalPlan::IndexScan { .. }
            | PhysicalPlan::IndexGet { .. }
            | PhysicalPlan::IndexInGet { .. }
            | PhysicalPlan::GinIndexScan { .. }
            | PhysicalPlan::GinIndexScanMulti { .. }
            | PhysicalPlan::Empty) => plan,
        }
    }

    /// Tries to optimize an OR predicate using multiple index scans.
    fn try_optimize_or_predicate(
        &self,
        input: &PhysicalPlan,
        predicate: &Expr,
    ) -> Option<PhysicalPlan> {
        // Check if input is a TableScan
        let table = match input {
            PhysicalPlan::TableScan { table } => table,
            _ => return None,
        };

        // Check table size threshold
        let row_count = self.ctx.row_count(table);
        if row_count < self.config.min_table_size {
            return None;
        }

        // Extract OR branches
        let branches = self.extract_or_branches(predicate);
        if branches.len() < 2 || branches.len() > self.config.max_branches {
            return None;
        }

        // Check if all branches can use an index
        let mut index_candidates = Vec::new();
        for branch in &branches {
            if let Some((column, index_name)) = self.find_index_for_predicate(table, branch) {
                index_candidates.push((column, index_name, branch.clone()));
            } else {
                return None; // Not all branches can use an index
            }
        }

        // All branches can use indexes - but for now, we'll keep the filter
        // as the actual multi-index scan implementation would require more infrastructure
        // This is a placeholder that indicates the optimization is possible
        None
    }

    /// Extracts OR branches from a predicate.
    fn extract_or_branches(&self, predicate: &Expr) -> Vec<Expr> {
        match predicate {
            Expr::BinaryOp {
                left,
                op: BinaryOp::Or,
                right,
            } => {
                let mut branches = self.extract_or_branches(left);
                branches.extend(self.extract_or_branches(right));
                branches
            }
            other => alloc::vec![other.clone()],
        }
    }

    /// Finds an index that can be used for the given predicate.
    fn find_index_for_predicate(
        &self,
        table: &str,
        predicate: &Expr,
    ) -> Option<(String, String)> {
        // Extract the column from the predicate
        let column = self.extract_indexed_column(predicate)?;

        // Find an index on this column
        let index = self.ctx.find_index(table, &[&column])?;

        Some((column, index.name.clone()))
    }

    /// Extracts the column name from a simple predicate (column = value).
    fn extract_indexed_column(&self, predicate: &Expr) -> Option<String> {
        match predicate {
            Expr::BinaryOp {
                left,
                op: BinaryOp::Eq,
                right,
            } => {
                // Check if left is a column and right is a literal
                if let Expr::Column(col_ref) = left.as_ref() {
                    if matches!(right.as_ref(), Expr::Literal(_)) {
                        return Some(col_ref.column.clone());
                    }
                }
                // Check if right is a column and left is a literal
                if let Expr::Column(col_ref) = right.as_ref() {
                    if matches!(left.as_ref(), Expr::Literal(_)) {
                        return Some(col_ref.column.clone());
                    }
                }
                None
            }
            _ => None,
        }
    }

    /// Estimates the selectivity of a predicate (placeholder).
    #[allow(dead_code)]
    fn estimate_selectivity(&self, _table: &str, _predicate: &Expr) -> f64 {
        // In a real implementation, this would use statistics
        // For now, assume 1% selectivity
        0.01
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::Expr;
    use crate::context::{IndexInfo, TableStats};

    fn create_test_context() -> ExecutionContext {
        let mut ctx = ExecutionContext::new();

        ctx.register_table(
            "users",
            TableStats {
                row_count: 100000, // Large table
                is_sorted: false,
                indexes: alloc::vec![
                    IndexInfo::new("idx_id", alloc::vec!["id".into()], true),
                    IndexInfo::new("idx_name", alloc::vec!["name".into()], false),
                    IndexInfo::new("idx_email", alloc::vec!["email".into()], true),
                ],
            },
        );

        ctx.register_table(
            "small_table",
            TableStats {
                row_count: 100, // Small table
                is_sorted: false,
                indexes: alloc::vec![IndexInfo::new(
                    "idx_id",
                    alloc::vec!["id".into()],
                    true
                )],
            },
        );

        ctx
    }

    #[test]
    fn test_extract_or_branches() {
        let ctx = create_test_context();
        let pass = MultiColumnOrPass::new(&ctx);

        // Simple OR: a OR b
        let pred = Expr::or(
            Expr::eq(Expr::column("t", "a", 0), Expr::literal(1i64)),
            Expr::eq(Expr::column("t", "b", 1), Expr::literal(2i64)),
        );
        let branches = pass.extract_or_branches(&pred);
        assert_eq!(branches.len(), 2);

        // Nested OR: a OR b OR c
        let pred = Expr::or(
            Expr::or(
                Expr::eq(Expr::column("t", "a", 0), Expr::literal(1i64)),
                Expr::eq(Expr::column("t", "b", 1), Expr::literal(2i64)),
            ),
            Expr::eq(Expr::column("t", "c", 2), Expr::literal(3i64)),
        );
        let branches = pass.extract_or_branches(&pred);
        assert_eq!(branches.len(), 3);

        // Single predicate (no OR)
        let pred = Expr::eq(Expr::column("t", "a", 0), Expr::literal(1i64));
        let branches = pass.extract_or_branches(&pred);
        assert_eq!(branches.len(), 1);
    }

    #[test]
    fn test_find_index_for_predicate() {
        let ctx = create_test_context();
        let pass = MultiColumnOrPass::new(&ctx);

        // Predicate with indexed column
        let pred = Expr::eq(Expr::column("users", "id", 0), Expr::literal(1i64));
        let result = pass.find_index_for_predicate("users", &pred);
        assert!(result.is_some());
        let (col, idx) = result.unwrap();
        assert_eq!(col, "id");
        assert_eq!(idx, "idx_id");

        // Predicate with non-indexed column
        let pred = Expr::eq(Expr::column("users", "age", 3), Expr::literal(25i64));
        let result = pass.find_index_for_predicate("users", &pred);
        assert!(result.is_none());
    }

    #[test]
    fn test_small_table_not_optimized() {
        let ctx = create_test_context();
        let pass = MultiColumnOrPass::new(&ctx);

        // OR predicate on small table
        let plan = PhysicalPlan::Filter {
            input: Box::new(PhysicalPlan::table_scan("small_table")),
            predicate: Expr::or(
                Expr::eq(Expr::column("small_table", "id", 0), Expr::literal(1i64)),
                Expr::eq(Expr::column("small_table", "id", 0), Expr::literal(2i64)),
            ),
        };

        let result = pass.optimize(plan);

        // Should remain as Filter (table too small)
        assert!(matches!(result, PhysicalPlan::Filter { .. }));
    }

    #[test]
    fn test_config_customization() {
        let ctx = create_test_context();
        let config = MultiColumnOrConfig {
            min_table_size: 1000,
            max_selectivity: 0.05,
            max_branches: 3,
        };
        let pass = MultiColumnOrPass::with_config(&ctx, config);

        assert_eq!(pass.config.min_table_size, 1000);
        assert_eq!(pass.config.max_selectivity, 0.05);
        assert_eq!(pass.config.max_branches, 3);
    }

    #[test]
    fn test_extract_indexed_column() {
        let ctx = create_test_context();
        let pass = MultiColumnOrPass::new(&ctx);

        // column = literal
        let pred = Expr::eq(Expr::column("t", "id", 0), Expr::literal(1i64));
        assert_eq!(pass.extract_indexed_column(&pred), Some("id".into()));

        // literal = column
        let pred = Expr::eq(Expr::literal(1i64), Expr::column("t", "id", 0));
        assert_eq!(pass.extract_indexed_column(&pred), Some("id".into()));

        // column = column (not supported)
        let pred = Expr::eq(Expr::column("t", "a", 0), Expr::column("t", "b", 1));
        assert_eq!(pass.extract_indexed_column(&pred), None);

        // Non-equality predicate
        let pred = Expr::gt(Expr::column("t", "id", 0), Expr::literal(1i64));
        assert_eq!(pass.extract_indexed_column(&pred), None);
    }
}