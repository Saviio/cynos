//! Index join pass - converts eligible joins to index nested loop joins.
//!
//! This pass identifies Join nodes where one side has an index on the join column,
//! and converts them to IndexNestedLoopJoin for better performance.
//!
//! Example:
//! ```text
//! HashJoin(a.id = b.a_id)       =>    IndexNestedLoopJoin
//!    /            \                    outer: Scan(a)
//! Scan(a)      Scan(b)                 inner: b (using idx_a_id)
//! ```
//!
//! Index nested loop join is beneficial when:
//! 1. One side has an index on the join column
//! 2. The outer relation is small enough that index lookups are efficient
//! 3. The join is an inner equi-join

use crate::ast::{BinaryOp, ColumnRef, Expr, JoinType};
use crate::context::{ExecutionContext, IndexInfo};
use crate::planner::PhysicalPlan;
use alloc::boxed::Box;
use alloc::string::String;

const INDEX_JOIN_ALWAYS_OUTER_ROWS: usize = 64;
const INDEX_JOIN_MAX_OUTER_ROWS: usize = 4096;
const INDEX_JOIN_MIN_INNER_OUTER_RATIO: usize = 4;

/// Pass that converts eligible joins to index nested loop joins.
pub struct IndexJoinPass<'a> {
    ctx: &'a ExecutionContext,
}

impl<'a> IndexJoinPass<'a> {
    /// Creates a new IndexJoinPass with the given execution context.
    pub fn new(ctx: &'a ExecutionContext) -> Self {
        Self { ctx }
    }

    /// Optimizes the physical plan by converting eligible joins to index joins.
    pub fn optimize(&self, plan: PhysicalPlan) -> PhysicalPlan {
        self.traverse(plan)
    }

    fn traverse(&self, plan: PhysicalPlan) -> PhysicalPlan {
        match plan {
            // Check hash joins for index join optimization
            PhysicalPlan::HashJoin {
                left,
                right,
                condition,
                join_type,
                output_tables,
            } => {
                let left = self.traverse(*left);
                let right = self.traverse(*right);

                // Only optimize inner equi-joins
                if join_type != JoinType::Inner || !condition.is_equi_join() {
                    return PhysicalPlan::HashJoin {
                        left: Box::new(left),
                        right: Box::new(right),
                        condition,
                        join_type,
                        output_tables,
                    };
                }

                // Try to find an index on either side
                if let Some((outer, inner_table, inner_index, outer_is_left)) =
                    self.find_index_join_candidate(&left, &right, &condition)
                {
                    return PhysicalPlan::IndexNestedLoopJoin {
                        outer: Box::new(outer),
                        inner_table,
                        inner_index,
                        condition,
                        join_type,
                        outer_is_left,
                        output_tables,
                    };
                }

                PhysicalPlan::HashJoin {
                    left: Box::new(left),
                    right: Box::new(right),
                    condition,
                    join_type,
                    output_tables,
                }
            }

            // Also check nested loop joins
            PhysicalPlan::NestedLoopJoin {
                left,
                right,
                condition,
                join_type,
                output_tables,
            } => {
                let left = self.traverse(*left);
                let right = self.traverse(*right);

                // Only optimize inner equi-joins
                if join_type != JoinType::Inner || !condition.is_equi_join() {
                    return PhysicalPlan::NestedLoopJoin {
                        left: Box::new(left),
                        right: Box::new(right),
                        condition,
                        join_type,
                        output_tables,
                    };
                }

                // Try to find an index on either side
                if let Some((outer, inner_table, inner_index, outer_is_left)) =
                    self.find_index_join_candidate(&left, &right, &condition)
                {
                    return PhysicalPlan::IndexNestedLoopJoin {
                        outer: Box::new(outer),
                        inner_table,
                        inner_index,
                        condition,
                        join_type,
                        outer_is_left,
                        output_tables,
                    };
                }

                PhysicalPlan::NestedLoopJoin {
                    left: Box::new(left),
                    right: Box::new(right),
                    condition,
                    join_type,
                    output_tables,
                }
            }

            // Recursively process other nodes
            PhysicalPlan::Filter { input, predicate } => PhysicalPlan::Filter {
                input: Box::new(self.traverse(*input)),
                predicate,
            },

            PhysicalPlan::Project { input, columns } => PhysicalPlan::Project {
                input: Box::new(self.traverse(*input)),
                columns,
            },

            PhysicalPlan::SortMergeJoin {
                left,
                right,
                condition,
                join_type,
                output_tables,
            } => PhysicalPlan::SortMergeJoin {
                left: Box::new(self.traverse(*left)),
                right: Box::new(self.traverse(*right)),
                condition,
                join_type,
                output_tables,
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

            PhysicalPlan::CrossProduct { left, right } => PhysicalPlan::CrossProduct {
                left: Box::new(self.traverse(*left)),
                right: Box::new(self.traverse(*right)),
            },

            PhysicalPlan::Union { left, right, all } => PhysicalPlan::Union {
                left: Box::new(self.traverse(*left)),
                right: Box::new(self.traverse(*right)),
                all,
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

            // Leaf nodes and already-optimized nodes - no transformation
            plan @ (PhysicalPlan::TableScan { .. }
            | PhysicalPlan::IndexScan { .. }
            | PhysicalPlan::IndexGet { .. }
            | PhysicalPlan::IndexInGet { .. }
            | PhysicalPlan::IndexNestedLoopJoin { .. }
            | PhysicalPlan::Empty
            | PhysicalPlan::GinIndexScan { .. }
            | PhysicalPlan::GinIndexScanMulti { .. }) => plan,
        }
    }

    /// Finds a candidate for index join optimization.
    /// Returns (outer_plan, inner_table, inner_index) if found.
    fn find_index_join_candidate(
        &self,
        left: &PhysicalPlan,
        right: &PhysicalPlan,
        condition: &Expr,
    ) -> Option<(PhysicalPlan, String, String, bool)> {
        // Extract join columns from the condition
        let (left_col, right_col) = self.extract_join_columns(condition)?;

        // Check if right side is a table scan with an index on the join column
        if let Some((table, index)) = self.get_indexed_table_scan(right, right_col) {
            if self.should_use_index_join(left, &table, &index) {
                return Some((left.clone(), table, index.name.clone(), true));
            }
        }

        // Check if left side is a table scan with an index on the join column
        if let Some((table, index)) = self.get_indexed_table_scan(left, left_col) {
            if self.should_use_index_join(right, &table, &index) {
                return Some((right.clone(), table, index.name.clone(), false));
            }
        }

        None
    }

    /// Extracts the column names from an equi-join condition.
    fn extract_join_columns<'b>(
        &self,
        condition: &'b Expr,
    ) -> Option<(&'b ColumnRef, &'b ColumnRef)> {
        match condition {
            Expr::BinaryOp {
                left,
                op: BinaryOp::Eq,
                right,
            } => {
                let left_col = self.extract_column_ref(left)?;
                let right_col = self.extract_column_ref(right)?;
                Some((left_col, right_col))
            }
            _ => None,
        }
    }

    fn extract_column_ref<'b>(&self, expr: &'b Expr) -> Option<&'b ColumnRef> {
        match expr {
            Expr::Column(col_ref) => Some(col_ref),
            _ => None,
        }
    }

    /// Checks if the plan is a table scan with an index on the given column.
    /// Returns (table_name, index_info) if found.
    fn get_indexed_table_scan(
        &self,
        plan: &PhysicalPlan,
        column: &ColumnRef,
    ) -> Option<(String, IndexInfo)> {
        match plan {
            PhysicalPlan::TableScan { table } => {
                if table != &column.table {
                    return None;
                }
                // Check if there's an index on this column
                let index = self.ctx.find_index(table, &[column.column.as_str()])?;
                if index.is_gin() {
                    return None;
                }
                Some((table.clone(), index.clone()))
            }
            _ => None,
        }
    }

    fn should_use_index_join(
        &self,
        outer: &PhysicalPlan,
        inner_table: &str,
        inner_index: &IndexInfo,
    ) -> bool {
        if outer.collect_tables().len() != 1 {
            return false;
        }

        let outer_rows = self.estimate_rows(outer);
        let inner_rows = self
            .ctx
            .get_stats(inner_table)
            .map(|stats| stats.row_count)
            .unwrap_or(1000);

        if outer_rows == 0 || inner_rows == 0 {
            return false;
        }

        if outer_rows <= INDEX_JOIN_ALWAYS_OUTER_ROWS {
            return true;
        }

        if inner_index.is_unique {
            return outer_rows <= inner_rows;
        }

        outer_rows <= INDEX_JOIN_MAX_OUTER_ROWS
            && inner_rows >= outer_rows.saturating_mul(INDEX_JOIN_MIN_INNER_OUTER_RATIO)
    }

    fn estimate_rows(&self, plan: &PhysicalPlan) -> usize {
        match plan {
            PhysicalPlan::TableScan { table } => self
                .ctx
                .get_stats(table)
                .map(|stats| stats.row_count)
                .unwrap_or(1000),
            PhysicalPlan::IndexGet { .. } => 1,
            PhysicalPlan::IndexInGet { keys, .. } => keys.len(),
            PhysicalPlan::IndexScan { table, .. } | PhysicalPlan::GinIndexScan { table, .. } => {
                self.ctx
                    .get_stats(table)
                    .map(|stats| core::cmp::max(stats.row_count / 10, 1))
                    .unwrap_or(100)
            }
            PhysicalPlan::GinIndexScanMulti { table, .. } => self
                .ctx
                .get_stats(table)
                .map(|stats| core::cmp::max(stats.row_count / 20, 1))
                .unwrap_or(50),
            PhysicalPlan::Filter { input, .. } => core::cmp::max(self.estimate_rows(input) / 10, 1),
            PhysicalPlan::Project { input, .. }
            | PhysicalPlan::Sort { input, .. }
            | PhysicalPlan::NoOp { input } => self.estimate_rows(input),
            PhysicalPlan::Limit {
                input,
                limit,
                offset,
            }
            | PhysicalPlan::TopN {
                input,
                limit,
                offset,
                ..
            } => core::cmp::min(self.estimate_rows(input), limit.saturating_add(*offset)),
            PhysicalPlan::HashAggregate {
                input, group_by, ..
            } => {
                if group_by.is_empty() {
                    1
                } else {
                    core::cmp::max(self.estimate_rows(input) / 10, 1)
                }
            }
            PhysicalPlan::HashJoin { left, right, .. }
            | PhysicalPlan::SortMergeJoin { left, right, .. }
            | PhysicalPlan::NestedLoopJoin { left, right, .. }
            | PhysicalPlan::CrossProduct { left, right } => core::cmp::max(
                self.estimate_rows(left)
                    .saturating_mul(self.estimate_rows(right))
                    / 10,
                1,
            ),
            PhysicalPlan::Union { left, right, .. } => self
                .estimate_rows(left)
                .saturating_add(self.estimate_rows(right)),
            PhysicalPlan::IndexNestedLoopJoin { outer, .. } => self.estimate_rows(outer),
            PhysicalPlan::Empty => 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::Expr;
    use crate::context::{IndexInfo, TableStats};

    fn create_test_context() -> ExecutionContext {
        let mut ctx = ExecutionContext::new();

        // Table 'a' with no index
        ctx.register_table(
            "a",
            TableStats {
                row_count: 100,
                is_sorted: false,
                indexes: alloc::vec![],
            },
        );

        // Table 'b' with index on 'a_id'
        ctx.register_table(
            "b",
            TableStats {
                row_count: 1000,
                is_sorted: false,
                indexes: alloc::vec![IndexInfo::new(
                    "idx_a_id",
                    alloc::vec!["a_id".into()],
                    false
                )],
            },
        );

        ctx
    }

    #[test]
    fn test_hash_join_to_index_join() {
        let ctx = create_test_context();
        let pass = IndexJoinPass::new(&ctx);

        // Create: HashJoin(a.id = b.a_id, Scan(a), Scan(b))
        let plan = PhysicalPlan::hash_join(
            PhysicalPlan::table_scan("a"),
            PhysicalPlan::table_scan("b"),
            Expr::eq(Expr::column("a", "id", 0), Expr::column("b", "a_id", 0)),
            JoinType::Inner,
        );

        let result = pass.optimize(plan);

        // Should become IndexNestedLoopJoin
        assert!(matches!(result, PhysicalPlan::IndexNestedLoopJoin { .. }));
        if let PhysicalPlan::IndexNestedLoopJoin {
            inner_table,
            inner_index,
            ..
        } = result
        {
            assert_eq!(inner_table, "b");
            assert_eq!(inner_index, "idx_a_id");
        }
    }

    #[test]
    fn test_index_join_tracks_logical_left_when_outer_flips() {
        let mut ctx = ExecutionContext::new();
        ctx.register_table(
            "employees",
            TableStats {
                row_count: 1_000,
                is_sorted: false,
                indexes: alloc::vec![IndexInfo::new(
                    "idx_dept_id",
                    alloc::vec!["dept_id".into()],
                    false,
                )],
            },
        );
        ctx.register_table(
            "departments",
            TableStats {
                row_count: 8,
                is_sorted: false,
                indexes: alloc::vec![],
            },
        );

        let pass = IndexJoinPass::new(&ctx);
        let plan = PhysicalPlan::hash_join(
            PhysicalPlan::table_scan("employees"),
            PhysicalPlan::table_scan("departments"),
            Expr::eq(
                Expr::column("employees", "dept_id", 1),
                Expr::column("departments", "id", 0),
            ),
            JoinType::Inner,
        );

        let result = pass.optimize(plan);

        if let PhysicalPlan::IndexNestedLoopJoin {
            inner_table,
            outer_is_left,
            ..
        } = result
        {
            assert_eq!(inner_table, "employees");
            assert!(!outer_is_left);
        } else {
            panic!("expected index nested loop join");
        }
    }

    #[test]
    fn test_no_index_remains_hash_join() {
        let ctx = ExecutionContext::new(); // Empty context, no indexes
        let pass = IndexJoinPass::new(&ctx);

        let plan = PhysicalPlan::hash_join(
            PhysicalPlan::table_scan("a"),
            PhysicalPlan::table_scan("b"),
            Expr::eq(Expr::column("a", "id", 0), Expr::column("b", "a_id", 0)),
            JoinType::Inner,
        );

        let result = pass.optimize(plan);

        // Should remain as HashJoin
        assert!(matches!(result, PhysicalPlan::HashJoin { .. }));
    }

    #[test]
    fn test_outer_join_not_optimized() {
        let ctx = create_test_context();
        let pass = IndexJoinPass::new(&ctx);

        // Left outer join should not be converted to index join
        let plan = PhysicalPlan::hash_join(
            PhysicalPlan::table_scan("a"),
            PhysicalPlan::table_scan("b"),
            Expr::eq(Expr::column("a", "id", 0), Expr::column("b", "a_id", 0)),
            JoinType::LeftOuter,
        );

        let result = pass.optimize(plan);

        // Should remain as HashJoin
        assert!(matches!(result, PhysicalPlan::HashJoin { .. }));
    }

    #[test]
    fn test_non_equi_join_not_optimized() {
        let ctx = create_test_context();
        let pass = IndexJoinPass::new(&ctx);

        // Range join should not be converted to index join
        let plan = PhysicalPlan::hash_join(
            PhysicalPlan::table_scan("a"),
            PhysicalPlan::table_scan("b"),
            Expr::gt(Expr::column("a", "id", 0), Expr::column("b", "a_id", 0)),
            JoinType::Inner,
        );

        let result = pass.optimize(plan);

        // Should remain as HashJoin
        assert!(matches!(result, PhysicalPlan::HashJoin { .. }));
    }

    #[test]
    fn test_nested_joins() {
        let ctx = create_test_context();
        let pass = IndexJoinPass::new(&ctx);

        // Create nested join: HashJoin(HashJoin(a, b), c)
        let inner_join = PhysicalPlan::hash_join(
            PhysicalPlan::table_scan("a"),
            PhysicalPlan::table_scan("b"),
            Expr::eq(Expr::column("a", "id", 0), Expr::column("b", "a_id", 0)),
            JoinType::Inner,
        );

        let outer_join = PhysicalPlan::hash_join(
            inner_join,
            PhysicalPlan::table_scan("c"),
            Expr::eq(Expr::column("b", "id", 0), Expr::column("c", "b_id", 0)),
            JoinType::Inner,
        );

        let result = pass.optimize(outer_join);

        // Inner join should be converted to index join
        // Outer join remains as hash join (no index on c.b_id)
        assert!(matches!(result, PhysicalPlan::HashJoin { .. }));
        if let PhysicalPlan::HashJoin { left, .. } = result {
            assert!(matches!(*left, PhysicalPlan::IndexNestedLoopJoin { .. }));
        }
    }

    #[test]
    fn test_large_outer_prefers_hash_join() {
        let mut ctx = ExecutionContext::new();
        ctx.register_table(
            "big_outer",
            TableStats {
                row_count: 20_000,
                is_sorted: false,
                indexes: alloc::vec![],
            },
        );
        ctx.register_table(
            "small_inner",
            TableStats {
                row_count: 1_000,
                is_sorted: false,
                indexes: alloc::vec![IndexInfo::new(
                    "idx_outer_id",
                    alloc::vec!["outer_id".into()],
                    false,
                )],
            },
        );

        let pass = IndexJoinPass::new(&ctx);
        let plan = PhysicalPlan::hash_join(
            PhysicalPlan::table_scan("big_outer"),
            PhysicalPlan::table_scan("small_inner"),
            Expr::eq(
                Expr::column("big_outer", "id", 0),
                Expr::column("small_inner", "outer_id", 0),
            ),
            JoinType::Inner,
        );

        let result = pass.optimize(plan);
        assert!(matches!(result, PhysicalPlan::HashJoin { .. }));
    }
}
