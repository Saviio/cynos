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

use crate::ast::{BinaryOp, Expr, JoinType};
use crate::context::ExecutionContext;
use crate::planner::PhysicalPlan;
use alloc::boxed::Box;
use alloc::string::String;

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
                    };
                }

                // Try to find an index on either side
                if let Some((outer, inner_table, inner_index)) =
                    self.find_index_join_candidate(&left, &right, &condition)
                {
                    return PhysicalPlan::IndexNestedLoopJoin {
                        outer: Box::new(outer),
                        inner_table,
                        inner_index,
                        condition,
                        join_type,
                    };
                }

                PhysicalPlan::HashJoin {
                    left: Box::new(left),
                    right: Box::new(right),
                    condition,
                    join_type,
                }
            }

            // Also check nested loop joins
            PhysicalPlan::NestedLoopJoin {
                left,
                right,
                condition,
                join_type,
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
                    };
                }

                // Try to find an index on either side
                if let Some((outer, inner_table, inner_index)) =
                    self.find_index_join_candidate(&left, &right, &condition)
                {
                    return PhysicalPlan::IndexNestedLoopJoin {
                        outer: Box::new(outer),
                        inner_table,
                        inner_index,
                        condition,
                        join_type,
                    };
                }

                PhysicalPlan::NestedLoopJoin {
                    left: Box::new(left),
                    right: Box::new(right),
                    condition,
                    join_type,
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
            } => PhysicalPlan::SortMergeJoin {
                left: Box::new(self.traverse(*left)),
                right: Box::new(self.traverse(*right)),
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
            | PhysicalPlan::Empty | PhysicalPlan::GinIndexScan { .. } | PhysicalPlan::GinIndexScanMulti { .. }) => plan,
        }
    }

    /// Finds a candidate for index join optimization.
    /// Returns (outer_plan, inner_table, inner_index) if found.
    fn find_index_join_candidate(
        &self,
        left: &PhysicalPlan,
        right: &PhysicalPlan,
        condition: &Expr,
    ) -> Option<(PhysicalPlan, String, String)> {
        // Extract join columns from the condition
        let (left_col, right_col) = self.extract_join_columns(condition)?;

        // Check if right side is a table scan with an index on the join column
        if let Some((table, index)) = self.get_indexed_table_scan(right, &right_col) {
            return Some((left.clone(), table, index));
        }

        // Check if left side is a table scan with an index on the join column
        if let Some((table, index)) = self.get_indexed_table_scan(left, &left_col) {
            return Some((right.clone(), table, index));
        }

        None
    }

    /// Extracts the column names from an equi-join condition.
    fn extract_join_columns(&self, condition: &Expr) -> Option<(String, String)> {
        match condition {
            Expr::BinaryOp {
                left,
                op: BinaryOp::Eq,
                right,
            } => {
                let left_col = self.extract_column_name(left)?;
                let right_col = self.extract_column_name(right)?;
                Some((left_col, right_col))
            }
            _ => None,
        }
    }

    /// Extracts the column name from a column expression.
    fn extract_column_name(&self, expr: &Expr) -> Option<String> {
        match expr {
            Expr::Column(col_ref) => Some(col_ref.column.clone()),
            _ => None,
        }
    }

    /// Checks if the plan is a table scan with an index on the given column.
    /// Returns (table_name, index_name) if found.
    fn get_indexed_table_scan(
        &self,
        plan: &PhysicalPlan,
        column: &str,
    ) -> Option<(String, String)> {
        match plan {
            PhysicalPlan::TableScan { table } => {
                // Check if there's an index on this column
                let index = self.ctx.find_index(table, &[column])?;
                Some((table.clone(), index.name.clone()))
            }
            _ => None,
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
        let plan = PhysicalPlan::HashJoin {
            left: Box::new(PhysicalPlan::table_scan("a")),
            right: Box::new(PhysicalPlan::table_scan("b")),
            condition: Expr::eq(Expr::column("a", "id", 0), Expr::column("b", "a_id", 0)),
            join_type: JoinType::Inner,
        };

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
    fn test_no_index_remains_hash_join() {
        let ctx = ExecutionContext::new(); // Empty context, no indexes
        let pass = IndexJoinPass::new(&ctx);

        let plan = PhysicalPlan::HashJoin {
            left: Box::new(PhysicalPlan::table_scan("a")),
            right: Box::new(PhysicalPlan::table_scan("b")),
            condition: Expr::eq(Expr::column("a", "id", 0), Expr::column("b", "a_id", 0)),
            join_type: JoinType::Inner,
        };

        let result = pass.optimize(plan);

        // Should remain as HashJoin
        assert!(matches!(result, PhysicalPlan::HashJoin { .. }));
    }

    #[test]
    fn test_outer_join_not_optimized() {
        let ctx = create_test_context();
        let pass = IndexJoinPass::new(&ctx);

        // Left outer join should not be converted to index join
        let plan = PhysicalPlan::HashJoin {
            left: Box::new(PhysicalPlan::table_scan("a")),
            right: Box::new(PhysicalPlan::table_scan("b")),
            condition: Expr::eq(Expr::column("a", "id", 0), Expr::column("b", "a_id", 0)),
            join_type: JoinType::LeftOuter,
        };

        let result = pass.optimize(plan);

        // Should remain as HashJoin
        assert!(matches!(result, PhysicalPlan::HashJoin { .. }));
    }

    #[test]
    fn test_non_equi_join_not_optimized() {
        let ctx = create_test_context();
        let pass = IndexJoinPass::new(&ctx);

        // Range join should not be converted to index join
        let plan = PhysicalPlan::HashJoin {
            left: Box::new(PhysicalPlan::table_scan("a")),
            right: Box::new(PhysicalPlan::table_scan("b")),
            condition: Expr::gt(Expr::column("a", "id", 0), Expr::column("b", "a_id", 0)),
            join_type: JoinType::Inner,
        };

        let result = pass.optimize(plan);

        // Should remain as HashJoin
        assert!(matches!(result, PhysicalPlan::HashJoin { .. }));
    }

    #[test]
    fn test_nested_joins() {
        let ctx = create_test_context();
        let pass = IndexJoinPass::new(&ctx);

        // Create nested join: HashJoin(HashJoin(a, b), c)
        let inner_join = PhysicalPlan::HashJoin {
            left: Box::new(PhysicalPlan::table_scan("a")),
            right: Box::new(PhysicalPlan::table_scan("b")),
            condition: Expr::eq(Expr::column("a", "id", 0), Expr::column("b", "a_id", 0)),
            join_type: JoinType::Inner,
        };

        let outer_join = PhysicalPlan::HashJoin {
            left: Box::new(inner_join),
            right: Box::new(PhysicalPlan::table_scan("c")),
            condition: Expr::eq(Expr::column("b", "id", 0), Expr::column("c", "b_id", 0)),
            join_type: JoinType::Inner,
        };

        let result = pass.optimize(outer_join);

        // Inner join should be converted to index join
        // Outer join remains as hash join (no index on c.b_id)
        assert!(matches!(result, PhysicalPlan::HashJoin { .. }));
        if let PhysicalPlan::HashJoin { left, .. } = result {
            assert!(matches!(*left, PhysicalPlan::IndexNestedLoopJoin { .. }));
        }
    }
}
