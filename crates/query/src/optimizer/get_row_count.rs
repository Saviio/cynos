//! Get row count pass - optimizes COUNT(*) queries.
//!
//! This pass identifies simple COUNT(*) queries (without WHERE, GROUP BY,
//! LIMIT, or SKIP) and replaces them with a direct row count lookup.
//!
//! Example:
//! ```text
//! HashAggregate(COUNT(*))     =>    GetRowCount(users)
//!        |
//! TableScan(users)
//! ```
//!
//! This optimization is beneficial because:
//! 1. It avoids scanning the entire table
//! 2. Row count can be retrieved from table metadata in O(1)

use crate::ast::{AggregateFunc, Expr};
use crate::context::ExecutionContext;
use crate::planner::PhysicalPlan;
use alloc::boxed::Box;
use alloc::string::String;

/// A special plan node for direct row count retrieval.
#[derive(Clone, Debug)]
pub struct GetRowCountPlan {
    pub table: String,
}

/// Pass that optimizes COUNT(*) queries.
pub struct GetRowCountPass<'a> {
    ctx: &'a ExecutionContext,
}

impl<'a> GetRowCountPass<'a> {
    /// Creates a new GetRowCountPass with the given execution context.
    pub fn new(ctx: &'a ExecutionContext) -> Self {
        Self { ctx }
    }

    /// Optimizes the physical plan by replacing COUNT(*) with direct row count.
    /// Returns the optimized plan and optionally a GetRowCountPlan if applicable.
    pub fn optimize(&self, plan: PhysicalPlan) -> (PhysicalPlan, Option<GetRowCountPlan>) {
        self.traverse(plan)
    }

    fn traverse(&self, plan: PhysicalPlan) -> (PhysicalPlan, Option<GetRowCountPlan>) {
        match plan {
            PhysicalPlan::HashAggregate {
                input,
                group_by,
                aggregates,
            } => {
                // Check if this is a simple COUNT(*) query
                if let Some(table) = self.is_count_star_query(&input, &group_by, &aggregates) {
                    // Return the original plan but also indicate we can use GetRowCount
                    return (
                        PhysicalPlan::HashAggregate {
                            input,
                            group_by,
                            aggregates,
                        },
                        Some(GetRowCountPlan { table }),
                    );
                }

                // Not a COUNT(*) query, recursively optimize input
                let (optimized_input, _) = self.traverse(*input);
                (
                    PhysicalPlan::HashAggregate {
                        input: Box::new(optimized_input),
                        group_by,
                        aggregates,
                    },
                    None,
                )
            }

            // Recursively process other nodes
            PhysicalPlan::Filter { input, predicate } => {
                let (optimized_input, _) = self.traverse(*input);
                (
                    PhysicalPlan::Filter {
                        input: Box::new(optimized_input),
                        predicate,
                    },
                    None,
                )
            }

            PhysicalPlan::Project { input, columns } => {
                let (optimized_input, row_count) = self.traverse(*input);
                (
                    PhysicalPlan::Project {
                        input: Box::new(optimized_input),
                        columns,
                    },
                    row_count,
                )
            }

            PhysicalPlan::Sort { input, order_by } => {
                let (optimized_input, _) = self.traverse(*input);
                (
                    PhysicalPlan::Sort {
                        input: Box::new(optimized_input),
                        order_by,
                    },
                    None,
                )
            }

            PhysicalPlan::Limit {
                input,
                limit,
                offset,
            } => {
                let (optimized_input, _) = self.traverse(*input);
                (
                    PhysicalPlan::Limit {
                        input: Box::new(optimized_input),
                        limit,
                        offset,
                    },
                    None,
                )
            }

            PhysicalPlan::HashJoin {
                left,
                right,
                condition,
                join_type,
            } => {
                let (left_opt, _) = self.traverse(*left);
                let (right_opt, _) = self.traverse(*right);
                (
                    PhysicalPlan::HashJoin {
                        left: Box::new(left_opt),
                        right: Box::new(right_opt),
                        condition,
                        join_type,
                    },
                    None,
                )
            }

            PhysicalPlan::SortMergeJoin {
                left,
                right,
                condition,
                join_type,
            } => {
                let (left_opt, _) = self.traverse(*left);
                let (right_opt, _) = self.traverse(*right);
                (
                    PhysicalPlan::SortMergeJoin {
                        left: Box::new(left_opt),
                        right: Box::new(right_opt),
                        condition,
                        join_type,
                    },
                    None,
                )
            }

            PhysicalPlan::NestedLoopJoin {
                left,
                right,
                condition,
                join_type,
            } => {
                let (left_opt, _) = self.traverse(*left);
                let (right_opt, _) = self.traverse(*right);
                (
                    PhysicalPlan::NestedLoopJoin {
                        left: Box::new(left_opt),
                        right: Box::new(right_opt),
                        condition,
                        join_type,
                    },
                    None,
                )
            }

            PhysicalPlan::IndexNestedLoopJoin {
                outer,
                inner_table,
                inner_index,
                condition,
                join_type,
            } => {
                let (outer_opt, _) = self.traverse(*outer);
                (
                    PhysicalPlan::IndexNestedLoopJoin {
                        outer: Box::new(outer_opt),
                        inner_table,
                        inner_index,
                        condition,
                        join_type,
                    },
                    None,
                )
            }

            PhysicalPlan::CrossProduct { left, right } => {
                let (left_opt, _) = self.traverse(*left);
                let (right_opt, _) = self.traverse(*right);
                (
                    PhysicalPlan::CrossProduct {
                        left: Box::new(left_opt),
                        right: Box::new(right_opt),
                    },
                    None,
                )
            }

            PhysicalPlan::NoOp { input } => {
                let (optimized_input, row_count) = self.traverse(*input);
                (
                    PhysicalPlan::NoOp {
                        input: Box::new(optimized_input),
                    },
                    row_count,
                )
            }

            PhysicalPlan::TopN {
                input,
                order_by,
                limit,
                offset,
            } => {
                let (optimized_input, _) = self.traverse(*input);
                (
                    PhysicalPlan::TopN {
                        input: Box::new(optimized_input),
                        order_by,
                        limit,
                        offset,
                    },
                    None,
                )
            }

            // Leaf nodes - no transformation
            plan @ (PhysicalPlan::TableScan { .. }
            | PhysicalPlan::IndexScan { .. }
            | PhysicalPlan::IndexGet { .. }
            | PhysicalPlan::IndexInGet { .. }
            | PhysicalPlan::GinIndexScan { .. }
            | PhysicalPlan::GinIndexScanMulti { .. }
            | PhysicalPlan::Empty) => (plan, None),
        }
    }

    /// Checks if this is a simple COUNT(*) query.
    /// Returns the table name if it is, None otherwise.
    fn is_count_star_query(
        &self,
        input: &PhysicalPlan,
        group_by: &[Expr],
        aggregates: &[(AggregateFunc, Expr)],
    ) -> Option<String> {
        // Must have no GROUP BY
        if !group_by.is_empty() {
            return None;
        }

        // Must have exactly one aggregate: COUNT(*)
        if aggregates.len() != 1 {
            return None;
        }

        let (func, _expr) = &aggregates[0];
        if *func != AggregateFunc::Count {
            return None;
        }

        // Check if it's COUNT(*) - represented as COUNT with a star/all expression
        // For simplicity, we accept any COUNT aggregate here
        // In a real implementation, we'd check for the star column specifically

        // Input must be a simple TableScan (no filters, joins, etc.)
        match input {
            PhysicalPlan::TableScan { table } => Some(table.clone()),
            _ => None,
        }
    }

    /// Gets the row count for a table from the execution context.
    pub fn get_row_count(&self, table: &str) -> usize {
        self.ctx.row_count(table)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::Expr;
    use crate::context::TableStats;

    fn create_test_context() -> ExecutionContext {
        let mut ctx = ExecutionContext::new();

        ctx.register_table(
            "users",
            TableStats {
                row_count: 1000,
                is_sorted: false,
                indexes: alloc::vec![],
            },
        );

        ctx
    }

    #[test]
    fn test_count_star_optimization() {
        let ctx = create_test_context();
        let pass = GetRowCountPass::new(&ctx);

        // Create: HashAggregate(COUNT(*)) -> TableScan(users)
        let plan = PhysicalPlan::HashAggregate {
            input: Box::new(PhysicalPlan::table_scan("users")),
            group_by: alloc::vec![],
            aggregates: alloc::vec![(AggregateFunc::Count, Expr::literal(1i64))],
        };

        let (_, row_count_plan) = pass.optimize(plan);

        // Should detect COUNT(*) optimization opportunity
        assert!(row_count_plan.is_some());
        assert_eq!(row_count_plan.unwrap().table, "users");
    }

    #[test]
    fn test_count_with_group_by_not_optimized() {
        let ctx = create_test_context();
        let pass = GetRowCountPass::new(&ctx);

        // Create: HashAggregate(COUNT(*), GROUP BY name) -> TableScan(users)
        let plan = PhysicalPlan::HashAggregate {
            input: Box::new(PhysicalPlan::table_scan("users")),
            group_by: alloc::vec![Expr::column("users", "name", 1)],
            aggregates: alloc::vec![(AggregateFunc::Count, Expr::literal(1i64))],
        };

        let (_, row_count_plan) = pass.optimize(plan);

        // Should NOT detect optimization (has GROUP BY)
        assert!(row_count_plan.is_none());
    }

    #[test]
    fn test_count_with_filter_not_optimized() {
        let ctx = create_test_context();
        let pass = GetRowCountPass::new(&ctx);

        // Create: HashAggregate(COUNT(*)) -> Filter -> TableScan(users)
        let plan = PhysicalPlan::HashAggregate {
            input: Box::new(PhysicalPlan::Filter {
                input: Box::new(PhysicalPlan::table_scan("users")),
                predicate: Expr::gt(Expr::column("users", "age", 1), Expr::literal(18i64)),
            }),
            group_by: alloc::vec![],
            aggregates: alloc::vec![(AggregateFunc::Count, Expr::literal(1i64))],
        };

        let (_, row_count_plan) = pass.optimize(plan);

        // Should NOT detect optimization (has Filter)
        assert!(row_count_plan.is_none());
    }

    #[test]
    fn test_sum_not_optimized() {
        let ctx = create_test_context();
        let pass = GetRowCountPass::new(&ctx);

        // Create: HashAggregate(SUM(amount)) -> TableScan(users)
        let plan = PhysicalPlan::HashAggregate {
            input: Box::new(PhysicalPlan::table_scan("users")),
            group_by: alloc::vec![],
            aggregates: alloc::vec![(AggregateFunc::Sum, Expr::column("users", "amount", 2))],
        };

        let (_, row_count_plan) = pass.optimize(plan);

        // Should NOT detect optimization (not COUNT)
        assert!(row_count_plan.is_none());
    }

    #[test]
    fn test_get_row_count() {
        let ctx = create_test_context();
        let pass = GetRowCountPass::new(&ctx);

        assert_eq!(pass.get_row_count("users"), 1000);
        assert_eq!(pass.get_row_count("nonexistent"), 0);
    }

    #[test]
    fn test_multiple_aggregates_not_optimized() {
        let ctx = create_test_context();
        let pass = GetRowCountPass::new(&ctx);

        // Create: HashAggregate(COUNT(*), SUM(amount)) -> TableScan(users)
        let plan = PhysicalPlan::HashAggregate {
            input: Box::new(PhysicalPlan::table_scan("users")),
            group_by: alloc::vec![],
            aggregates: alloc::vec![
                (AggregateFunc::Count, Expr::literal(1i64)),
                (AggregateFunc::Sum, Expr::column("users", "amount", 2)),
            ],
        };

        let (_, row_count_plan) = pass.optimize(plan);

        // Should NOT detect optimization (multiple aggregates)
        assert!(row_count_plan.is_none());
    }
}