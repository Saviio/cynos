//! Implicit joins pass - converts CrossProduct + Filter patterns to Join nodes.
//!
//! This pass identifies patterns where a Filter with a join predicate sits
//! directly above a CrossProduct, and converts them to proper Join nodes.
//!
//! Example:
//! ```text
//! Filter(a.id = b.a_id)       =>       Join(a.id = b.a_id)
//!        |                              /            \
//!   CrossProduct                     Scan(a)       Scan(b)
//!    /        \
//! Scan(a)   Scan(b)
//! ```
//!
//! This transformation is important because:
//! 1. Join nodes can use optimized join algorithms (hash join, merge join)
//! 2. It enables further optimizations like index join selection

use crate::ast::{Expr, JoinType};
use crate::optimizer::OptimizerPass;
use crate::planner::LogicalPlan;
use alloc::boxed::Box;

/// Pass that converts CrossProduct + Filter patterns to Join nodes.
pub struct ImplicitJoinsPass;

impl OptimizerPass for ImplicitJoinsPass {
    fn optimize(&self, plan: LogicalPlan) -> LogicalPlan {
        self.traverse(plan)
    }

    fn name(&self) -> &'static str {
        "implicit_joins"
    }
}

impl ImplicitJoinsPass {
    /// Recursively traverses the plan tree and converts implicit joins.
    fn traverse(&self, plan: LogicalPlan) -> LogicalPlan {
        match plan {
            LogicalPlan::Filter { input, predicate } => {
                // First, recursively optimize the input
                let optimized_input = self.traverse(*input);

                // Check if this is a join predicate over a cross product
                if let LogicalPlan::CrossProduct { left, right } = &optimized_input {
                    if self.is_join_predicate(&predicate, left, right) {
                        // Convert to Join
                        return LogicalPlan::Join {
                            left: left.clone(),
                            right: right.clone(),
                            condition: predicate,
                            join_type: JoinType::Inner,
                        };
                    }
                }

                // Not a join pattern, keep as Filter
                LogicalPlan::Filter {
                    input: Box::new(optimized_input),
                    predicate,
                }
            }

            LogicalPlan::CrossProduct { left, right } => LogicalPlan::CrossProduct {
                left: Box::new(self.traverse(*left)),
                right: Box::new(self.traverse(*right)),
            },

            LogicalPlan::Project { input, columns } => LogicalPlan::Project {
                input: Box::new(self.traverse(*input)),
                columns,
            },

            LogicalPlan::Join {
                left,
                right,
                condition,
                join_type,
            } => LogicalPlan::Join {
                left: Box::new(self.traverse(*left)),
                right: Box::new(self.traverse(*right)),
                condition,
                join_type,
            },

            LogicalPlan::Aggregate {
                input,
                group_by,
                aggregates,
            } => LogicalPlan::Aggregate {
                input: Box::new(self.traverse(*input)),
                group_by,
                aggregates,
            },

            LogicalPlan::Sort { input, order_by } => LogicalPlan::Sort {
                input: Box::new(self.traverse(*input)),
                order_by,
            },

            LogicalPlan::Limit {
                input,
                limit,
                offset,
            } => LogicalPlan::Limit {
                input: Box::new(self.traverse(*input)),
                limit,
                offset,
            },

            LogicalPlan::Union { left, right, all } => LogicalPlan::Union {
                left: Box::new(self.traverse(*left)),
                right: Box::new(self.traverse(*right)),
                all,
            },

            // Leaf nodes - no transformation needed
            plan @ (LogicalPlan::Scan { .. }
            | LogicalPlan::IndexScan { .. }
            | LogicalPlan::IndexGet { .. }
            | LogicalPlan::IndexInGet { .. }
            | LogicalPlan::GinIndexScan { .. }
            | LogicalPlan::GinIndexScanMulti { .. }
            | LogicalPlan::Empty) => plan,
        }
    }

    /// Checks if the predicate is a join predicate that references both sides
    /// of the cross product.
    fn is_join_predicate(
        &self,
        predicate: &Expr,
        left: &LogicalPlan,
        right: &LogicalPlan,
    ) -> bool {
        let left_tables = self.collect_tables(left);
        let right_tables = self.collect_tables(right);
        let predicate_tables = self.collect_predicate_tables(predicate);

        // A join predicate must reference at least one table from each side
        let refs_left = predicate_tables.iter().any(|t| left_tables.contains(t));
        let refs_right = predicate_tables.iter().any(|t| right_tables.contains(t));

        refs_left && refs_right
    }

    /// Collects all table names referenced in a plan.
    fn collect_tables(&self, plan: &LogicalPlan) -> alloc::vec::Vec<alloc::string::String> {
        let mut tables = alloc::vec::Vec::new();
        self.collect_tables_recursive(plan, &mut tables);
        tables
    }

    fn collect_tables_recursive(
        &self,
        plan: &LogicalPlan,
        tables: &mut alloc::vec::Vec<alloc::string::String>,
    ) {
        match plan {
            LogicalPlan::Scan { table } => tables.push(table.clone()),
            LogicalPlan::IndexScan { table, .. }
            | LogicalPlan::IndexGet { table, .. }
            | LogicalPlan::IndexInGet { table, .. }
            | LogicalPlan::GinIndexScan { table, .. }
            | LogicalPlan::GinIndexScanMulti { table, .. } => {
                tables.push(table.clone())
            }
            LogicalPlan::Filter { input, .. }
            | LogicalPlan::Project { input, .. }
            | LogicalPlan::Aggregate { input, .. }
            | LogicalPlan::Sort { input, .. }
            | LogicalPlan::Limit { input, .. } => {
                self.collect_tables_recursive(input, tables);
            }
            LogicalPlan::Join { left, right, .. }
            | LogicalPlan::CrossProduct { left, right }
            | LogicalPlan::Union { left, right, .. } => {
                self.collect_tables_recursive(left, tables);
                self.collect_tables_recursive(right, tables);
            }
            LogicalPlan::Empty => {}
        }
    }

    /// Collects all table names referenced in a predicate expression.
    fn collect_predicate_tables(&self, expr: &Expr) -> alloc::vec::Vec<alloc::string::String> {
        let mut tables = alloc::vec::Vec::new();
        self.collect_expr_tables(expr, &mut tables);
        tables
    }

    fn collect_expr_tables(
        &self,
        expr: &Expr,
        tables: &mut alloc::vec::Vec<alloc::string::String>,
    ) {
        match expr {
            Expr::Column(col_ref) => {
                if !tables.contains(&col_ref.table) {
                    tables.push(col_ref.table.clone());
                }
            }
            Expr::BinaryOp { left, right, .. } => {
                self.collect_expr_tables(left, tables);
                self.collect_expr_tables(right, tables);
            }
            Expr::UnaryOp { expr, .. } => {
                self.collect_expr_tables(expr, tables);
            }
            Expr::Aggregate { expr, .. } => {
                if let Some(e) = expr {
                    self.collect_expr_tables(e, tables);
                }
            }
            Expr::Literal(_) => {}
            // Handle other expression types
            Expr::Function { args, .. } => {
                for arg in args {
                    self.collect_expr_tables(arg, tables);
                }
            }
            Expr::Between { expr, low, high } => {
                self.collect_expr_tables(expr, tables);
                self.collect_expr_tables(low, tables);
                self.collect_expr_tables(high, tables);
            }
            Expr::In { expr, list } => {
                self.collect_expr_tables(expr, tables);
                for item in list {
                    self.collect_expr_tables(item, tables);
                }
            }
            Expr::Like { expr, .. } => {
                self.collect_expr_tables(expr, tables);
            }
            Expr::NotBetween { expr, low, high } => {
                self.collect_expr_tables(expr, tables);
                self.collect_expr_tables(low, tables);
                self.collect_expr_tables(high, tables);
            }
            Expr::NotIn { expr, list } => {
                self.collect_expr_tables(expr, tables);
                for item in list {
                    self.collect_expr_tables(item, tables);
                }
            }
            Expr::NotLike { expr, .. } => {
                self.collect_expr_tables(expr, tables);
            }
            Expr::Match { expr, .. } => {
                self.collect_expr_tables(expr, tables);
            }
            Expr::NotMatch { expr, .. } => {
                self.collect_expr_tables(expr, tables);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::Expr;

    #[test]
    fn test_cross_product_with_join_predicate() {
        let pass = ImplicitJoinsPass;

        // Create: Filter(a.id = b.a_id) -> CrossProduct(Scan(a), Scan(b))
        let cross = LogicalPlan::cross_product(LogicalPlan::scan("a"), LogicalPlan::scan("b"));
        let join_pred = Expr::eq(Expr::column("a", "id", 0), Expr::column("b", "a_id", 0));
        let plan = LogicalPlan::filter(cross, join_pred);

        let result = pass.optimize(plan);

        // Should become: Join(a.id = b.a_id, Scan(a), Scan(b))
        assert!(matches!(result, LogicalPlan::Join { .. }));
        if let LogicalPlan::Join {
            left,
            right,
            join_type,
            ..
        } = result
        {
            assert!(matches!(*left, LogicalPlan::Scan { table } if table == "a"));
            assert!(matches!(*right, LogicalPlan::Scan { table } if table == "b"));
            assert!(matches!(join_type, JoinType::Inner));
        }
    }

    #[test]
    fn test_cross_product_with_non_join_predicate() {
        let pass = ImplicitJoinsPass;

        // Create: Filter(a.id = 1) -> CrossProduct(Scan(a), Scan(b))
        // This is NOT a join predicate (only references one table)
        let cross = LogicalPlan::cross_product(LogicalPlan::scan("a"), LogicalPlan::scan("b"));
        let filter_pred = Expr::eq(Expr::column("a", "id", 0), Expr::literal(1i64));
        let plan = LogicalPlan::filter(cross, filter_pred);

        let result = pass.optimize(plan);

        // Should remain as Filter -> CrossProduct
        assert!(matches!(result, LogicalPlan::Filter { .. }));
        if let LogicalPlan::Filter { input, .. } = result {
            assert!(matches!(*input, LogicalPlan::CrossProduct { .. }));
        }
    }

    #[test]
    fn test_filter_without_cross_product() {
        let pass = ImplicitJoinsPass;

        // Create: Filter(id = 1) -> Scan(a)
        let plan = LogicalPlan::filter(
            LogicalPlan::scan("a"),
            Expr::eq(Expr::column("a", "id", 0), Expr::literal(1i64)),
        );

        let result = pass.optimize(plan);

        // Should remain unchanged
        assert!(matches!(result, LogicalPlan::Filter { .. }));
        if let LogicalPlan::Filter { input, .. } = result {
            assert!(matches!(*input, LogicalPlan::Scan { .. }));
        }
    }

    #[test]
    fn test_nested_cross_products_with_join() {
        let pass = ImplicitJoinsPass;

        // Create: Filter(a.id = b.a_id) -> CrossProduct(CrossProduct(Scan(a), Scan(b)), Scan(c))
        // The predicate references a and b, which are both in the left subtree of the outer cross product
        // So this is NOT a join predicate for the outer cross product
        let inner_cross =
            LogicalPlan::cross_product(LogicalPlan::scan("a"), LogicalPlan::scan("b"));
        let outer_cross = LogicalPlan::cross_product(inner_cross, LogicalPlan::scan("c"));
        let join_pred = Expr::eq(Expr::column("a", "id", 0), Expr::column("b", "a_id", 0));
        let plan = LogicalPlan::filter(outer_cross, join_pred);

        let result = pass.optimize(plan);

        // The outer cross product should remain as Filter -> CrossProduct
        // because the predicate only references tables in the left subtree
        assert!(matches!(result, LogicalPlan::Filter { .. }));
        if let LogicalPlan::Filter { input, .. } = result {
            assert!(matches!(*input, LogicalPlan::CrossProduct { .. }));
        }
    }

    #[test]
    fn test_is_join_predicate() {
        let pass = ImplicitJoinsPass;

        let left = LogicalPlan::scan("a");
        let right = LogicalPlan::scan("b");

        // Join predicate: a.id = b.a_id
        let join_pred = Expr::eq(Expr::column("a", "id", 0), Expr::column("b", "a_id", 0));
        assert!(pass.is_join_predicate(&join_pred, &left, &right));

        // Non-join predicate: a.id = 1
        let filter_pred = Expr::eq(Expr::column("a", "id", 0), Expr::literal(1i64));
        assert!(!pass.is_join_predicate(&filter_pred, &left, &right));

        // Non-join predicate: b.name = 'test'
        let filter_pred2 = Expr::eq(Expr::column("b", "name", 1), Expr::literal("test"));
        assert!(!pass.is_join_predicate(&filter_pred2, &left, &right));
    }

    #[test]
    fn test_collect_tables() {
        let pass = ImplicitJoinsPass;

        let plan = LogicalPlan::cross_product(
            LogicalPlan::scan("a"),
            LogicalPlan::cross_product(LogicalPlan::scan("b"), LogicalPlan::scan("c")),
        );

        let tables = pass.collect_tables(&plan);
        assert_eq!(tables.len(), 3);
        assert!(tables.contains(&"a".into()));
        assert!(tables.contains(&"b".into()));
        assert!(tables.contains(&"c".into()));
    }
}
