//! AND predicate pass - breaks down AND predicates into chained Filter nodes.
//!
//! This pass transforms a single Filter node with a complex AND predicate
//! into multiple chained Filter nodes, each with a simple predicate.
//!
//! Example:
//! ```text
//! Filter(a AND b AND c)    =>    Filter(a)
//!        |                          |
//!      Scan                      Filter(b)
//!                                   |
//!                                Filter(c)
//!                                   |
//!                                 Scan
//! ```
//!
//! This transformation enables other optimization passes (like index selection)
//! to work on individual predicates more effectively.

use crate::ast::{BinaryOp, Expr};
use crate::optimizer::OptimizerPass;
use crate::planner::LogicalPlan;
use alloc::boxed::Box;
use alloc::vec::Vec;

/// Pass that breaks down AND predicates into chained Filter nodes.
pub struct AndPredicatePass;

impl OptimizerPass for AndPredicatePass {
    fn optimize(&self, plan: LogicalPlan) -> LogicalPlan {
        self.traverse(plan)
    }

    fn name(&self) -> &'static str {
        "and_predicate"
    }
}

impl AndPredicatePass {
    /// Recursively traverses the plan tree and transforms AND predicates.
    fn traverse(&self, plan: LogicalPlan) -> LogicalPlan {
        match plan {
            LogicalPlan::Filter { input, predicate } => {
                // First, recursively optimize the input
                let optimized_input = self.traverse(*input);

                // Break down the AND predicate into components
                let predicates = self.break_and_predicate(predicate);

                // Create a chain of Filter nodes
                self.create_filter_chain(optimized_input, predicates)
            }

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

            LogicalPlan::CrossProduct { left, right } => LogicalPlan::CrossProduct {
                left: Box::new(self.traverse(*left)),
                right: Box::new(self.traverse(*right)),
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

    /// Recursively breaks down an AND predicate into its components.
    /// OR predicates and other predicate types are left unchanged.
    ///
    /// Example: (a AND (b AND c)) AND (d OR e) becomes [a, b, c, (d OR e)]
    fn break_and_predicate(&self, predicate: Expr) -> Vec<Expr> {
        match predicate {
            Expr::BinaryOp {
                left,
                op: BinaryOp::And,
                right,
            } => {
                let mut result = self.break_and_predicate(*left);
                result.extend(self.break_and_predicate(*right));
                result
            }
            // Non-AND predicates are returned as-is
            other => alloc::vec![other],
        }
    }

    /// Creates a chain of Filter nodes from a list of predicates.
    /// The first predicate becomes the outermost Filter.
    fn create_filter_chain(&self, input: LogicalPlan, predicates: Vec<Expr>) -> LogicalPlan {
        if predicates.is_empty() {
            return input;
        }

        // Build the chain from bottom to top
        let mut result = input;
        for predicate in predicates.into_iter().rev() {
            result = LogicalPlan::Filter {
                input: Box::new(result),
                predicate,
            };
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::Expr;

    #[test]
    fn test_simple_filter_unchanged() {
        let pass = AndPredicatePass;
        let plan = LogicalPlan::filter(
            LogicalPlan::scan("users"),
            Expr::eq(Expr::column("users", "id", 0), Expr::literal(1i64)),
        );

        let result = pass.optimize(plan);

        // Single predicate should remain as single Filter
        assert!(matches!(result, LogicalPlan::Filter { .. }));
        if let LogicalPlan::Filter { input, .. } = result {
            assert!(matches!(*input, LogicalPlan::Scan { .. }));
        }
    }

    #[test]
    fn test_and_predicate_split() {
        let pass = AndPredicatePass;

        // Create: Filter(a AND b) -> Scan
        let pred_a = Expr::eq(Expr::column("users", "id", 0), Expr::literal(1i64));
        let pred_b = Expr::eq(Expr::column("users", "name", 1), Expr::literal("Alice"));
        let and_pred = Expr::and(pred_a.clone(), pred_b.clone());

        let plan = LogicalPlan::filter(LogicalPlan::scan("users"), and_pred);

        let result = pass.optimize(plan);

        // Should become: Filter(a) -> Filter(b) -> Scan
        assert!(matches!(result, LogicalPlan::Filter { .. }));
        if let LogicalPlan::Filter { input, .. } = result {
            assert!(matches!(*input, LogicalPlan::Filter { .. }));
            if let LogicalPlan::Filter { input: inner, .. } = *input {
                assert!(matches!(*inner, LogicalPlan::Scan { .. }));
            }
        }
    }

    #[test]
    fn test_nested_and_predicate_flattened() {
        let pass = AndPredicatePass;

        // Create: Filter((a AND b) AND c) -> Scan
        let pred_a = Expr::eq(Expr::column("t", "a", 0), Expr::literal(1i64));
        let pred_b = Expr::eq(Expr::column("t", "b", 1), Expr::literal(2i64));
        let pred_c = Expr::eq(Expr::column("t", "c", 2), Expr::literal(3i64));
        let nested_and = Expr::and(Expr::and(pred_a, pred_b), pred_c);

        let plan = LogicalPlan::filter(LogicalPlan::scan("t"), nested_and);

        let result = pass.optimize(plan);

        // Should become: Filter(a) -> Filter(b) -> Filter(c) -> Scan
        // Count the depth of Filter nodes
        let mut depth = 0;
        let mut current = &result;
        while let LogicalPlan::Filter { input, .. } = current {
            depth += 1;
            current = input;
        }
        assert_eq!(depth, 3);
        assert!(matches!(current, LogicalPlan::Scan { .. }));
    }

    #[test]
    fn test_or_predicate_preserved() {
        let pass = AndPredicatePass;

        // Create: Filter(a OR b) -> Scan
        let pred_a = Expr::eq(Expr::column("t", "a", 0), Expr::literal(1i64));
        let pred_b = Expr::eq(Expr::column("t", "b", 1), Expr::literal(2i64));
        let or_pred = Expr::or(pred_a, pred_b);

        let plan = LogicalPlan::filter(LogicalPlan::scan("t"), or_pred);

        let result = pass.optimize(plan);

        // OR predicate should remain as single Filter
        assert!(matches!(result, LogicalPlan::Filter { .. }));
        if let LogicalPlan::Filter { input, predicate } = result {
            assert!(matches!(*input, LogicalPlan::Scan { .. }));
            assert!(matches!(
                predicate,
                Expr::BinaryOp {
                    op: BinaryOp::Or,
                    ..
                }
            ));
        }
    }

    #[test]
    fn test_mixed_and_or_predicate() {
        let pass = AndPredicatePass;

        // Create: Filter(a AND (b OR c)) -> Scan
        let pred_a = Expr::eq(Expr::column("t", "a", 0), Expr::literal(1i64));
        let pred_b = Expr::eq(Expr::column("t", "b", 1), Expr::literal(2i64));
        let pred_c = Expr::eq(Expr::column("t", "c", 2), Expr::literal(3i64));
        let or_pred = Expr::or(pred_b, pred_c);
        let and_pred = Expr::and(pred_a, or_pred);

        let plan = LogicalPlan::filter(LogicalPlan::scan("t"), and_pred);

        let result = pass.optimize(plan);

        // Should become: Filter(a) -> Filter(b OR c) -> Scan
        let mut depth = 0;
        let mut current = &result;
        while let LogicalPlan::Filter { input, .. } = current {
            depth += 1;
            current = input;
        }
        assert_eq!(depth, 2);
    }

    #[test]
    fn test_break_and_predicate() {
        let pass = AndPredicatePass;

        let pred_a = Expr::eq(Expr::column("t", "a", 0), Expr::literal(1i64));
        let pred_b = Expr::eq(Expr::column("t", "b", 1), Expr::literal(2i64));
        let pred_c = Expr::eq(Expr::column("t", "c", 2), Expr::literal(3i64));

        // Simple AND
        let and_pred = Expr::and(pred_a.clone(), pred_b.clone());
        let result = pass.break_and_predicate(and_pred);
        assert_eq!(result.len(), 2);

        // Nested AND
        let nested = Expr::and(Expr::and(pred_a.clone(), pred_b.clone()), pred_c.clone());
        let result = pass.break_and_predicate(nested);
        assert_eq!(result.len(), 3);

        // Single predicate
        let result = pass.break_and_predicate(pred_a);
        assert_eq!(result.len(), 1);
    }
}
