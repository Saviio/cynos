//! NOT expression simplification optimization pass.
//!
//! This pass simplifies NOT expressions by:
//! - Eliminating double negation: NOT(NOT(x)) → x
//! - Applying De Morgan's laws: NOT(a AND b) → NOT(a) OR NOT(b)
//! - Converting NOT(IN/BETWEEN/LIKE) to dedicated operators

use crate::ast::{BinaryOp, Expr, UnaryOp};
use crate::optimizer::OptimizerPass;
use crate::planner::LogicalPlan;
use alloc::boxed::Box;

/// NOT expression simplification pass.
pub struct NotSimplification;

impl OptimizerPass for NotSimplification {
    fn optimize(&self, plan: LogicalPlan) -> LogicalPlan {
        self.simplify_plan(plan)
    }

    fn name(&self) -> &'static str {
        "not_simplification"
    }
}

impl NotSimplification {
    fn simplify_plan(&self, plan: LogicalPlan) -> LogicalPlan {
        match plan {
            LogicalPlan::Filter { input, predicate } => {
                let simplified_predicate = self.simplify_expr(predicate);
                LogicalPlan::Filter {
                    input: Box::new(self.simplify_plan(*input)),
                    predicate: simplified_predicate,
                }
            }
            LogicalPlan::Join {
                left,
                right,
                condition,
                join_type,
            } => LogicalPlan::Join {
                left: Box::new(self.simplify_plan(*left)),
                right: Box::new(self.simplify_plan(*right)),
                condition: self.simplify_expr(condition),
                join_type,
            },
            LogicalPlan::Project { input, columns } => LogicalPlan::Project {
                input: Box::new(self.simplify_plan(*input)),
                columns,
            },
            LogicalPlan::Aggregate {
                input,
                group_by,
                aggregates,
            } => LogicalPlan::Aggregate {
                input: Box::new(self.simplify_plan(*input)),
                group_by,
                aggregates,
            },
            LogicalPlan::Sort { input, order_by } => LogicalPlan::Sort {
                input: Box::new(self.simplify_plan(*input)),
                order_by,
            },
            LogicalPlan::Limit {
                input,
                limit,
                offset,
            } => LogicalPlan::Limit {
                input: Box::new(self.simplify_plan(*input)),
                limit,
                offset,
            },
            LogicalPlan::CrossProduct { left, right } => LogicalPlan::CrossProduct {
                left: Box::new(self.simplify_plan(*left)),
                right: Box::new(self.simplify_plan(*right)),
            },
            other => other,
        }
    }

    fn simplify_expr(&self, expr: Expr) -> Expr {
        match expr {
            // Double negation elimination: NOT(NOT(x)) → x
            Expr::UnaryOp {
                op: UnaryOp::Not,
                expr: inner,
            } => {
                let simplified_inner = self.simplify_expr(*inner);
                self.simplify_not(simplified_inner)
            }

            // Recursively simplify binary operations
            Expr::BinaryOp { left, op, right } => Expr::BinaryOp {
                left: Box::new(self.simplify_expr(*left)),
                op,
                right: Box::new(self.simplify_expr(*right)),
            },

            // Recursively simplify other unary operations
            Expr::UnaryOp { op, expr } => Expr::UnaryOp {
                op,
                expr: Box::new(self.simplify_expr(*expr)),
            },

            // Recursively simplify BETWEEN
            Expr::Between { expr, low, high } => Expr::Between {
                expr: Box::new(self.simplify_expr(*expr)),
                low: Box::new(self.simplify_expr(*low)),
                high: Box::new(self.simplify_expr(*high)),
            },

            Expr::NotBetween { expr, low, high } => Expr::NotBetween {
                expr: Box::new(self.simplify_expr(*expr)),
                low: Box::new(self.simplify_expr(*low)),
                high: Box::new(self.simplify_expr(*high)),
            },

            // Recursively simplify IN
            Expr::In { expr, list } => Expr::In {
                expr: Box::new(self.simplify_expr(*expr)),
                list: list.into_iter().map(|e| self.simplify_expr(e)).collect(),
            },

            Expr::NotIn { expr, list } => Expr::NotIn {
                expr: Box::new(self.simplify_expr(*expr)),
                list: list.into_iter().map(|e| self.simplify_expr(e)).collect(),
            },

            // Recursively simplify LIKE
            Expr::Like { expr, pattern } => Expr::Like {
                expr: Box::new(self.simplify_expr(*expr)),
                pattern,
            },

            Expr::NotLike { expr, pattern } => Expr::NotLike {
                expr: Box::new(self.simplify_expr(*expr)),
                pattern,
            },

            // Recursively simplify MATCH
            Expr::Match { expr, pattern } => Expr::Match {
                expr: Box::new(self.simplify_expr(*expr)),
                pattern,
            },

            Expr::NotMatch { expr, pattern } => Expr::NotMatch {
                expr: Box::new(self.simplify_expr(*expr)),
                pattern,
            },

            // Recursively simplify functions
            Expr::Function { name, args } => Expr::Function {
                name,
                args: args.into_iter().map(|e| self.simplify_expr(e)).collect(),
            },

            // Recursively simplify aggregates
            Expr::Aggregate {
                func,
                expr,
                distinct,
            } => Expr::Aggregate {
                func,
                expr: expr.map(|e| Box::new(self.simplify_expr(*e))),
                distinct,
            },

            // Leaf nodes remain unchanged
            other => other,
        }
    }

    /// Simplifies a NOT expression.
    fn simplify_not(&self, inner: Expr) -> Expr {
        match inner {
            // Double negation: NOT(NOT(x)) → x
            Expr::UnaryOp {
                op: UnaryOp::Not,
                expr,
            } => *expr,

            // NOT(a AND b) → NOT(a) OR NOT(b) (De Morgan)
            Expr::BinaryOp {
                left,
                op: BinaryOp::And,
                right,
            } => Expr::BinaryOp {
                left: Box::new(self.simplify_not(*left)),
                op: BinaryOp::Or,
                right: Box::new(self.simplify_not(*right)),
            },

            // NOT(a OR b) → NOT(a) AND NOT(b) (De Morgan)
            Expr::BinaryOp {
                left,
                op: BinaryOp::Or,
                right,
            } => Expr::BinaryOp {
                left: Box::new(self.simplify_not(*left)),
                op: BinaryOp::And,
                right: Box::new(self.simplify_not(*right)),
            },

            // NOT(a = b) → a != b
            Expr::BinaryOp {
                left,
                op: BinaryOp::Eq,
                right,
            } => Expr::BinaryOp {
                left,
                op: BinaryOp::Ne,
                right,
            },

            // NOT(a != b) → a = b
            Expr::BinaryOp {
                left,
                op: BinaryOp::Ne,
                right,
            } => Expr::BinaryOp {
                left,
                op: BinaryOp::Eq,
                right,
            },

            // NOT(a < b) → a >= b
            Expr::BinaryOp {
                left,
                op: BinaryOp::Lt,
                right,
            } => Expr::BinaryOp {
                left,
                op: BinaryOp::Ge,
                right,
            },

            // NOT(a <= b) → a > b
            Expr::BinaryOp {
                left,
                op: BinaryOp::Le,
                right,
            } => Expr::BinaryOp {
                left,
                op: BinaryOp::Gt,
                right,
            },

            // NOT(a > b) → a <= b
            Expr::BinaryOp {
                left,
                op: BinaryOp::Gt,
                right,
            } => Expr::BinaryOp {
                left,
                op: BinaryOp::Le,
                right,
            },

            // NOT(a >= b) → a < b
            Expr::BinaryOp {
                left,
                op: BinaryOp::Ge,
                right,
            } => Expr::BinaryOp {
                left,
                op: BinaryOp::Lt,
                right,
            },

            // NOT(IN) → NOT IN
            Expr::In { expr, list } => Expr::NotIn { expr, list },

            // NOT(NOT IN) → IN
            Expr::NotIn { expr, list } => Expr::In { expr, list },

            // NOT(BETWEEN) → NOT BETWEEN
            Expr::Between { expr, low, high } => Expr::NotBetween { expr, low, high },

            // NOT(NOT BETWEEN) → BETWEEN
            Expr::NotBetween { expr, low, high } => Expr::Between { expr, low, high },

            // NOT(LIKE) → NOT LIKE
            Expr::Like { expr, pattern } => Expr::NotLike { expr, pattern },

            // NOT(NOT LIKE) → LIKE
            Expr::NotLike { expr, pattern } => Expr::Like { expr, pattern },

            // NOT(MATCH) → NOT MATCH
            Expr::Match { expr, pattern } => Expr::NotMatch { expr, pattern },

            // NOT(NOT MATCH) → MATCH
            Expr::NotMatch { expr, pattern } => Expr::Match { expr, pattern },

            // NOT(IS NULL) → IS NOT NULL
            Expr::UnaryOp {
                op: UnaryOp::IsNull,
                expr,
            } => Expr::UnaryOp {
                op: UnaryOp::IsNotNull,
                expr,
            },

            // NOT(IS NOT NULL) → IS NULL
            Expr::UnaryOp {
                op: UnaryOp::IsNotNull,
                expr,
            } => Expr::UnaryOp {
                op: UnaryOp::IsNull,
                expr,
            },

            // Default: wrap in NOT
            other => Expr::UnaryOp {
                op: UnaryOp::Not,
                expr: Box::new(other),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cynos_core::Value;

    #[test]
    fn test_double_negation() {
        let pass = NotSimplification;

        // NOT(NOT(x)) → x
        let expr = Expr::not(Expr::not(Expr::column("t", "c", 0)));
        let simplified = pass.simplify_expr(expr);

        assert!(matches!(simplified, Expr::Column(_)));
    }

    #[test]
    fn test_not_eq_to_ne() {
        let pass = NotSimplification;

        // NOT(a = b) → a != b
        let expr = Expr::not(Expr::eq(
            Expr::column("t", "a", 0),
            Expr::literal(1i64),
        ));
        let simplified = pass.simplify_expr(expr);

        assert!(matches!(
            simplified,
            Expr::BinaryOp {
                op: BinaryOp::Ne,
                ..
            }
        ));
    }

    #[test]
    fn test_not_in_to_not_in() {
        let pass = NotSimplification;

        // NOT(IN) → NOT IN
        let expr = Expr::not(Expr::in_list(
            Expr::column("t", "c", 0),
            alloc::vec![Value::Int64(1), Value::Int64(2)],
        ));
        let simplified = pass.simplify_expr(expr);

        assert!(matches!(simplified, Expr::NotIn { .. }));
    }

    #[test]
    fn test_not_between_to_not_between() {
        let pass = NotSimplification;

        // NOT(BETWEEN) → NOT BETWEEN
        let expr = Expr::not(Expr::between(
            Expr::column("t", "c", 0),
            Expr::literal(1i64),
            Expr::literal(10i64),
        ));
        let simplified = pass.simplify_expr(expr);

        assert!(matches!(simplified, Expr::NotBetween { .. }));
    }

    #[test]
    fn test_de_morgan_and() {
        let pass = NotSimplification;

        // NOT(a AND b) → NOT(a) OR NOT(b)
        let expr = Expr::not(Expr::and(
            Expr::eq(Expr::column("t", "a", 0), Expr::literal(1i64)),
            Expr::eq(Expr::column("t", "b", 1), Expr::literal(2i64)),
        ));
        let simplified = pass.simplify_expr(expr);

        // Should be: (a != 1) OR (b != 2)
        assert!(matches!(
            simplified,
            Expr::BinaryOp {
                op: BinaryOp::Or,
                ..
            }
        ));
    }

    #[test]
    fn test_de_morgan_or() {
        let pass = NotSimplification;

        // NOT(a OR b) → NOT(a) AND NOT(b)
        let expr = Expr::not(Expr::or(
            Expr::eq(Expr::column("t", "a", 0), Expr::literal(1i64)),
            Expr::eq(Expr::column("t", "b", 1), Expr::literal(2i64)),
        ));
        let simplified = pass.simplify_expr(expr);

        // Should be: (a != 1) AND (b != 2)
        assert!(matches!(
            simplified,
            Expr::BinaryOp {
                op: BinaryOp::And,
                ..
            }
        ));
    }
}
