//! Outer join simplification optimization pass.
//!
//! This pass converts outer joins to inner joins when the WHERE clause
//! contains predicates that would filter out NULL values from the outer side.
//!
//! Example:
//! ```text
//! Filter(orders.amount > 100)     =>    InnerJoin(users, orders)
//!        |                                     |
//! LeftJoin(users, orders)              (same condition)
//! ```
//!
//! This optimization is safe because:
//! - LEFT JOIN produces NULL for right-side columns when there's no match
//! - If the WHERE clause filters on right-side columns with non-NULL conditions,
//!   those NULL rows would be filtered out anyway
//! - Converting to INNER JOIN is more efficient (no NULL handling needed)
//!
//! Conditions that reject NULL:
//! - `col = value` (equality with non-NULL value)
//! - `col > value`, `col < value`, etc. (comparisons)
//! - `col IS NOT NULL`
//! - `col IN (...)` with non-NULL values
//! - `col BETWEEN a AND b`

use crate::ast::{BinaryOp, Expr, JoinType, UnaryOp};
use crate::optimizer::OptimizerPass;
use crate::planner::LogicalPlan;
use alloc::boxed::Box;
use alloc::string::String;
use hashbrown::HashSet;

/// Outer join simplification optimization.
///
/// Converts outer joins to inner joins when predicates reject NULL values.
pub struct OuterJoinSimplification;

impl OptimizerPass for OuterJoinSimplification {
    fn optimize(&self, plan: LogicalPlan) -> LogicalPlan {
        self.simplify(plan)
    }

    fn name(&self) -> &'static str {
        "outer_join_simplification"
    }
}

impl OuterJoinSimplification {
    fn simplify(&self, plan: LogicalPlan) -> LogicalPlan {
        match plan {
            // Look for Filter above Join pattern
            LogicalPlan::Filter { input, predicate } => {
                let optimized_input = self.simplify(*input);

                // Check if we can simplify an outer join
                if let LogicalPlan::Join {
                    left,
                    right,
                    condition,
                    join_type,
                } = optimized_input
                {
                    if let Some(new_join_type) =
                        self.try_simplify_join(&predicate, &left, &right, join_type)
                    {
                        return LogicalPlan::Filter {
                            input: Box::new(LogicalPlan::Join {
                                left,
                                right,
                                condition,
                                join_type: new_join_type,
                            }),
                            predicate,
                        };
                    }

                    // No simplification possible
                    return LogicalPlan::Filter {
                        input: Box::new(LogicalPlan::Join {
                            left,
                            right,
                            condition,
                            join_type,
                        }),
                        predicate,
                    };
                }

                LogicalPlan::Filter {
                    input: Box::new(optimized_input),
                    predicate,
                }
            }

            LogicalPlan::Project { input, columns } => LogicalPlan::Project {
                input: Box::new(self.simplify(*input)),
                columns,
            },

            LogicalPlan::Join {
                left,
                right,
                condition,
                join_type,
            } => LogicalPlan::Join {
                left: Box::new(self.simplify(*left)),
                right: Box::new(self.simplify(*right)),
                condition,
                join_type,
            },

            LogicalPlan::Aggregate {
                input,
                group_by,
                aggregates,
            } => LogicalPlan::Aggregate {
                input: Box::new(self.simplify(*input)),
                group_by,
                aggregates,
            },

            LogicalPlan::Sort { input, order_by } => LogicalPlan::Sort {
                input: Box::new(self.simplify(*input)),
                order_by,
            },

            LogicalPlan::Limit {
                input,
                limit,
                offset,
            } => LogicalPlan::Limit {
                input: Box::new(self.simplify(*input)),
                limit,
                offset,
            },

            LogicalPlan::CrossProduct { left, right } => LogicalPlan::CrossProduct {
                left: Box::new(self.simplify(*left)),
                right: Box::new(self.simplify(*right)),
            },

            LogicalPlan::Union { left, right, all } => LogicalPlan::Union {
                left: Box::new(self.simplify(*left)),
                right: Box::new(self.simplify(*right)),
                all,
            },

            // Leaf nodes - no transformation
            LogicalPlan::Scan { .. }
            | LogicalPlan::IndexScan { .. }
            | LogicalPlan::IndexGet { .. }
            | LogicalPlan::IndexInGet { .. }
            | LogicalPlan::GinIndexScan { .. }
            | LogicalPlan::GinIndexScanMulti { .. }
            | LogicalPlan::Empty => plan,
        }
    }

    /// Try to simplify an outer join to inner join based on the predicate.
    fn try_simplify_join(
        &self,
        predicate: &Expr,
        left: &LogicalPlan,
        right: &LogicalPlan,
        join_type: JoinType,
    ) -> Option<JoinType> {
        match join_type {
            JoinType::LeftOuter => {
                // For LEFT JOIN, check if predicate rejects NULLs from right side
                let right_tables = self.extract_tables(right);
                if self.predicate_rejects_null(predicate, &right_tables) {
                    return Some(JoinType::Inner);
                }
                None
            }

            JoinType::RightOuter => {
                // For RIGHT JOIN, check if predicate rejects NULLs from left side
                let left_tables = self.extract_tables(left);
                if self.predicate_rejects_null(predicate, &left_tables) {
                    return Some(JoinType::Inner);
                }
                None
            }

            JoinType::FullOuter => {
                // For FULL OUTER JOIN, need predicates rejecting NULLs from both sides
                let left_tables = self.extract_tables(left);
                let right_tables = self.extract_tables(right);

                let rejects_left_null = self.predicate_rejects_null(predicate, &left_tables);
                let rejects_right_null = self.predicate_rejects_null(predicate, &right_tables);

                if rejects_left_null && rejects_right_null {
                    return Some(JoinType::Inner);
                } else if rejects_right_null {
                    return Some(JoinType::LeftOuter);
                } else if rejects_left_null {
                    return Some(JoinType::RightOuter);
                }
                None
            }

            // Inner and Cross joins don't need simplification
            JoinType::Inner | JoinType::Cross => None,
        }
    }

    /// Check if a predicate would reject NULL values from the given tables.
    fn predicate_rejects_null(&self, predicate: &Expr, tables: &HashSet<String>) -> bool {
        match predicate {
            // IS NOT NULL explicitly rejects NULL
            Expr::UnaryOp {
                op: UnaryOp::IsNotNull,
                expr,
            } => self.expr_references_tables(expr, tables),

            // Comparisons with literals reject NULL (NULL compared to anything is NULL/false)
            Expr::BinaryOp { left, op, right } => {
                match op {
                    // Equality and comparison operators reject NULL
                    BinaryOp::Eq
                    | BinaryOp::Ne
                    | BinaryOp::Lt
                    | BinaryOp::Le
                    | BinaryOp::Gt
                    | BinaryOp::Ge => {
                        // Check if one side is a column from target tables and other is literal
                        let left_refs_tables = self.expr_references_tables(left, tables);
                        let right_refs_tables = self.expr_references_tables(right, tables);
                        let left_is_literal = matches!(left.as_ref(), Expr::Literal(_));
                        let right_is_literal = matches!(right.as_ref(), Expr::Literal(_));

                        // col = literal or literal = col rejects NULL
                        (left_refs_tables && right_is_literal)
                            || (right_refs_tables && left_is_literal)
                            // col = col from same tables also rejects NULL
                            || (left_refs_tables && right_refs_tables)
                    }

                    // AND: both sides must reject NULL for the whole predicate to reject NULL
                    // But if either side rejects NULL, the row is filtered
                    BinaryOp::And => {
                        self.predicate_rejects_null(left, tables)
                            || self.predicate_rejects_null(right, tables)
                    }

                    // OR: both sides must reject NULL
                    BinaryOp::Or => {
                        self.predicate_rejects_null(left, tables)
                            && self.predicate_rejects_null(right, tables)
                    }

                    // LIKE rejects NULL
                    BinaryOp::Like => self.expr_references_tables(left, tables),

                    // IN rejects NULL
                    BinaryOp::In => self.expr_references_tables(left, tables),

                    // BETWEEN rejects NULL
                    BinaryOp::Between => self.expr_references_tables(left, tables),

                    _ => false,
                }
            }

            // IN expression rejects NULL
            Expr::In { expr, .. } => self.expr_references_tables(expr, tables),

            // BETWEEN rejects NULL
            Expr::Between { expr, .. } => self.expr_references_tables(expr, tables),

            // LIKE rejects NULL
            Expr::Like { expr, .. } => self.expr_references_tables(expr, tables),

            // IS NULL does NOT reject NULL (it accepts NULL)
            Expr::UnaryOp {
                op: UnaryOp::IsNull,
                ..
            } => false,

            // NOT of something that accepts NULL might reject NULL
            Expr::UnaryOp {
                op: UnaryOp::Not,
                expr,
            } => {
                // NOT (IS NULL) = IS NOT NULL, which rejects NULL
                if let Expr::UnaryOp {
                    op: UnaryOp::IsNull,
                    expr: inner,
                } = expr.as_ref()
                {
                    return self.expr_references_tables(inner, tables);
                }
                false
            }

            _ => false,
        }
    }

    /// Check if an expression references any of the given tables.
    fn expr_references_tables(&self, expr: &Expr, tables: &HashSet<String>) -> bool {
        match expr {
            Expr::Column(col) => tables.contains(&col.table),
            Expr::BinaryOp { left, right, .. } => {
                self.expr_references_tables(left, tables)
                    || self.expr_references_tables(right, tables)
            }
            Expr::UnaryOp { expr, .. } => self.expr_references_tables(expr, tables),
            Expr::Function { args, .. } => {
                args.iter().any(|arg| self.expr_references_tables(arg, tables))
            }
            Expr::Aggregate { expr, .. } => {
                expr.as_ref()
                    .map(|e| self.expr_references_tables(e, tables))
                    .unwrap_or(false)
            }
            Expr::Between { expr, low, high } => {
                self.expr_references_tables(expr, tables)
                    || self.expr_references_tables(low, tables)
                    || self.expr_references_tables(high, tables)
            }
            Expr::In { expr, list } => {
                self.expr_references_tables(expr, tables)
                    || list.iter().any(|e| self.expr_references_tables(e, tables))
            }
            Expr::Like { expr, .. } => self.expr_references_tables(expr, tables),
            Expr::NotBetween { expr, low, high } => {
                self.expr_references_tables(expr, tables)
                    || self.expr_references_tables(low, tables)
                    || self.expr_references_tables(high, tables)
            }
            Expr::NotIn { expr, list } => {
                self.expr_references_tables(expr, tables)
                    || list.iter().any(|e| self.expr_references_tables(e, tables))
            }
            Expr::NotLike { expr, .. } => self.expr_references_tables(expr, tables),
            Expr::Match { expr, .. } => self.expr_references_tables(expr, tables),
            Expr::NotMatch { expr, .. } => self.expr_references_tables(expr, tables),
            Expr::Literal(_) => false,
        }
    }

    /// Extract all table names referenced by a plan.
    fn extract_tables(&self, plan: &LogicalPlan) -> HashSet<String> {
        let mut tables = HashSet::new();
        self.collect_tables(plan, &mut tables);
        tables
    }

    fn collect_tables(&self, plan: &LogicalPlan, tables: &mut HashSet<String>) {
        match plan {
            LogicalPlan::Scan { table } => {
                tables.insert(table.clone());
            }
            LogicalPlan::IndexScan { table, .. }
            | LogicalPlan::IndexGet { table, .. }
            | LogicalPlan::IndexInGet { table, .. }
            | LogicalPlan::GinIndexScan { table, .. }
            | LogicalPlan::GinIndexScanMulti { table, .. } => {
                tables.insert(table.clone());
            }
            LogicalPlan::Filter { input, .. }
            | LogicalPlan::Project { input, .. }
            | LogicalPlan::Aggregate { input, .. }
            | LogicalPlan::Sort { input, .. }
            | LogicalPlan::Limit { input, .. } => {
                self.collect_tables(input, tables);
            }
            LogicalPlan::Join { left, right, .. }
            | LogicalPlan::CrossProduct { left, right }
            | LogicalPlan::Union { left, right, .. } => {
                self.collect_tables(left, tables);
                self.collect_tables(right, tables);
            }
            LogicalPlan::Empty => {}
        }
    }
}

impl Default for OuterJoinSimplification {
    fn default() -> Self {
        Self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_left_join_to_inner_with_equality() {
        let pass = OuterJoinSimplification;

        // Filter(orders.amount = 100) -> LeftJoin
        // Should convert to InnerJoin because equality rejects NULL
        let plan = LogicalPlan::filter(
            LogicalPlan::left_join(
                LogicalPlan::scan("users"),
                LogicalPlan::scan("orders"),
                Expr::eq(
                    Expr::column("users", "id", 0),
                    Expr::column("orders", "user_id", 0),
                ),
            ),
            Expr::eq(Expr::column("orders", "amount", 1), Expr::literal(100i64)),
        );

        let optimized = pass.optimize(plan);

        if let LogicalPlan::Filter { input, .. } = optimized {
            if let LogicalPlan::Join { join_type, .. } = *input {
                assert_eq!(join_type, JoinType::Inner);
            } else {
                panic!("Expected Join");
            }
        } else {
            panic!("Expected Filter");
        }
    }

    #[test]
    fn test_left_join_to_inner_with_is_not_null() {
        let pass = OuterJoinSimplification;

        // Filter(orders.id IS NOT NULL) -> LeftJoin
        let plan = LogicalPlan::filter(
            LogicalPlan::left_join(
                LogicalPlan::scan("users"),
                LogicalPlan::scan("orders"),
                Expr::eq(
                    Expr::column("users", "id", 0),
                    Expr::column("orders", "user_id", 0),
                ),
            ),
            Expr::is_not_null(Expr::column("orders", "id", 0)),
        );

        let optimized = pass.optimize(plan);

        if let LogicalPlan::Filter { input, .. } = optimized {
            if let LogicalPlan::Join { join_type, .. } = *input {
                assert_eq!(join_type, JoinType::Inner);
            } else {
                panic!("Expected Join");
            }
        } else {
            panic!("Expected Filter");
        }
    }

    #[test]
    fn test_left_join_to_inner_with_comparison() {
        let pass = OuterJoinSimplification;

        // Filter(orders.amount > 100) -> LeftJoin
        let plan = LogicalPlan::filter(
            LogicalPlan::left_join(
                LogicalPlan::scan("users"),
                LogicalPlan::scan("orders"),
                Expr::eq(
                    Expr::column("users", "id", 0),
                    Expr::column("orders", "user_id", 0),
                ),
            ),
            Expr::gt(Expr::column("orders", "amount", 1), Expr::literal(100i64)),
        );

        let optimized = pass.optimize(plan);

        if let LogicalPlan::Filter { input, .. } = optimized {
            if let LogicalPlan::Join { join_type, .. } = *input {
                assert_eq!(join_type, JoinType::Inner);
            } else {
                panic!("Expected Join");
            }
        } else {
            panic!("Expected Filter");
        }
    }

    #[test]
    fn test_left_join_unchanged_with_left_predicate() {
        let pass = OuterJoinSimplification;

        // Filter on LEFT side should NOT convert to inner join
        let plan = LogicalPlan::filter(
            LogicalPlan::left_join(
                LogicalPlan::scan("users"),
                LogicalPlan::scan("orders"),
                Expr::eq(
                    Expr::column("users", "id", 0),
                    Expr::column("orders", "user_id", 0),
                ),
            ),
            Expr::eq(Expr::column("users", "active", 1), Expr::literal(true)),
        );

        let optimized = pass.optimize(plan);

        if let LogicalPlan::Filter { input, .. } = optimized {
            if let LogicalPlan::Join { join_type, .. } = *input {
                assert_eq!(join_type, JoinType::LeftOuter);
            } else {
                panic!("Expected Join");
            }
        } else {
            panic!("Expected Filter");
        }
    }

    #[test]
    fn test_left_join_unchanged_with_is_null() {
        let pass = OuterJoinSimplification;

        // IS NULL does NOT reject NULL, so should stay as LEFT JOIN
        let plan = LogicalPlan::filter(
            LogicalPlan::left_join(
                LogicalPlan::scan("users"),
                LogicalPlan::scan("orders"),
                Expr::eq(
                    Expr::column("users", "id", 0),
                    Expr::column("orders", "user_id", 0),
                ),
            ),
            Expr::is_null(Expr::column("orders", "id", 0)),
        );

        let optimized = pass.optimize(plan);

        if let LogicalPlan::Filter { input, .. } = optimized {
            if let LogicalPlan::Join { join_type, .. } = *input {
                assert_eq!(join_type, JoinType::LeftOuter);
            } else {
                panic!("Expected Join");
            }
        } else {
            panic!("Expected Filter");
        }
    }

    #[test]
    fn test_right_join_to_inner() {
        let pass = OuterJoinSimplification;

        // Filter on LEFT side of RIGHT JOIN should convert to INNER
        let plan = LogicalPlan::filter(
            LogicalPlan::Join {
                left: Box::new(LogicalPlan::scan("users")),
                right: Box::new(LogicalPlan::scan("orders")),
                condition: Expr::eq(
                    Expr::column("users", "id", 0),
                    Expr::column("orders", "user_id", 0),
                ),
                join_type: JoinType::RightOuter,
            },
            Expr::eq(Expr::column("users", "active", 1), Expr::literal(true)),
        );

        let optimized = pass.optimize(plan);

        if let LogicalPlan::Filter { input, .. } = optimized {
            if let LogicalPlan::Join { join_type, .. } = *input {
                assert_eq!(join_type, JoinType::Inner);
            } else {
                panic!("Expected Join");
            }
        } else {
            panic!("Expected Filter");
        }
    }

    #[test]
    fn test_and_predicate_rejects_null() {
        let pass = OuterJoinSimplification;

        // Filter(orders.amount > 100 AND orders.status = 'active') -> LeftJoin
        // Either condition rejects NULL, so should convert
        let plan = LogicalPlan::filter(
            LogicalPlan::left_join(
                LogicalPlan::scan("users"),
                LogicalPlan::scan("orders"),
                Expr::eq(
                    Expr::column("users", "id", 0),
                    Expr::column("orders", "user_id", 0),
                ),
            ),
            Expr::and(
                Expr::gt(Expr::column("orders", "amount", 1), Expr::literal(100i64)),
                Expr::eq(
                    Expr::column("orders", "status", 2),
                    Expr::literal("active"),
                ),
            ),
        );

        let optimized = pass.optimize(plan);

        if let LogicalPlan::Filter { input, .. } = optimized {
            if let LogicalPlan::Join { join_type, .. } = *input {
                assert_eq!(join_type, JoinType::Inner);
            } else {
                panic!("Expected Join");
            }
        } else {
            panic!("Expected Filter");
        }
    }

    #[test]
    fn test_inner_join_unchanged() {
        let pass = OuterJoinSimplification;

        // Inner join should remain unchanged
        let plan = LogicalPlan::filter(
            LogicalPlan::inner_join(
                LogicalPlan::scan("users"),
                LogicalPlan::scan("orders"),
                Expr::eq(
                    Expr::column("users", "id", 0),
                    Expr::column("orders", "user_id", 0),
                ),
            ),
            Expr::gt(Expr::column("orders", "amount", 1), Expr::literal(100i64)),
        );

        let optimized = pass.optimize(plan);

        if let LogicalPlan::Filter { input, .. } = optimized {
            if let LogicalPlan::Join { join_type, .. } = *input {
                assert_eq!(join_type, JoinType::Inner);
            } else {
                panic!("Expected Join");
            }
        } else {
            panic!("Expected Filter");
        }
    }

    #[test]
    fn test_nested_joins() {
        let pass = OuterJoinSimplification;

        // Nested joins should be processed recursively
        let inner_join = LogicalPlan::left_join(
            LogicalPlan::scan("orders"),
            LogicalPlan::scan("items"),
            Expr::eq(
                Expr::column("orders", "id", 0),
                Expr::column("items", "order_id", 0),
            ),
        );

        let plan = LogicalPlan::filter(
            LogicalPlan::left_join(
                LogicalPlan::scan("users"),
                inner_join,
                Expr::eq(
                    Expr::column("users", "id", 0),
                    Expr::column("orders", "user_id", 0),
                ),
            ),
            Expr::gt(Expr::column("orders", "amount", 1), Expr::literal(100i64)),
        );

        let optimized = pass.optimize(plan);

        // The outer join should be converted because predicate references orders
        if let LogicalPlan::Filter { input, .. } = optimized {
            if let LogicalPlan::Join { join_type, .. } = *input {
                assert_eq!(join_type, JoinType::Inner);
            } else {
                panic!("Expected Join");
            }
        } else {
            panic!("Expected Filter");
        }
    }
}
