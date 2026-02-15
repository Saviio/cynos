//! Cross product pass - converts multi-way cross products to binary tree structure.
//!
//! This pass transforms a CrossProduct node with more than 2 children
//! (represented as nested CrossProducts) into a balanced binary tree structure.
//!
//! Example:
//! ```text
//! CrossProduct(A, B, C, D)    =>    CrossProduct
//!                                    /        \
//!                              CrossProduct  CrossProduct
//!                               /    \        /    \
//!                              A      B      C      D
//! ```
//!
//! This transformation is necessary because:
//! 1. The execution engine expects binary cross products
//! 2. It enables subsequent passes (like ImplicitJoinsPass) to convert
//!    cross products to joins more effectively

use crate::optimizer::OptimizerPass;
use crate::planner::LogicalPlan;
use alloc::boxed::Box;
use alloc::vec::Vec;

/// Pass that converts multi-way cross products to binary tree structure.
pub struct CrossProductPass;

impl OptimizerPass for CrossProductPass {
    fn optimize(&self, plan: LogicalPlan) -> LogicalPlan {
        self.traverse(plan)
    }

    fn name(&self) -> &'static str {
        "cross_product"
    }
}

impl CrossProductPass {
    /// Recursively traverses the plan tree and transforms cross products.
    fn traverse(&self, plan: LogicalPlan) -> LogicalPlan {
        match plan {
            LogicalPlan::CrossProduct { left, right } => {
                // First collect all tables from nested cross products
                let mut tables = Vec::new();
                self.collect_cross_product_children(*left, &mut tables);
                self.collect_cross_product_children(*right, &mut tables);

                // If we have more than 2 tables, restructure into binary tree
                if tables.len() > 2 {
                    self.build_binary_cross_product(tables)
                } else if tables.len() == 2 {
                    LogicalPlan::CrossProduct {
                        left: Box::new(self.traverse(tables.remove(0))),
                        right: Box::new(self.traverse(tables.remove(0))),
                    }
                } else if tables.len() == 1 {
                    self.traverse(tables.remove(0))
                } else {
                    LogicalPlan::Empty
                }
            }

            LogicalPlan::Filter { input, predicate } => LogicalPlan::Filter {
                input: Box::new(self.traverse(*input)),
                predicate,
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

    /// Collects all children from nested cross products.
    /// Non-cross-product nodes are added directly to the list.
    fn collect_cross_product_children(&self, plan: LogicalPlan, children: &mut Vec<LogicalPlan>) {
        match plan {
            LogicalPlan::CrossProduct { left, right } => {
                self.collect_cross_product_children(*left, children);
                self.collect_cross_product_children(*right, children);
            }
            other => children.push(other),
        }
    }

    /// Builds a balanced binary tree of cross products from a list of tables.
    /// Uses left-to-right pairing: ((A × B) × (C × D))
    fn build_binary_cross_product(&self, mut tables: Vec<LogicalPlan>) -> LogicalPlan {
        // Recursively optimize each table first
        tables = tables.into_iter().map(|t| self.traverse(t)).collect();

        // Build binary tree by pairing adjacent tables
        while tables.len() > 1 {
            let mut new_level = Vec::new();
            let mut i = 0;
            while i < tables.len() {
                if i + 1 < tables.len() {
                    new_level.push(LogicalPlan::CrossProduct {
                        left: Box::new(tables[i].clone()),
                        right: Box::new(tables[i + 1].clone()),
                    });
                    i += 2;
                } else {
                    // Odd number of tables - carry the last one up
                    new_level.push(tables[i].clone());
                    i += 1;
                }
            }
            tables = new_level;
        }

        tables.pop().unwrap_or(LogicalPlan::Empty)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn count_cross_products(plan: &LogicalPlan) -> usize {
        match plan {
            LogicalPlan::CrossProduct { left, right } => {
                1 + count_cross_products(left) + count_cross_products(right)
            }
            LogicalPlan::Filter { input, .. }
            | LogicalPlan::Project { input, .. }
            | LogicalPlan::Aggregate { input, .. }
            | LogicalPlan::Sort { input, .. }
            | LogicalPlan::Limit { input, .. } => count_cross_products(input),
            LogicalPlan::Join { left, right, .. } | LogicalPlan::Union { left, right, .. } => {
                count_cross_products(left) + count_cross_products(right)
            }
            _ => 0,
        }
    }

    fn count_scans(plan: &LogicalPlan) -> usize {
        match plan {
            LogicalPlan::Scan { .. } => 1,
            LogicalPlan::CrossProduct { left, right } => count_scans(left) + count_scans(right),
            LogicalPlan::Filter { input, .. }
            | LogicalPlan::Project { input, .. }
            | LogicalPlan::Aggregate { input, .. }
            | LogicalPlan::Sort { input, .. }
            | LogicalPlan::Limit { input, .. } => count_scans(input),
            LogicalPlan::Join { left, right, .. } | LogicalPlan::Union { left, right, .. } => {
                count_scans(left) + count_scans(right)
            }
            _ => 0,
        }
    }

    #[test]
    fn test_two_table_cross_product_unchanged() {
        let pass = CrossProductPass;
        let plan = LogicalPlan::cross_product(LogicalPlan::scan("a"), LogicalPlan::scan("b"));

        let result = pass.optimize(plan);

        assert!(matches!(result, LogicalPlan::CrossProduct { .. }));
        assert_eq!(count_cross_products(&result), 1);
        assert_eq!(count_scans(&result), 2);
    }

    #[test]
    fn test_three_table_cross_product() {
        let pass = CrossProductPass;

        // Create: CrossProduct(CrossProduct(A, B), C)
        let plan = LogicalPlan::cross_product(
            LogicalPlan::cross_product(LogicalPlan::scan("a"), LogicalPlan::scan("b")),
            LogicalPlan::scan("c"),
        );

        let result = pass.optimize(plan);

        // Should have 2 cross products (binary tree with 3 leaves)
        assert_eq!(count_cross_products(&result), 2);
        assert_eq!(count_scans(&result), 3);
    }

    #[test]
    fn test_four_table_cross_product() {
        let pass = CrossProductPass;

        // Create a chain: CrossProduct(CrossProduct(CrossProduct(A, B), C), D)
        let plan = LogicalPlan::cross_product(
            LogicalPlan::cross_product(
                LogicalPlan::cross_product(LogicalPlan::scan("a"), LogicalPlan::scan("b")),
                LogicalPlan::scan("c"),
            ),
            LogicalPlan::scan("d"),
        );

        let result = pass.optimize(plan);

        // Should have 3 cross products (balanced binary tree with 4 leaves)
        // Structure: ((A × B) × (C × D))
        assert_eq!(count_cross_products(&result), 3);
        assert_eq!(count_scans(&result), 4);
    }

    #[test]
    fn test_cross_product_with_filter() {
        let pass = CrossProductPass;

        // Create: Filter(CrossProduct(A, B, C))
        let cross = LogicalPlan::cross_product(
            LogicalPlan::cross_product(LogicalPlan::scan("a"), LogicalPlan::scan("b")),
            LogicalPlan::scan("c"),
        );
        let plan = LogicalPlan::filter(
            cross,
            crate::ast::Expr::eq(
                crate::ast::Expr::column("a", "id", 0),
                crate::ast::Expr::literal(1i64),
            ),
        );

        let result = pass.optimize(plan);

        // Filter should be preserved, cross products restructured
        assert!(matches!(result, LogicalPlan::Filter { .. }));
        if let LogicalPlan::Filter { input, .. } = result {
            assert_eq!(count_cross_products(&input), 2);
            assert_eq!(count_scans(&input), 3);
        }
    }

    #[test]
    fn test_single_table_no_cross_product() {
        let pass = CrossProductPass;
        let plan = LogicalPlan::scan("a");

        let result = pass.optimize(plan);

        assert!(matches!(result, LogicalPlan::Scan { .. }));
    }
}
