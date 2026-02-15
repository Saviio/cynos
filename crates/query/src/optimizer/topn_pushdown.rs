//! TopN pushdown optimization pass.
//!
//! This pass identifies `Limit -> Sort` patterns and converts them to a single
//! `TopN` operator, which is more efficient for selecting the top K elements.
//!
//! Example:
//! ```text
//! Limit(10, 0)                =>    TopN(order_by, limit=10, offset=0)
//!      |                                  |
//! Sort(order_by)                      input
//!      |
//!   input
//! ```
//!
//! Performance benefit:
//! - Full sort: O(n log n) time, O(n) space
//! - TopN with heap: O(n log k) time, O(k) space (where k = limit + offset)
//!
//! This optimization is safe because:
//! - TopN produces the same result as Sort + Limit
//! - It only applies when Limit is directly above Sort (no intervening operators)

use crate::planner::PhysicalPlan;
use alloc::boxed::Box;

/// TopN pushdown optimization pass.
///
/// Converts `Limit -> Sort` patterns to `TopN` for more efficient top-k selection.
pub struct TopNPushdown;

impl TopNPushdown {
    /// Creates a new TopNPushdown pass.
    pub fn new() -> Self {
        Self
    }

    /// Optimizes the physical plan by converting Limit+Sort to TopN.
    pub fn optimize(&self, plan: PhysicalPlan) -> PhysicalPlan {
        self.traverse(plan)
    }

    fn traverse(&self, plan: PhysicalPlan) -> PhysicalPlan {
        match plan {
            PhysicalPlan::Limit {
                input,
                limit,
                offset,
            } => {
                let optimized_input = self.traverse(*input);

                // Check if input is a Sort - if so, convert to TopN
                if let PhysicalPlan::Sort { input: sort_input, order_by } = optimized_input {
                    // Convert to TopN - more efficient for top-k selection
                    return PhysicalPlan::TopN {
                        input: sort_input,
                        order_by,
                        limit,
                        offset,
                    };
                }

                // Check if input is IndexGet - push limit into it
                if let PhysicalPlan::IndexGet { table, index, key, limit: _ } = optimized_input {
                    // Push limit into IndexGet for early termination
                    // Note: offset is handled by skipping rows after IndexGet
                    if offset == 0 {
                        return PhysicalPlan::IndexGet {
                            table,
                            index,
                            key,
                            limit: Some(limit),
                        };
                    } else {
                        // With offset, we need to fetch limit + offset rows
                        return PhysicalPlan::Limit {
                            input: Box::new(PhysicalPlan::IndexGet {
                                table,
                                index,
                                key,
                                limit: Some(limit + offset),
                            }),
                            limit,
                            offset,
                        };
                    }
                }

                // Check if input is IndexScan without limit - push limit into it
                if let PhysicalPlan::IndexScan {
                    table,
                    index,
                    range_start,
                    range_end,
                    include_start,
                    include_end,
                    limit: None,
                    offset: None,
                    reverse,
                } = optimized_input
                {
                    // Push limit into IndexScan for early termination
                    return PhysicalPlan::IndexScan {
                        table,
                        index,
                        range_start,
                        range_end,
                        include_start,
                        include_end,
                        limit: Some(limit + offset),
                        offset: Some(offset),
                        reverse,
                    };
                }

                // Not a Sort/IndexGet/IndexScan, keep as Limit
                PhysicalPlan::Limit {
                    input: Box::new(optimized_input),
                    limit,
                    offset,
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

            PhysicalPlan::Sort { input, order_by } => PhysicalPlan::Sort {
                input: Box::new(self.traverse(*input)),
                order_by,
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
}

impl Default for TopNPushdown {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::{Expr, SortOrder};

    #[test]
    fn test_limit_sort_converted_to_topn() {
        let pass = TopNPushdown::new();

        // Create: Limit(10, 5) -> Sort -> TableScan
        let plan = PhysicalPlan::Limit {
            input: Box::new(PhysicalPlan::Sort {
                input: Box::new(PhysicalPlan::table_scan("users")),
                order_by: alloc::vec![(Expr::column("users", "id", 0), SortOrder::Asc)],
            }),
            limit: 10,
            offset: 5,
        };

        let result = pass.optimize(plan);

        // Should be TopN -> TableScan
        if let PhysicalPlan::TopN {
            input,
            order_by,
            limit,
            offset,
        } = result
        {
            assert_eq!(limit, 10);
            assert_eq!(offset, 5);
            assert_eq!(order_by.len(), 1);
            assert!(matches!(*input, PhysicalPlan::TableScan { .. }));
        } else {
            panic!("Expected TopN, got {:?}", result);
        }
    }

    #[test]
    fn test_limit_without_sort_unchanged() {
        let pass = TopNPushdown::new();

        // Create: Limit(10, 0) -> TableScan (no Sort)
        let plan = PhysicalPlan::Limit {
            input: Box::new(PhysicalPlan::table_scan("users")),
            limit: 10,
            offset: 0,
        };

        let result = pass.optimize(plan);

        // Should remain as Limit -> TableScan
        assert!(matches!(result, PhysicalPlan::Limit { .. }));
        if let PhysicalPlan::Limit { input, .. } = result {
            assert!(matches!(*input, PhysicalPlan::TableScan { .. }));
        }
    }

    #[test]
    fn test_limit_filter_sort_not_converted() {
        let pass = TopNPushdown::new();

        // Create: Limit -> Filter -> Sort -> TableScan
        // Filter between Limit and Sort blocks conversion
        let plan = PhysicalPlan::Limit {
            input: Box::new(PhysicalPlan::Filter {
                input: Box::new(PhysicalPlan::Sort {
                    input: Box::new(PhysicalPlan::table_scan("users")),
                    order_by: alloc::vec![(Expr::column("users", "id", 0), SortOrder::Asc)],
                }),
                predicate: Expr::eq(Expr::column("users", "active", 1), Expr::literal(true)),
            }),
            limit: 10,
            offset: 0,
        };

        let result = pass.optimize(plan);

        // Should remain as Limit -> Filter -> Sort (not converted)
        // because Filter is between Limit and Sort
        assert!(matches!(result, PhysicalPlan::Limit { .. }));
        if let PhysicalPlan::Limit { input, .. } = result {
            assert!(matches!(*input, PhysicalPlan::Filter { .. }));
        }
    }

    #[test]
    fn test_nested_limit_sort_converted() {
        let pass = TopNPushdown::new();

        // Create: Project -> Limit -> Sort -> TableScan
        let plan = PhysicalPlan::Project {
            input: Box::new(PhysicalPlan::Limit {
                input: Box::new(PhysicalPlan::Sort {
                    input: Box::new(PhysicalPlan::table_scan("users")),
                    order_by: alloc::vec![(Expr::column("users", "id", 0), SortOrder::Desc)],
                }),
                limit: 5,
                offset: 0,
            }),
            columns: alloc::vec![Expr::column("users", "name", 1)],
        };

        let result = pass.optimize(plan);

        // Should be Project -> TopN -> TableScan
        if let PhysicalPlan::Project { input, .. } = result {
            if let PhysicalPlan::TopN { limit, offset, .. } = *input {
                assert_eq!(limit, 5);
                assert_eq!(offset, 0);
            } else {
                panic!("Expected TopN inside Project");
            }
        } else {
            panic!("Expected Project, got {:?}", result);
        }
    }

    #[test]
    fn test_multiple_sort_columns() {
        let pass = TopNPushdown::new();

        // Create: Limit -> Sort(col1 ASC, col2 DESC) -> TableScan
        let plan = PhysicalPlan::Limit {
            input: Box::new(PhysicalPlan::Sort {
                input: Box::new(PhysicalPlan::table_scan("users")),
                order_by: alloc::vec![
                    (Expr::column("users", "name", 1), SortOrder::Asc),
                    (Expr::column("users", "id", 0), SortOrder::Desc),
                ],
            }),
            limit: 20,
            offset: 10,
        };

        let result = pass.optimize(plan);

        // Should be TopN with both sort columns preserved
        if let PhysicalPlan::TopN {
            order_by,
            limit,
            offset,
            ..
        } = result
        {
            assert_eq!(limit, 20);
            assert_eq!(offset, 10);
            assert_eq!(order_by.len(), 2);
            assert_eq!(order_by[0].1, SortOrder::Asc);
            assert_eq!(order_by[1].1, SortOrder::Desc);
        } else {
            panic!("Expected TopN, got {:?}", result);
        }
    }

    #[test]
    fn test_sort_in_subquery_converted() {
        let pass = TopNPushdown::new();

        // Create: HashJoin(left: Limit -> Sort, right: TableScan)
        let plan = PhysicalPlan::HashJoin {
            left: Box::new(PhysicalPlan::Limit {
                input: Box::new(PhysicalPlan::Sort {
                    input: Box::new(PhysicalPlan::table_scan("orders")),
                    order_by: alloc::vec![(Expr::column("orders", "amount", 1), SortOrder::Desc)],
                }),
                limit: 100,
                offset: 0,
            }),
            right: Box::new(PhysicalPlan::table_scan("users")),
            condition: Expr::eq(
                Expr::column("orders", "user_id", 2),
                Expr::column("users", "id", 0),
            ),
            join_type: crate::ast::JoinType::Inner,
        };

        let result = pass.optimize(plan);

        // Left side should be converted to TopN
        if let PhysicalPlan::HashJoin { left, .. } = result {
            assert!(matches!(*left, PhysicalPlan::TopN { .. }));
        } else {
            panic!("Expected HashJoin, got {:?}", result);
        }
    }
}
