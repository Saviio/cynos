//! Limit/Skip by index pass - leverages indexes to implement LIMIT and SKIP.
//!
//! This pass identifies Limit nodes that can be pushed down to an IndexScan,
//! allowing the index to directly return only the needed rows.
//!
//! Example:
//! ```text
//! Limit(10, 5)                =>    IndexScan(idx_id, limit=10, skip=5)
//!      |                                  |
//! IndexScan(idx_id)                 (no Limit needed)
//! ```
//!
//! Also converts TableScan + Limit to IndexScan when a primary key index exists:
//! ```text
//! Limit(100, 0)               =>    IndexScan(pk_idx, limit=100, offset=0)
//!      |
//! TableScan(users)
//! ```
//!
//! This optimization is beneficial when:
//! 1. There's an IndexScan that can support limit/skip
//! 2. There are no intervening operations that would change row count
//!    (like aggregations, joins, or filters)

use crate::context::ExecutionContext;
use crate::planner::PhysicalPlan;
use alloc::boxed::Box;

/// Pass that leverages indexes to implement LIMIT and SKIP.
pub struct LimitSkipByIndexPass<'a> {
    ctx: &'a ExecutionContext,
}

impl<'a> LimitSkipByIndexPass<'a> {
    /// Creates a new LimitSkipByIndexPass with the given execution context.
    pub fn new(ctx: &'a ExecutionContext) -> Self {
        Self { ctx }
    }

    /// Optimizes the physical plan by pushing LIMIT/SKIP to indexes.
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

                // Try to push limit/offset directly into IndexScan
                if let Some(optimized) =
                    self.try_push_to_index_scan(optimized_input.clone(), limit, offset)
                {
                    return optimized;
                }

                // No optimization possible, keep the Limit
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

    /// Tries to push LIMIT/SKIP down to an IndexScan.
    /// Only pushes when there are no intervening operations that could change row count.
    fn try_push_to_index_scan(
        &self,
        plan: PhysicalPlan,
        limit: usize,
        offset: usize,
    ) -> Option<PhysicalPlan> {
        match plan {
            // Direct IndexScan - can push limit/offset
            PhysicalPlan::IndexScan {
                table,
                index,
                range_start,
                range_end,
                include_start,
                include_end,
                limit: existing_limit,
                offset: existing_offset,
                reverse,
            } => {
                // If there's already a limit, combine them conservatively
                let (new_limit, new_offset) = if existing_limit.is_some() {
                    // Already has limit, don't override - keep outer Limit
                    return None;
                } else {
                    (Some(limit), Some(offset))
                };

                // Also check existing offset
                if existing_offset.is_some() && existing_offset.unwrap() > 0 {
                    return None;
                }

                Some(PhysicalPlan::IndexScan {
                    table,
                    index,
                    range_start,
                    range_end,
                    include_start,
                    include_end,
                    limit: new_limit,
                    offset: new_offset,
                    reverse,
                })
            }

            // Can push through Project (doesn't change row count)
            PhysicalPlan::Project { input, columns } => {
                let optimized = self.try_push_to_index_scan(*input, limit, offset)?;
                Some(PhysicalPlan::Project {
                    input: Box::new(optimized),
                    columns,
                })
            }

            // TableScan + Limit can be converted to IndexScan if primary key exists
            PhysicalPlan::TableScan { table } => {
                // Find primary key index for this table
                let pk_index = self.ctx.find_primary_index(&table)?;

                Some(PhysicalPlan::IndexScan {
                    table,
                    index: pk_index.name.clone(),
                    range_start: None,
                    range_end: None,
                    include_start: true,
                    include_end: true,
                    limit: Some(limit),
                    offset: Some(offset),
                    reverse: false,
                })
            }

            // Cannot push through Filter - it changes row count unpredictably
            // Cannot push through Sort - needs all rows first
            // Cannot push through joins, aggregates, etc.
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::{Expr, SortOrder};
    use crate::context::{IndexInfo, TableStats};

    fn create_test_context() -> ExecutionContext {
        let mut ctx = ExecutionContext::new();

        ctx.register_table(
            "users",
            TableStats {
                row_count: 1000,
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
    fn test_limit_pushed_to_index_scan() {
        let ctx = create_test_context();
        let pass = LimitSkipByIndexPass::new(&ctx);

        // Create: Limit(10, 5) -> IndexScan
        let plan = PhysicalPlan::Limit {
            input: Box::new(PhysicalPlan::IndexScan {
                table: "users".into(),
                index: "idx_id".into(),
                range_start: None,
                range_end: None,
                include_start: true,
                include_end: true,
                limit: None,
                offset: None,
                reverse: false,
            }),
            limit: 10,
            offset: 5,
        };

        let result = pass.optimize(plan);

        // Should be IndexScan with limit/offset embedded
        if let PhysicalPlan::IndexScan { limit, offset, .. } = result {
            assert_eq!(limit, Some(10));
            assert_eq!(offset, Some(5));
        } else {
            panic!("Expected IndexScan, got {:?}", result);
        }
    }

    #[test]
    fn test_limit_pushed_through_project() {
        let ctx = create_test_context();
        let pass = LimitSkipByIndexPass::new(&ctx);

        // Create: Limit(10, 0) -> Project -> IndexScan
        let plan = PhysicalPlan::Limit {
            input: Box::new(PhysicalPlan::Project {
                input: Box::new(PhysicalPlan::IndexScan {
                    table: "users".into(),
                    index: "idx_id".into(),
                    range_start: None,
                    range_end: None,
                    include_start: true,
                    include_end: true,
                    limit: None,
                    offset: None,
                    reverse: false,
                }),
                columns: alloc::vec![Expr::column("users", "id", 0)],
            }),
            limit: 10,
            offset: 0,
        };

        let result = pass.optimize(plan);

        // Should be Project -> IndexScan(limit=10)
        if let PhysicalPlan::Project { input, .. } = result {
            if let PhysicalPlan::IndexScan { limit, offset, .. } = *input {
                assert_eq!(limit, Some(10));
                assert_eq!(offset, Some(0));
            } else {
                panic!("Expected IndexScan inside Project");
            }
        } else {
            panic!("Expected Project, got {:?}", result);
        }
    }

    #[test]
    fn test_limit_not_pushed_through_filter() {
        let ctx = create_test_context();
        let pass = LimitSkipByIndexPass::new(&ctx);

        // Create: Limit(10, 0) -> Filter -> IndexScan
        let plan = PhysicalPlan::Limit {
            input: Box::new(PhysicalPlan::Filter {
                input: Box::new(PhysicalPlan::IndexScan {
                    table: "users".into(),
                    index: "idx_id".into(),
                    range_start: None,
                    range_end: None,
                    include_start: true,
                    include_end: true,
                    limit: None,
                    offset: None,
                    reverse: false,
                }),
                predicate: Expr::eq(Expr::column("users", "active", 1), Expr::literal(true)),
            }),
            limit: 10,
            offset: 0,
        };

        let result = pass.optimize(plan);

        // Should remain as Limit -> Filter -> IndexScan (not pushed)
        assert!(matches!(result, PhysicalPlan::Limit { .. }));
    }

    #[test]
    fn test_limit_on_table_scan_uses_primary_key() {
        let ctx = create_test_context();
        let pass = LimitSkipByIndexPass::new(&ctx);

        // Create: Limit(10, 0) -> TableScan
        let plan = PhysicalPlan::Limit {
            input: Box::new(PhysicalPlan::table_scan("users")),
            limit: 10,
            offset: 0,
        };

        let result = pass.optimize(plan);

        // Should be converted to IndexScan using primary key
        if let PhysicalPlan::IndexScan { table, index, limit, offset, .. } = result {
            assert_eq!(table, "users");
            assert_eq!(index, "idx_id"); // Primary key index
            assert_eq!(limit, Some(10));
            assert_eq!(offset, Some(0));
        } else {
            panic!("Expected IndexScan, got {:?}", result);
        }
    }

    #[test]
    fn test_limit_on_table_scan_no_pk_not_optimized() {
        // Create context without primary key index
        let mut ctx = ExecutionContext::new();
        ctx.register_table(
            "logs",
            TableStats {
                row_count: 1000,
                is_sorted: false,
                indexes: alloc::vec![], // No indexes
            },
        );

        let pass = LimitSkipByIndexPass::new(&ctx);

        // Create: Limit(10, 0) -> TableScan
        let plan = PhysicalPlan::Limit {
            input: Box::new(PhysicalPlan::table_scan("logs")),
            limit: 10,
            offset: 0,
        };

        let result = pass.optimize(plan);

        // Should remain as Limit -> TableScan (no PK to use)
        assert!(matches!(result, PhysicalPlan::Limit { .. }));
        if let PhysicalPlan::Limit { input, .. } = result {
            assert!(matches!(*input, PhysicalPlan::TableScan { .. }));
        }
    }

    #[test]
    fn test_limit_after_sort_not_optimized() {
        let ctx = create_test_context();
        let pass = LimitSkipByIndexPass::new(&ctx);

        // Create: Limit -> Sort -> IndexScan
        // Sort blocks limit pushdown
        let plan = PhysicalPlan::Limit {
            input: Box::new(PhysicalPlan::Sort {
                input: Box::new(PhysicalPlan::IndexScan {
                    table: "users".into(),
                    index: "idx_id".into(),
                    range_start: None,
                    range_end: None,
                    include_start: true,
                    include_end: true,
                    limit: None,
                    offset: None,
                    reverse: false,
                }),
                order_by: alloc::vec![(Expr::column("users", "id", 0), SortOrder::Asc)],
            }),
            limit: 10,
            offset: 0,
        };

        let result = pass.optimize(plan);

        // Should remain as Limit -> Sort -> IndexScan
        assert!(matches!(result, PhysicalPlan::Limit { .. }));
        if let PhysicalPlan::Limit { input, .. } = result {
            assert!(matches!(*input, PhysicalPlan::Sort { .. }));
        }
    }

    #[test]
    fn test_limit_after_aggregate_not_optimized() {
        let ctx = create_test_context();
        let pass = LimitSkipByIndexPass::new(&ctx);

        // Create: Limit -> HashAggregate -> IndexScan
        // Aggregation blocks limit pushdown
        let plan = PhysicalPlan::Limit {
            input: Box::new(PhysicalPlan::HashAggregate {
                input: Box::new(PhysicalPlan::IndexScan {
                    table: "users".into(),
                    index: "idx_id".into(),
                    range_start: None,
                    range_end: None,
                    include_start: true,
                    include_end: true,
                    limit: None,
                    offset: None,
                    reverse: false,
                }),
                group_by: alloc::vec![],
                aggregates: alloc::vec![],
            }),
            limit: 10,
            offset: 0,
        };

        let result = pass.optimize(plan);

        // Should remain as Limit -> HashAggregate -> IndexScan
        assert!(matches!(result, PhysicalPlan::Limit { .. }));
        if let PhysicalPlan::Limit { input, .. } = result {
            assert!(matches!(*input, PhysicalPlan::HashAggregate { .. }));
        }
    }

    #[test]
    fn test_existing_limit_not_overridden() {
        let ctx = create_test_context();
        let pass = LimitSkipByIndexPass::new(&ctx);

        // Create: Limit(5, 0) -> IndexScan(limit=10)
        let plan = PhysicalPlan::Limit {
            input: Box::new(PhysicalPlan::IndexScan {
                table: "users".into(),
                index: "idx_id".into(),
                range_start: None,
                range_end: None,
                include_start: true,
                include_end: true,
                limit: Some(10),
                offset: None,
                reverse: false,
            }),
            limit: 5,
            offset: 0,
        };

        let result = pass.optimize(plan);

        // Should keep outer Limit since IndexScan already has limit
        assert!(matches!(result, PhysicalPlan::Limit { .. }));
    }
}
