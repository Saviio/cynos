//! Unified query planner with ExecutionContext support.
//!
//! This module provides a unified entry point for query planning that handles
//! both logical and physical plan optimizations with proper ExecutionContext support.
//!
//! ## Architecture
//!
//! The query planning pipeline consists of:
//!
//! 1. **Logical Optimization** - Context-free transformations:
//!    - NotSimplification
//!    - AndPredicatePass
//!    - CrossProductPass
//!    - ImplicitJoinsPass
//!    - OuterJoinSimplification
//!    - PredicatePushdown
//!    - JoinReorder
//!
//! 2. **Context-Aware Logical Optimization** - Requires ExecutionContext:
//!    - IndexSelection (converts Filter+Scan to IndexScan/IndexGet)
//!
//! 3. **Physical Plan Conversion** - Converts logical to physical plan
//!
//! 4. **Physical Optimization** - Context-aware physical transformations:
//!    - TopNPushdown (converts Sort+Limit to TopN)
//!    - OrderByIndexPass (leverages indexes for sorting)
//!    - LimitSkipByIndexPass (pushes limit/offset to IndexScan)
//!
//! ## Usage
//!
//! ```ignore
//! let ctx = build_execution_context(&cache, "users");
//! let planner = QueryPlanner::new(ctx);
//! let physical_plan = planner.plan(logical_plan);
//! ```

use crate::context::ExecutionContext;
use crate::optimizer::{
    AndPredicatePass, CrossProductPass, ImplicitJoinsPass, IndexSelection, JoinReorder,
    LimitSkipByIndexPass, NotSimplification, OptimizerPass, OrderByIndexPass,
    OuterJoinSimplification, PredicatePushdown, TopNPushdown,
};
use crate::planner::{LogicalPlan, PhysicalPlan};
use alloc::boxed::Box;
use alloc::vec::Vec;

/// Unified query planner that handles the complete optimization pipeline.
///
/// Unlike the basic `Optimizer`, `QueryPlanner` supports `ExecutionContext`
/// throughout the entire pipeline, enabling context-aware optimizations
/// for both logical and physical plans.
pub struct QueryPlanner {
    ctx: ExecutionContext,
    /// Logical optimization passes (context-free)
    logical_passes: Vec<Box<dyn OptimizerPass>>,
}

impl QueryPlanner {
    /// Creates a new QueryPlanner with the given execution context.
    ///
    /// The planner is initialized with default optimization passes:
    /// - Logical: NotSimplification, AndPredicatePass, CrossProductPass,
    ///   ImplicitJoinsPass, OuterJoinSimplification, PredicatePushdown, JoinReorder
    /// - Context-aware logical: IndexSelection
    /// - Physical: TopNPushdown, OrderByIndexPass, LimitSkipByIndexPass
    pub fn new(ctx: ExecutionContext) -> Self {
        Self {
            ctx,
            logical_passes: alloc::vec![
                Box::new(NotSimplification),
                Box::new(AndPredicatePass),
                Box::new(CrossProductPass),
                Box::new(ImplicitJoinsPass),
                Box::new(OuterJoinSimplification),
                Box::new(PredicatePushdown),
                Box::new(JoinReorder::new()),
            ],
        }
    }

    /// Creates a QueryPlanner with custom logical passes.
    ///
    /// Context-aware passes (IndexSelection, OrderByIndexPass, etc.) are
    /// still applied automatically using the provided context.
    pub fn with_logical_passes(ctx: ExecutionContext, passes: Vec<Box<dyn OptimizerPass>>) -> Self {
        Self {
            ctx,
            logical_passes: passes,
        }
    }

    /// Returns a reference to the execution context.
    pub fn context(&self) -> &ExecutionContext {
        &self.ctx
    }

    /// Plans a logical query into an optimized physical plan.
    ///
    /// This is the main entry point that runs the complete optimization pipeline:
    /// 1. Apply context-free logical optimizations
    /// 2. Apply context-aware logical optimizations (IndexSelection)
    /// 3. Convert to physical plan
    /// 4. Apply physical optimizations (TopNPushdown, OrderByIndexPass, LimitSkipByIndexPass)
    pub fn plan(&self, plan: LogicalPlan) -> PhysicalPlan {
        // Phase 1: Context-free logical optimizations
        let mut logical = plan;
        for pass in &self.logical_passes {
            logical = pass.optimize(logical);
        }

        // Phase 2: Context-aware logical optimizations
        let index_selection = IndexSelection::with_context(self.ctx.clone());
        logical = index_selection.optimize(logical);

        // Phase 3: Convert to physical plan
        let mut physical = self.logical_to_physical(logical);

        // Phase 4: Physical optimizations
        // TopNPushdown: Sort + Limit -> TopN
        physical = TopNPushdown::new().optimize(physical);

        // OrderByIndexPass: leverage indexes for sorting (needs context)
        physical = OrderByIndexPass::new(&self.ctx).optimize(physical);

        // LimitSkipByIndexPass: push limit/offset to IndexScan (needs context)
        physical = LimitSkipByIndexPass::new(&self.ctx).optimize(physical);

        physical
    }

    /// Optimizes only the logical plan without converting to physical.
    ///
    /// Useful for debugging or when you need to inspect the optimized logical plan.
    pub fn optimize_logical(&self, plan: LogicalPlan) -> LogicalPlan {
        let mut logical = plan;

        // Context-free passes
        for pass in &self.logical_passes {
            logical = pass.optimize(logical);
        }

        // Context-aware passes
        let index_selection = IndexSelection::with_context(self.ctx.clone());
        logical = index_selection.optimize(logical);

        logical
    }

    /// Converts a logical plan to physical and applies physical optimizations.
    ///
    /// Assumes the logical plan has already been optimized.
    pub fn to_physical(&self, plan: LogicalPlan) -> PhysicalPlan {
        let mut physical = self.logical_to_physical(plan);

        // Physical optimizations
        physical = TopNPushdown::new().optimize(physical);
        physical = OrderByIndexPass::new(&self.ctx).optimize(physical);
        physical = LimitSkipByIndexPass::new(&self.ctx).optimize(physical);

        physical
    }

    /// Converts a logical plan to a physical plan without optimizations.
    fn logical_to_physical(&self, plan: LogicalPlan) -> PhysicalPlan {
        use crate::planner::JoinAlgorithm;

        match plan {
            LogicalPlan::Scan { table } => PhysicalPlan::table_scan(table),

            LogicalPlan::IndexScan {
                table,
                index,
                range_start,
                range_end,
                include_start,
                include_end,
            } => PhysicalPlan::IndexScan {
                table,
                index,
                range_start,
                range_end,
                include_start,
                include_end,
                limit: None,
                offset: None,
                reverse: false,
            },

            LogicalPlan::IndexGet { table, index, key } => {
                PhysicalPlan::index_get(table, index, key)
            }

            LogicalPlan::IndexInGet { table, index, keys } => {
                PhysicalPlan::index_in_get(table, index, keys)
            }

            LogicalPlan::GinIndexScan {
                table,
                index,
                column: _,
                column_index: _,
                path,
                value,
                query_type,
            } => {
                let key: alloc::string::String = path.trim_start_matches("$.").into();
                let value_str = value.map(|v| match v {
                    cynos_core::Value::String(s) => s,
                    cynos_core::Value::Int32(i) => alloc::format!("{}", i),
                    cynos_core::Value::Int64(i) => alloc::format!("{}", i),
                    cynos_core::Value::Float64(f) => alloc::format!("{}", f),
                    cynos_core::Value::Boolean(b) => alloc::format!("{}", b),
                    _ => alloc::format!("{:?}", v),
                });
                PhysicalPlan::gin_index_scan(table, index, key, value_str, query_type)
            }

            LogicalPlan::GinIndexScanMulti {
                table,
                index,
                column: _,
                pairs,
            } => {
                let string_pairs: Vec<(alloc::string::String, alloc::string::String)> = pairs
                    .into_iter()
                    .map(|(path, value)| {
                        let key: alloc::string::String = path.trim_start_matches("$.").into();
                        let value_str = match value {
                            cynos_core::Value::String(s) => s,
                            cynos_core::Value::Int32(i) => alloc::format!("{}", i),
                            cynos_core::Value::Int64(i) => alloc::format!("{}", i),
                            cynos_core::Value::Float64(f) => alloc::format!("{}", f),
                            cynos_core::Value::Boolean(b) => alloc::format!("{}", b),
                            _ => alloc::format!("{:?}", value),
                        };
                        (key, value_str)
                    })
                    .collect();
                PhysicalPlan::gin_index_scan_multi(table, index, string_pairs)
            }

            LogicalPlan::Filter { input, predicate } => {
                let input_physical = self.logical_to_physical(*input);
                PhysicalPlan::filter(input_physical, predicate)
            }

            LogicalPlan::Project { input, columns } => {
                let input_physical = self.logical_to_physical(*input);
                PhysicalPlan::project(input_physical, columns)
            }

            LogicalPlan::Join {
                left,
                right,
                condition,
                join_type,
            } => {
                let left_physical = self.logical_to_physical(*left);
                let right_physical = self.logical_to_physical(*right);
                let algorithm = self.choose_join_algorithm(&condition);

                match algorithm {
                    JoinAlgorithm::Hash => {
                        PhysicalPlan::hash_join(left_physical, right_physical, condition, join_type)
                    }
                    JoinAlgorithm::SortMerge => PhysicalPlan::sort_merge_join(
                        left_physical,
                        right_physical,
                        condition,
                        join_type,
                    ),
                    JoinAlgorithm::NestedLoop | JoinAlgorithm::IndexNestedLoop => {
                        PhysicalPlan::nested_loop_join(
                            left_physical,
                            right_physical,
                            condition,
                            join_type,
                        )
                    }
                }
            }

            LogicalPlan::Aggregate {
                input,
                group_by,
                aggregates,
            } => {
                let input_physical = self.logical_to_physical(*input);
                PhysicalPlan::hash_aggregate(input_physical, group_by, aggregates)
            }

            LogicalPlan::Sort { input, order_by } => {
                let input_physical = self.logical_to_physical(*input);
                PhysicalPlan::sort(input_physical, order_by)
            }

            LogicalPlan::Limit {
                input,
                limit,
                offset,
            } => {
                let input_physical = self.logical_to_physical(*input);
                PhysicalPlan::limit(input_physical, limit, offset)
            }

            LogicalPlan::CrossProduct { left, right } => {
                let left_physical = self.logical_to_physical(*left);
                let right_physical = self.logical_to_physical(*right);
                PhysicalPlan::CrossProduct {
                    left: Box::new(left_physical),
                    right: Box::new(right_physical),
                }
            }

            LogicalPlan::Union { .. } => PhysicalPlan::Empty,

            LogicalPlan::Empty => PhysicalPlan::Empty,
        }
    }

    fn choose_join_algorithm(&self, condition: &crate::ast::Expr) -> crate::planner::JoinAlgorithm {
        if condition.is_equi_join() {
            return crate::planner::JoinAlgorithm::Hash;
        }
        if condition.is_range_join() {
            return crate::planner::JoinAlgorithm::NestedLoop;
        }
        crate::planner::JoinAlgorithm::NestedLoop
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
                indexes: alloc::vec![
                    IndexInfo::new("idx_id", alloc::vec!["id".into()], true),
                    IndexInfo::new("idx_name", alloc::vec!["name".into()], false),
                ],
            },
        );
        ctx
    }

    #[test]
    fn test_query_planner_basic() {
        let ctx = create_test_context();
        let planner = QueryPlanner::new(ctx);

        let plan = LogicalPlan::scan("users");
        let physical = planner.plan(plan);

        assert!(matches!(physical, PhysicalPlan::TableScan { .. }));
    }

    #[test]
    fn test_query_planner_index_selection() {
        let ctx = create_test_context();
        let planner = QueryPlanner::new(ctx);

        // Filter: id = 42
        let plan = LogicalPlan::filter(
            LogicalPlan::scan("users"),
            Expr::eq(Expr::column("users", "id", 0), Expr::literal(42i64)),
        );

        let physical = planner.plan(plan);

        // Should use IndexGet
        assert!(matches!(physical, PhysicalPlan::IndexGet { .. }));
    }

    #[test]
    fn test_query_planner_order_by_index() {
        let ctx = create_test_context();
        let planner = QueryPlanner::new(ctx);

        // Sort by id ASC
        let plan = LogicalPlan::Sort {
            input: Box::new(LogicalPlan::scan("users")),
            order_by: alloc::vec![(Expr::column("users", "id", 0), SortOrder::Asc)],
        };

        let physical = planner.plan(plan);

        // Should use IndexScan instead of Sort
        assert!(matches!(physical, PhysicalPlan::IndexScan { .. }));
    }

    #[test]
    fn test_query_planner_topn_pushdown() {
        let ctx = create_test_context();
        let planner = QueryPlanner::new(ctx);

        // Sort by id DESC + Limit 10
        let plan = LogicalPlan::Limit {
            input: Box::new(LogicalPlan::Sort {
                input: Box::new(LogicalPlan::scan("users")),
                order_by: alloc::vec![(Expr::column("users", "id", 0), SortOrder::Desc)],
            }),
            limit: 10,
            offset: 0,
        };

        let physical = planner.plan(plan);

        // Should become IndexScan with limit and reverse
        match physical {
            PhysicalPlan::IndexScan {
                limit,
                reverse,
                ..
            } => {
                assert_eq!(limit, Some(10));
                assert!(reverse);
            }
            _ => panic!("Expected IndexScan, got {:?}", physical),
        }
    }

    #[test]
    fn test_query_planner_optimize_logical() {
        let ctx = create_test_context();
        let planner = QueryPlanner::new(ctx);

        let plan = LogicalPlan::filter(
            LogicalPlan::scan("users"),
            Expr::eq(Expr::column("users", "id", 0), Expr::literal(42i64)),
        );

        let optimized = planner.optimize_logical(plan);

        // Should convert to IndexGet
        assert!(matches!(optimized, LogicalPlan::IndexGet { .. }));
    }
}
