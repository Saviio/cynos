//! Order by index pass - leverages indexes to avoid explicit sorting.
//!
//! This pass identifies Sort nodes where the sort columns match an available
//! index, and replaces the TableScan + Sort with an IndexScan that produces
//! results in the desired order.
//!
//! Example:
//! ```text
//! Sort(id ASC)                =>    IndexScan(idx_id, ASC)
//!      |                                  |
//! TableScan(users)                  (no Sort needed)
//! ```
//!
//! This optimization is beneficial when:
//! 1. The sort columns match an index's columns in order
//! 2. The sort direction matches (or is exactly reversed from) the index order
//! 3. There are no intervening operations that would disrupt ordering

use crate::ast::{Expr, SortOrder};
use crate::context::ExecutionContext;
use crate::planner::PhysicalPlan;
use alloc::boxed::Box;
use alloc::string::String;
use alloc::vec::Vec;

/// Pass that leverages indexes to avoid explicit sorting.
pub struct OrderByIndexPass<'a> {
    ctx: &'a ExecutionContext,
}

impl<'a> OrderByIndexPass<'a> {
    /// Creates a new OrderByIndexPass with the given execution context.
    pub fn new(ctx: &'a ExecutionContext) -> Self {
        Self { ctx }
    }

    /// Optimizes the physical plan by leveraging indexes for sorting.
    pub fn optimize(&self, plan: PhysicalPlan) -> PhysicalPlan {
        self.traverse(plan)
    }

    fn traverse(&self, plan: PhysicalPlan) -> PhysicalPlan {
        match plan {
            PhysicalPlan::Sort { input, order_by } => {
                let optimized_input = self.traverse(*input);

                // Try to optimize using TableScan
                if let Some(optimized) =
                    self.try_optimize_table_scan(&optimized_input, &order_by)
                {
                    return optimized;
                }

                // Try to optimize using existing IndexScan
                if let Some(optimized) =
                    self.try_optimize_index_scan(optimized_input.clone(), &order_by)
                {
                    return optimized;
                }

                // No optimization possible, keep the Sort
                PhysicalPlan::Sort {
                    input: Box::new(optimized_input),
                    order_by,
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
            } => {
                let optimized_input = self.traverse(*input);

                // Try to optimize TopN(TableScan) -> IndexScan with limit
                if let Some(optimized) =
                    self.try_optimize_topn_table_scan(&optimized_input, &order_by, limit, offset)
                {
                    return optimized;
                }

                // Try to optimize TopN over existing IndexScan by setting reverse flag
                if let Some(optimized) =
                    self.try_optimize_topn_index_scan(optimized_input.clone(), &order_by, limit, offset)
                {
                    return optimized;
                }

                // No optimization possible, keep the TopN
                PhysicalPlan::TopN {
                    input: Box::new(optimized_input),
                    order_by,
                    limit,
                    offset,
                }
            }

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

    /// Tries to replace TableScan + Sort with IndexScan.
    fn try_optimize_table_scan(
        &self,
        plan: &PhysicalPlan,
        order_by: &[(Expr, SortOrder)],
    ) -> Option<PhysicalPlan> {
        // Find a TableScan that can be optimized
        let table_scan = self.find_table_scan(plan)?;

        // Extract column names from order_by
        let order_columns: Vec<String> = order_by
            .iter()
            .filter_map(|(expr, _)| self.extract_column_name(expr))
            .collect();

        if order_columns.len() != order_by.len() {
            return None; // Not all order_by expressions are simple columns
        }

        // Find an index that matches the order_by columns
        let column_refs: Vec<&str> = order_columns.iter().map(|s| s.as_str()).collect();
        let index = self.ctx.find_index(&table_scan, &column_refs)?;

        // Check if the sort order matches (natural or reversed)
        let is_reverse = self.check_order_match(order_by, &index.columns)?;

        // Create IndexScan with appropriate range (full scan)
        Some(PhysicalPlan::IndexScan {
            table: table_scan,
            index: index.name.clone(),
            range_start: None,
            range_end: None,
            include_start: true,
            include_end: true,
            limit: None,
            offset: None,
            reverse: is_reverse,
        })
    }

    /// Tries to replace TopN(TableScan) with IndexScan(limit, offset).
    /// This enables true LIMIT pushdown to the storage layer.
    fn try_optimize_topn_table_scan(
        &self,
        plan: &PhysicalPlan,
        order_by: &[(Expr, SortOrder)],
        limit: usize,
        offset: usize,
    ) -> Option<PhysicalPlan> {
        // Find a TableScan that can be optimized
        let table_scan = self.find_table_scan(plan)?;

        // Extract column names from order_by
        let order_columns: Vec<String> = order_by
            .iter()
            .filter_map(|(expr, _)| self.extract_column_name(expr))
            .collect();

        if order_columns.len() != order_by.len() {
            return None; // Not all order_by expressions are simple columns
        }

        // Find an index that matches the order_by columns
        let column_refs: Vec<&str> = order_columns.iter().map(|s| s.as_str()).collect();
        let index = self.ctx.find_index(&table_scan, &column_refs)?;

        // Check if the sort order matches (natural or reversed)
        let is_reverse = self.check_order_match(order_by, &index.columns)?;

        // Create IndexScan with limit and offset pushed down
        Some(PhysicalPlan::IndexScan {
            table: table_scan,
            index: index.name.clone(),
            range_start: None,
            range_end: None,
            include_start: true,
            include_end: true,
            limit: Some(limit),
            offset: Some(offset),
            reverse: is_reverse,
        })
    }

    /// Tries to optimize TopN over existing IndexScan by setting reverse flag and pushing limit.
    fn try_optimize_topn_index_scan(
        &self,
        plan: PhysicalPlan,
        order_by: &[(Expr, SortOrder)],
        limit: usize,
        offset: usize,
    ) -> Option<PhysicalPlan> {
        match plan {
            // Direct IndexScan
            PhysicalPlan::IndexScan {
                table,
                index,
                range_start,
                range_end,
                include_start,
                include_end,
                ..
            } => {
                // Get index info by name
                let index_info = self.ctx.find_index_by_name(&table, &index)?;

                // Check if this index matches the order_by
                let order_columns: Vec<String> = order_by
                    .iter()
                    .filter_map(|(expr, _)| self.extract_column_name(expr))
                    .collect();

                if order_columns.len() != order_by.len() {
                    return None;
                }

                // Check if columns match
                if !index_info
                    .columns
                    .iter()
                    .zip(order_columns.iter())
                    .all(|(a, b)| a == b)
                {
                    return None;
                }

                // Check if the sort order matches (natural or reversed)
                let is_reverse = self.check_order_match(order_by, &index_info.columns)?;

                // Return IndexScan with reverse flag and limit pushed down
                Some(PhysicalPlan::IndexScan {
                    table,
                    index,
                    range_start,
                    range_end,
                    include_start,
                    include_end,
                    limit: Some(limit),
                    offset: Some(offset),
                    reverse: is_reverse,
                })
            }

            // Look through Project nodes
            PhysicalPlan::Project { input, columns } => {
                let optimized_input = self.try_optimize_topn_index_scan(*input, order_by, limit, offset)?;
                Some(PhysicalPlan::Project {
                    input: Box::new(optimized_input),
                    columns,
                })
            }

            // Can't optimize through other nodes
            _ => None,
        }
    }

    /// Tries to optimize an existing IndexScan by setting its reverse flag.
    /// Can look through Project and Limit nodes to find the IndexScan.
    fn try_optimize_index_scan(
        &self,
        plan: PhysicalPlan,
        order_by: &[(Expr, SortOrder)],
    ) -> Option<PhysicalPlan> {
        match plan {
            // Direct IndexScan
            PhysicalPlan::IndexScan {
                table,
                index,
                range_start,
                range_end,
                include_start,
                include_end,
                limit,
                offset,
                ..
            } => {
                // Get index info by name (not by columns)
                let index_info = self.ctx.find_index_by_name(&table, &index)?;

                // Check if this index matches the order_by
                let order_columns: Vec<String> = order_by
                    .iter()
                    .filter_map(|(expr, _)| self.extract_column_name(expr))
                    .collect();

                if order_columns.len() != order_by.len() {
                    return None;
                }

                // Check if columns match
                if !index_info
                    .columns
                    .iter()
                    .zip(order_columns.iter())
                    .all(|(a, b)| a == b)
                {
                    return None;
                }

                // Check if the sort order matches (natural or reversed)
                let is_reverse = self.check_order_match(order_by, &index_info.columns)?;

                // Return IndexScan with correct reverse flag
                Some(PhysicalPlan::IndexScan {
                    table,
                    index,
                    range_start,
                    range_end,
                    include_start,
                    include_end,
                    limit,
                    offset,
                    reverse: is_reverse,
                })
            }

            // Look through Project nodes
            PhysicalPlan::Project { input, columns } => {
                let optimized_input = self.try_optimize_index_scan(*input, order_by)?;
                Some(PhysicalPlan::Project {
                    input: Box::new(optimized_input),
                    columns,
                })
            }

            // Look through Limit nodes
            PhysicalPlan::Limit {
                input,
                limit,
                offset,
            } => {
                let optimized_input = self.try_optimize_index_scan(*input, order_by)?;
                Some(PhysicalPlan::Limit {
                    input: Box::new(optimized_input),
                    limit,
                    offset,
                })
            }

            // Can't optimize through other nodes
            _ => None,
        }
    }

    /// Finds a TableScan in the plan (only if it's directly accessible).
    fn find_table_scan(&self, plan: &PhysicalPlan) -> Option<String> {
        match plan {
            PhysicalPlan::TableScan { table } => Some(table.clone()),
            // Can look through Filter and Project nodes
            PhysicalPlan::Filter { input, .. } | PhysicalPlan::Project { input, .. } => {
                self.find_table_scan(input)
            }
            // Stop at nodes that would disrupt ordering
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

    /// Checks if the order_by matches the index columns (natural or reversed).
    /// Returns Some(true) for reversed, Some(false) for natural, None for no match.
    fn check_order_match(
        &self,
        order_by: &[(Expr, SortOrder)],
        index_columns: &[String],
    ) -> Option<bool> {
        if order_by.len() > index_columns.len() {
            return None;
        }

        // Check if all columns match
        for (i, (expr, _)) in order_by.iter().enumerate() {
            let col_name = self.extract_column_name(expr)?;
            if col_name != index_columns[i] {
                return None;
            }
        }

        // Check if all orders are the same (all ASC or all DESC)
        let first_order = order_by.first().map(|(_, o)| o)?;
        let all_same = order_by.iter().all(|(_, o)| o == first_order);

        if !all_same {
            return None; // Mixed orders not supported
        }

        // ASC = natural order, DESC = reversed
        Some(*first_order == SortOrder::Desc)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::Expr;
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
    fn test_sort_to_index_scan() {
        let ctx = create_test_context();
        let pass = OrderByIndexPass::new(&ctx);

        // Create: Sort(id ASC) -> TableScan(users)
        let plan = PhysicalPlan::Sort {
            input: Box::new(PhysicalPlan::table_scan("users")),
            order_by: alloc::vec![(Expr::column("users", "id", 0), SortOrder::Asc)],
        };

        let result = pass.optimize(plan);

        // Should become IndexScan
        assert!(matches!(result, PhysicalPlan::IndexScan { .. }));
        if let PhysicalPlan::IndexScan { table, index, .. } = result {
            assert_eq!(table, "users");
            assert_eq!(index, "idx_id");
        }
    }

    #[test]
    fn test_sort_no_matching_index() {
        let ctx = create_test_context();
        let pass = OrderByIndexPass::new(&ctx);

        // Create: Sort(email ASC) -> TableScan(users)
        // No index on 'email'
        let plan = PhysicalPlan::Sort {
            input: Box::new(PhysicalPlan::table_scan("users")),
            order_by: alloc::vec![(Expr::column("users", "email", 2), SortOrder::Asc)],
        };

        let result = pass.optimize(plan);

        // Should remain as Sort
        assert!(matches!(result, PhysicalPlan::Sort { .. }));
    }

    #[test]
    fn test_sort_with_filter() {
        let ctx = create_test_context();
        let pass = OrderByIndexPass::new(&ctx);

        // Create: Sort(id ASC) -> Filter -> TableScan(users)
        let plan = PhysicalPlan::Sort {
            input: Box::new(PhysicalPlan::Filter {
                input: Box::new(PhysicalPlan::table_scan("users")),
                predicate: Expr::gt(Expr::column("users", "age", 1), Expr::literal(18i64)),
            }),
            order_by: alloc::vec![(Expr::column("users", "id", 0), SortOrder::Asc)],
        };

        let result = pass.optimize(plan);

        // Should become IndexScan (Filter is transparent for this optimization)
        assert!(matches!(result, PhysicalPlan::IndexScan { .. }));
    }

    #[test]
    fn test_sort_after_join_not_optimized() {
        let ctx = create_test_context();
        let pass = OrderByIndexPass::new(&ctx);

        // Create: Sort -> HashJoin -> (TableScan, TableScan)
        // Joins disrupt ordering, so this shouldn't be optimized
        let plan = PhysicalPlan::Sort {
            input: Box::new(PhysicalPlan::HashJoin {
                left: Box::new(PhysicalPlan::table_scan("users")),
                right: Box::new(PhysicalPlan::table_scan("orders")),
                condition: Expr::eq(
                    Expr::column("users", "id", 0),
                    Expr::column("orders", "user_id", 0),
                ),
                join_type: crate::ast::JoinType::Inner,
            }),
            order_by: alloc::vec![(Expr::column("users", "id", 0), SortOrder::Asc)],
        };

        let result = pass.optimize(plan);

        // Should remain as Sort (join disrupts ordering)
        assert!(matches!(result, PhysicalPlan::Sort { .. }));
    }

    #[test]
    fn test_check_order_match() {
        let ctx = create_test_context();
        let pass = OrderByIndexPass::new(&ctx);

        let index_columns = alloc::vec!["id".into(), "name".into()];

        // Natural order (ASC)
        let order_by = alloc::vec![(Expr::column("t", "id", 0), SortOrder::Asc)];
        assert_eq!(pass.check_order_match(&order_by, &index_columns), Some(false));

        // Reversed order (DESC)
        let order_by = alloc::vec![(Expr::column("t", "id", 0), SortOrder::Desc)];
        assert_eq!(pass.check_order_match(&order_by, &index_columns), Some(true));

        // Column mismatch
        let order_by = alloc::vec![(Expr::column("t", "email", 0), SortOrder::Asc)];
        assert_eq!(pass.check_order_match(&order_by, &index_columns), None);
    }

    #[test]
    fn test_sort_desc_to_index_scan_reverse() {
        let ctx = create_test_context();
        let pass = OrderByIndexPass::new(&ctx);

        // Create: Sort(id DESC) -> TableScan(users)
        let plan = PhysicalPlan::Sort {
            input: Box::new(PhysicalPlan::table_scan("users")),
            order_by: alloc::vec![(Expr::column("users", "id", 0), SortOrder::Desc)],
        };

        let result = pass.optimize(plan);

        // Should become IndexScan with reverse=true
        assert!(matches!(result, PhysicalPlan::IndexScan { .. }));
        if let PhysicalPlan::IndexScan { table, index, reverse, .. } = result {
            assert_eq!(table, "users");
            assert_eq!(index, "idx_id");
            assert!(reverse, "IndexScan should have reverse=true for DESC ordering");
        }
    }

    #[test]
    fn test_topn_desc_to_index_scan_reverse() {
        let ctx = create_test_context();
        let pass = OrderByIndexPass::new(&ctx);

        // Create: TopN(id DESC, limit=10, offset=5) -> TableScan(users)
        let plan = PhysicalPlan::TopN {
            input: Box::new(PhysicalPlan::table_scan("users")),
            order_by: alloc::vec![(Expr::column("users", "id", 0), SortOrder::Desc)],
            limit: 10,
            offset: 5,
        };

        let result = pass.optimize(plan);

        // Should become IndexScan with reverse=true, limit=10, offset=5
        assert!(matches!(result, PhysicalPlan::IndexScan { .. }));
        if let PhysicalPlan::IndexScan { table, index, reverse, limit, offset, .. } = result {
            assert_eq!(table, "users");
            assert_eq!(index, "idx_id");
            assert!(reverse, "IndexScan should have reverse=true for DESC ordering");
            assert_eq!(limit, Some(10));
            assert_eq!(offset, Some(5));
        }
    }

    #[test]
    fn test_sort_asc_to_index_scan_forward() {
        let ctx = create_test_context();
        let pass = OrderByIndexPass::new(&ctx);

        // Create: Sort(id ASC) -> TableScan(users)
        let plan = PhysicalPlan::Sort {
            input: Box::new(PhysicalPlan::table_scan("users")),
            order_by: alloc::vec![(Expr::column("users", "id", 0), SortOrder::Asc)],
        };

        let result = pass.optimize(plan);

        // Should become IndexScan with reverse=false
        assert!(matches!(result, PhysicalPlan::IndexScan { .. }));
        if let PhysicalPlan::IndexScan { reverse, .. } = result {
            assert!(!reverse, "IndexScan should have reverse=false for ASC ordering");
        }
    }

    /// Test: Sort DESC over existing IndexScan should set reverse=true
    ///
    /// When we have Sort(price DESC) -> IndexScan(idx_price, range),
    /// the optimizer should set reverse=true on the IndexScan.
    #[test]
    fn test_sort_desc_over_existing_index_scan_should_set_reverse() {
        let mut ctx = ExecutionContext::new();
        ctx.register_table(
            "stocks",
            TableStats {
                row_count: 10000,
                is_sorted: false,
                indexes: alloc::vec![
                    IndexInfo::new("idx_id", alloc::vec!["id".into()], true),
                    IndexInfo::new("idx_price", alloc::vec!["price".into()], false),
                ],
            },
        );
        let pass = OrderByIndexPass::new(&ctx);

        // Simulate: Sort(price DESC) -> IndexScan(idx_price, range > 980)
        // This is what happens after IndexSelection converts Filter+Scan to IndexScan
        let plan = PhysicalPlan::Sort {
            input: Box::new(PhysicalPlan::IndexScan {
                table: "stocks".into(),
                index: "idx_price".into(),
                range_start: Some(cynos_core::Value::Float64(980.0)),
                range_end: None,
                include_start: false,
                include_end: true,
                limit: None,
                offset: None,
                reverse: false,
            }),
            order_by: alloc::vec![(Expr::column("stocks", "price", 0), SortOrder::Desc)],
        };

        let result = pass.optimize(plan);

        // Should become IndexScan with reverse=true (no Sort node)
        match result {
            PhysicalPlan::IndexScan { reverse, index, .. } => {
                assert_eq!(index, "idx_price");
                assert!(reverse, "IndexScan should have reverse=true for DESC ordering over existing IndexScan");
            }
            PhysicalPlan::Sort { .. } => {
                panic!("BUG: Sort node was not eliminated - IndexScan should have reverse=true");
            }
            _ => panic!("Unexpected plan: {:?}", result),
        }
    }

    /// Test: Sort DESC over Limit -> Project -> IndexScan should set reverse=true
    #[test]
    fn test_sort_desc_over_limit_project_index_scan_should_set_reverse() {
        let mut ctx = ExecutionContext::new();
        ctx.register_table(
            "stocks",
            TableStats {
                row_count: 10000,
                is_sorted: false,
                indexes: alloc::vec![
                    IndexInfo::new("idx_id", alloc::vec!["id".into()], true),
                    IndexInfo::new("idx_price", alloc::vec!["price".into()], false),
                ],
            },
        );
        let pass = OrderByIndexPass::new(&ctx);

        // Simulate: Sort(price DESC) -> Limit -> Project -> IndexScan(idx_price, range > 980)
        // This is the structure from: .where(price > 980).orderBy('price', 'Desc').limit(100)
        let plan = PhysicalPlan::Sort {
            input: Box::new(PhysicalPlan::Limit {
                input: Box::new(PhysicalPlan::Project {
                    input: Box::new(PhysicalPlan::IndexScan {
                        table: "stocks".into(),
                        index: "idx_price".into(),
                        range_start: Some(cynos_core::Value::Float64(980.0)),
                        range_end: None,
                        include_start: false,
                        include_end: true,
                        limit: None,
                        offset: None,
                        reverse: false,
                    }),
                    columns: alloc::vec![
                        Expr::column("stocks", "id", 0),
                        Expr::column("stocks", "price", 1),
                    ],
                }),
                limit: 100,
                offset: 0,
            }),
            order_by: alloc::vec![(Expr::column("stocks", "price", 0), SortOrder::Desc)],
        };

        let result = pass.optimize(plan);

        // Should become Limit -> Project -> IndexScan with reverse=true (no Sort node)
        match result {
            PhysicalPlan::Limit { input, .. } => {
                if let PhysicalPlan::Project { input, .. } = *input {
                    if let PhysicalPlan::IndexScan { reverse, index, .. } = *input {
                        assert_eq!(index, "idx_price");
                        assert!(reverse, "IndexScan should have reverse=true");
                    } else {
                        panic!("Expected IndexScan inside Project, got {:?}", input);
                    }
                } else {
                    panic!("Expected Project inside Limit, got {:?}", input);
                }
            }
            PhysicalPlan::Sort { .. } => {
                panic!("BUG: Sort node was not eliminated");
            }
            _ => panic!("Unexpected plan: {:?}", result),
        }
    }

    /// Test: Sort DESC over Project -> IndexScan should set reverse=true
    #[test]
    fn test_sort_desc_over_project_index_scan_should_set_reverse() {
        let mut ctx = ExecutionContext::new();
        ctx.register_table(
            "stocks",
            TableStats {
                row_count: 10000,
                is_sorted: false,
                indexes: alloc::vec![
                    IndexInfo::new("idx_id", alloc::vec!["id".into()], true),
                    IndexInfo::new("idx_price", alloc::vec!["price".into()], false),
                ],
            },
        );
        let pass = OrderByIndexPass::new(&ctx);

        // Simulate: Sort(price DESC) -> Project -> IndexScan(idx_price, range > 100)
        let plan = PhysicalPlan::Sort {
            input: Box::new(PhysicalPlan::Project {
                input: Box::new(PhysicalPlan::IndexScan {
                    table: "stocks".into(),
                    index: "idx_price".into(),
                    range_start: Some(cynos_core::Value::Float64(100.0)),
                    range_end: None,
                    include_start: false,
                    include_end: true,
                    limit: None,
                    offset: None,
                    reverse: false,
                }),
                columns: alloc::vec![
                    Expr::column("stocks", "id", 0),
                    Expr::column("stocks", "price", 1),
                ],
            }),
            order_by: alloc::vec![(Expr::column("stocks", "price", 0), SortOrder::Desc)],
        };

        let result = pass.optimize(plan);

        // Should become Project -> IndexScan with reverse=true (no Sort node)
        match result {
            PhysicalPlan::Project { input, .. } => {
                if let PhysicalPlan::IndexScan { reverse, index, .. } = *input {
                    assert_eq!(index, "idx_price");
                    assert!(reverse, "IndexScan should have reverse=true");
                } else {
                    panic!("Expected IndexScan inside Project, got {:?}", input);
                }
            }
            PhysicalPlan::Sort { .. } => {
                panic!("BUG: Sort node was not eliminated");
            }
            _ => panic!("Unexpected plan: {:?}", result),
        }
    }

    /// Test: TopN DESC over IndexScan should set reverse=true and push limit
    /// This is the actual structure when WHERE + ORDER BY + LIMIT are combined:
    /// Project -> TopN -> IndexScan
    #[test]
    fn test_topn_desc_over_index_scan_should_set_reverse() {
        let mut ctx = ExecutionContext::new();
        ctx.register_table(
            "stocks",
            TableStats {
                row_count: 10000,
                is_sorted: false,
                indexes: alloc::vec![
                    IndexInfo::new("idx_id", alloc::vec!["id".into()], true),
                    IndexInfo::new("idx_price", alloc::vec!["price".into()], false),
                ],
            },
        );
        let pass = OrderByIndexPass::new(&ctx);

        // Simulate: Project -> TopN(price DESC, limit=100) -> IndexScan(idx_price, range > 100)
        // This is the actual structure from: .where(price > 100).orderBy('price', 'Desc').limit(100)
        let plan = PhysicalPlan::Project {
            input: Box::new(PhysicalPlan::TopN {
                input: Box::new(PhysicalPlan::IndexScan {
                    table: "stocks".into(),
                    index: "idx_price".into(),
                    range_start: Some(cynos_core::Value::Float64(100.0)),
                    range_end: None,
                    include_start: true,
                    include_end: true,
                    limit: None,
                    offset: None,
                    reverse: false,
                }),
                order_by: alloc::vec![(Expr::column("stocks", "price", 3), SortOrder::Desc)],
                limit: 100,
                offset: 0,
            }),
            columns: alloc::vec![
                Expr::column("stocks", "id", 0),
                Expr::column("stocks", "price", 3),
            ],
        };

        let result = pass.optimize(plan);

        // Should become Project -> IndexScan with reverse=true and limit=100 (no TopN node)
        match result {
            PhysicalPlan::Project { input, .. } => {
                if let PhysicalPlan::IndexScan { reverse, index, limit, offset, .. } = *input {
                    assert_eq!(index, "idx_price");
                    assert!(reverse, "IndexScan should have reverse=true for DESC ordering");
                    assert_eq!(limit, Some(100), "Limit should be pushed to IndexScan");
                    assert_eq!(offset, Some(0), "Offset should be pushed to IndexScan");
                } else {
                    panic!("Expected IndexScan inside Project, got {:?}", input);
                }
            }
            _ => panic!("Unexpected plan: {:?}", result),
        }
    }
}
