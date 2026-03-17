//! Lightweight physical properties used by the optimizer.

use crate::ast::{Expr, SortOrder};
use crate::context::ExecutionContext;
use crate::planner::PhysicalPlan;
use alloc::string::String;
use alloc::vec::Vec;

/// Ordering for a single output column.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OrderingColumn {
    /// Source table name.
    pub table: String,
    /// Column name.
    pub column: String,
    /// Sort direction.
    pub order: SortOrder,
}

/// Output ordering for a plan.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OrderingProperty {
    columns: Vec<OrderingColumn>,
}

impl OrderingProperty {
    /// Builds an ordering property directly from an ORDER BY clause.
    pub fn from_order_by(order_by: &[(Expr, SortOrder)]) -> Option<Self> {
        let mut columns = Vec::with_capacity(order_by.len());
        for (expr, order) in order_by {
            let Expr::Column(col) = expr else {
                return None;
            };
            columns.push(OrderingColumn {
                table: col.table.clone(),
                column: col.column.clone(),
                order: *order,
            });
        }
        Some(Self { columns })
    }

    /// Returns true when this property satisfies the given ORDER BY clause.
    pub fn satisfies(&self, order_by: &[(Expr, SortOrder)]) -> bool {
        if order_by.len() > self.columns.len() {
            return false;
        }

        self.columns
            .iter()
            .zip(order_by.iter())
            .all(|(actual, (expr, order))| match expr {
                Expr::Column(col) => {
                    actual.table == col.table && actual.column == col.column && actual.order == *order
                }
                _ => false,
            })
    }

    fn from_index_columns(table: &str, columns: &[String], order: SortOrder) -> Self {
        Self {
            columns: columns
                .iter()
                .map(|column| OrderingColumn {
                    table: table.into(),
                    column: column.clone(),
                    order,
                })
                .collect(),
        }
    }

    fn projected(self, projections: &[Expr]) -> Option<Self> {
        let preserved = self.columns.iter().all(|ordered| {
            projections.iter().any(|expr| match expr {
                Expr::Column(col) => col.table == ordered.table && col.column == ordered.column,
                _ => false,
            })
        });
        if preserved {
            Some(self)
        } else {
            None
        }
    }
}

/// Lightweight physical properties.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PhysicalProperties {
    /// Estimated output row count. This is advisory only.
    pub estimated_rows: Option<usize>,
    /// Output ordering when known.
    pub ordering: Option<OrderingProperty>,
}

impl PhysicalProperties {
    /// Derives properties for a physical plan.
    pub fn derive(plan: &PhysicalPlan, ctx: &ExecutionContext) -> Self {
        match plan {
            PhysicalPlan::TableScan { table } => {
                let estimated_rows = ctx.get_stats(table).map(|stats| stats.row_count);
                let ordering = ctx.get_stats(table).and_then(|stats| {
                    if stats.is_sorted {
                        ctx.find_primary_index(table).map(|idx| {
                            OrderingProperty::from_index_columns(table, &idx.columns, SortOrder::Asc)
                        })
                    } else {
                        None
                    }
                });
                Self {
                    estimated_rows,
                    ordering,
                }
            }
            PhysicalPlan::IndexScan {
                table,
                index,
                limit,
                offset,
                reverse,
                ..
            } => {
                let estimated_rows = ctx.get_stats(table).map(|stats| {
                    let base = stats.row_count;
                    match limit {
                        Some(limit) => {
                            let offset = offset.unwrap_or(0);
                            base.saturating_sub(offset).min(*limit)
                        }
                        None => base.saturating_sub(offset.unwrap_or(0)),
                    }
                });
                let ordering = ctx.find_index_by_name(table, index).map(|idx| {
                    OrderingProperty::from_index_columns(
                        table,
                        &idx.columns,
                        if *reverse {
                            SortOrder::Desc
                        } else {
                            SortOrder::Asc
                        },
                    )
                });
                Self {
                    estimated_rows,
                    ordering,
                }
            }
            PhysicalPlan::IndexGet { .. } => Self {
                estimated_rows: Some(1),
                ordering: None,
            },
            PhysicalPlan::IndexInGet { keys, .. } => Self {
                estimated_rows: Some(keys.len()),
                ordering: None,
            },
            PhysicalPlan::Filter { input, .. } => {
                let input = Self::derive(input, ctx);
                Self {
                    estimated_rows: input.estimated_rows.map(|rows| {
                        if rows == 0 {
                            0
                        } else {
                            core::cmp::max(rows / 4, 1)
                        }
                    }),
                    ordering: input.ordering,
                }
            }
            PhysicalPlan::Project { input, columns } => {
                let input = Self::derive(input, ctx);
                Self {
                    estimated_rows: input.estimated_rows,
                    ordering: input.ordering.and_then(|ordering| ordering.projected(columns)),
                }
            }
            PhysicalPlan::Sort { order_by, input } => Self {
                estimated_rows: Self::derive(input, ctx).estimated_rows,
                ordering: OrderingProperty::from_order_by(order_by),
            },
            PhysicalPlan::TopN {
                order_by,
                limit,
                offset,
                ..
            } => Self {
                estimated_rows: Some(limit.saturating_add(*offset)),
                ordering: OrderingProperty::from_order_by(order_by),
            },
            PhysicalPlan::Limit {
                input,
                limit,
                offset,
            } => {
                let input = Self::derive(input, ctx);
                let estimated_rows = input.estimated_rows.map(|rows| {
                    if rows <= *offset {
                        0
                    } else {
                        (rows - offset).min(*limit)
                    }
                });
                Self {
                    estimated_rows,
                    ordering: input.ordering,
                }
            }
            PhysicalPlan::NoOp { input } => Self::derive(input, ctx),
            PhysicalPlan::GinIndexScan { table, .. } => Self {
                estimated_rows: ctx.get_stats(table).map(|stats| {
                    if stats.row_count == 0 {
                        0
                    } else {
                        core::cmp::max(stats.row_count / 10, 1)
                    }
                }),
                ordering: None,
            },
            PhysicalPlan::GinIndexScanMulti { table, .. } => Self {
                estimated_rows: ctx.get_stats(table).map(|stats| {
                    if stats.row_count == 0 {
                        0
                    } else {
                        core::cmp::max(stats.row_count / 20, 1)
                    }
                }),
                ordering: None,
            },
            PhysicalPlan::HashAggregate {
                group_by, input, ..
            } => Self {
                estimated_rows: if group_by.is_empty() {
                    Some(1)
                } else {
                    Self::derive(input, ctx)
                        .estimated_rows
                        .map(|rows| {
                            if rows == 0 {
                                0
                            } else {
                                core::cmp::max(rows / 4, 1)
                            }
                        })
                },
                ordering: None,
            },
            PhysicalPlan::HashJoin { .. }
            | PhysicalPlan::SortMergeJoin { .. }
            | PhysicalPlan::NestedLoopJoin { .. }
            | PhysicalPlan::IndexNestedLoopJoin { .. }
            | PhysicalPlan::CrossProduct { .. } => Self {
                estimated_rows: None,
                ordering: None,
            },
            PhysicalPlan::Empty => Self {
                estimated_rows: Some(0),
                ordering: None,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::{IndexInfo, TableStats};
    use alloc::boxed::Box;

    fn context() -> ExecutionContext {
        let mut ctx = ExecutionContext::new();
        ctx.register_table(
            "scores",
            TableStats {
                row_count: 1000,
                is_sorted: false,
                indexes: alloc::vec![IndexInfo::new("idx_score", alloc::vec!["score".into()], false)],
            },
        );
        ctx
    }

    #[test]
    fn test_filter_project_preserve_index_ordering() {
        let plan = PhysicalPlan::Project {
            input: Box::new(PhysicalPlan::Filter {
                input: Box::new(PhysicalPlan::IndexScan {
                    table: "scores".into(),
                    index: "idx_score".into(),
                    bounds: crate::planner::IndexBounds::all(),
                    limit: None,
                    offset: None,
                    reverse: false,
                }),
                predicate: Expr::eq(Expr::column("scores", "bucket", 2), Expr::literal(1i64)),
            }),
            columns: alloc::vec![
                Expr::column("scores", "id", 0),
                Expr::column("scores", "score", 1),
            ],
        };

        let props = PhysicalProperties::derive(&plan, &context());
        assert!(props
            .ordering
            .as_ref()
            .map(|ordering| ordering.satisfies(&[(Expr::column("scores", "score", 1), SortOrder::Asc)]))
            .unwrap_or(false));
    }

    #[test]
    fn test_topn_reports_output_ordering() {
        let plan = PhysicalPlan::TopN {
            input: Box::new(PhysicalPlan::table_scan("scores")),
            order_by: alloc::vec![(Expr::column("scores", "score", 1), SortOrder::Desc)],
            limit: 10,
            offset: 0,
        };

        let props = PhysicalProperties::derive(&plan, &context());
        assert!(props
            .ordering
            .as_ref()
            .map(|ordering| ordering.satisfies(&[(Expr::column("scores", "score", 1), SortOrder::Desc)]))
            .unwrap_or(false));
    }
}
