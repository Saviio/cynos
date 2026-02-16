//! PhysicalPlan → DataflowNode compiler.
//!
//! Compiles a query optimizer's PhysicalPlan into a DataflowNode graph
//! for incremental view maintenance. The optimizer's decisions (join order,
//! predicate pushdown, projection pushdown) are preserved in the dataflow.
//!
//! The PhysicalPlan is used for:
//!   1. Bootstrap: execute once to get initial result set
//!   2. Compile: produce DataflowNode graph for incremental maintenance
//!
//! Non-incrementalizable operators (Sort, Limit, TopN) cause the compiler
//! to return None, signaling fallback to re-query strategy.

use alloc::boxed::Box;
use alloc::string::String;
use alloc::vec::Vec;
use cynos_core::{Row, Value};
use cynos_incremental::{
    AggregateType, DataflowNode, JoinType as IvmJoinType, KeyExtractorFn, TableId,
};
use cynos_query::ast::{AggregateFunc, BinaryOp, Expr, UnaryOp};
use cynos_query::ast::JoinType as QueryJoinType;
use cynos_query::planner::PhysicalPlan;
use hashbrown::HashMap;

/// Result of compiling a PhysicalPlan to a DataflowNode.
pub struct CompileResult {
    /// The dataflow node graph for incremental maintenance
    pub dataflow: DataflowNode,
    /// Mapping from table name → table ID used in the dataflow
    pub table_ids: HashMap<String, TableId>,
}

/// Compiles a PhysicalPlan into a DataflowNode for IVM.
///
/// Returns None if the plan contains non-incrementalizable operators
/// (Sort, Limit, TopN), signaling that re-query should be used instead.
pub fn compile_to_dataflow(
    plan: &PhysicalPlan,
    table_id_map: &HashMap<String, TableId>,
) -> Option<CompileResult> {
    if !plan.is_incrementalizable() {
        return None;
    }

    let mut table_ids = table_id_map.clone();
    let dataflow = compile_node(plan, &mut table_ids)?;
    Some(CompileResult { dataflow, table_ids })
}

fn compile_node(
    plan: &PhysicalPlan,
    table_ids: &mut HashMap<String, TableId>,
) -> Option<DataflowNode> {
    match plan {
        // All scan types map to Source nodes
        PhysicalPlan::TableScan { table }
        | PhysicalPlan::IndexScan { table, .. }
        | PhysicalPlan::IndexGet { table, .. }
        | PhysicalPlan::IndexInGet { table, .. }
        | PhysicalPlan::GinIndexScan { table, .. }
        | PhysicalPlan::GinIndexScanMulti { table, .. } => {
            let table_id = get_or_assign_table_id(table, table_ids);
            Some(DataflowNode::source(table_id))
        }

        PhysicalPlan::Filter { input, predicate } => {
            let input_node = compile_node(input, table_ids)?;
            let pred_fn = compile_predicate(predicate);
            Some(DataflowNode::Filter {
                input: Box::new(input_node),
                predicate: pred_fn,
            })
        }

        PhysicalPlan::Project { input, columns } => {
            let input_node = compile_node(input, table_ids)?;
            // Extract column indices from projection expressions
            let col_indices: Vec<usize> = columns
                .iter()
                .filter_map(|expr| extract_column_index(expr))
                .collect();

            if col_indices.len() == columns.len() {
                // Pure column projection — use Project node
                Some(DataflowNode::project(input_node, col_indices))
            } else {
                // Has computed expressions — use Map node
                let exprs = columns.clone();
                Some(DataflowNode::Map {
                    input: Box::new(input_node),
                    mapper: Box::new(move |row: &Row| {
                        let values: Vec<Value> = exprs
                            .iter()
                            .map(|expr| eval_expr(expr, row))
                            .collect();
                        Row::dummy(values)
                    }),
                })
            }
        }

        // All join types compile to Join node with appropriate JoinType
        PhysicalPlan::HashJoin { left, right, condition, join_type }
        | PhysicalPlan::SortMergeJoin { left, right, condition, join_type }
        | PhysicalPlan::NestedLoopJoin { left, right, condition, join_type } => {
            let left_node = compile_node(left, table_ids)?;
            let right_node = compile_node(right, table_ids)?;
            let ivm_join_type = convert_join_type(join_type);

            let (left_key, right_key) = extract_join_keys(condition);

            Some(DataflowNode::Join {
                left: Box::new(left_node),
                right: Box::new(right_node),
                left_key,
                right_key,
                join_type: ivm_join_type,
            })
        }

        PhysicalPlan::IndexNestedLoopJoin {
            outer, inner_table, condition, join_type, ..
        } => {
            let outer_node = compile_node(outer, table_ids)?;
            let inner_table_id = get_or_assign_table_id(inner_table, table_ids);
            let inner_node = DataflowNode::source(inner_table_id);
            let ivm_join_type = convert_join_type(join_type);
            let (left_key, right_key) = extract_join_keys(condition);

            Some(DataflowNode::Join {
                left: Box::new(outer_node),
                right: Box::new(inner_node),
                left_key,
                right_key,
                join_type: ivm_join_type,
            })
        }

        PhysicalPlan::CrossProduct { left, right } => {
            let left_node = compile_node(left, table_ids)?;
            let right_node = compile_node(right, table_ids)?;
            // Cross product = join with constant key (everything matches)
            Some(DataflowNode::Join {
                left: Box::new(left_node),
                right: Box::new(right_node),
                left_key: Box::new(|_| vec![Value::Int64(0)]),
                right_key: Box::new(|_| vec![Value::Int64(0)]),
                join_type: IvmJoinType::Inner,
            })
        }

        PhysicalPlan::HashAggregate { input, group_by, aggregates } => {
            let input_node = compile_node(input, table_ids)?;

            let group_by_indices: Vec<usize> = group_by
                .iter()
                .filter_map(|expr| extract_column_index(expr))
                .collect();

            let functions: Vec<(usize, AggregateType)> = aggregates
                .iter()
                .filter_map(|(func, expr)| {
                    let col_idx = match expr {
                        Expr::Aggregate { expr: Some(inner), .. } => extract_column_index(inner),
                        Expr::Column(col_ref) => Some(col_ref.index),
                        _ => Some(0), // COUNT(*) uses column 0
                    };
                    col_idx.map(|idx| (idx, convert_aggregate_func(func)))
                })
                .collect();

            Some(DataflowNode::Aggregate {
                input: Box::new(input_node),
                group_by: group_by_indices,
                functions,
            })
        }

        PhysicalPlan::NoOp { input } => compile_node(input, table_ids),
        PhysicalPlan::Empty => Some(DataflowNode::source(u32::MAX)), // sentinel

        // Non-incrementalizable — should have been caught by is_incrementalizable()
        PhysicalPlan::Sort { .. }
        | PhysicalPlan::Limit { .. }
        | PhysicalPlan::TopN { .. } => None,
    }
}

// ---------------------------------------------------------------------------
// Expression compilation: Expr → closures
// ---------------------------------------------------------------------------

/// Compiles an Expr predicate into a closure for DataflowNode::Filter.
fn compile_predicate(expr: &Expr) -> Box<dyn Fn(&Row) -> bool + Send + Sync> {
    let expr = expr.clone();
    Box::new(move |row: &Row| {
        match eval_expr(&expr, row) {
            Value::Boolean(b) => b,
            _ => false,
        }
    })
}

/// Evaluates an expression against a row.
fn eval_expr(expr: &Expr, row: &Row) -> Value {
    match expr {
        Expr::Column(col_ref) => {
            row.get(col_ref.index).cloned().unwrap_or(Value::Null)
        }
        Expr::Literal(val) => val.clone(),
        Expr::BinaryOp { left, op, right } => {
            let lval = eval_expr(left, row);
            let rval = eval_expr(right, row);
            eval_binary_op(&lval, op, &rval)
        }
        Expr::UnaryOp { op, expr: inner } => {
            let val = eval_expr(inner, row);
            eval_unary_op(op, &val)
        }
        Expr::In { expr, list } => {
            let val = eval_expr(expr, row);
            let found = list.iter().any(|item| eval_expr(item, row) == val);
            Value::Boolean(found)
        }
        Expr::NotIn { expr, list } => {
            let val = eval_expr(expr, row);
            let found = list.iter().any(|item| eval_expr(item, row) == val);
            Value::Boolean(!found)
        }
        Expr::Between { expr, low, high } => {
            let val = eval_expr(expr, row);
            let lo = eval_expr(low, row);
            let hi = eval_expr(high, row);
            Value::Boolean(val >= lo && val <= hi)
        }
        Expr::NotBetween { expr, low, high } => {
            let val = eval_expr(expr, row);
            let lo = eval_expr(low, row);
            let hi = eval_expr(high, row);
            Value::Boolean(val < lo || val > hi)
        }
        Expr::Like { expr, pattern } => {
            let val = eval_expr(expr, row);
            if let Value::String(s) = val {
                Value::Boolean(cynos_core::pattern_match::like(&s, pattern))
            } else {
                Value::Boolean(false)
            }
        }
        Expr::NotLike { expr, pattern } => {
            let val = eval_expr(expr, row);
            if let Value::String(s) = val {
                Value::Boolean(!cynos_core::pattern_match::like(&s, pattern))
            } else {
                Value::Boolean(true)
            }
        }
        Expr::Match { expr, pattern } => {
            let val = eval_expr(expr, row);
            if let Value::String(s) = val {
                Value::Boolean(cynos_core::pattern_match::regex(&s, pattern))
            } else {
                Value::Boolean(false)
            }
        }
        Expr::NotMatch { expr, pattern } => {
            let val = eval_expr(expr, row);
            if let Value::String(s) = val {
                Value::Boolean(!cynos_core::pattern_match::regex(&s, pattern))
            } else {
                Value::Boolean(true)
            }
        }
        // Function and Aggregate are not expected in filter predicates
        _ => Value::Null,
    }
}

fn eval_binary_op(left: &Value, op: &BinaryOp, right: &Value) -> Value {
    match op {
        BinaryOp::Eq => Value::Boolean(left == right),
        BinaryOp::Ne => Value::Boolean(left != right),
        BinaryOp::Lt => Value::Boolean(left < right),
        BinaryOp::Le => Value::Boolean(left <= right),
        BinaryOp::Gt => Value::Boolean(left > right),
        BinaryOp::Ge => Value::Boolean(left >= right),
        BinaryOp::And => {
            let lb = matches!(left, Value::Boolean(true));
            let rb = matches!(right, Value::Boolean(true));
            Value::Boolean(lb && rb)
        }
        BinaryOp::Or => {
            let lb = matches!(left, Value::Boolean(true));
            let rb = matches!(right, Value::Boolean(true));
            Value::Boolean(lb || rb)
        }
        BinaryOp::Add => numeric_op(left, right, |a, b| a + b),
        BinaryOp::Sub => numeric_op(left, right, |a, b| a - b),
        BinaryOp::Mul => numeric_op(left, right, |a, b| a * b),
        BinaryOp::Div => numeric_op(left, right, |a, b| if b != 0.0 { a / b } else { 0.0 }),
        BinaryOp::Mod => numeric_op(left, right, |a, b| if b != 0.0 { a % b } else { 0.0 }),
        _ => Value::Null,
    }
}

fn eval_unary_op(op: &UnaryOp, val: &Value) -> Value {
    match op {
        UnaryOp::Not => match val {
            Value::Boolean(b) => Value::Boolean(!b),
            _ => Value::Null,
        },
        UnaryOp::Neg => match val {
            Value::Int32(v) => Value::Int32(-v),
            Value::Int64(v) => Value::Int64(-v),
            Value::Float64(v) => Value::Float64(-v),
            _ => Value::Null,
        },
        UnaryOp::IsNull => Value::Boolean(matches!(val, Value::Null)),
        UnaryOp::IsNotNull => Value::Boolean(!matches!(val, Value::Null)),
    }
}

fn numeric_op(left: &Value, right: &Value, op: fn(f64, f64) -> f64) -> Value {
    let l = as_f64(left);
    let r = as_f64(right);
    match (l, r) {
        (Some(a), Some(b)) => Value::Float64(op(a, b)),
        _ => Value::Null,
    }
}

fn as_f64(val: &Value) -> Option<f64> {
    match val {
        Value::Int32(v) => Some(*v as f64),
        Value::Int64(v) => Some(*v as f64),
        Value::Float64(v) => Some(*v),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Join key extraction
// ---------------------------------------------------------------------------

/// Extracts left and right key extractor functions from a join condition.
/// Handles equi-join conditions like `left.col = right.col`.
fn extract_join_keys(condition: &Expr) -> (KeyExtractorFn, KeyExtractorFn) {
    match condition {
        Expr::BinaryOp { left, op: BinaryOp::Eq, right } => {
            let left_idx = extract_column_index(left).unwrap_or(0);
            let right_idx = extract_column_index(right).unwrap_or(0);
            (
                Box::new(move |row: &Row| {
                    vec![row.get(left_idx).cloned().unwrap_or(Value::Null)]
                }),
                Box::new(move |row: &Row| {
                    vec![row.get(right_idx).cloned().unwrap_or(Value::Null)]
                }),
            )
        }
        Expr::BinaryOp { op: BinaryOp::And, .. } => {
            // Compound join key: a.x = b.x AND a.y = b.y
            let mut left_indices = Vec::new();
            let mut right_indices = Vec::new();
            collect_equi_join_keys(condition, &mut left_indices, &mut right_indices);

            (
                Box::new(move |row: &Row| {
                    left_indices.iter()
                        .map(|&idx| row.get(idx).cloned().unwrap_or(Value::Null))
                        .collect()
                }),
                Box::new(move |row: &Row| {
                    right_indices.iter()
                        .map(|&idx| row.get(idx).cloned().unwrap_or(Value::Null))
                        .collect()
                }),
            )
        }
        _ => {
            // Fallback: use entire row as key (degenerate case)
            (
                Box::new(|row: &Row| row.values().to_vec()),
                Box::new(|row: &Row| row.values().to_vec()),
            )
        }
    }
}

fn collect_equi_join_keys(expr: &Expr, left_indices: &mut Vec<usize>, right_indices: &mut Vec<usize>) {
    match expr {
        Expr::BinaryOp { left, op: BinaryOp::Eq, right } => {
            if let Some(li) = extract_column_index(left) {
                left_indices.push(li);
            }
            if let Some(ri) = extract_column_index(right) {
                right_indices.push(ri);
            }
        }
        Expr::BinaryOp { left, op: BinaryOp::And, right } => {
            collect_equi_join_keys(left, left_indices, right_indices);
            collect_equi_join_keys(right, left_indices, right_indices);
        }
        _ => {}
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn extract_column_index(expr: &Expr) -> Option<usize> {
    match expr {
        Expr::Column(col_ref) => Some(col_ref.index),
        _ => None,
    }
}

fn get_or_assign_table_id(table: &str, table_ids: &mut HashMap<String, TableId>) -> TableId {
    let next_id = table_ids.len() as TableId;
    *table_ids.entry(table.into()).or_insert(next_id)
}

fn convert_join_type(jt: &QueryJoinType) -> IvmJoinType {
    match jt {
        QueryJoinType::Inner | QueryJoinType::Cross => IvmJoinType::Inner,
        QueryJoinType::LeftOuter => IvmJoinType::LeftOuter,
        QueryJoinType::RightOuter => IvmJoinType::RightOuter,
        QueryJoinType::FullOuter => IvmJoinType::FullOuter,
    }
}

fn convert_aggregate_func(func: &AggregateFunc) -> AggregateType {
    match func {
        AggregateFunc::Count => AggregateType::Count,
        AggregateFunc::Sum => AggregateType::Sum,
        AggregateFunc::Avg => AggregateType::Avg,
        AggregateFunc::Min => AggregateType::Min,
        AggregateFunc::Max => AggregateType::Max,
        // Unsupported aggregates fall back to Count
        _ => AggregateType::Count,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cynos_query::ast::Expr;

    #[test]
    fn test_compile_table_scan() {
        let plan = PhysicalPlan::table_scan("users");
        let mut table_ids = HashMap::new();
        table_ids.insert("users".into(), 1u32);

        let result = compile_to_dataflow(&plan, &table_ids).unwrap();
        assert!(matches!(result.dataflow, DataflowNode::Source { table_id: 1 }));
    }

    #[test]
    fn test_compile_filter() {
        let plan = PhysicalPlan::filter(
            PhysicalPlan::table_scan("users"),
            Expr::gt(Expr::column("users", "age", 1), Expr::literal(18i64)),
        );
        let mut table_ids = HashMap::new();
        table_ids.insert("users".into(), 1u32);

        let result = compile_to_dataflow(&plan, &table_ids).unwrap();
        assert!(matches!(result.dataflow, DataflowNode::Filter { .. }));
    }

    #[test]
    fn test_compile_non_incrementalizable() {
        let plan = PhysicalPlan::sort(
            PhysicalPlan::table_scan("users"),
            alloc::vec![(Expr::column("users", "id", 0), cynos_query::ast::SortOrder::Asc)],
        );
        let table_ids = HashMap::new();
        assert!(compile_to_dataflow(&plan, &table_ids).is_none());
    }

    #[test]
    fn test_compile_hash_join() {
        use cynos_query::ast::JoinType;
        let plan = PhysicalPlan::hash_join(
            PhysicalPlan::table_scan("employees"),
            PhysicalPlan::table_scan("departments"),
            Expr::eq(
                Expr::column("employees", "dept_id", 2),
                Expr::column("departments", "id", 0),
            ),
            JoinType::LeftOuter,
        );
        let mut table_ids = HashMap::new();
        table_ids.insert("employees".into(), 1u32);
        table_ids.insert("departments".into(), 2u32);

        let result = compile_to_dataflow(&plan, &table_ids).unwrap();
        match &result.dataflow {
            DataflowNode::Join { join_type, .. } => {
                assert_eq!(*join_type, IvmJoinType::LeftOuter);
            }
            _ => panic!("Expected Join node"),
        }
    }

    #[test]
    fn test_compile_aggregate() {
        let plan = PhysicalPlan::hash_aggregate(
            PhysicalPlan::table_scan("orders"),
            alloc::vec![Expr::column("orders", "customer_id", 0)],
            alloc::vec![
                (AggregateFunc::Count, Expr::column("orders", "id", 1)),
                (AggregateFunc::Sum, Expr::column("orders", "amount", 2)),
            ],
        );
        let mut table_ids = HashMap::new();
        table_ids.insert("orders".into(), 1u32);

        let result = compile_to_dataflow(&plan, &table_ids).unwrap();
        match &result.dataflow {
            DataflowNode::Aggregate { group_by, functions, .. } => {
                assert_eq!(group_by, &[0]);
                assert_eq!(functions.len(), 2);
                assert_eq!(functions[0].1, AggregateType::Count);
                assert_eq!(functions[1].1, AggregateType::Sum);
            }
            _ => panic!("Expected Aggregate node"),
        }
    }

    #[test]
    fn test_eval_in_expr() {
        let row = Row::new(1, vec![Value::Int64(3), Value::String("Alice".into())]);
        let expr = Expr::In {
            expr: Box::new(Expr::column("t", "id", 0)),
            list: vec![
                Expr::literal(Value::Int64(1)),
                Expr::literal(Value::Int64(3)),
                Expr::literal(Value::Int64(5)),
            ],
        };
        assert_eq!(eval_expr(&expr, &row), Value::Boolean(true));

        let expr_miss = Expr::In {
            expr: Box::new(Expr::column("t", "id", 0)),
            list: vec![
                Expr::literal(Value::Int64(2)),
                Expr::literal(Value::Int64(4)),
            ],
        };
        assert_eq!(eval_expr(&expr_miss, &row), Value::Boolean(false));
    }

    #[test]
    fn test_eval_not_in_expr() {
        let row = Row::new(1, vec![Value::Int64(3)]);
        let expr = Expr::NotIn {
            expr: Box::new(Expr::column("t", "id", 0)),
            list: vec![
                Expr::literal(Value::Int64(1)),
                Expr::literal(Value::Int64(3)),
            ],
        };
        assert_eq!(eval_expr(&expr, &row), Value::Boolean(false));
    }

    #[test]
    fn test_eval_between_expr() {
        let row = Row::new(1, vec![Value::Int64(15)]);
        let expr = Expr::Between {
            expr: Box::new(Expr::column("t", "v", 0)),
            low: Box::new(Expr::literal(Value::Int64(10))),
            high: Box::new(Expr::literal(Value::Int64(20))),
        };
        assert_eq!(eval_expr(&expr, &row), Value::Boolean(true));

        let row_out = Row::new(2, vec![Value::Int64(25)]);
        assert_eq!(eval_expr(&expr, &row_out), Value::Boolean(false));
    }

    #[test]
    fn test_eval_like_expr() {
        let row = Row::new(1, vec![Value::String("Alice".into())]);
        let expr = Expr::Like {
            expr: Box::new(Expr::column("t", "name", 0)),
            pattern: "Al%".into(),
        };
        assert_eq!(eval_expr(&expr, &row), Value::Boolean(true));

        let expr2 = Expr::Like {
            expr: Box::new(Expr::column("t", "name", 0)),
            pattern: "Bo%".into(),
        };
        assert_eq!(eval_expr(&expr2, &row), Value::Boolean(false));

        // underscore wildcard
        let expr3 = Expr::Like {
            expr: Box::new(Expr::column("t", "name", 0)),
            pattern: "A_ice".into(),
        };
        assert_eq!(eval_expr(&expr3, &row), Value::Boolean(true));
    }

    #[test]
    fn test_eval_match_expr() {
        let row = Row::new(1, vec![Value::String("abc123".into())]);
        let expr = Expr::Match {
            expr: Box::new(Expr::column("t", "v", 0)),
            pattern: "\\d+".into(),
        };
        assert_eq!(eval_expr(&expr, &row), Value::Boolean(true));

        let expr2 = Expr::Match {
            expr: Box::new(Expr::column("t", "v", 0)),
            pattern: "^[A-Z]".into(),
        };
        assert_eq!(eval_expr(&expr2, &row), Value::Boolean(false));
    }

    #[test]
    fn test_in_filter_via_compile() {
        // End-to-end: compile a Filter with IN predicate, then run through MaterializedView
        use cynos_incremental::{Delta, MaterializedView};

        let plan = PhysicalPlan::filter(
            PhysicalPlan::table_scan("users"),
            Expr::In {
                expr: Box::new(Expr::column("users", "id", 0)),
                list: vec![
                    Expr::literal(Value::Int64(1)),
                    Expr::literal(Value::Int64(3)),
                ],
            },
        );
        let mut table_ids = HashMap::new();
        table_ids.insert("users".into(), 1u32);

        let result = compile_to_dataflow(&plan, &table_ids).unwrap();
        let mut view = MaterializedView::new(result.dataflow);

        // Insert rows: id=1 (match), id=2 (no match), id=3 (match)
        view.on_table_change(1, vec![
            Delta::insert(Row::new(1, vec![Value::Int64(1), Value::String("Alice".into())])),
            Delta::insert(Row::new(2, vec![Value::Int64(2), Value::String("Bob".into())])),
            Delta::insert(Row::new(3, vec![Value::Int64(3), Value::String("Carol".into())])),
        ]);

        assert_eq!(view.len(), 2); // only id=1 and id=3
        let rows = view.result();
        let ids: Vec<i64> = rows.iter()
            .filter_map(|r| r.get(0).and_then(|v| v.as_i64()))
            .collect();
        assert!(ids.contains(&1));
        assert!(ids.contains(&3));
        assert!(!ids.contains(&2));
    }
}
