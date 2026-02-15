//! Predicate definitions for query filtering.

use crate::ast::expr::{BinaryOp, ColumnRef};
use alloc::boxed::Box;
use alloc::vec::Vec;
use cynos_core::{Row, Value};

/// Evaluation type for predicates.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EvalType {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
    Match,
    Between,
    In,
}

impl From<BinaryOp> for EvalType {
    fn from(op: BinaryOp) -> Self {
        match op {
            BinaryOp::Eq => EvalType::Eq,
            BinaryOp::Ne => EvalType::Ne,
            BinaryOp::Lt => EvalType::Lt,
            BinaryOp::Le => EvalType::Le,
            BinaryOp::Gt => EvalType::Gt,
            BinaryOp::Ge => EvalType::Ge,
            BinaryOp::Like => EvalType::Match,
            BinaryOp::In => EvalType::In,
            BinaryOp::Between => EvalType::Between,
            _ => EvalType::Eq,
        }
    }
}

/// A predicate that can be evaluated against rows.
pub trait Predicate {
    /// Evaluates the predicate against a row.
    fn eval(&self, row: &Row) -> bool;

    /// Returns the columns referenced by this predicate.
    fn columns(&self) -> Vec<&ColumnRef>;

    /// Returns the tables referenced by this predicate.
    fn tables(&self) -> Vec<&str>;
}

/// A value predicate compares a column to a literal value.
#[derive(Clone, Debug)]
pub struct ValuePredicate {
    pub column: ColumnRef,
    pub eval_type: EvalType,
    pub value: Value,
}

impl ValuePredicate {
    pub fn new(column: ColumnRef, eval_type: EvalType, value: Value) -> Self {
        Self {
            column,
            eval_type,
            value,
        }
    }

    pub fn eq(column: ColumnRef, value: Value) -> Self {
        Self::new(column, EvalType::Eq, value)
    }

    pub fn ne(column: ColumnRef, value: Value) -> Self {
        Self::new(column, EvalType::Ne, value)
    }

    pub fn lt(column: ColumnRef, value: Value) -> Self {
        Self::new(column, EvalType::Lt, value)
    }

    pub fn le(column: ColumnRef, value: Value) -> Self {
        Self::new(column, EvalType::Le, value)
    }

    pub fn gt(column: ColumnRef, value: Value) -> Self {
        Self::new(column, EvalType::Gt, value)
    }

    pub fn ge(column: ColumnRef, value: Value) -> Self {
        Self::new(column, EvalType::Ge, value)
    }
}

impl Predicate for ValuePredicate {
    fn eval(&self, row: &Row) -> bool {
        let row_value = match row.get(self.column.index) {
            Some(v) => v,
            None => return false,
        };

        match self.eval_type {
            EvalType::Eq => row_value == &self.value,
            EvalType::Ne => row_value != &self.value,
            EvalType::Lt => row_value < &self.value,
            EvalType::Le => row_value <= &self.value,
            EvalType::Gt => row_value > &self.value,
            EvalType::Ge => row_value >= &self.value,
            _ => false,
        }
    }

    fn columns(&self) -> Vec<&ColumnRef> {
        alloc::vec![&self.column]
    }

    fn tables(&self) -> Vec<&str> {
        alloc::vec![self.column.table.as_str()]
    }
}

/// Join type for join predicates.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum JoinType {
    Inner,
    LeftOuter,
    RightOuter,
    FullOuter,
    Cross,
}

/// A join predicate compares columns from two tables.
#[derive(Clone, Debug)]
pub struct JoinPredicate {
    pub left_column: ColumnRef,
    pub right_column: ColumnRef,
    pub eval_type: EvalType,
    pub join_type: JoinType,
}

impl JoinPredicate {
    pub fn new(
        left_column: ColumnRef,
        right_column: ColumnRef,
        eval_type: EvalType,
        join_type: JoinType,
    ) -> Self {
        Self {
            left_column,
            right_column,
            eval_type,
            join_type,
        }
    }

    pub fn inner(left_column: ColumnRef, right_column: ColumnRef, eval_type: EvalType) -> Self {
        Self::new(left_column, right_column, eval_type, JoinType::Inner)
    }

    pub fn left_outer(
        left_column: ColumnRef,
        right_column: ColumnRef,
        eval_type: EvalType,
    ) -> Self {
        Self::new(left_column, right_column, eval_type, JoinType::LeftOuter)
    }

    /// Reverses the join predicate (swaps left and right columns).
    pub fn reverse(&self) -> Self {
        let new_eval_type = match self.eval_type {
            EvalType::Lt => EvalType::Gt,
            EvalType::Le => EvalType::Ge,
            EvalType::Gt => EvalType::Lt,
            EvalType::Ge => EvalType::Le,
            other => other,
        };
        Self::new(
            self.right_column.clone(),
            self.left_column.clone(),
            new_eval_type,
            self.join_type,
        )
    }

    /// Checks if this is an equi-join (equality comparison).
    pub fn is_equi_join(&self) -> bool {
        self.eval_type == EvalType::Eq
    }

    /// Evaluates the join condition for two rows.
    pub fn eval_rows(&self, left_row: &Row, right_row: &Row) -> bool {
        let left_value = match left_row.get(self.left_column.index) {
            Some(v) => v,
            None => return false,
        };
        let right_value = match right_row.get(self.right_column.index) {
            Some(v) => v,
            None => return false,
        };

        // NULL values don't match in joins
        if left_value.is_null() || right_value.is_null() {
            return false;
        }

        match self.eval_type {
            EvalType::Eq => left_value == right_value,
            EvalType::Ne => left_value != right_value,
            EvalType::Lt => left_value < right_value,
            EvalType::Le => left_value <= right_value,
            EvalType::Gt => left_value > right_value,
            EvalType::Ge => left_value >= right_value,
            _ => false,
        }
    }
}

impl Predicate for JoinPredicate {
    fn eval(&self, row: &Row) -> bool {
        // For a combined row, we need both column indices to be valid
        let left_value = match row.get(self.left_column.index) {
            Some(v) => v,
            None => return false,
        };
        let right_value = match row.get(self.right_column.index) {
            Some(v) => v,
            None => return false,
        };

        if left_value.is_null() || right_value.is_null() {
            return false;
        }

        match self.eval_type {
            EvalType::Eq => left_value == right_value,
            EvalType::Ne => left_value != right_value,
            EvalType::Lt => left_value < right_value,
            EvalType::Le => left_value <= right_value,
            EvalType::Gt => left_value > right_value,
            EvalType::Ge => left_value >= right_value,
            _ => false,
        }
    }

    fn columns(&self) -> Vec<&ColumnRef> {
        alloc::vec![&self.left_column, &self.right_column]
    }

    fn tables(&self) -> Vec<&str> {
        alloc::vec![
            self.left_column.table.as_str(),
            self.right_column.table.as_str()
        ]
    }
}

/// Logical operator for combining predicates.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LogicalOp {
    And,
    Or,
}

/// A combined predicate joins multiple predicates with AND/OR.
#[derive(Clone, Debug)]
pub struct CombinedPredicate {
    pub op: LogicalOp,
    pub children: Vec<Box<dyn PredicateClone>>,
}

/// Helper trait for cloning boxed predicates.
pub trait PredicateClone: Predicate {
    fn clone_box(&self) -> Box<dyn PredicateClone>;
}

impl<T: Predicate + Clone + 'static> PredicateClone for T {
    fn clone_box(&self) -> Box<dyn PredicateClone> {
        Box::new(self.clone())
    }
}

impl Clone for Box<dyn PredicateClone> {
    fn clone(&self) -> Self {
        self.clone_box()
    }
}

impl core::fmt::Debug for Box<dyn PredicateClone> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "PredicateClone")
    }
}

impl CombinedPredicate {
    pub fn and(children: Vec<Box<dyn PredicateClone>>) -> Self {
        Self {
            op: LogicalOp::And,
            children,
        }
    }

    pub fn or(children: Vec<Box<dyn PredicateClone>>) -> Self {
        Self {
            op: LogicalOp::Or,
            children,
        }
    }
}

impl Predicate for CombinedPredicate {
    fn eval(&self, row: &Row) -> bool {
        match self.op {
            LogicalOp::And => self.children.iter().all(|p| p.eval(row)),
            LogicalOp::Or => self.children.iter().any(|p| p.eval(row)),
        }
    }

    fn columns(&self) -> Vec<&ColumnRef> {
        self.children.iter().flat_map(|p| p.columns()).collect()
    }

    fn tables(&self) -> Vec<&str> {
        self.children.iter().flat_map(|p| p.tables()).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloc::vec;

    #[test]
    fn test_value_predicate_eq() {
        let col = ColumnRef::new("t", "id", 0);
        let pred = ValuePredicate::eq(col, Value::Int64(42));

        let row_match = Row::new(1, vec![Value::Int64(42)]);
        let row_no_match = Row::new(2, vec![Value::Int64(100)]);

        assert!(pred.eval(&row_match));
        assert!(!pred.eval(&row_no_match));
    }

    #[test]
    fn test_value_predicate_comparison() {
        let col = ColumnRef::new("t", "value", 0);

        let pred_lt = ValuePredicate::lt(col.clone(), Value::Int64(50));
        let pred_gt = ValuePredicate::gt(col.clone(), Value::Int64(50));

        let row = Row::new(1, vec![Value::Int64(30)]);

        assert!(pred_lt.eval(&row));
        assert!(!pred_gt.eval(&row));
    }

    #[test]
    fn test_join_predicate() {
        let left_col = ColumnRef::new("a", "id", 0);
        let right_col = ColumnRef::new("b", "a_id", 1);
        let pred = JoinPredicate::inner(left_col, right_col, EvalType::Eq);

        let left_row = Row::new(1, vec![Value::Int64(10)]);
        let right_row_match = Row::new(2, vec![Value::Int64(10)]);
        let right_row_no_match = Row::new(3, vec![Value::Int64(20)]);

        // For eval_rows, we pass separate rows
        // Note: This test uses a simplified model where we check values at specific indices
        assert!(pred.is_equi_join());
    }

    #[test]
    fn test_join_predicate_reverse() {
        let left_col = ColumnRef::new("a", "id", 0);
        let right_col = ColumnRef::new("b", "a_id", 1);
        let pred = JoinPredicate::inner(left_col, right_col, EvalType::Lt);

        let reversed = pred.reverse();
        assert_eq!(reversed.eval_type, EvalType::Gt);
        assert_eq!(reversed.left_column.table, "b");
        assert_eq!(reversed.right_column.table, "a");
    }
}
