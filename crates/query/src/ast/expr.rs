//! Expression AST definitions.

use alloc::boxed::Box;
use alloc::string::String;
use alloc::vec::Vec;
use cynos_core::Value;

/// Reference to a column in a table.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ColumnRef {
    /// Table name (or alias).
    pub table: String,
    /// Column name.
    pub column: String,
    /// Column index in the table schema.
    pub index: usize,
}

impl ColumnRef {
    /// Creates a new column reference.
    pub fn new(table: impl Into<String>, column: impl Into<String>, index: usize) -> Self {
        Self {
            table: table.into(),
            column: column.into(),
            index,
        }
    }

    /// Returns the normalized name (table.column).
    pub fn normalized_name(&self) -> String {
        alloc::format!("{}.{}", self.table, self.column)
    }
}

/// Binary operators.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BinaryOp {
    // Comparison
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
    // Logical
    And,
    Or,
    // Arithmetic
    Add,
    Sub,
    Mul,
    Div,
    Mod,
    // String/Pattern
    Like,
    // Set
    In,
    Between,
}

/// Unary operators.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum UnaryOp {
    Not,
    Neg,
    IsNull,
    IsNotNull,
}

/// Aggregate functions.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AggregateFunc {
    Count,
    Sum,
    Avg,
    Min,
    Max,
    Distinct,
    StdDev,
    GeoMean,
}

/// Sort order.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum SortOrder {
    #[default]
    Asc,
    Desc,
}

/// Expression AST node.
#[derive(Clone, Debug)]
pub enum Expr {
    /// Column reference.
    Column(ColumnRef),
    /// Literal value.
    Literal(Value),
    /// Binary operation.
    BinaryOp {
        left: Box<Expr>,
        op: BinaryOp,
        right: Box<Expr>,
    },
    /// Unary operation.
    UnaryOp { op: UnaryOp, expr: Box<Expr> },
    /// Function call.
    Function { name: String, args: Vec<Expr> },
    /// Aggregate function.
    Aggregate {
        func: AggregateFunc,
        expr: Option<Box<Expr>>,
        distinct: bool,
    },
    /// BETWEEN expression.
    Between {
        expr: Box<Expr>,
        low: Box<Expr>,
        high: Box<Expr>,
    },
    /// NOT BETWEEN expression.
    NotBetween {
        expr: Box<Expr>,
        low: Box<Expr>,
        high: Box<Expr>,
    },
    /// IN expression.
    In {
        expr: Box<Expr>,
        list: Vec<Expr>,
    },
    /// NOT IN expression.
    NotIn {
        expr: Box<Expr>,
        list: Vec<Expr>,
    },
    /// LIKE expression.
    Like {
        expr: Box<Expr>,
        pattern: String,
    },
    /// NOT LIKE expression.
    NotLike {
        expr: Box<Expr>,
        pattern: String,
    },
    /// MATCH (regex) expression.
    Match {
        expr: Box<Expr>,
        pattern: String,
    },
    /// NOT MATCH (regex) expression.
    NotMatch {
        expr: Box<Expr>,
        pattern: String,
    },
}

impl Expr {
    /// Creates a column reference expression.
    pub fn column(table: impl Into<String>, column: impl Into<String>, index: usize) -> Self {
        Expr::Column(ColumnRef::new(table, column, index))
    }

    /// Creates a literal expression.
    pub fn literal(value: impl Into<Value>) -> Self {
        Expr::Literal(value.into())
    }

    /// Creates an equality expression.
    pub fn eq(left: Expr, right: Expr) -> Self {
        Expr::BinaryOp {
            left: Box::new(left),
            op: BinaryOp::Eq,
            right: Box::new(right),
        }
    }

    /// Creates a not-equal expression.
    pub fn ne(left: Expr, right: Expr) -> Self {
        Expr::BinaryOp {
            left: Box::new(left),
            op: BinaryOp::Ne,
            right: Box::new(right),
        }
    }

    /// Creates a less-than expression.
    pub fn lt(left: Expr, right: Expr) -> Self {
        Expr::BinaryOp {
            left: Box::new(left),
            op: BinaryOp::Lt,
            right: Box::new(right),
        }
    }

    /// Creates a less-than-or-equal expression.
    pub fn le(left: Expr, right: Expr) -> Self {
        Expr::BinaryOp {
            left: Box::new(left),
            op: BinaryOp::Le,
            right: Box::new(right),
        }
    }

    /// Creates a greater-than expression.
    pub fn gt(left: Expr, right: Expr) -> Self {
        Expr::BinaryOp {
            left: Box::new(left),
            op: BinaryOp::Gt,
            right: Box::new(right),
        }
    }

    /// Creates a greater-than-or-equal expression.
    pub fn ge(left: Expr, right: Expr) -> Self {
        Expr::BinaryOp {
            left: Box::new(left),
            op: BinaryOp::Ge,
            right: Box::new(right),
        }
    }

    /// Creates an AND expression.
    pub fn and(left: Expr, right: Expr) -> Self {
        Expr::BinaryOp {
            left: Box::new(left),
            op: BinaryOp::And,
            right: Box::new(right),
        }
    }

    /// Creates an OR expression.
    pub fn or(left: Expr, right: Expr) -> Self {
        Expr::BinaryOp {
            left: Box::new(left),
            op: BinaryOp::Or,
            right: Box::new(right),
        }
    }

    /// Creates a NOT expression.
    pub fn not(expr: Expr) -> Self {
        Expr::UnaryOp {
            op: UnaryOp::Not,
            expr: Box::new(expr),
        }
    }

    /// Creates an IS NULL expression.
    pub fn is_null(expr: Expr) -> Self {
        Expr::UnaryOp {
            op: UnaryOp::IsNull,
            expr: Box::new(expr),
        }
    }

    /// Creates an IS NOT NULL expression.
    pub fn is_not_null(expr: Expr) -> Self {
        Expr::UnaryOp {
            op: UnaryOp::IsNotNull,
            expr: Box::new(expr),
        }
    }

    /// Creates a COUNT(*) aggregate.
    pub fn count_star() -> Self {
        Expr::Aggregate {
            func: AggregateFunc::Count,
            expr: None,
            distinct: false,
        }
    }

    /// Creates a COUNT(expr) aggregate.
    pub fn count(expr: Expr) -> Self {
        Expr::Aggregate {
            func: AggregateFunc::Count,
            expr: Some(Box::new(expr)),
            distinct: false,
        }
    }

    /// Creates a SUM aggregate.
    pub fn sum(expr: Expr) -> Self {
        Expr::Aggregate {
            func: AggregateFunc::Sum,
            expr: Some(Box::new(expr)),
            distinct: false,
        }
    }

    /// Creates an AVG aggregate.
    pub fn avg(expr: Expr) -> Self {
        Expr::Aggregate {
            func: AggregateFunc::Avg,
            expr: Some(Box::new(expr)),
            distinct: false,
        }
    }

    /// Creates a MIN aggregate.
    pub fn min(expr: Expr) -> Self {
        Expr::Aggregate {
            func: AggregateFunc::Min,
            expr: Some(Box::new(expr)),
            distinct: false,
        }
    }

    /// Creates a MAX aggregate.
    pub fn max(expr: Expr) -> Self {
        Expr::Aggregate {
            func: AggregateFunc::Max,
            expr: Some(Box::new(expr)),
            distinct: false,
        }
    }

    /// Creates a greater-than-or-equal expression.
    pub fn gte(left: Expr, right: Expr) -> Self {
        Expr::BinaryOp {
            left: Box::new(left),
            op: BinaryOp::Ge,
            right: Box::new(right),
        }
    }

    /// Creates a less-than-or-equal expression.
    pub fn lte(left: Expr, right: Expr) -> Self {
        Expr::BinaryOp {
            left: Box::new(left),
            op: BinaryOp::Le,
            right: Box::new(right),
        }
    }

    /// Creates a BETWEEN expression.
    pub fn between(expr: Expr, low: Expr, high: Expr) -> Self {
        Expr::Between {
            expr: Box::new(expr),
            low: Box::new(low),
            high: Box::new(high),
        }
    }

    /// Creates a NOT BETWEEN expression.
    pub fn not_between(expr: Expr, low: Expr, high: Expr) -> Self {
        Expr::NotBetween {
            expr: Box::new(expr),
            low: Box::new(low),
            high: Box::new(high),
        }
    }

    /// Creates an IN expression.
    pub fn in_list(expr: Expr, values: Vec<Value>) -> Self {
        Expr::In {
            expr: Box::new(expr),
            list: values.into_iter().map(Expr::Literal).collect(),
        }
    }

    /// Creates a NOT IN expression.
    pub fn not_in_list(expr: Expr, values: Vec<Value>) -> Self {
        Expr::NotIn {
            expr: Box::new(expr),
            list: values.into_iter().map(Expr::Literal).collect(),
        }
    }

    /// Creates a LIKE expression.
    pub fn like(expr: Expr, pattern: &str) -> Self {
        Expr::Like {
            expr: Box::new(expr),
            pattern: pattern.into(),
        }
    }

    /// Creates a NOT LIKE expression.
    pub fn not_like(expr: Expr, pattern: &str) -> Self {
        Expr::NotLike {
            expr: Box::new(expr),
            pattern: pattern.into(),
        }
    }

    /// Creates a MATCH (regex) expression.
    pub fn regex_match(expr: Expr, pattern: &str) -> Self {
        Expr::Match {
            expr: Box::new(expr),
            pattern: pattern.into(),
        }
    }

    /// Creates a NOT MATCH (regex) expression.
    pub fn not_regex_match(expr: Expr, pattern: &str) -> Self {
        Expr::NotMatch {
            expr: Box::new(expr),
            pattern: pattern.into(),
        }
    }

    /// Creates a JSONB path equality expression.
    pub fn jsonb_path_eq(expr: Expr, path: &str, value: Value) -> Self {
        // Simplified: treat as function call
        Expr::Function {
            name: "jsonb_path_eq".into(),
            args: alloc::vec![expr, Expr::literal(path), Expr::Literal(value)],
        }
    }

    /// Creates a JSONB contains expression.
    pub fn jsonb_contains(expr: Expr, path: &str) -> Self {
        Expr::Function {
            name: "jsonb_contains".into(),
            args: alloc::vec![expr, Expr::literal(path)],
        }
    }

    /// Creates a JSONB exists expression.
    pub fn jsonb_exists(expr: Expr, path: &str) -> Self {
        Expr::Function {
            name: "jsonb_exists".into(),
            args: alloc::vec![expr, Expr::literal(path)],
        }
    }

    /// Checks if this is an equi-join condition (column = column).
    pub fn is_equi_join(&self) -> bool {
        matches!(
            self,
            Expr::BinaryOp {
                op: BinaryOp::Eq,
                left,
                right
            } if matches!(left.as_ref(), Expr::Column(_)) && matches!(right.as_ref(), Expr::Column(_))
        )
    }

    /// Checks if this is a range join condition (>, <, >=, <=).
    pub fn is_range_join(&self) -> bool {
        matches!(
            self,
            Expr::BinaryOp {
                op: BinaryOp::Lt | BinaryOp::Le | BinaryOp::Gt | BinaryOp::Ge,
                left,
                right
            } if matches!(left.as_ref(), Expr::Column(_)) && matches!(right.as_ref(), Expr::Column(_))
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_column_ref() {
        let col = ColumnRef::new("users", "id", 0);
        assert_eq!(col.table, "users");
        assert_eq!(col.column, "id");
        assert_eq!(col.index, 0);
        assert_eq!(col.normalized_name(), "users.id");
    }

    #[test]
    fn test_expr_builders() {
        let col = Expr::column("t", "c", 0);
        assert!(matches!(col, Expr::Column(_)));

        let lit = Expr::literal(42i64);
        assert!(matches!(lit, Expr::Literal(Value::Int64(42))));

        let eq = Expr::eq(Expr::column("t", "a", 0), Expr::column("t", "b", 1));
        assert!(matches!(eq, Expr::BinaryOp { op: BinaryOp::Eq, .. }));
    }

    #[test]
    fn test_is_equi_join() {
        let equi = Expr::eq(Expr::column("a", "id", 0), Expr::column("b", "id", 0));
        assert!(equi.is_equi_join());

        let non_equi = Expr::eq(Expr::column("a", "id", 0), Expr::literal(1i64));
        assert!(!non_equi.is_equi_join());

        let range = Expr::gt(Expr::column("a", "id", 0), Expr::column("b", "id", 0));
        assert!(!range.is_equi_join());
        assert!(range.is_range_join());
    }
}
