//! Expression builders for query predicates.
//!
//! This module provides the `Column` and `Expr` types for building
//! query predicates in a fluent API style.

use crate::convert::js_to_value;
use alloc::boxed::Box;
use alloc::string::{String, ToString};
use alloc::vec::Vec;
use cynos_core::{DataType, Value};
use cynos_query::ast::Expr as AstExpr;
use wasm_bindgen::prelude::*;

/// A column reference for building expressions.
#[wasm_bindgen]
#[derive(Clone, Debug)]
pub struct Column {
    table: Option<String>,
    name: String,
    index: Option<usize>,
}

#[wasm_bindgen]
impl Column {
    /// Creates a new column reference with table name.
    #[wasm_bindgen(constructor)]
    pub fn new(table: &str, name: &str) -> Self {
        Self {
            table: Some(table.to_string()),
            name: name.to_string(),
            index: None,
        }
    }

    /// Creates a simple column reference without table name.
    /// If the name contains a dot (e.g., "orders.year"), it will be parsed
    /// as table.column.
    pub fn new_simple(name: &str) -> Self {
        if let Some(dot_pos) = name.find('.') {
            let table = &name[..dot_pos];
            let col = &name[dot_pos + 1..];
            Self {
                table: Some(table.to_string()),
                name: col.to_string(),
                index: None,
            }
        } else {
            Self {
                table: None,
                name: name.to_string(),
                index: None,
            }
        }
    }

    /// Sets the column index.
    pub fn with_index(mut self, index: usize) -> Self {
        self.index = Some(index);
        self
    }

    /// Returns the column name.
    #[wasm_bindgen(getter)]
    pub fn name(&self) -> String {
        self.name.clone()
    }

    /// Returns the table name if set.
    #[wasm_bindgen(getter, js_name = tableName)]
    pub fn table_name(&self) -> Option<String> {
        self.table.clone()
    }

    /// Creates an equality expression: column = value
    pub fn eq(&self, value: &JsValue) -> Expr {
        Expr::comparison(self.clone(), ComparisonOp::Eq, value.clone())
    }

    /// Creates a not-equal expression: column != value
    pub fn ne(&self, value: &JsValue) -> Expr {
        Expr::comparison(self.clone(), ComparisonOp::Ne, value.clone())
    }

    /// Creates a greater-than expression: column > value
    pub fn gt(&self, value: &JsValue) -> Expr {
        Expr::comparison(self.clone(), ComparisonOp::Gt, value.clone())
    }

    /// Creates a greater-than-or-equal expression: column >= value
    pub fn gte(&self, value: &JsValue) -> Expr {
        Expr::comparison(self.clone(), ComparisonOp::Gte, value.clone())
    }

    /// Creates a less-than expression: column < value
    pub fn lt(&self, value: &JsValue) -> Expr {
        Expr::comparison(self.clone(), ComparisonOp::Lt, value.clone())
    }

    /// Creates a less-than-or-equal expression: column <= value
    pub fn lte(&self, value: &JsValue) -> Expr {
        Expr::comparison(self.clone(), ComparisonOp::Lte, value.clone())
    }

    /// Creates a BETWEEN expression: column BETWEEN low AND high
    pub fn between(&self, low: &JsValue, high: &JsValue) -> Expr {
        Expr::between(self.clone(), low.clone(), high.clone())
    }

    /// Creates a NOT BETWEEN expression: column NOT BETWEEN low AND high
    #[wasm_bindgen(js_name = notBetween)]
    pub fn not_between(&self, low: &JsValue, high: &JsValue) -> Expr {
        Expr::not_between(self.clone(), low.clone(), high.clone())
    }

    /// Creates an IN expression: column IN (values)
    #[wasm_bindgen(js_name = "in")]
    pub fn in_(&self, values: &JsValue) -> Expr {
        Expr::in_list(self.clone(), values.clone())
    }

    /// Creates a NOT IN expression: column NOT IN (values)
    #[wasm_bindgen(js_name = notIn)]
    pub fn not_in(&self, values: &JsValue) -> Expr {
        Expr::not_in_list(self.clone(), values.clone())
    }

    /// Creates a LIKE expression: column LIKE pattern
    pub fn like(&self, pattern: &str) -> Expr {
        Expr::like(self.clone(), pattern)
    }

    /// Creates a NOT LIKE expression: column NOT LIKE pattern
    #[wasm_bindgen(js_name = notLike)]
    pub fn not_like(&self, pattern: &str) -> Expr {
        Expr::not_like(self.clone(), pattern)
    }

    /// Creates a MATCH (regex) expression: column MATCH pattern
    #[wasm_bindgen(js_name = "match")]
    pub fn regex_match(&self, pattern: &str) -> Expr {
        Expr::regex_match(self.clone(), pattern)
    }

    /// Creates a NOT MATCH (regex) expression: column NOT MATCH pattern
    #[wasm_bindgen(js_name = notMatch)]
    pub fn not_regex_match(&self, pattern: &str) -> Expr {
        Expr::not_regex_match(self.clone(), pattern)
    }

    /// Creates an IS NULL expression
    #[wasm_bindgen(js_name = isNull)]
    pub fn is_null(&self) -> Expr {
        Expr::is_null(self.clone())
    }

    /// Creates an IS NOT NULL expression
    #[wasm_bindgen(js_name = isNotNull)]
    pub fn is_not_null(&self) -> Expr {
        Expr::is_not_null(self.clone())
    }

    /// Creates a JSONB path access expression
    pub fn get(&self, path: &str) -> JsonbColumn {
        JsonbColumn {
            column: self.clone(),
            path: path.to_string(),
        }
    }

    /// Converts to AST expression.
    pub(crate) fn to_ast(&self) -> AstExpr {
        AstExpr::column(
            self.table.as_deref().unwrap_or(""),
            &self.name,
            self.index.unwrap_or(0),
        )
    }
}

/// A JSONB column with path access.
#[wasm_bindgen]
#[derive(Clone, Debug)]
pub struct JsonbColumn {
    column: Column,
    path: String,
}

#[wasm_bindgen]
impl JsonbColumn {
    /// Creates an equality expression for the JSONB path value.
    pub fn eq(&self, value: &JsValue) -> Expr {
        Expr::jsonb_eq(self.column.clone(), &self.path, value.clone())
    }

    /// Creates a contains expression for the JSONB path.
    pub fn contains(&self, value: &JsValue) -> Expr {
        Expr::jsonb_contains(self.column.clone(), &self.path, value.clone())
    }

    /// Creates an exists expression for the JSONB path.
    pub fn exists(&self) -> Expr {
        Expr::jsonb_exists(self.column.clone(), &self.path)
    }
}

/// Comparison operators.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ComparisonOp {
    Eq,
    Ne,
    Gt,
    Gte,
    Lt,
    Lte,
}

/// Expression type for query predicates.
#[wasm_bindgen]
#[derive(Clone, Debug)]
pub struct Expr {
    inner: ExprInner,
}

#[derive(Clone, Debug)]
#[allow(dead_code)]
pub(crate) enum ExprInner {
    Comparison {
        column: Column,
        op: ComparisonOp,
        value: JsValue,
    },
    Between {
        column: Column,
        low: JsValue,
        high: JsValue,
    },
    NotBetween {
        column: Column,
        low: JsValue,
        high: JsValue,
    },
    InList {
        column: Column,
        values: JsValue,
    },
    NotInList {
        column: Column,
        values: JsValue,
    },
    Like {
        column: Column,
        pattern: String,
    },
    NotLike {
        column: Column,
        pattern: String,
    },
    Match {
        column: Column,
        pattern: String,
    },
    NotMatch {
        column: Column,
        pattern: String,
    },
    IsNull {
        column: Column,
    },
    IsNotNull {
        column: Column,
    },
    JsonbEq {
        column: Column,
        path: String,
        value: JsValue,
    },
    JsonbContains {
        column: Column,
        path: String,
        value: JsValue,
    },
    JsonbExists {
        column: Column,
        path: String,
    },
    And {
        left: Box<Expr>,
        right: Box<Expr>,
    },
    Or {
        left: Box<Expr>,
        right: Box<Expr>,
    },
    Not {
        inner: Box<Expr>,
    },
    ColumnRef {
        column: Column,
    },
    Literal {
        value: JsValue,
    },
    True,
}

impl Expr {
    pub(crate) fn comparison(column: Column, op: ComparisonOp, value: JsValue) -> Self {
        Self {
            inner: ExprInner::Comparison { column, op, value },
        }
    }

    pub(crate) fn between(column: Column, low: JsValue, high: JsValue) -> Self {
        Self {
            inner: ExprInner::Between { column, low, high },
        }
    }

    pub(crate) fn not_between(column: Column, low: JsValue, high: JsValue) -> Self {
        Self {
            inner: ExprInner::NotBetween { column, low, high },
        }
    }

    pub(crate) fn in_list(column: Column, values: JsValue) -> Self {
        Self {
            inner: ExprInner::InList { column, values },
        }
    }

    pub(crate) fn not_in_list(column: Column, values: JsValue) -> Self {
        Self {
            inner: ExprInner::NotInList { column, values },
        }
    }

    pub(crate) fn like(column: Column, pattern: &str) -> Self {
        Self {
            inner: ExprInner::Like {
                column,
                pattern: pattern.to_string(),
            },
        }
    }

    pub(crate) fn not_like(column: Column, pattern: &str) -> Self {
        Self {
            inner: ExprInner::NotLike {
                column,
                pattern: pattern.to_string(),
            },
        }
    }

    pub(crate) fn regex_match(column: Column, pattern: &str) -> Self {
        Self {
            inner: ExprInner::Match {
                column,
                pattern: pattern.to_string(),
            },
        }
    }

    pub(crate) fn not_regex_match(column: Column, pattern: &str) -> Self {
        Self {
            inner: ExprInner::NotMatch {
                column,
                pattern: pattern.to_string(),
            },
        }
    }

    pub(crate) fn is_null(column: Column) -> Self {
        Self {
            inner: ExprInner::IsNull { column },
        }
    }

    pub(crate) fn is_not_null(column: Column) -> Self {
        Self {
            inner: ExprInner::IsNotNull { column },
        }
    }

    pub(crate) fn jsonb_eq(column: Column, path: &str, value: JsValue) -> Self {
        Self {
            inner: ExprInner::JsonbEq {
                column,
                path: path.to_string(),
                value,
            },
        }
    }

    pub(crate) fn jsonb_contains(column: Column, path: &str, value: JsValue) -> Self {
        Self {
            inner: ExprInner::JsonbContains {
                column,
                path: path.to_string(),
                value,
            },
        }
    }

    pub(crate) fn jsonb_exists(column: Column, path: &str) -> Self {
        Self {
            inner: ExprInner::JsonbExists {
                column,
                path: path.to_string(),
            },
        }
    }

    #[allow(dead_code)]
    pub(crate) fn column_ref(column: Column) -> Self {
        Self {
            inner: ExprInner::ColumnRef { column },
        }
    }

    #[allow(dead_code)]
    pub(crate) fn literal(value: JsValue) -> Self {
        Self {
            inner: ExprInner::Literal { value },
        }
    }

    #[allow(dead_code)]
    pub(crate) fn true_expr() -> Self {
        Self {
            inner: ExprInner::True,
        }
    }

    /// Returns the inner expression type.
    pub(crate) fn inner(&self) -> &ExprInner {
        &self.inner
    }
}

#[wasm_bindgen]
impl Expr {
    /// Creates an AND expression: self AND other
    pub fn and(&self, other: &Expr) -> Expr {
        Expr {
            inner: ExprInner::And {
                left: Box::new(self.clone()),
                right: Box::new(other.clone()),
            },
        }
    }

    /// Creates an OR expression: self OR other
    pub fn or(&self, other: &Expr) -> Expr {
        Expr {
            inner: ExprInner::Or {
                left: Box::new(self.clone()),
                right: Box::new(other.clone()),
            },
        }
    }

    /// Creates a NOT expression: NOT self
    pub fn not(&self) -> Expr {
        Expr {
            inner: ExprInner::Not {
                inner: Box::new(self.clone()),
            },
        }
    }
}

impl Expr {
    /// Converts to AST expression for JOIN conditions where table names are needed.
    pub(crate) fn to_ast_with_table(&self, get_column_info: &impl Fn(&str) -> Option<(String, usize, DataType)>) -> AstExpr {
        match &self.inner {
            ExprInner::Comparison { column, op, value } => {
                // Build the lookup key: if table is set, use "table.column", otherwise just "column"
                let lookup_key = if let Some(ref table) = column.table {
                    alloc::format!("{}.{}", table, column.name)
                } else {
                    column.name.clone()
                };

                let col_expr = if let Some((table, idx, _dt)) = get_column_info(&lookup_key) {
                    let table_name = if table.is_empty() {
                        column.table.as_deref().unwrap_or("")
                    } else {
                        &table
                    };
                    AstExpr::column(table_name, &column.name, idx)
                } else {
                    column.to_ast()
                };

                // Check if value is a column reference (string that matches a column name)
                // or a Column object (check by looking for 'name' property)
                let right_expr = if let Some(s) = value.as_string() {
                    if let Some((table, idx, _dt)) = get_column_info(&s) {
                        // Value is a column reference - extract just the column name if qualified
                        let col_name = if let Some(dot_pos) = s.find('.') {
                            &s[dot_pos + 1..]
                        } else {
                            &s
                        };
                        AstExpr::column(&table, col_name, idx)
                    } else {
                        // Value is a string literal
                        let val = if let Some((_, _, dt)) = get_column_info(&lookup_key) {
                            js_to_value(value, dt).unwrap_or(Value::String(s))
                        } else {
                            Value::String(s)
                        };
                        AstExpr::literal(val)
                    }
                } else if value.is_object() {
                    // Check if it's a Column object by looking for 'name' property
                    if let Ok(name_val) = js_sys::Reflect::get(value, &JsValue::from_str("name")) {
                        if let Some(col_name) = name_val.as_string() {
                            // Get table name if present
                            let table_name = js_sys::Reflect::get(value, &JsValue::from_str("tableName"))
                                .ok()
                                .and_then(|v| v.as_string());

                            // Build lookup key with table prefix if present
                            let col_lookup = if let Some(ref tbl) = table_name {
                                alloc::format!("{}.{}", tbl, col_name)
                            } else {
                                col_name.clone()
                            };

                            if let Some((table, idx, _dt)) = get_column_info(&col_lookup) {
                                AstExpr::column(&table, &col_name, idx)
                            } else {
                                // Fallback: use the column's own info
                                AstExpr::column(table_name.as_deref().unwrap_or(""), &col_name, 0)
                            }
                        } else {
                            // Not a Column object, treat as literal
                            AstExpr::literal(Value::Null)
                        }
                    } else {
                        // Not a Column object, treat as literal
                        AstExpr::literal(Value::Null)
                    }
                } else {
                    let val = if let Some((_, _, dt)) = get_column_info(&lookup_key) {
                        js_to_value(value, dt).unwrap_or(Value::Null)
                    } else {
                        // Try to infer type
                        if let Some(n) = value.as_f64() {
                            if n.fract() == 0.0 {
                                Value::Int64(n as i64)
                            } else {
                                Value::Float64(n)
                            }
                        } else if let Some(b) = value.as_bool() {
                            Value::Boolean(b)
                        } else {
                            Value::Null
                        }
                    };
                    AstExpr::literal(val)
                };

                match op {
                    ComparisonOp::Eq => AstExpr::eq(col_expr, right_expr),
                    ComparisonOp::Ne => AstExpr::ne(col_expr, right_expr),
                    ComparisonOp::Gt => AstExpr::gt(col_expr, right_expr),
                    ComparisonOp::Gte => AstExpr::gte(col_expr, right_expr),
                    ComparisonOp::Lt => AstExpr::lt(col_expr, right_expr),
                    ComparisonOp::Lte => AstExpr::lte(col_expr, right_expr),
                }
            }
            ExprInner::Between { column, low, high } => {
                let (table, idx, dt) = get_column_info(&column.name).unwrap_or((String::new(), 0, DataType::Float64));
                let table_name = if table.is_empty() {
                    column.table.as_deref().unwrap_or("")
                } else {
                    &table
                };
                let col_expr = AstExpr::column(table_name, &column.name, idx);
                let low_val = js_to_value(low, dt).unwrap_or(Value::Null);
                let high_val = js_to_value(high, dt).unwrap_or(Value::Null);
                AstExpr::between(col_expr, AstExpr::literal(low_val), AstExpr::literal(high_val))
            }
            ExprInner::NotBetween { column, low, high } => {
                let (_, idx, dt) = get_column_info(&column.name).unwrap_or((String::new(), 0, DataType::Float64));
                let col_expr = AstExpr::column(column.table.as_deref().unwrap_or(""), &column.name, idx);
                let low_val = js_to_value(low, dt).unwrap_or(Value::Null);
                let high_val = js_to_value(high, dt).unwrap_or(Value::Null);
                AstExpr::not_between(col_expr, AstExpr::literal(low_val), AstExpr::literal(high_val))
            }
            ExprInner::InList { column, values } => {
                let (_, idx, dt) = get_column_info(&column.name).unwrap_or((String::new(), 0, DataType::String));
                let col_expr = AstExpr::column(column.table.as_deref().unwrap_or(""), &column.name, idx);

                let arr = js_sys::Array::from(values);
                let vals: Vec<Value> = arr
                    .iter()
                    .filter_map(|v| js_to_value(&v, dt).ok())
                    .collect();

                AstExpr::in_list(col_expr, vals)
            }
            ExprInner::NotInList { column, values } => {
                let (_, idx, dt) = get_column_info(&column.name).unwrap_or((String::new(), 0, DataType::String));
                let col_expr = AstExpr::column(column.table.as_deref().unwrap_or(""), &column.name, idx);

                let arr = js_sys::Array::from(values);
                let vals: Vec<Value> = arr
                    .iter()
                    .filter_map(|v| js_to_value(&v, dt).ok())
                    .collect();

                AstExpr::not_in_list(col_expr, vals)
            }
            ExprInner::Like { column, pattern } => {
                let idx = get_column_info(&column.name).map(|(_, i, _)| i).unwrap_or(0);
                let col_expr = AstExpr::column(column.table.as_deref().unwrap_or(""), &column.name, idx);
                AstExpr::like(col_expr, pattern)
            }
            ExprInner::NotLike { column, pattern } => {
                let idx = get_column_info(&column.name).map(|(_, i, _)| i).unwrap_or(0);
                let col_expr = AstExpr::column(column.table.as_deref().unwrap_or(""), &column.name, idx);
                AstExpr::not_like(col_expr, pattern)
            }
            ExprInner::Match { column, pattern } => {
                let idx = get_column_info(&column.name).map(|(_, i, _)| i).unwrap_or(0);
                let col_expr = AstExpr::column(column.table.as_deref().unwrap_or(""), &column.name, idx);
                AstExpr::regex_match(col_expr, pattern)
            }
            ExprInner::NotMatch { column, pattern } => {
                let idx = get_column_info(&column.name).map(|(_, i, _)| i).unwrap_or(0);
                let col_expr = AstExpr::column(column.table.as_deref().unwrap_or(""), &column.name, idx);
                AstExpr::not_regex_match(col_expr, pattern)
            }
            ExprInner::IsNull { column } => {
                let idx = get_column_info(&column.name).map(|(_, i, _)| i).unwrap_or(0);
                let col_expr = AstExpr::column(column.table.as_deref().unwrap_or(""), &column.name, idx);
                AstExpr::is_null(col_expr)
            }
            ExprInner::IsNotNull { column } => {
                let idx = get_column_info(&column.name).map(|(_, i, _)| i).unwrap_or(0);
                let col_expr = AstExpr::column(column.table.as_deref().unwrap_or(""), &column.name, idx);
                AstExpr::is_not_null(col_expr)
            }
            ExprInner::JsonbEq { column, path, value } => {
                // JSONB path equality - use get_column_info to get correct index
                let col_expr = if let Some((_, idx, _)) = get_column_info(&column.name) {
                    AstExpr::column(column.table.as_deref().unwrap_or(""), &column.name, idx)
                } else {
                    column.to_ast()
                };
                let val = if let Some(s) = value.as_string() {
                    Value::String(s)
                } else if let Some(n) = value.as_f64() {
                    Value::Float64(n)
                } else {
                    Value::Null
                };
                AstExpr::jsonb_path_eq(col_expr, path, val)
            }
            ExprInner::JsonbContains { column, path, value: _ } => {
                let col_expr = if let Some((_, idx, _)) = get_column_info(&column.name) {
                    AstExpr::column(column.table.as_deref().unwrap_or(""), &column.name, idx)
                } else {
                    column.to_ast()
                };
                AstExpr::jsonb_contains(col_expr, path)
            }
            ExprInner::JsonbExists { column, path } => {
                let col_expr = if let Some((_, idx, _)) = get_column_info(&column.name) {
                    AstExpr::column(column.table.as_deref().unwrap_or(""), &column.name, idx)
                } else {
                    column.to_ast()
                };
                AstExpr::jsonb_exists(col_expr, path)
            }
            ExprInner::And { left, right } => {
                let left_ast = left.to_ast_with_table(get_column_info);
                let right_ast = right.to_ast_with_table(get_column_info);
                AstExpr::and(left_ast, right_ast)
            }
            ExprInner::Or { left, right } => {
                let left_ast = left.to_ast_with_table(get_column_info);
                let right_ast = right.to_ast_with_table(get_column_info);
                AstExpr::or(left_ast, right_ast)
            }
            ExprInner::Not { inner } => {
                let inner_ast = inner.to_ast_with_table(get_column_info);
                AstExpr::not(inner_ast)
            }
            ExprInner::ColumnRef { column } => column.to_ast(),
            ExprInner::Literal { value } => {
                let val = if let Some(n) = value.as_f64() {
                    if n.fract() == 0.0 {
                        Value::Int64(n as i64)
                    } else {
                        Value::Float64(n)
                    }
                } else if let Some(s) = value.as_string() {
                    Value::String(s)
                } else if let Some(b) = value.as_bool() {
                    Value::Boolean(b)
                } else {
                    Value::Null
                };
                AstExpr::literal(val)
            }
            ExprInner::True => AstExpr::literal(Value::Boolean(true)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use wasm_bindgen_test::*;

    wasm_bindgen_test_configure!(run_in_browser);

    #[wasm_bindgen_test]
    fn test_column_eq() {
        let col = Column::new_simple("age");
        let expr = col.eq(&JsValue::from_f64(25.0));

        match &expr.inner {
            ExprInner::Comparison { column, op, .. } => {
                assert_eq!(column.name, "age");
                assert_eq!(*op, ComparisonOp::Eq);
            }
            _ => panic!("Expected Comparison"),
        }
    }

    #[wasm_bindgen_test]
    fn test_column_gt() {
        let col = Column::new_simple("age");
        let expr = col.gt(&JsValue::from_f64(18.0));

        match &expr.inner {
            ExprInner::Comparison { op, .. } => {
                assert_eq!(*op, ComparisonOp::Gt);
            }
            _ => panic!("Expected Comparison"),
        }
    }

    #[wasm_bindgen_test]
    fn test_expr_and() {
        let col = Column::new_simple("age");
        let expr1 = col.gt(&JsValue::from_f64(18.0));
        let expr2 = col.lt(&JsValue::from_f64(65.0));
        let combined = expr1.and(&expr2);

        match &combined.inner {
            ExprInner::And { .. } => {}
            _ => panic!("Expected And"),
        }
    }

    #[wasm_bindgen_test]
    fn test_expr_or() {
        let col = Column::new_simple("status");
        let expr1 = col.eq(&JsValue::from_str("active"));
        let expr2 = col.eq(&JsValue::from_str("pending"));
        let combined = expr1.or(&expr2);

        match &combined.inner {
            ExprInner::Or { .. } => {}
            _ => panic!("Expected Or"),
        }
    }

    #[wasm_bindgen_test]
    fn test_expr_not() {
        let col = Column::new_simple("deleted");
        let expr = col.eq(&JsValue::from_bool(true)).not();

        match &expr.inner {
            ExprInner::Not { .. } => {}
            _ => panic!("Expected Not"),
        }
    }

    #[wasm_bindgen_test]
    fn test_column_is_null() {
        let col = Column::new_simple("email");
        let expr = col.is_null();

        match &expr.inner {
            ExprInner::IsNull { column } => {
                assert_eq!(column.name, "email");
            }
            _ => panic!("Expected IsNull"),
        }
    }

    #[wasm_bindgen_test]
    fn test_column_between() {
        let col = Column::new_simple("age");
        let expr = col.between(&JsValue::from_f64(18.0), &JsValue::from_f64(65.0));

        match &expr.inner {
            ExprInner::Between { column, .. } => {
                assert_eq!(column.name, "age");
            }
            _ => panic!("Expected Between"),
        }
    }

    #[wasm_bindgen_test]
    fn test_column_like() {
        let col = Column::new_simple("name");
        let expr = col.like("Alice%");

        match &expr.inner {
            ExprInner::Like { column, pattern } => {
                assert_eq!(column.name, "name");
                assert_eq!(pattern, "Alice%");
            }
            _ => panic!("Expected Like"),
        }
    }
}
