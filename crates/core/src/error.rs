//! Error types for Cynos database.

use crate::types::DataType;
use crate::value::Value;
use alloc::string::String;
use core::fmt;

/// Result type alias for Cynos operations.
pub type Result<T> = core::result::Result<T, Error>;

/// Error types for Cynos database operations.
#[derive(Debug)]
pub enum Error {
    /// Type mismatch error.
    TypeMismatch {
        expected: DataType,
        got: DataType,
    },
    /// Null constraint violation.
    NullConstraint {
        column: String,
    },
    /// Unique constraint violation.
    UniqueConstraint {
        column: String,
        value: Value,
    },
    /// Row or record not found.
    NotFound {
        table: String,
        key: Value,
    },
    /// Invalid schema definition.
    InvalidSchema {
        message: String,
    },
    /// Column not found.
    ColumnNotFound {
        table: String,
        column: String,
    },
    /// Table not found.
    TableNotFound {
        name: String,
    },
    /// Index not found.
    IndexNotFound {
        table: String,
        index: String,
    },
    /// Foreign key constraint violation.
    ForeignKeyViolation {
        constraint: String,
        message: String,
    },
    /// Invalid operation.
    InvalidOperation {
        message: String,
    },
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::TypeMismatch { expected, got } => {
                write!(f, "Type mismatch: expected {:?}, got {:?}", expected, got)
            }
            Error::NullConstraint { column } => {
                write!(f, "Null constraint violation on column: {}", column)
            }
            Error::UniqueConstraint { column, value } => {
                write!(
                    f,
                    "Unique constraint violation on column {}: {:?}",
                    column, value
                )
            }
            Error::NotFound { table, key } => {
                write!(f, "Not found in table {}: {:?}", table, key)
            }
            Error::InvalidSchema { message } => {
                write!(f, "Invalid schema: {}", message)
            }
            Error::ColumnNotFound { table, column } => {
                write!(f, "Column {} not found in table {}", column, table)
            }
            Error::TableNotFound { name } => {
                write!(f, "Table not found: {}", name)
            }
            Error::IndexNotFound { table, index } => {
                write!(f, "Index {} not found in table {}", index, table)
            }
            Error::ForeignKeyViolation { constraint, message } => {
                write!(f, "Foreign key violation ({}): {}", constraint, message)
            }
            Error::InvalidOperation { message } => {
                write!(f, "Invalid operation: {}", message)
            }
        }
    }
}

impl Error {
    /// Creates a type mismatch error.
    pub fn type_mismatch(expected: DataType, got: DataType) -> Self {
        Error::TypeMismatch { expected, got }
    }

    /// Creates a null constraint error.
    pub fn null_constraint(column: impl Into<String>) -> Self {
        Error::NullConstraint {
            column: column.into(),
        }
    }

    /// Creates a unique constraint error.
    pub fn unique_constraint(column: impl Into<String>, value: Value) -> Self {
        Error::UniqueConstraint {
            column: column.into(),
            value,
        }
    }

    /// Creates a not found error.
    pub fn not_found(table: impl Into<String>, key: Value) -> Self {
        Error::NotFound {
            table: table.into(),
            key,
        }
    }

    /// Creates an invalid schema error.
    pub fn invalid_schema(message: impl Into<String>) -> Self {
        Error::InvalidSchema {
            message: message.into(),
        }
    }

    /// Creates a column not found error.
    pub fn column_not_found(table: impl Into<String>, column: impl Into<String>) -> Self {
        Error::ColumnNotFound {
            table: table.into(),
            column: column.into(),
        }
    }

    /// Creates a table not found error.
    pub fn table_not_found(name: impl Into<String>) -> Self {
        Error::TableNotFound { name: name.into() }
    }

    /// Creates an invalid operation error.
    pub fn invalid_operation(message: impl Into<String>) -> Self {
        Error::InvalidOperation {
            message: message.into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloc::string::ToString;

    #[test]
    fn test_error_display() {
        let err = Error::type_mismatch(DataType::Int32, DataType::String);
        assert!(err.to_string().contains("Type mismatch"));

        let err = Error::null_constraint("name");
        assert!(err.to_string().contains("name"));

        let err = Error::table_not_found("users");
        assert!(err.to_string().contains("users"));
    }

    #[test]
    fn test_error_constructors() {
        let err = Error::unique_constraint("email", Value::String("test@example.com".into()));
        match err {
            Error::UniqueConstraint { column, .. } => assert_eq!(column, "email"),
            _ => panic!("Wrong error type"),
        }
    }
}
