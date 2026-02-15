//! Data type definitions for Cynos database.
//!
//! This module defines the supported data types that can be stored in the database.

/// Supported data types in Cynos database.
///
/// Maps to the original Lovefield `lf.Type` enum with additions for JSONB support.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum DataType {
    /// Boolean type (true/false)
    Boolean,
    /// 32-bit signed integer
    Int32,
    /// 64-bit signed integer
    Int64,
    /// 64-bit floating point number
    Float64,
    /// UTF-8 string
    String,
    /// Date and time stored as Unix timestamp (milliseconds)
    DateTime,
    /// Binary data
    Bytes,
    /// JSONB type for structured data
    Jsonb,
}

impl DataType {
    /// Returns the default value for this data type as a string representation.
    pub fn default_value_repr(&self) -> &'static str {
        match self {
            DataType::Boolean => "false",
            DataType::Int32 => "0",
            DataType::Int64 => "0",
            DataType::Float64 => "0.0",
            DataType::String => "\"\"",
            DataType::DateTime => "0",
            DataType::Bytes => "[]",
            DataType::Jsonb => "null",
        }
    }

    /// Returns whether this type is nullable by default.
    pub fn is_nullable_by_default(&self) -> bool {
        matches!(self, DataType::Bytes | DataType::Jsonb)
    }

    /// Returns whether this type can be used as an index key.
    pub fn is_indexable(&self) -> bool {
        !matches!(self, DataType::Bytes | DataType::Jsonb)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_data_type_equality() {
        assert_eq!(DataType::Int32, DataType::Int32);
        assert_ne!(DataType::Int32, DataType::Int64);
    }

    #[test]
    fn test_data_type_clone() {
        let dt = DataType::String;
        let dt_clone = dt;
        assert_eq!(dt, dt_clone);
    }

    #[test]
    fn test_nullable_by_default() {
        assert!(!DataType::Boolean.is_nullable_by_default());
        assert!(!DataType::Int32.is_nullable_by_default());
        assert!(!DataType::String.is_nullable_by_default());
        assert!(DataType::Bytes.is_nullable_by_default());
        assert!(DataType::Jsonb.is_nullable_by_default());
    }

    #[test]
    fn test_indexable() {
        assert!(DataType::Boolean.is_indexable());
        assert!(DataType::Int32.is_indexable());
        assert!(DataType::Int64.is_indexable());
        assert!(DataType::Float64.is_indexable());
        assert!(DataType::String.is_indexable());
        assert!(DataType::DateTime.is_indexable());
        assert!(!DataType::Bytes.is_indexable());
        assert!(!DataType::Jsonb.is_indexable());
    }
}
