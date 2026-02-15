//! Value type definitions for Cynos database.
//!
//! This module defines the `Value` enum which represents any value that can be stored
//! in a database cell.

use crate::types::DataType;
use alloc::string::{String, ToString};
use alloc::vec::Vec;
use core::cmp::Ordering;
use core::hash::{Hash, Hasher};

/// Placeholder for JSONB values. Will be implemented by the jsonb crate.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JsonbValue(pub Vec<u8>);

impl Hash for JsonbValue {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.hash(state);
    }
}

impl JsonbValue {
    pub fn new(data: Vec<u8>) -> Self {
        Self(data)
    }
}

/// A value that can be stored in a database cell.
#[derive(Clone, Debug)]
pub enum Value {
    /// Null value
    Null,
    /// Boolean value
    Boolean(bool),
    /// 32-bit signed integer
    Int32(i32),
    /// 64-bit signed integer
    Int64(i64),
    /// 64-bit floating point
    Float64(f64),
    /// UTF-8 string
    String(String),
    /// DateTime stored as Unix timestamp in milliseconds
    DateTime(i64),
    /// Binary data
    Bytes(Vec<u8>),
    /// JSONB structured data
    Jsonb(JsonbValue),
}

impl Value {
    /// Returns the data type of this value, or None if it's Null.
    pub fn data_type(&self) -> Option<DataType> {
        match self {
            Value::Null => None,
            Value::Boolean(_) => Some(DataType::Boolean),
            Value::Int32(_) => Some(DataType::Int32),
            Value::Int64(_) => Some(DataType::Int64),
            Value::Float64(_) => Some(DataType::Float64),
            Value::String(_) => Some(DataType::String),
            Value::DateTime(_) => Some(DataType::DateTime),
            Value::Bytes(_) => Some(DataType::Bytes),
            Value::Jsonb(_) => Some(DataType::Jsonb),
        }
    }

    /// Returns true if this value is Null.
    #[inline]
    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    /// Returns the boolean value if this is a Boolean, None otherwise.
    pub fn as_bool(&self) -> Option<bool> {
        match self {
            Value::Boolean(v) => Some(*v),
            _ => None,
        }
    }

    /// Returns the i32 value if this is an Int32, None otherwise.
    pub fn as_i32(&self) -> Option<i32> {
        match self {
            Value::Int32(v) => Some(*v),
            _ => None,
        }
    }

    /// Returns the i64 value if this is an Int64, None otherwise.
    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Value::Int64(v) => Some(*v),
            _ => None,
        }
    }

    /// Returns the f64 value if this is a Float64, None otherwise.
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Value::Float64(v) => Some(*v),
            _ => None,
        }
    }

    /// Returns a reference to the string if this is a String, None otherwise.
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Value::String(v) => Some(v.as_str()),
            _ => None,
        }
    }

    /// Returns the datetime timestamp if this is a DateTime, None otherwise.
    pub fn as_datetime(&self) -> Option<i64> {
        match self {
            Value::DateTime(v) => Some(*v),
            _ => None,
        }
    }

    /// Returns a reference to the bytes if this is Bytes, None otherwise.
    pub fn as_bytes(&self) -> Option<&[u8]> {
        match self {
            Value::Bytes(v) => Some(v.as_slice()),
            _ => None,
        }
    }

    /// Creates a default value for the given data type.
    pub fn default_for_type(dt: DataType) -> Self {
        match dt {
            DataType::Boolean => Value::Boolean(false),
            DataType::Int32 => Value::Int32(0),
            DataType::Int64 => Value::Int64(0),
            DataType::Float64 => Value::Float64(0.0),
            DataType::String => Value::String(String::new()),
            DataType::DateTime => Value::DateTime(0),
            DataType::Bytes => Value::Null,
            DataType::Jsonb => Value::Null,
        }
    }
}

impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Value::Null, Value::Null) => true,
            (Value::Boolean(a), Value::Boolean(b)) => a == b,
            (Value::Int32(a), Value::Int32(b)) => a == b,
            (Value::Int64(a), Value::Int64(b)) => a == b,
            (Value::Float64(a), Value::Float64(b)) => {
                // Handle NaN comparison
                if a.is_nan() && b.is_nan() {
                    true
                } else {
                    a == b
                }
            }
            (Value::String(a), Value::String(b)) => a == b,
            (Value::DateTime(a), Value::DateTime(b)) => a == b,
            (Value::Bytes(a), Value::Bytes(b)) => a == b,
            (Value::Jsonb(a), Value::Jsonb(b)) => a == b,
            _ => false,
        }
    }
}

impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Eq for Value {}

impl Hash for Value {
    fn hash<H: Hasher>(&self, state: &mut H) {
        core::mem::discriminant(self).hash(state);
        match self {
            Value::Null => {}
            Value::Boolean(b) => b.hash(state),
            Value::Int32(i) => i.hash(state),
            Value::Int64(i) => i.hash(state),
            Value::Float64(f) => f.to_bits().hash(state),
            Value::String(s) => s.hash(state),
            Value::DateTime(d) => d.hash(state),
            Value::Bytes(b) => b.hash(state),
            Value::Jsonb(j) => j.hash(state),
        }
    }
}

impl Ord for Value {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            (Value::Null, Value::Null) => Ordering::Equal,
            (Value::Null, _) => Ordering::Less,
            (_, Value::Null) => Ordering::Greater,
            (Value::Boolean(a), Value::Boolean(b)) => a.cmp(b),
            (Value::Int32(a), Value::Int32(b)) => a.cmp(b),
            (Value::Int64(a), Value::Int64(b)) => a.cmp(b),
            // Cross-type numeric comparisons
            (Value::Int32(a), Value::Int64(b)) => (*a as i64).cmp(b),
            (Value::Int64(a), Value::Int32(b)) => a.cmp(&(*b as i64)),
            (Value::Int32(a), Value::Float64(b)) => {
                let a_f64 = *a as f64;
                if b.is_nan() {
                    Ordering::Less
                } else {
                    a_f64.partial_cmp(b).unwrap_or(Ordering::Equal)
                }
            }
            (Value::Float64(a), Value::Int32(b)) => {
                let b_f64 = *b as f64;
                if a.is_nan() {
                    Ordering::Greater
                } else {
                    a.partial_cmp(&b_f64).unwrap_or(Ordering::Equal)
                }
            }
            (Value::Int64(a), Value::Float64(b)) => {
                let a_f64 = *a as f64;
                if b.is_nan() {
                    Ordering::Less
                } else {
                    a_f64.partial_cmp(b).unwrap_or(Ordering::Equal)
                }
            }
            (Value::Float64(a), Value::Int64(b)) => {
                let b_f64 = *b as f64;
                if a.is_nan() {
                    Ordering::Greater
                } else {
                    a.partial_cmp(&b_f64).unwrap_or(Ordering::Equal)
                }
            }
            (Value::Float64(a), Value::Float64(b)) => {
                // Handle NaN: treat NaN as greater than all other values
                match (a.is_nan(), b.is_nan()) {
                    (true, true) => Ordering::Equal,
                    (true, false) => Ordering::Greater,
                    (false, true) => Ordering::Less,
                    (false, false) => a.partial_cmp(b).unwrap_or(Ordering::Equal),
                }
            }
            (Value::String(a), Value::String(b)) => a.cmp(b),
            (Value::DateTime(a), Value::DateTime(b)) => a.cmp(b),
            (Value::Bytes(a), Value::Bytes(b)) => a.cmp(b),
            (Value::Jsonb(a), Value::Jsonb(b)) => a.0.cmp(&b.0),
            // Different types: order by type discriminant
            _ => self.type_order().cmp(&other.type_order()),
        }
    }
}

impl Value {
    /// Returns a type ordering value for comparing different types.
    fn type_order(&self) -> u8 {
        match self {
            Value::Null => 0,
            Value::Boolean(_) => 1,
            Value::Int32(_) => 2,
            Value::Int64(_) => 3,
            Value::Float64(_) => 4,
            Value::String(_) => 5,
            Value::DateTime(_) => 6,
            Value::Bytes(_) => 7,
            Value::Jsonb(_) => 8,
        }
    }
}

impl From<bool> for Value {
    fn from(v: bool) -> Self {
        Value::Boolean(v)
    }
}

impl From<i32> for Value {
    fn from(v: i32) -> Self {
        Value::Int32(v)
    }
}

impl From<i64> for Value {
    fn from(v: i64) -> Self {
        Value::Int64(v)
    }
}

impl From<f64> for Value {
    fn from(v: f64) -> Self {
        Value::Float64(v)
    }
}

impl From<String> for Value {
    fn from(v: String) -> Self {
        Value::String(v)
    }
}

impl From<&str> for Value {
    fn from(v: &str) -> Self {
        Value::String(v.to_string())
    }
}

impl From<Vec<u8>> for Value {
    fn from(v: Vec<u8>) -> Self {
        Value::Bytes(v)
    }
}

impl<T> From<Option<T>> for Value
where
    T: Into<Value>,
{
    fn from(v: Option<T>) -> Self {
        match v {
            Some(val) => val.into(),
            None => Value::Null,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloc::vec;

    #[test]
    fn test_value_type_check() {
        let v = Value::Int64(42);
        assert_eq!(v.data_type(), Some(DataType::Int64));
    }

    #[test]
    fn test_value_null() {
        let v = Value::Null;
        assert_eq!(v.data_type(), None);
        assert!(v.is_null());
    }

    #[test]
    fn test_value_accessors() {
        assert_eq!(Value::Boolean(true).as_bool(), Some(true));
        assert_eq!(Value::Int32(42).as_i32(), Some(42));
        assert_eq!(Value::Int64(100).as_i64(), Some(100));
        assert_eq!(Value::Float64(3.14).as_f64(), Some(3.14));
        assert_eq!(Value::String("hello".into()).as_str(), Some("hello"));
        assert_eq!(Value::DateTime(1234567890).as_datetime(), Some(1234567890));
        assert_eq!(Value::Bytes(vec![1, 2, 3]).as_bytes(), Some(&[1, 2, 3][..]));
    }

    #[test]
    fn test_value_equality() {
        assert_eq!(Value::Int32(42), Value::Int32(42));
        assert_ne!(Value::Int32(42), Value::Int64(42));
        assert_eq!(Value::Null, Value::Null);
        assert_eq!(Value::String("test".into()), Value::String("test".into()));
    }

    #[test]
    fn test_value_ordering() {
        assert!(Value::Int32(1) < Value::Int32(2));
        assert!(Value::String("a".into()) < Value::String("b".into()));
        assert!(Value::Null < Value::Int32(0));
    }

    #[test]
    fn test_value_from_impls() {
        let v: Value = 42i32.into();
        assert_eq!(v.as_i32(), Some(42));

        let v: Value = "hello".into();
        assert_eq!(v.as_str(), Some("hello"));

        let v: Value = Some(100i64).into();
        assert_eq!(v.as_i64(), Some(100));

        let v: Value = None::<i32>.into();
        assert!(v.is_null());
    }

    #[test]
    fn test_default_for_type() {
        assert_eq!(Value::default_for_type(DataType::Boolean), Value::Boolean(false));
        assert_eq!(Value::default_for_type(DataType::Int32), Value::Int32(0));
        assert_eq!(Value::default_for_type(DataType::String), Value::String(String::new()));
    }
}
