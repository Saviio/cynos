//! JsonbValue type definitions for Cynos database.
//!
//! This module defines the `JsonbValue` enum which represents JSON values
//! with optimized storage and query capabilities.

use alloc::string::{String, ToString};
use alloc::vec::Vec;
use core::cmp::Ordering;

/// A JSON value with optimized storage for database operations.
#[derive(Clone, Debug)]
pub enum JsonbValue {
    /// JSON null
    Null,
    /// JSON boolean
    Bool(bool),
    /// JSON number (stored as f64)
    Number(f64),
    /// JSON string
    String(String),
    /// JSON array
    Array(Vec<JsonbValue>),
    /// JSON object with sorted keys for O(log n) lookup
    Object(JsonbObject),
}

/// A JSON object with keys sorted for efficient lookup.
#[derive(Clone, Debug, Default)]
pub struct JsonbObject {
    /// Entries stored sorted by key for binary search
    entries: Vec<(String, JsonbValue)>,
}

impl JsonbObject {
    /// Creates a new empty JsonbObject.
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }

    /// Creates a JsonbObject with pre-allocated capacity.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            entries: Vec::with_capacity(capacity),
        }
    }

    /// Returns the number of entries.
    #[inline]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Returns true if the object is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Gets a value by key using binary search. O(log n)
    pub fn get(&self, key: &str) -> Option<&JsonbValue> {
        self.entries
            .binary_search_by(|(k, _)| k.as_str().cmp(key))
            .ok()
            .map(|idx| &self.entries[idx].1)
    }

    /// Gets a mutable value by key using binary search. O(log n)
    pub fn get_mut(&mut self, key: &str) -> Option<&mut JsonbValue> {
        self.entries
            .binary_search_by(|(k, _)| k.as_str().cmp(key))
            .ok()
            .map(|idx| &mut self.entries[idx].1)
    }

    /// Inserts a key-value pair, maintaining sorted order.
    pub fn insert(&mut self, key: String, value: JsonbValue) {
        match self.entries.binary_search_by(|(k, _)| k.as_str().cmp(&key)) {
            Ok(idx) => {
                self.entries[idx].1 = value;
            }
            Err(idx) => {
                self.entries.insert(idx, (key, value));
            }
        }
    }

    /// Removes a key and returns its value if present.
    pub fn remove(&mut self, key: &str) -> Option<JsonbValue> {
        self.entries
            .binary_search_by(|(k, _)| k.as_str().cmp(key))
            .ok()
            .map(|idx| self.entries.remove(idx).1)
    }

    /// Returns true if the object contains the given key.
    pub fn contains_key(&self, key: &str) -> bool {
        self.entries
            .binary_search_by(|(k, _)| k.as_str().cmp(key))
            .is_ok()
    }

    /// Returns an iterator over the keys.
    pub fn keys(&self) -> impl Iterator<Item = &str> {
        self.entries.iter().map(|(k, _)| k.as_str())
    }

    /// Returns an iterator over the values.
    pub fn values(&self) -> impl Iterator<Item = &JsonbValue> {
        self.entries.iter().map(|(_, v)| v)
    }

    /// Returns an iterator over key-value pairs.
    pub fn iter(&self) -> impl Iterator<Item = (&str, &JsonbValue)> {
        self.entries.iter().map(|(k, v)| (k.as_str(), v))
    }

    /// Returns a mutable iterator over key-value pairs.
    pub fn iter_mut(&mut self) -> impl Iterator<Item = (&str, &mut JsonbValue)> {
        self.entries.iter_mut().map(|(k, v)| (k.as_str(), v))
    }
}

impl PartialEq for JsonbObject {
    fn eq(&self, other: &Self) -> bool {
        if self.entries.len() != other.entries.len() {
            return false;
        }
        self.entries
            .iter()
            .zip(other.entries.iter())
            .all(|((k1, v1), (k2, v2))| k1 == k2 && v1 == v2)
    }
}

impl JsonbValue {
    /// Returns true if this is a null value.
    #[inline]
    pub fn is_null(&self) -> bool {
        matches!(self, JsonbValue::Null)
    }

    /// Returns true if this is a boolean value.
    #[inline]
    pub fn is_bool(&self) -> bool {
        matches!(self, JsonbValue::Bool(_))
    }

    /// Returns true if this is a number value.
    #[inline]
    pub fn is_number(&self) -> bool {
        matches!(self, JsonbValue::Number(_))
    }

    /// Returns true if this is a string value.
    #[inline]
    pub fn is_string(&self) -> bool {
        matches!(self, JsonbValue::String(_))
    }

    /// Returns true if this is an array value.
    #[inline]
    pub fn is_array(&self) -> bool {
        matches!(self, JsonbValue::Array(_))
    }

    /// Returns true if this is an object value.
    #[inline]
    pub fn is_object(&self) -> bool {
        matches!(self, JsonbValue::Object(_))
    }

    /// Returns the boolean value if this is a Bool.
    pub fn as_bool(&self) -> Option<bool> {
        match self {
            JsonbValue::Bool(b) => Some(*b),
            _ => None,
        }
    }

    /// Returns the number value if this is a Number.
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            JsonbValue::Number(n) => Some(*n),
            _ => None,
        }
    }

    /// Returns the number as i64 if this is a Number and it's an integer.
    pub fn as_i64(&self) -> Option<i64> {
        match self {
            JsonbValue::Number(n) => {
                let i = *n as i64;
                if (i as f64) == *n {
                    Some(i)
                } else {
                    None
                }
            }
            _ => None,
        }
    }

    /// Returns a reference to the string if this is a String.
    pub fn as_str(&self) -> Option<&str> {
        match self {
            JsonbValue::String(s) => Some(s.as_str()),
            _ => None,
        }
    }

    /// Returns a reference to the array if this is an Array.
    pub fn as_array(&self) -> Option<&Vec<JsonbValue>> {
        match self {
            JsonbValue::Array(arr) => Some(arr),
            _ => None,
        }
    }

    /// Returns a mutable reference to the array if this is an Array.
    pub fn as_array_mut(&mut self) -> Option<&mut Vec<JsonbValue>> {
        match self {
            JsonbValue::Array(arr) => Some(arr),
            _ => None,
        }
    }

    /// Returns a reference to the object if this is an Object.
    pub fn as_object(&self) -> Option<&JsonbObject> {
        match self {
            JsonbValue::Object(obj) => Some(obj),
            _ => None,
        }
    }

    /// Returns a mutable reference to the object if this is an Object.
    pub fn as_object_mut(&mut self) -> Option<&mut JsonbObject> {
        match self {
            JsonbValue::Object(obj) => Some(obj),
            _ => None,
        }
    }

    /// Gets a value by key if this is an Object.
    pub fn get(&self, key: &str) -> Option<&JsonbValue> {
        self.as_object().and_then(|obj| obj.get(key))
    }

    /// Gets a value by index if this is an Array.
    pub fn get_index(&self, index: usize) -> Option<&JsonbValue> {
        self.as_array().and_then(|arr| arr.get(index))
    }
}

impl PartialEq for JsonbValue {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (JsonbValue::Null, JsonbValue::Null) => true,
            (JsonbValue::Bool(a), JsonbValue::Bool(b)) => a == b,
            (JsonbValue::Number(a), JsonbValue::Number(b)) => {
                if a.is_nan() && b.is_nan() {
                    true
                } else {
                    a == b
                }
            }
            (JsonbValue::String(a), JsonbValue::String(b)) => a == b,
            (JsonbValue::Array(a), JsonbValue::Array(b)) => a == b,
            (JsonbValue::Object(a), JsonbValue::Object(b)) => a == b,
            _ => false,
        }
    }
}

impl PartialOrd for JsonbValue {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match (self, other) {
            (JsonbValue::Null, JsonbValue::Null) => Some(Ordering::Equal),
            (JsonbValue::Null, _) => Some(Ordering::Less),
            (_, JsonbValue::Null) => Some(Ordering::Greater),
            (JsonbValue::Bool(a), JsonbValue::Bool(b)) => a.partial_cmp(b),
            (JsonbValue::Number(a), JsonbValue::Number(b)) => a.partial_cmp(b),
            (JsonbValue::String(a), JsonbValue::String(b)) => a.partial_cmp(b),
            _ => None,
        }
    }
}

// From implementations for convenient construction
impl From<bool> for JsonbValue {
    fn from(v: bool) -> Self {
        JsonbValue::Bool(v)
    }
}

impl From<i32> for JsonbValue {
    fn from(v: i32) -> Self {
        JsonbValue::Number(v as f64)
    }
}

impl From<i64> for JsonbValue {
    fn from(v: i64) -> Self {
        JsonbValue::Number(v as f64)
    }
}

impl From<f64> for JsonbValue {
    fn from(v: f64) -> Self {
        JsonbValue::Number(v)
    }
}

impl From<String> for JsonbValue {
    fn from(v: String) -> Self {
        JsonbValue::String(v)
    }
}

impl From<&str> for JsonbValue {
    fn from(v: &str) -> Self {
        JsonbValue::String(v.to_string())
    }
}

impl From<Vec<JsonbValue>> for JsonbValue {
    fn from(v: Vec<JsonbValue>) -> Self {
        JsonbValue::Array(v)
    }
}

impl From<JsonbObject> for JsonbValue {
    fn from(v: JsonbObject) -> Self {
        JsonbValue::Object(v)
    }
}

impl<T> From<Option<T>> for JsonbValue
where
    T: Into<JsonbValue>,
{
    fn from(v: Option<T>) -> Self {
        match v {
            Some(val) => val.into(),
            None => JsonbValue::Null,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloc::vec;

    #[test]
    fn test_jsonb_object_insert_and_get() {
        let mut obj = JsonbObject::new();
        obj.insert("name".into(), JsonbValue::String("Alice".into()));
        obj.insert("age".into(), JsonbValue::Number(25.0));

        assert_eq!(obj.get("name"), Some(&JsonbValue::String("Alice".into())));
        assert_eq!(obj.get("age"), Some(&JsonbValue::Number(25.0)));
        assert_eq!(obj.get("missing"), None);
    }

    #[test]
    fn test_jsonb_object_sorted_keys() {
        let mut obj = JsonbObject::new();
        obj.insert("z".into(), JsonbValue::Number(1.0));
        obj.insert("a".into(), JsonbValue::Number(2.0));
        obj.insert("m".into(), JsonbValue::Number(3.0));

        let keys: Vec<_> = obj.keys().collect();
        assert_eq!(keys, vec!["a", "m", "z"]);
    }

    #[test]
    fn test_jsonb_object_remove() {
        let mut obj = JsonbObject::new();
        obj.insert("key".into(), JsonbValue::Number(42.0));

        assert!(obj.contains_key("key"));
        let removed = obj.remove("key");
        assert_eq!(removed, Some(JsonbValue::Number(42.0)));
        assert!(!obj.contains_key("key"));
    }

    #[test]
    fn test_jsonb_value_type_checks() {
        assert!(JsonbValue::Null.is_null());
        assert!(JsonbValue::Bool(true).is_bool());
        assert!(JsonbValue::Number(42.0).is_number());
        assert!(JsonbValue::String("test".into()).is_string());
        assert!(JsonbValue::Array(vec![]).is_array());
        assert!(JsonbValue::Object(JsonbObject::new()).is_object());
    }

    #[test]
    fn test_jsonb_value_accessors() {
        assert_eq!(JsonbValue::Bool(true).as_bool(), Some(true));
        assert_eq!(JsonbValue::Number(3.14).as_f64(), Some(3.14));
        assert_eq!(JsonbValue::Number(42.0).as_i64(), Some(42));
        assert_eq!(JsonbValue::String("hello".into()).as_str(), Some("hello"));
    }

    #[test]
    fn test_jsonb_value_equality() {
        assert_eq!(JsonbValue::Null, JsonbValue::Null);
        assert_eq!(JsonbValue::Bool(true), JsonbValue::Bool(true));
        assert_eq!(JsonbValue::Number(42.0), JsonbValue::Number(42.0));
        assert_eq!(
            JsonbValue::String("test".into()),
            JsonbValue::String("test".into())
        );

        assert_ne!(JsonbValue::Bool(true), JsonbValue::Bool(false));
        assert_ne!(JsonbValue::Number(1.0), JsonbValue::Number(2.0));
    }

    #[test]
    fn test_jsonb_value_ordering() {
        assert!(JsonbValue::Null < JsonbValue::Bool(false));
        assert!(JsonbValue::Number(1.0) < JsonbValue::Number(2.0));
        assert!(JsonbValue::String("a".into()) < JsonbValue::String("b".into()));
    }

    #[test]
    fn test_jsonb_value_from_impls() {
        let v: JsonbValue = true.into();
        assert_eq!(v.as_bool(), Some(true));

        let v: JsonbValue = 42i32.into();
        assert_eq!(v.as_f64(), Some(42.0));

        let v: JsonbValue = "hello".into();
        assert_eq!(v.as_str(), Some("hello"));

        let v: JsonbValue = None::<i32>.into();
        assert!(v.is_null());
    }

    #[test]
    fn test_jsonb_nested_access() {
        let mut obj = JsonbObject::new();
        obj.insert("name".into(), JsonbValue::String("Alice".into()));

        let value = JsonbValue::Object(obj);
        assert_eq!(value.get("name"), Some(&JsonbValue::String("Alice".into())));
        assert_eq!(value.get("missing"), None);
    }

    #[test]
    fn test_jsonb_array_access() {
        let arr = JsonbValue::Array(vec![
            JsonbValue::Number(1.0),
            JsonbValue::Number(2.0),
            JsonbValue::Number(3.0),
        ]);

        assert_eq!(arr.get_index(0), Some(&JsonbValue::Number(1.0)));
        assert_eq!(arr.get_index(2), Some(&JsonbValue::Number(3.0)));
        assert_eq!(arr.get_index(10), None);
    }
}
