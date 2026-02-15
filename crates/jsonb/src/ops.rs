//! JSONB operators for database queries.
//!
//! This module implements PostgreSQL-compatible JSONB operators:
//! - `->` : Get field as JSONB
//! - `->>` : Get field as text
//! - `#>` : Get path as JSONB
//! - `@>` : Contains
//! - `<@` : Contained by
//! - `?` : Has key
//! - `?|` : Has any key
//! - `?&` : Has all keys
//! - `||` : Concatenate
//! - `-` : Delete key

use crate::value::JsonbValue;
use alloc::string::{String, ToString};
use alloc::vec::Vec;

/// JSONB operations that can be applied to values.
#[derive(Clone, Debug, PartialEq)]
pub enum JsonbOp {
    /// Get field as JSONB (->)
    GetField(String),
    /// Get field as text (->>)
    GetFieldText(String),
    /// Get path as JSONB (#>)
    GetPath(Vec<String>),
    /// Contains (@>)
    Contains(JsonbValue),
    /// Contained by (<@)
    ContainedBy(JsonbValue),
    /// Has key (?)
    HasKey(String),
    /// Has any key (?|)
    HasAnyKey(Vec<String>),
    /// Has all keys (?&)
    HasAllKeys(Vec<String>),
    /// Concatenate (||)
    Concat(JsonbValue),
    /// Delete key (-)
    DeleteKey(String),
    /// Delete keys by array
    DeleteKeys(Vec<String>),
    /// Delete by index
    DeleteIndex(usize),
}

impl JsonbValue {
    /// Applies a JSONB operation and returns the result.
    pub fn apply_op(&self, op: &JsonbOp) -> Option<JsonbValue> {
        match op {
            JsonbOp::GetField(key) => self.get(key).cloned(),
            JsonbOp::GetFieldText(key) => self.get(key).and_then(|v| match v {
                JsonbValue::String(s) => Some(JsonbValue::String(s.clone())),
                JsonbValue::Number(n) => Some(JsonbValue::String(alloc::format!("{}", n))),
                JsonbValue::Bool(b) => {
                    Some(JsonbValue::String(if *b { "true" } else { "false" }.into()))
                }
                JsonbValue::Null => Some(JsonbValue::String("null".into())),
                _ => None,
            }),
            JsonbOp::GetPath(path) => {
                let mut current = self;
                for key in path {
                    match current.get(key) {
                        Some(v) => current = v,
                        None => return None,
                    }
                }
                Some(current.clone())
            }
            JsonbOp::Contains(other) => Some(JsonbValue::Bool(self.contains(other))),
            JsonbOp::ContainedBy(other) => Some(JsonbValue::Bool(other.contains(self))),
            JsonbOp::HasKey(key) => Some(JsonbValue::Bool(self.has_key(key))),
            JsonbOp::HasAnyKey(keys) => Some(JsonbValue::Bool(self.has_any_key(keys))),
            JsonbOp::HasAllKeys(keys) => Some(JsonbValue::Bool(self.has_all_keys(keys))),
            JsonbOp::Concat(other) => Some(self.concat(other)),
            JsonbOp::DeleteKey(key) => Some(self.delete_key(key)),
            JsonbOp::DeleteKeys(keys) => Some(self.delete_keys(keys)),
            JsonbOp::DeleteIndex(idx) => Some(self.delete_index(*idx)),
        }
    }

    /// Checks if this JSONB value contains another value.
    /// For objects: all key-value pairs in `other` must exist in `self`.
    /// For arrays: all elements in `other` must exist in `self`.
    pub fn contains(&self, other: &JsonbValue) -> bool {
        match (self, other) {
            (JsonbValue::Object(a), JsonbValue::Object(b)) => {
                for (key, val) in b.iter() {
                    match a.get(key) {
                        Some(self_val) => {
                            if !self_val.contains(val) {
                                return false;
                            }
                        }
                        None => return false,
                    }
                }
                true
            }
            (JsonbValue::Array(a), JsonbValue::Array(b)) => {
                for item in b {
                    if !a.iter().any(|x| x.contains(item)) {
                        return false;
                    }
                }
                true
            }
            (a, b) => a == b,
        }
    }

    /// Checks if this object has the given key.
    pub fn has_key(&self, key: &str) -> bool {
        match self {
            JsonbValue::Object(obj) => obj.contains_key(key),
            JsonbValue::Array(arr) => {
                if let Ok(idx) = key.parse::<usize>() {
                    idx < arr.len()
                } else {
                    false
                }
            }
            _ => false,
        }
    }

    /// Checks if this object has any of the given keys.
    pub fn has_any_key(&self, keys: &[String]) -> bool {
        keys.iter().any(|k| self.has_key(k))
    }

    /// Checks if this object has all of the given keys.
    pub fn has_all_keys(&self, keys: &[String]) -> bool {
        keys.iter().all(|k| self.has_key(k))
    }

    /// Concatenates two JSONB values.
    pub fn concat(&self, other: &JsonbValue) -> JsonbValue {
        match (self, other) {
            (JsonbValue::Object(a), JsonbValue::Object(b)) => {
                let mut result = a.clone();
                for (key, val) in b.iter() {
                    result.insert(key.to_string(), val.clone());
                }
                JsonbValue::Object(result)
            }
            (JsonbValue::Array(a), JsonbValue::Array(b)) => {
                let mut result = a.clone();
                result.extend(b.iter().cloned());
                JsonbValue::Array(result)
            }
            (JsonbValue::Array(a), other) => {
                let mut result = a.clone();
                result.push(other.clone());
                JsonbValue::Array(result)
            }
            (other, JsonbValue::Array(b)) => {
                let mut result = Vec::with_capacity(b.len() + 1);
                result.push(other.clone());
                result.extend(b.iter().cloned());
                JsonbValue::Array(result)
            }
            _ => self.clone(),
        }
    }

    /// Deletes a key from an object.
    pub fn delete_key(&self, key: &str) -> JsonbValue {
        match self {
            JsonbValue::Object(obj) => {
                let mut result = obj.clone();
                result.remove(key);
                JsonbValue::Object(result)
            }
            _ => self.clone(),
        }
    }

    /// Deletes multiple keys from an object.
    pub fn delete_keys(&self, keys: &[String]) -> JsonbValue {
        match self {
            JsonbValue::Object(obj) => {
                let mut result = obj.clone();
                for key in keys {
                    result.remove(key);
                }
                JsonbValue::Object(result)
            }
            _ => self.clone(),
        }
    }

    /// Deletes an element at the given index from an array.
    pub fn delete_index(&self, index: usize) -> JsonbValue {
        match self {
            JsonbValue::Array(arr) => {
                if index < arr.len() {
                    let mut result = arr.clone();
                    result.remove(index);
                    JsonbValue::Array(result)
                } else {
                    self.clone()
                }
            }
            _ => self.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::value::JsonbObject;
    use alloc::vec;

    fn make_test_object() -> JsonbValue {
        let mut obj = JsonbObject::new();
        obj.insert("name".into(), JsonbValue::String("Alice".into()));
        obj.insert("age".into(), JsonbValue::Number(25.0));
        obj.insert("active".into(), JsonbValue::Bool(true));
        JsonbValue::Object(obj)
    }

    #[test]
    fn test_get_field() {
        let json = make_test_object();
        let result = json.apply_op(&JsonbOp::GetField("name".into()));
        assert_eq!(result, Some(JsonbValue::String("Alice".into())));
    }

    #[test]
    fn test_get_field_text() {
        let json = make_test_object();
        let result = json.apply_op(&JsonbOp::GetFieldText("age".into()));
        assert_eq!(result, Some(JsonbValue::String("25".into())));
    }

    #[test]
    fn test_get_path() {
        let mut inner = JsonbObject::new();
        inner.insert("city".into(), JsonbValue::String("NYC".into()));

        let mut outer = JsonbObject::new();
        outer.insert("address".into(), JsonbValue::Object(inner));

        let json = JsonbValue::Object(outer);
        let result = json.apply_op(&JsonbOp::GetPath(vec!["address".into(), "city".into()]));
        assert_eq!(result, Some(JsonbValue::String("NYC".into())));
    }

    #[test]
    fn test_contains() {
        let mut full = JsonbObject::new();
        full.insert("a".into(), JsonbValue::Number(1.0));
        full.insert("b".into(), JsonbValue::Number(2.0));
        full.insert("c".into(), JsonbValue::Number(3.0));

        let mut partial = JsonbObject::new();
        partial.insert("a".into(), JsonbValue::Number(1.0));

        let json = JsonbValue::Object(full);
        assert!(json.contains(&JsonbValue::Object(partial)));
    }

    #[test]
    fn test_contains_nested() {
        let mut inner = JsonbObject::new();
        inner.insert("c".into(), JsonbValue::Number(2.0));

        let mut outer = JsonbObject::new();
        outer.insert("a".into(), JsonbValue::Number(1.0));
        outer.insert("b".into(), JsonbValue::Object(inner));

        let mut check_inner = JsonbObject::new();
        check_inner.insert("c".into(), JsonbValue::Number(2.0));

        let mut check = JsonbObject::new();
        check.insert("b".into(), JsonbValue::Object(check_inner));

        let json = JsonbValue::Object(outer);
        assert!(json.contains(&JsonbValue::Object(check)));
    }

    #[test]
    fn test_has_key() {
        let json = make_test_object();
        assert!(json.has_key("name"));
        assert!(json.has_key("age"));
        assert!(!json.has_key("email"));
    }

    #[test]
    fn test_has_any_key() {
        let json = make_test_object();
        assert!(json.has_any_key(&["name".into(), "email".into()]));
        assert!(!json.has_any_key(&["email".into(), "phone".into()]));
    }

    #[test]
    fn test_has_all_keys() {
        let json = make_test_object();
        assert!(json.has_all_keys(&["name".into(), "age".into()]));
        assert!(!json.has_all_keys(&["name".into(), "email".into()]));
    }

    #[test]
    fn test_concat_objects() {
        let mut obj1 = JsonbObject::new();
        obj1.insert("a".into(), JsonbValue::Number(1.0));

        let mut obj2 = JsonbObject::new();
        obj2.insert("b".into(), JsonbValue::Number(2.0));

        let json1 = JsonbValue::Object(obj1);
        let json2 = JsonbValue::Object(obj2);

        let result = json1.concat(&json2);
        if let JsonbValue::Object(obj) = result {
            assert_eq!(obj.get("a"), Some(&JsonbValue::Number(1.0)));
            assert_eq!(obj.get("b"), Some(&JsonbValue::Number(2.0)));
        } else {
            panic!("Expected object");
        }
    }

    #[test]
    fn test_concat_arrays() {
        let arr1 = JsonbValue::Array(vec![JsonbValue::Number(1.0), JsonbValue::Number(2.0)]);
        let arr2 = JsonbValue::Array(vec![JsonbValue::Number(3.0)]);

        let result = arr1.concat(&arr2);
        if let JsonbValue::Array(arr) = result {
            assert_eq!(arr.len(), 3);
        } else {
            panic!("Expected array");
        }
    }

    #[test]
    fn test_delete_key() {
        let json = make_test_object();
        let result = json.delete_key("age");
        if let JsonbValue::Object(obj) = result {
            assert!(obj.get("name").is_some());
            assert!(obj.get("age").is_none());
        } else {
            panic!("Expected object");
        }
    }

    #[test]
    fn test_delete_index() {
        let arr = JsonbValue::Array(vec![
            JsonbValue::Number(1.0),
            JsonbValue::Number(2.0),
            JsonbValue::Number(3.0),
        ]);

        let result = arr.delete_index(1);
        if let JsonbValue::Array(a) = result {
            assert_eq!(a.len(), 2);
            assert_eq!(a[0], JsonbValue::Number(1.0));
            assert_eq!(a[1], JsonbValue::Number(3.0));
        } else {
            panic!("Expected array");
        }
    }

    // Edge case tests
    #[test]
    fn test_get_field_nonexistent() {
        let json = make_test_object();
        let result = json.apply_op(&JsonbOp::GetField("nonexistent".into()));
        assert_eq!(result, None);
    }

    #[test]
    fn test_get_field_on_non_object() {
        let json = JsonbValue::Array(vec![JsonbValue::Number(1.0)]);
        let result = json.apply_op(&JsonbOp::GetField("name".into()));
        assert_eq!(result, None);

        let json = JsonbValue::String("hello".into());
        let result = json.apply_op(&JsonbOp::GetField("name".into()));
        assert_eq!(result, None);
    }

    #[test]
    fn test_array_indexing_via_path() {
        use crate::path::JsonPath;
        // Arrays can be accessed via JSONPath, not direct GetIndex
        let arr = JsonbValue::Array(vec![
            JsonbValue::Number(1.0),
            JsonbValue::Number(2.0),
        ]);
        // Test array access via query
        let path = JsonPath::parse("$[0]").unwrap();
        let results = arr.query(&path);
        assert_eq!(results.len(), 1);
        assert_eq!(*results[0], JsonbValue::Number(1.0));
    }

    #[test]
    fn test_get_path_empty() {
        let json = make_test_object();
        let result = json.apply_op(&JsonbOp::GetPath(vec![]));
        assert_eq!(result, Some(json));
    }

    #[test]
    fn test_get_path_nonexistent() {
        let json = make_test_object();
        let result = json.apply_op(&JsonbOp::GetPath(vec!["a".into(), "b".into(), "c".into()]));
        assert_eq!(result, None);
    }

    #[test]
    fn test_contains_empty_object() {
        let mut obj = JsonbObject::new();
        obj.insert("a".into(), JsonbValue::Number(1.0));
        let json = JsonbValue::Object(obj);

        let empty = JsonbValue::Object(JsonbObject::new());
        assert!(json.contains(&empty));
    }

    #[test]
    fn test_contains_array() {
        let arr = JsonbValue::Array(vec![
            JsonbValue::Number(1.0),
            JsonbValue::Number(2.0),
            JsonbValue::Number(3.0),
        ]);

        let subset = JsonbValue::Array(vec![JsonbValue::Number(1.0)]);
        assert!(arr.contains(&subset));

        let not_subset = JsonbValue::Array(vec![JsonbValue::Number(5.0)]);
        assert!(!arr.contains(&not_subset));
    }

    #[test]
    fn test_contains_primitives() {
        let num = JsonbValue::Number(42.0);
        assert!(num.contains(&JsonbValue::Number(42.0)));
        assert!(!num.contains(&JsonbValue::Number(43.0)));

        let s = JsonbValue::String("hello".into());
        assert!(s.contains(&JsonbValue::String("hello".into())));
        assert!(!s.contains(&JsonbValue::String("world".into())));
    }

    #[test]
    fn test_has_key_on_non_object() {
        let arr = JsonbValue::Array(vec![JsonbValue::Number(1.0)]);
        assert!(!arr.has_key("name"));

        let num = JsonbValue::Number(42.0);
        assert!(!num.has_key("name"));
    }

    #[test]
    fn test_has_any_key_empty_keys() {
        let json = make_test_object();
        assert!(!json.has_any_key(&[]));
    }

    #[test]
    fn test_has_all_keys_empty_keys() {
        let json = make_test_object();
        assert!(json.has_all_keys(&[]));
    }

    #[test]
    fn test_concat_mixed_types() {
        let obj = make_test_object();
        let arr = JsonbValue::Array(vec![JsonbValue::Number(1.0)]);

        // Object concat with non-object wraps both in array
        let result = obj.concat(&arr);
        if let JsonbValue::Array(a) = result {
            assert_eq!(a.len(), 2);
        } else {
            panic!("Expected array when concatenating object with array");
        }

        // Array concat with non-array appends the element
        let result = arr.concat(&obj);
        if let JsonbValue::Array(a) = result {
            assert_eq!(a.len(), 2);
        } else {
            panic!("Expected array");
        }
    }

    #[test]
    fn test_concat_empty_arrays() {
        let arr1 = JsonbValue::Array(vec![]);
        let arr2 = JsonbValue::Array(vec![JsonbValue::Number(1.0)]);

        let result = arr1.concat(&arr2);
        if let JsonbValue::Array(a) = result {
            assert_eq!(a.len(), 1);
        } else {
            panic!("Expected array");
        }
    }

    #[test]
    fn test_concat_object_override() {
        let mut obj1 = JsonbObject::new();
        obj1.insert("a".into(), JsonbValue::Number(1.0));

        let mut obj2 = JsonbObject::new();
        obj2.insert("a".into(), JsonbValue::Number(2.0));

        let json1 = JsonbValue::Object(obj1);
        let json2 = JsonbValue::Object(obj2);

        let result = json1.concat(&json2);
        if let JsonbValue::Object(obj) = result {
            assert_eq!(obj.get("a"), Some(&JsonbValue::Number(2.0)));
        } else {
            panic!("Expected object");
        }
    }

    #[test]
    fn test_delete_key_nonexistent() {
        let json = make_test_object();
        let result = json.delete_key("nonexistent");
        if let JsonbValue::Object(obj) = result {
            assert_eq!(obj.len(), 3); // Original keys still present
        } else {
            panic!("Expected object");
        }
    }

    #[test]
    fn test_delete_key_on_non_object() {
        let arr = JsonbValue::Array(vec![JsonbValue::Number(1.0)]);
        let result = arr.delete_key("name");
        assert_eq!(result, arr);
    }

    #[test]
    fn test_delete_index_out_of_bounds() {
        let arr = JsonbValue::Array(vec![JsonbValue::Number(1.0)]);
        let result = arr.delete_index(10);
        assert_eq!(result, arr);
    }

    #[test]
    fn test_delete_index_on_non_array() {
        let json = make_test_object();
        let result = json.delete_index(0);
        assert_eq!(result, json);
    }

    #[test]
    fn test_get_field_text_null() {
        let mut obj = JsonbObject::new();
        obj.insert("value".into(), JsonbValue::Null);
        let json = JsonbValue::Object(obj);

        let result = json.apply_op(&JsonbOp::GetFieldText("value".into()));
        assert_eq!(result, Some(JsonbValue::String("null".into())));
    }

    #[test]
    fn test_get_field_text_bool() {
        let mut obj = JsonbObject::new();
        obj.insert("flag".into(), JsonbValue::Bool(true));
        let json = JsonbValue::Object(obj);

        let result = json.apply_op(&JsonbOp::GetFieldText("flag".into()));
        assert_eq!(result, Some(JsonbValue::String("true".into())));
    }

    #[test]
    fn test_get_field_text_array() {
        let mut obj = JsonbObject::new();
        obj.insert("arr".into(), JsonbValue::Array(vec![JsonbValue::Number(1.0)]));
        let json = JsonbValue::Object(obj);

        let result = json.apply_op(&JsonbOp::GetFieldText("arr".into()));
        // GetFieldText returns None for arrays/objects (only works for primitives)
        assert!(result.is_none());
    }
}
