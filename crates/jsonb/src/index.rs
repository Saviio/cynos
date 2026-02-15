//! GIN index support for JSONB values.
//!
//! This module provides methods to extract indexable keys and paths
//! from JSONB values for use with GIN (Generalized Inverted Index).

use crate::value::JsonbValue;
use alloc::string::{String, ToString};
use alloc::vec::Vec;

impl JsonbValue {
    /// Extracts all keys from this JSONB value (for GIN indexing).
    /// Only extracts top-level keys from objects.
    pub fn extract_keys(&self) -> Vec<String> {
        match self {
            JsonbValue::Object(obj) => obj.keys().map(|k| k.to_string()).collect(),
            _ => Vec::new(),
        }
    }

    /// Extracts all key-value pairs from this JSONB value (for GIN indexing).
    /// Only extracts top-level pairs from objects.
    pub fn extract_key_values(&self) -> Vec<(String, JsonbValue)> {
        match self {
            JsonbValue::Object(obj) => obj
                .iter()
                .map(|(k, v)| (k.to_string(), v.clone()))
                .collect(),
            _ => Vec::new(),
        }
    }

    /// Extracts all paths with their values (for GIN indexing).
    /// Recursively traverses the structure.
    pub fn extract_paths(&self) -> Vec<(Vec<String>, JsonbValue)> {
        let mut results = Vec::new();
        extract_paths_recursive(self, &mut Vec::new(), &mut results);
        results
    }

    /// Extracts all scalar values with their paths (for full-text search).
    pub fn extract_scalars(&self) -> Vec<(Vec<String>, JsonbValue)> {
        let mut results = Vec::new();
        extract_scalars_recursive(self, &mut Vec::new(), &mut results);
        results
    }
}

fn extract_paths_recursive(
    value: &JsonbValue,
    current_path: &mut Vec<String>,
    results: &mut Vec<(Vec<String>, JsonbValue)>,
) {
    results.push((current_path.clone(), value.clone()));

    match value {
        JsonbValue::Object(obj) => {
            for (key, val) in obj.iter() {
                current_path.push(key.to_string());
                extract_paths_recursive(val, current_path, results);
                current_path.pop();
            }
        }
        JsonbValue::Array(arr) => {
            for (idx, item) in arr.iter().enumerate() {
                current_path.push(idx.to_string());
                extract_paths_recursive(item, current_path, results);
                current_path.pop();
            }
        }
        _ => {}
    }
}

fn extract_scalars_recursive(
    value: &JsonbValue,
    current_path: &mut Vec<String>,
    results: &mut Vec<(Vec<String>, JsonbValue)>,
) {
    match value {
        JsonbValue::Object(obj) => {
            for (key, val) in obj.iter() {
                current_path.push(key.to_string());
                extract_scalars_recursive(val, current_path, results);
                current_path.pop();
            }
        }
        JsonbValue::Array(arr) => {
            for (idx, item) in arr.iter().enumerate() {
                current_path.push(idx.to_string());
                extract_scalars_recursive(item, current_path, results);
                current_path.pop();
            }
        }
        _ => {
            results.push((current_path.clone(), value.clone()));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::value::JsonbObject;
    use alloc::vec;

    #[test]
    fn test_extract_keys() {
        let mut obj = JsonbObject::new();
        obj.insert("name".into(), JsonbValue::String("Alice".into()));
        obj.insert("age".into(), JsonbValue::Number(25.0));

        let json = JsonbValue::Object(obj);
        let keys = json.extract_keys();
        assert_eq!(keys.len(), 2);
        assert!(keys.contains(&"name".to_string()));
        assert!(keys.contains(&"age".to_string()));
    }

    #[test]
    fn test_extract_keys_non_object() {
        let json = JsonbValue::Array(vec![JsonbValue::Number(1.0)]);
        let keys = json.extract_keys();
        assert!(keys.is_empty());
    }

    #[test]
    fn test_extract_key_values() {
        let mut obj = JsonbObject::new();
        obj.insert("name".into(), JsonbValue::String("Alice".into()));
        obj.insert("age".into(), JsonbValue::Number(25.0));

        let json = JsonbValue::Object(obj);
        let pairs = json.extract_key_values();
        assert_eq!(pairs.len(), 2);
    }

    #[test]
    fn test_extract_paths() {
        let mut inner = JsonbObject::new();
        inner.insert("city".into(), JsonbValue::String("NYC".into()));

        let mut outer = JsonbObject::new();
        outer.insert("name".into(), JsonbValue::String("Alice".into()));
        outer.insert("address".into(), JsonbValue::Object(inner));

        let json = JsonbValue::Object(outer);
        let paths = json.extract_paths();

        // Should have: root, name, address, address.city
        assert!(paths.len() >= 4);

        // Check that we have the nested path
        let has_city_path = paths
            .iter()
            .any(|(path, _)| path == &vec!["address".to_string(), "city".to_string()]);
        assert!(has_city_path);
    }

    #[test]
    fn test_extract_paths_with_array() {
        let mut obj = JsonbObject::new();
        obj.insert(
            "tags".into(),
            JsonbValue::Array(vec![
                JsonbValue::String("a".into()),
                JsonbValue::String("b".into()),
            ]),
        );

        let json = JsonbValue::Object(obj);
        let paths = json.extract_paths();

        // Should have paths for array indices
        let has_index_path = paths
            .iter()
            .any(|(path, _)| path == &vec!["tags".to_string(), "0".to_string()]);
        assert!(has_index_path);
    }

    #[test]
    fn test_extract_scalars() {
        let mut inner = JsonbObject::new();
        inner.insert("city".into(), JsonbValue::String("NYC".into()));

        let mut outer = JsonbObject::new();
        outer.insert("name".into(), JsonbValue::String("Alice".into()));
        outer.insert("address".into(), JsonbValue::Object(inner));

        let json = JsonbValue::Object(outer);
        let scalars = json.extract_scalars();

        // Should only have scalar values (name and city)
        assert_eq!(scalars.len(), 2);

        // All results should be scalars
        for (_, value) in &scalars {
            assert!(!value.is_object());
            assert!(!value.is_array());
        }
    }
}
