//! Cynos JSONB - JSONB data type implementation for Cynos database.
//!
//! This crate provides a complete JSONB implementation including:
//!
//! - `JsonbValue`: The core JSON value type with sorted object keys
//! - `JsonbBinary`: Binary encoding/decoding for efficient storage
//! - `JsonPath`: JSONPath query language support
//! - `JsonbOp`: PostgreSQL-compatible JSONB operators
//! - GIN index support for efficient querying
//!
//! # Example
//!
//! ```rust
//! use cynos_jsonb::{JsonbValue, JsonbObject, JsonPath, JsonbBinary};
//!
//! // Create a JSON object
//! let mut obj = JsonbObject::new();
//! obj.insert("name".into(), JsonbValue::String("Alice".into()));
//! obj.insert("age".into(), JsonbValue::Number(25.0));
//!
//! let json = JsonbValue::Object(obj);
//!
//! // Query with JSONPath
//! let path = JsonPath::parse("$.name").unwrap();
//! let results = json.query(&path);
//! assert_eq!(results[0], &JsonbValue::String("Alice".into()));
//!
//! // Binary encoding
//! let binary = JsonbBinary::encode(&json);
//! let decoded = binary.decode();
//! assert_eq!(json, decoded);
//! ```

#![no_std]

extern crate alloc;

mod binary;
mod index;
mod ops;
pub mod path;
mod value;

pub use binary::JsonbBinary;
pub use ops::JsonbOp;
pub use path::{CompareOp, JsonPath, JsonPathPredicate, ParseError, PredicateValue};
pub use value::{JsonbObject, JsonbValue};
