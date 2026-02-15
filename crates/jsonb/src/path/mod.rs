//! JSONPath module for JSONB queries.

pub mod eval;
pub mod parser;

pub use parser::{CompareOp, JsonPath, JsonPathPredicate, ParseError, PredicateValue};
