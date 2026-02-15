//! Operator trait for query execution.

use crate::executor::Relation;
use cynos_core::Result;

/// A query operator that produces a relation.
pub trait Operator {
    /// Executes the operator and returns the result relation.
    fn execute(&self) -> Result<Relation>;
}
