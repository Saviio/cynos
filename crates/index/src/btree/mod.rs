//! B+Tree index implementation for Cynos database.
//!
//! This module provides a B+Tree-based index for efficient range queries.

mod iter;
mod node;
mod tree;

pub use iter::BTreeIterator;
pub use node::{Node, NodeId};
pub use tree::BTreeIndex;
