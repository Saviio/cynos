//! Incremental operators for IVM.
//!
//! This module provides incremental versions of common relational operators:
//! - Filter: passes through deltas matching a predicate
//! - Map: transforms deltas using a mapper function
//! - Join: incrementally maintains join results
//! - Aggregate: incrementally maintains aggregate values

mod aggregate;
mod filter;
mod join;
mod map;

pub use aggregate::{IncrementalAvg, IncrementalCount, IncrementalMax, IncrementalMin, IncrementalSum};
pub use filter::filter_incremental;
pub use join::IncrementalHashJoin;
pub use map::{map_incremental, project_incremental};
