//! Cynos Reactive - Reactive query system for Cynos database.
//!
//! This crate implements a reactive query system that allows subscribing to
//! query result changes. When underlying data changes, subscribers are notified
//! with the incremental changes (additions, removals, modifications).
//!
//! # Core Concepts
//!
//! - `ChangeSet`: Represents changes to query results (added, removed, modified rows)
//! - `ObservableQuery`: A query that tracks changes and notifies subscribers
//! - `SubscriptionManager`: Manages subscriptions to query changes
//! - `QueryRegistry`: Routes table changes to dependent queries
//!
//! # Key Features
//!
//! - `observe()`: Subscribe to query changes with a callback
//! - `changes()`: Get an iterator that yields initial result + incremental changes
//!
//! # Example
//!
//! ```ignore
//! use cynos_reactive::{ObservableQuery, ChangeSet};
//! use cynos_incremental::DataflowNode;
//!
//! // Create an observable query
//! let dataflow = DataflowNode::filter(
//!     DataflowNode::source(1),
//!     |row| row.get(1).and_then(|v| v.as_i64()).map(|age| age > 18).unwrap_or(false)
//! );
//!
//! let mut query = ObservableQuery::new(dataflow);
//!
//! // Subscribe to changes (observe pattern)
//! query.subscribe(|changes| {
//!     println!("Added: {}, Removed: {}", changes.added.len(), changes.removed.len());
//! });
//!
//! // Or use the changes() API for initial + incremental
//! let mut changes = query.changes();
//! let initial = changes.initial(); // First push: initial result
//! let delta_changes = changes.process(1, deltas); // Subsequent: incremental changes
//! ```

#![no_std]

extern crate alloc;

pub mod change_set;
pub mod notify;
pub mod observable;
pub mod subscription;

pub use change_set::ChangeSet;
pub use notify::{QueryId, QueryRegistry};
pub use observable::{Changes, ObservableQuery};
pub use subscription::{ChangeCallback, Subscription, SubscriptionId, SubscriptionManager};

// Re-export commonly used types from dependencies
pub use cynos_incremental::{DataflowNode, Delta, TableId};
