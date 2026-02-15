//! Query executor module.

mod aggregate;
mod filter;
pub mod join;
mod limit;
mod operator;
mod project;
mod relation;
mod runner;
mod scan;
mod sort;

pub use aggregate::AggregateExecutor;
pub use filter::FilterExecutor;
pub use join::{HashJoin, NestedLoopJoin, SortMergeJoin};
pub use limit::LimitExecutor;
pub use operator::Operator;
pub use project::ProjectExecutor;
pub use relation::{Relation, RelationEntry, SharedTables};
pub use runner::{DataSource, ExecutionError, ExecutionResult, InMemoryDataSource, PhysicalPlanRunner};
pub use scan::{IndexScanExecutor, TableScanExecutor};
pub use sort::SortExecutor;
