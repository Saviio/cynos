//! Dataflow node definitions.
//!
//! Based on DBSP (Database Stream Processing) theory, each node represents
//! a lifted relational operator that processes Z-set deltas incrementally.

use alloc::boxed::Box;
use alloc::vec::Vec;
use cynos_core::Row;

/// Type alias for table identifier.
pub type TableId = u32;

/// Type alias for column identifier.
pub type ColumnId = usize;

/// Predicate for filtering rows.
pub type PredicateFn = Box<dyn Fn(&Row) -> bool + Send + Sync>;

/// Mapper function for transforming rows.
pub type MapperFn = Box<dyn Fn(&Row) -> Row + Send + Sync>;

/// Key extractor function for joins.
pub type KeyExtractorFn = Box<dyn Fn(&Row) -> Vec<cynos_core::Value> + Send + Sync>;

/// Aggregate function types.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AggregateType {
    Count,
    Sum,
    Avg,
    Min,
    Max,
}

/// Join type for dataflow join nodes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum JoinType {
    Inner,
    LeftOuter,
    RightOuter,
    FullOuter,
}

/// A node in the dataflow graph.
///
/// Each node represents an operation that can process incremental changes.
/// Based on DBSP, each operator is "lifted" to work on Z-sets (multisets with
/// integer multiplicities), enabling automatic incrementalization.
pub enum DataflowNode {
    /// Source table - entry point for changes
    Source { table_id: TableId },

    /// Filter operation - passes through rows matching predicate
    Filter {
        input: Box<DataflowNode>,
        predicate: PredicateFn,
    },

    /// Project operation - selects specific columns
    Project {
        input: Box<DataflowNode>,
        columns: Vec<ColumnId>,
    },

    /// Map operation - transforms rows
    Map {
        input: Box<DataflowNode>,
        mapper: MapperFn,
    },

    /// Join operation - combines two inputs.
    /// Supports Inner, Left, Right, and Full Outer joins.
    /// Outer joins are decomposed as: LEFT JOIN = INNER JOIN ∪ ANTIJOIN
    Join {
        left: Box<DataflowNode>,
        right: Box<DataflowNode>,
        left_key: KeyExtractorFn,
        right_key: KeyExtractorFn,
        join_type: JoinType,
    },

    /// Aggregate operation - computes aggregates per group.
    /// Uses DBSP indexed Z-set approach: group by key partitions the Z-set,
    /// then each partition is aggregated incrementally.
    Aggregate {
        input: Box<DataflowNode>,
        group_by: Vec<ColumnId>,
        functions: Vec<(ColumnId, AggregateType)>,
    },
}

impl DataflowNode {
    /// Creates a source node.
    pub fn source(table_id: TableId) -> Self {
        DataflowNode::Source { table_id }
    }

    /// Creates a filter node.
    pub fn filter<F>(input: DataflowNode, predicate: F) -> Self
    where
        F: Fn(&Row) -> bool + Send + Sync + 'static,
    {
        DataflowNode::Filter {
            input: Box::new(input),
            predicate: Box::new(predicate),
        }
    }

    /// Creates a project node.
    pub fn project(input: DataflowNode, columns: Vec<ColumnId>) -> Self {
        DataflowNode::Project {
            input: Box::new(input),
            columns,
        }
    }

    /// Creates a map node.
    pub fn map<F>(input: DataflowNode, mapper: F) -> Self
    where
        F: Fn(&Row) -> Row + Send + Sync + 'static,
    {
        DataflowNode::Map {
            input: Box::new(input),
            mapper: Box::new(mapper),
        }
    }

    /// Creates an inner join node (backward compatible).
    pub fn join(
        left: DataflowNode,
        right: DataflowNode,
        left_key: KeyExtractorFn,
        right_key: KeyExtractorFn,
    ) -> Self {
        DataflowNode::Join {
            left: Box::new(left),
            right: Box::new(right),
            left_key,
            right_key,
            join_type: JoinType::Inner,
        }
    }

    /// Creates a join node with explicit join type.
    pub fn join_with_type(
        left: DataflowNode,
        right: DataflowNode,
        left_key: KeyExtractorFn,
        right_key: KeyExtractorFn,
        join_type: JoinType,
    ) -> Self {
        DataflowNode::Join {
            left: Box::new(left),
            right: Box::new(right),
            left_key,
            right_key,
            join_type,
        }
    }

    /// Returns the table ID if this is a source node.
    pub fn source_table_id(&self) -> Option<TableId> {
        match self {
            DataflowNode::Source { table_id } => Some(*table_id),
            _ => None,
        }
    }

    /// Collects all source table IDs in this dataflow.
    pub fn collect_sources(&self) -> Vec<TableId> {
        let mut sources = Vec::new();
        self.collect_sources_inner(&mut sources);
        sources
    }

    fn collect_sources_inner(&self, sources: &mut Vec<TableId>) {
        match self {
            DataflowNode::Source { table_id } => {
                if !sources.contains(table_id) {
                    sources.push(*table_id);
                }
            }
            DataflowNode::Filter { input, .. }
            | DataflowNode::Project { input, .. }
            | DataflowNode::Map { input, .. }
            | DataflowNode::Aggregate { input, .. } => {
                input.collect_sources_inner(sources);
            }
            DataflowNode::Join { left, right, .. } => {
                left.collect_sources_inner(sources);
                right.collect_sources_inner(sources);
            }
        }
    }
}