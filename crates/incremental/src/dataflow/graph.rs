//! Dataflow graph management.

use crate::dataflow::node::{DataflowNode, TableId};
use alloc::vec::Vec;
use hashbrown::HashMap;

/// Unique identifier for a node in the dataflow graph.
pub type NodeId = u32;

/// A dataflow graph that manages multiple dataflow nodes.
///
/// The graph tracks dependencies between nodes and propagates
/// changes through the appropriate paths.
pub struct DataflowGraph {
    /// Counter for generating node IDs
    next_id: NodeId,
    /// Map from node ID to dataflow node
    nodes: HashMap<NodeId, DataflowNode>,
    /// Map from table ID to nodes that depend on it
    table_dependencies: HashMap<TableId, Vec<NodeId>>,
}

impl Default for DataflowGraph {
    fn default() -> Self {
        Self::new()
    }
}

impl DataflowGraph {
    /// Creates a new empty dataflow graph.
    pub fn new() -> Self {
        Self {
            next_id: 0,
            nodes: HashMap::new(),
            table_dependencies: HashMap::new(),
        }
    }

    /// Adds a dataflow node to the graph.
    ///
    /// Returns the node ID assigned to this node.
    pub fn add_node(&mut self, node: DataflowNode) -> NodeId {
        let id = self.next_id;
        self.next_id += 1;

        // Track table dependencies
        for table_id in node.collect_sources() {
            self.table_dependencies
                .entry(table_id)
                .or_default()
                .push(id);
        }

        self.nodes.insert(id, node);
        id
    }

    /// Removes a node from the graph.
    pub fn remove_node(&mut self, id: NodeId) -> Option<DataflowNode> {
        if let Some(node) = self.nodes.remove(&id) {
            // Remove from table dependencies
            for table_id in node.collect_sources() {
                if let Some(deps) = self.table_dependencies.get_mut(&table_id) {
                    deps.retain(|&dep_id| dep_id != id);
                }
            }
            Some(node)
        } else {
            None
        }
    }

    /// Gets a reference to a node by ID.
    pub fn get_node(&self, id: NodeId) -> Option<&DataflowNode> {
        self.nodes.get(&id)
    }

    /// Returns the node IDs that depend on a given table.
    pub fn get_dependents(&self, table_id: TableId) -> &[NodeId] {
        self.table_dependencies
            .get(&table_id)
            .map(|v| v.as_slice())
            .unwrap_or(&[])
    }

    /// Returns the number of nodes in the graph.
    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    /// Returns true if the graph has no nodes.
    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }

    /// Returns an iterator over all node IDs.
    pub fn node_ids(&self) -> impl Iterator<Item = NodeId> + '_ {
        self.nodes.keys().copied()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_graph_add_node() {
        let mut graph = DataflowGraph::new();
        let id = graph.add_node(DataflowNode::source(1));
        assert_eq!(id, 0);
        assert_eq!(graph.len(), 1);
    }

    #[test]
    fn test_graph_dependencies() {
        let mut graph = DataflowGraph::new();

        let source = DataflowNode::source(1);
        let filter = DataflowNode::filter(DataflowNode::source(1), |_| true);

        let id1 = graph.add_node(source);
        let id2 = graph.add_node(filter);

        let deps = graph.get_dependents(1);
        assert_eq!(deps.len(), 2);
        assert!(deps.contains(&id1));
        assert!(deps.contains(&id2));
    }

    #[test]
    fn test_graph_remove_node() {
        let mut graph = DataflowGraph::new();
        let id = graph.add_node(DataflowNode::source(1));

        assert!(graph.remove_node(id).is_some());
        assert!(graph.is_empty());
        assert!(graph.get_dependents(1).is_empty());
    }
}
