//! B+Tree node definitions.

use alloc::vec::Vec;
use cynos_core::RowId;

/// Node identifier in the B+Tree arena.
pub type NodeId = usize;

/// Sentinel value for null node references.
#[allow(dead_code)]
pub const NULL_NODE: NodeId = usize::MAX;

/// A node in the B+Tree.
#[derive(Clone, Debug)]
pub struct Node<K> {
    /// Keys stored in this node.
    pub keys: Vec<K>,
    /// For leaf nodes: row IDs associated with each key.
    /// For internal nodes: empty.
    pub values: Vec<Vec<RowId>>,
    /// For internal nodes: child node IDs.
    /// For leaf nodes: empty.
    pub children: Vec<NodeId>,
    /// For leaf nodes: pointer to the next leaf node.
    pub next: Option<NodeId>,
    /// For leaf nodes: pointer to the previous leaf node.
    pub prev: Option<NodeId>,
    /// Whether this is a leaf node.
    pub is_leaf: bool,
    /// Parent node ID.
    pub parent: Option<NodeId>,
}

impl<K: Clone + Ord> Node<K> {
    /// Creates a new leaf node.
    pub fn new_leaf() -> Self {
        Self {
            keys: Vec::new(),
            values: Vec::new(),
            children: Vec::new(),
            next: None,
            prev: None,
            is_leaf: true,
            parent: None,
        }
    }

    /// Creates a new internal node.
    pub fn new_internal() -> Self {
        Self {
            keys: Vec::new(),
            values: Vec::new(),
            children: Vec::new(),
            next: None,
            prev: None,
            is_leaf: false,
            parent: None,
        }
    }

    /// Returns the number of keys in this node.
    pub fn key_count(&self) -> usize {
        self.keys.len()
    }

    /// Returns true if this node is empty.
    pub fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }

    /// Finds the position where a key should be inserted.
    pub fn find_key_position(&self, key: &K) -> usize {
        self.keys.partition_point(|k| k < key)
    }

    /// Finds the exact position of a key, or None if not found.
    pub fn find_key(&self, key: &K) -> Option<usize> {
        let pos = self.find_key_position(key);
        if pos < self.keys.len() && &self.keys[pos] == key {
            Some(pos)
        } else {
            None
        }
    }

    /// Inserts a key-value pair at the given position in a leaf node.
    pub fn insert_at(&mut self, pos: usize, key: K, value: RowId) {
        debug_assert!(self.is_leaf);
        if pos < self.keys.len() && self.keys[pos] == key {
            // Key exists, add to existing values
            self.values[pos].push(value);
        } else {
            // New key
            self.keys.insert(pos, key);
            self.values.insert(pos, alloc::vec![value]);
        }
    }

    /// Inserts a key and child at the given position in an internal node.
    pub fn insert_child_at(&mut self, pos: usize, key: K, child: NodeId) {
        debug_assert!(!self.is_leaf);
        self.keys.insert(pos, key);
        self.children.insert(pos + 1, child);
    }

    /// Removes a key-value pair at the given position.
    /// If value is Some, only removes that specific value.
    /// Returns the number of values removed.
    pub fn remove_at(&mut self, pos: usize, value: Option<RowId>) -> usize {
        debug_assert!(self.is_leaf);
        match value {
            Some(v) => {
                let values = &mut self.values[pos];
                let original_len = values.len();
                values.retain(|&x| x != v);
                let removed = original_len - values.len();
                if values.is_empty() {
                    self.keys.remove(pos);
                    self.values.remove(pos);
                }
                removed
            }
            None => {
                self.keys.remove(pos);
                let removed = self.values.remove(pos).len();
                removed
            }
        }
    }

    /// Gets the leftmost key in this subtree.
    pub fn get_leftmost_key(&self) -> Option<&K> {
        self.keys.first()
    }
}
