//! B+Tree iterator implementation.

use super::node::{Node, NodeId};
use alloc::vec::Vec;
use cynos_core::RowId;

/// Iterator over B+Tree leaf nodes.
#[allow(dead_code)]
pub struct BTreeIterator<'a, K> {
    /// Reference to the arena of nodes.
    arena: &'a [Node<K>],
    /// Current node ID.
    current_node: Option<NodeId>,
    /// Current position within the node.
    current_pos: usize,
    /// Current position within the values of the current key.
    value_pos: usize,
    /// Whether to iterate in reverse.
    reverse: bool,
}

impl<'a, K: Clone + Ord> BTreeIterator<'a, K> {
    /// Creates a new iterator starting at the given node.
    pub fn new(arena: &'a [Node<K>], start_node: Option<NodeId>, reverse: bool) -> Self {
        let (current_node, current_pos) = if let Some(node_id) = start_node {
            if reverse {
                let node = &arena[node_id];
                let pos = if node.keys.is_empty() {
                    0
                } else {
                    node.keys.len() - 1
                };
                (Some(node_id), pos)
            } else {
                (Some(node_id), 0)
            }
        } else {
            (None, 0)
        };

        Self {
            arena,
            current_node,
            current_pos,
            value_pos: 0,
            reverse,
        }
    }

    /// Creates an iterator starting at a specific position.
    pub fn new_at(
        arena: &'a [Node<K>],
        node_id: NodeId,
        pos: usize,
        reverse: bool,
    ) -> Self {
        Self {
            arena,
            current_node: Some(node_id),
            current_pos: pos,
            value_pos: 0,
            reverse,
        }
    }

    /// Advances to the next key (skipping remaining values of current key).
    fn advance_key(&mut self) {
        if let Some(node_id) = self.current_node {
            let node = &self.arena[node_id];

            if self.reverse {
                if self.current_pos > 0 {
                    self.current_pos -= 1;
                } else {
                    // Move to previous node
                    self.current_node = node.prev;
                    if let Some(prev_id) = self.current_node {
                        let prev_node = &self.arena[prev_id];
                        self.current_pos = if prev_node.keys.is_empty() {
                            0
                        } else {
                            prev_node.keys.len() - 1
                        };
                    }
                }
            } else {
                self.current_pos += 1;
                if self.current_pos >= node.keys.len() {
                    // Move to next node
                    self.current_node = node.next;
                    self.current_pos = 0;
                }
            }
            self.value_pos = 0;
        }
    }
}

impl<'a, K: Clone + Ord> Iterator for BTreeIterator<'a, K> {
    type Item = (&'a K, RowId);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let node_id = self.current_node?;
            let node = &self.arena[node_id];

            if node.keys.is_empty() {
                return None;
            }

            if self.current_pos >= node.keys.len() {
                self.advance_key();
                continue;
            }

            let key = &node.keys[self.current_pos];
            let values = &node.values[self.current_pos];

            if self.value_pos < values.len() {
                let value = values[self.value_pos];
                self.value_pos += 1;
                return Some((key, value));
            } else {
                self.advance_key();
            }
        }
    }
}

/// Collects row IDs from an iterator with optional limit and skip.
#[allow(dead_code)]
pub fn collect_with_limit<'a, K: Clone + Ord>(
    iter: BTreeIterator<'a, K>,
    limit: Option<usize>,
    skip: usize,
) -> Vec<RowId> {
    let mut result = Vec::new();
    let mut skipped = 0;
    let mut collected = 0;

    for (_, value) in iter {
        if skipped < skip {
            skipped += 1;
            continue;
        }

        if let Some(lim) = limit {
            if collected >= lim {
                break;
            }
        }

        result.push(value);
        collected += 1;
    }

    result
}
