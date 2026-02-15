//! Incremental hash join operator.

use crate::delta::Delta;
use alloc::vec::Vec;
use core::hash::Hash;
use hashbrown::HashMap;

/// Incremental hash join that maintains indexes for both sides.
///
/// When a row is inserted/deleted on either side, the join
/// efficiently finds matching rows from the other side.
pub struct IncrementalHashJoin<K, L, R>
where
    K: Eq + Hash,
{
    /// Left side index: key -> list of rows
    left_index: HashMap<K, Vec<L>>,
    /// Right side index: key -> list of rows
    right_index: HashMap<K, Vec<R>>,
    /// Function to extract key from left row
    left_key_fn: fn(&L) -> K,
    /// Function to extract key from right row
    right_key_fn: fn(&R) -> K,
}

impl<K, L, R> IncrementalHashJoin<K, L, R>
where
    K: Eq + Hash + Clone,
    L: Clone + PartialEq,
    R: Clone + PartialEq,
{
    /// Creates a new incremental hash join with the given key extractors.
    pub fn new(left_key_fn: fn(&L) -> K, right_key_fn: fn(&R) -> K) -> Self {
        Self {
            left_index: HashMap::new(),
            right_index: HashMap::new(),
            left_key_fn,
            right_key_fn,
        }
    }

    /// Handles a left-side insertion.
    ///
    /// Returns the new join results produced by this insertion.
    pub fn on_left_insert(&mut self, row: L) -> Vec<(L, R)> {
        let key = (self.left_key_fn)(&row);
        let mut output = Vec::new();

        // Find matching rows from right side
        if let Some(right_rows) = self.right_index.get(&key) {
            for r in right_rows {
                output.push((row.clone(), r.clone()));
            }
        }

        // Add to left index
        self.left_index.entry(key).or_default().push(row);

        output
    }

    /// Handles a left-side deletion.
    ///
    /// Returns the join results that should be removed.
    pub fn on_left_delete(&mut self, row: &L) -> Vec<(L, R)> {
        let key = (self.left_key_fn)(row);
        let mut output = Vec::new();

        // Find matching rows from right side
        if let Some(right_rows) = self.right_index.get(&key) {
            for r in right_rows {
                output.push((row.clone(), r.clone()));
            }
        }

        // Remove from left index
        if let Some(left_rows) = self.left_index.get_mut(&key) {
            left_rows.retain(|l| l != row);
            if left_rows.is_empty() {
                self.left_index.remove(&key);
            }
        }

        output
    }

    /// Handles a right-side insertion.
    ///
    /// Returns the new join results produced by this insertion.
    pub fn on_right_insert(&mut self, row: R) -> Vec<(L, R)> {
        let key = (self.right_key_fn)(&row);
        let mut output = Vec::new();

        // Find matching rows from left side
        if let Some(left_rows) = self.left_index.get(&key) {
            for l in left_rows {
                output.push((l.clone(), row.clone()));
            }
        }

        // Add to right index
        self.right_index.entry(key).or_default().push(row);

        output
    }

    /// Handles a right-side deletion.
    ///
    /// Returns the join results that should be removed.
    pub fn on_right_delete(&mut self, row: &R) -> Vec<(L, R)> {
        let key = (self.right_key_fn)(row);
        let mut output = Vec::new();

        // Find matching rows from left side
        if let Some(left_rows) = self.left_index.get(&key) {
            for l in left_rows {
                output.push((l.clone(), row.clone()));
            }
        }

        // Remove from right index
        if let Some(right_rows) = self.right_index.get_mut(&key) {
            right_rows.retain(|r| r != row);
            if right_rows.is_empty() {
                self.right_index.remove(&key);
            }
        }

        output
    }

    /// Processes a batch of left-side deltas.
    pub fn process_left_deltas(&mut self, deltas: &[Delta<L>]) -> Vec<Delta<(L, R)>> {
        let mut output = Vec::new();

        for delta in deltas {
            if delta.is_insert() {
                for pair in self.on_left_insert(delta.data.clone()) {
                    output.push(Delta::insert(pair));
                }
            } else if delta.is_delete() {
                for pair in self.on_left_delete(&delta.data) {
                    output.push(Delta::delete(pair));
                }
            }
        }

        output
    }

    /// Processes a batch of right-side deltas.
    pub fn process_right_deltas(&mut self, deltas: &[Delta<R>]) -> Vec<Delta<(L, R)>> {
        let mut output = Vec::new();

        for delta in deltas {
            if delta.is_insert() {
                for pair in self.on_right_insert(delta.data.clone()) {
                    output.push(Delta::insert(pair));
                }
            } else if delta.is_delete() {
                for pair in self.on_right_delete(&delta.data) {
                    output.push(Delta::delete(pair));
                }
            }
        }

        output
    }

    /// Returns the number of entries in the left index.
    pub fn left_count(&self) -> usize {
        self.left_index.values().map(|v| v.len()).sum()
    }

    /// Returns the number of entries in the right index.
    pub fn right_count(&self) -> usize {
        self.right_index.values().map(|v| v.len()).sum()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloc::vec;

    #[derive(Clone, Debug, PartialEq)]
    struct Employee {
        id: i32,
        name: &'static str,
        dept_id: i32,
    }

    #[derive(Clone, Debug, PartialEq)]
    struct Department {
        id: i32,
        name: &'static str,
    }

    fn emp_key(e: &Employee) -> i32 {
        e.dept_id
    }

    fn dept_key(d: &Department) -> i32 {
        d.id
    }

    #[test]
    fn test_join_left_insert() {
        let mut join = IncrementalHashJoin::new(emp_key, dept_key);

        // Insert department first
        join.on_right_insert(Department {
            id: 10,
            name: "Engineering",
        });

        // Insert employee
        let results = join.on_left_insert(Employee {
            id: 1,
            name: "Alice",
            dept_id: 10,
        });

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0.name, "Alice");
        assert_eq!(results[0].1.name, "Engineering");
    }

    #[test]
    fn test_join_no_match() {
        let mut join = IncrementalHashJoin::new(emp_key, dept_key);

        // Insert department
        join.on_right_insert(Department {
            id: 10,
            name: "Engineering",
        });

        // Insert employee with different dept_id
        let results = join.on_left_insert(Employee {
            id: 1,
            name: "Alice",
            dept_id: 20,
        });

        assert!(results.is_empty());
    }

    #[test]
    fn test_join_right_insert_matches_existing() {
        let mut join = IncrementalHashJoin::new(emp_key, dept_key);

        // Insert employee first
        join.on_left_insert(Employee {
            id: 1,
            name: "Alice",
            dept_id: 20,
        });

        // Insert matching department
        let results = join.on_right_insert(Department {
            id: 20,
            name: "Sales",
        });

        assert_eq!(results.len(), 1);
    }

    #[test]
    fn test_join_delete() {
        let mut join = IncrementalHashJoin::new(emp_key, dept_key);

        let dept = Department {
            id: 10,
            name: "Engineering",
        };
        let emp = Employee {
            id: 1,
            name: "Alice",
            dept_id: 10,
        };

        join.on_right_insert(dept.clone());
        join.on_left_insert(emp.clone());

        // Delete employee
        let removed = join.on_left_delete(&emp);
        assert_eq!(removed.len(), 1);
        assert_eq!(join.left_count(), 0);
    }

    #[test]
    fn test_join_process_deltas() {
        let mut join = IncrementalHashJoin::new(emp_key, dept_key);

        // Add department
        join.on_right_insert(Department {
            id: 10,
            name: "Engineering",
        });

        // Process employee deltas
        let deltas = vec![
            Delta::insert(Employee {
                id: 1,
                name: "Alice",
                dept_id: 10,
            }),
            Delta::insert(Employee {
                id: 2,
                name: "Bob",
                dept_id: 10,
            }),
        ];

        let results = join.process_left_deltas(&deltas);
        assert_eq!(results.len(), 2);
        assert!(results.iter().all(|d| d.is_insert()));
    }
}