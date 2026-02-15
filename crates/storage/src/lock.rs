//! Lock management for Cynos database.
//!
//! This module provides lock management for concurrent access control.

use alloc::collections::BTreeMap;
use alloc::collections::BTreeSet;
use alloc::string::{String, ToString};
use alloc::vec::Vec;
use cynos_core::{Error, Result};

/// Lock type.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LockType {
    /// Shared lock (read).
    Shared,
    /// Exclusive lock (write).
    Exclusive,
}

/// Lock request.
#[derive(Clone, Debug)]
pub struct LockRequest {
    /// Transaction ID requesting the lock.
    pub tx_id: u64,
    /// Lock type.
    pub lock_type: LockType,
}

/// Lock state for a resource.
#[derive(Clone, Debug, Default)]
struct LockState {
    /// Transactions holding shared locks.
    shared_holders: BTreeSet<u64>,
    /// Transaction holding exclusive lock (if any).
    exclusive_holder: Option<u64>,
}

impl LockState {
    fn new() -> Self {
        Self::default()
    }

    fn is_free(&self) -> bool {
        self.shared_holders.is_empty() && self.exclusive_holder.is_none()
    }

    fn can_grant_shared(&self, tx_id: u64) -> bool {
        // Can grant shared if no exclusive lock or we already hold it
        self.exclusive_holder.is_none() || self.exclusive_holder == Some(tx_id)
    }

    fn can_grant_exclusive(&self, tx_id: u64) -> bool {
        // Can grant exclusive if no locks or only we hold shared
        (self.exclusive_holder.is_none() && self.shared_holders.is_empty())
            || (self.exclusive_holder.is_none()
                && self.shared_holders.len() == 1
                && self.shared_holders.contains(&tx_id))
            || self.exclusive_holder == Some(tx_id)
    }
}

/// Lock manager for managing resource locks.
pub struct LockManager {
    /// Locks by resource name (table name).
    locks: BTreeMap<String, LockState>,
}

impl LockManager {
    /// Creates a new lock manager.
    pub fn new() -> Self {
        Self {
            locks: BTreeMap::new(),
        }
    }

    /// Acquires a lock on a resource.
    pub fn acquire(&mut self, resource: &str, tx_id: u64, lock_type: LockType) -> Result<()> {
        let state = self.locks.entry(resource.to_string()).or_insert_with(LockState::new);

        match lock_type {
            LockType::Shared => {
                if state.can_grant_shared(tx_id) {
                    state.shared_holders.insert(tx_id);
                    Ok(())
                } else {
                    Err(Error::invalid_operation("Cannot acquire shared lock"))
                }
            }
            LockType::Exclusive => {
                if state.can_grant_exclusive(tx_id) {
                    // Upgrade from shared if needed
                    state.shared_holders.remove(&tx_id);
                    state.exclusive_holder = Some(tx_id);
                    Ok(())
                } else {
                    Err(Error::invalid_operation("Cannot acquire exclusive lock"))
                }
            }
        }
    }

    /// Releases all locks held by a transaction.
    pub fn release_all(&mut self, tx_id: u64) {
        for state in self.locks.values_mut() {
            state.shared_holders.remove(&tx_id);
            if state.exclusive_holder == Some(tx_id) {
                state.exclusive_holder = None;
            }
        }

        // Clean up empty lock states
        self.locks.retain(|_, state| !state.is_free());
    }

    /// Releases a specific lock.
    pub fn release(&mut self, resource: &str, tx_id: u64) {
        if let Some(state) = self.locks.get_mut(resource) {
            state.shared_holders.remove(&tx_id);
            if state.exclusive_holder == Some(tx_id) {
                state.exclusive_holder = None;
            }

            if state.is_free() {
                self.locks.remove(resource);
            }
        }
    }

    /// Checks if a transaction holds a lock on a resource.
    pub fn holds_lock(&self, resource: &str, tx_id: u64) -> bool {
        if let Some(state) = self.locks.get(resource) {
            state.shared_holders.contains(&tx_id) || state.exclusive_holder == Some(tx_id)
        } else {
            false
        }
    }

    /// Checks if a transaction holds an exclusive lock on a resource.
    pub fn holds_exclusive(&self, resource: &str, tx_id: u64) -> bool {
        if let Some(state) = self.locks.get(resource) {
            state.exclusive_holder == Some(tx_id)
        } else {
            false
        }
    }

    /// Returns all resources locked by a transaction.
    pub fn get_locked_resources(&self, tx_id: u64) -> Vec<&str> {
        self.locks
            .iter()
            .filter(|(_, state)| {
                state.shared_holders.contains(&tx_id) || state.exclusive_holder == Some(tx_id)
            })
            .map(|(name, _)| name.as_str())
            .collect()
    }
}

impl Default for LockManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_acquire_shared_lock() {
        let mut lm = LockManager::new();

        assert!(lm.acquire("table1", 1, LockType::Shared).is_ok());
        assert!(lm.holds_lock("table1", 1));
    }

    #[test]
    fn test_acquire_exclusive_lock() {
        let mut lm = LockManager::new();

        assert!(lm.acquire("table1", 1, LockType::Exclusive).is_ok());
        assert!(lm.holds_exclusive("table1", 1));
    }

    #[test]
    fn test_multiple_shared_locks() {
        let mut lm = LockManager::new();

        assert!(lm.acquire("table1", 1, LockType::Shared).is_ok());
        assert!(lm.acquire("table1", 2, LockType::Shared).is_ok());
        assert!(lm.holds_lock("table1", 1));
        assert!(lm.holds_lock("table1", 2));
    }

    #[test]
    fn test_exclusive_blocks_shared() {
        let mut lm = LockManager::new();

        assert!(lm.acquire("table1", 1, LockType::Exclusive).is_ok());
        assert!(lm.acquire("table1", 2, LockType::Shared).is_err());
    }

    #[test]
    fn test_shared_blocks_exclusive() {
        let mut lm = LockManager::new();

        assert!(lm.acquire("table1", 1, LockType::Shared).is_ok());
        assert!(lm.acquire("table1", 2, LockType::Exclusive).is_err());
    }

    #[test]
    fn test_upgrade_shared_to_exclusive() {
        let mut lm = LockManager::new();

        assert!(lm.acquire("table1", 1, LockType::Shared).is_ok());
        assert!(lm.acquire("table1", 1, LockType::Exclusive).is_ok());
        assert!(lm.holds_exclusive("table1", 1));
    }

    #[test]
    fn test_release_lock() {
        let mut lm = LockManager::new();

        lm.acquire("table1", 1, LockType::Exclusive).unwrap();
        lm.release("table1", 1);

        assert!(!lm.holds_lock("table1", 1));
        assert!(lm.acquire("table1", 2, LockType::Exclusive).is_ok());
    }

    #[test]
    fn test_release_all() {
        let mut lm = LockManager::new();

        lm.acquire("table1", 1, LockType::Shared).unwrap();
        lm.acquire("table2", 1, LockType::Exclusive).unwrap();

        lm.release_all(1);

        assert!(!lm.holds_lock("table1", 1));
        assert!(!lm.holds_lock("table2", 1));
    }

    #[test]
    fn test_get_locked_resources() {
        let mut lm = LockManager::new();

        lm.acquire("table1", 1, LockType::Shared).unwrap();
        lm.acquire("table2", 1, LockType::Exclusive).unwrap();
        lm.acquire("table3", 2, LockType::Shared).unwrap();

        let resources = lm.get_locked_resources(1);
        assert_eq!(resources.len(), 2);
        assert!(resources.contains(&"table1"));
        assert!(resources.contains(&"table2"));
    }
}
