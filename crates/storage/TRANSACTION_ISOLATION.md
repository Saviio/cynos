# Transaction Isolation Design Document

## Current Implementation

The current transaction uses a "direct write + rollback" model:
- Changes are written directly to `TableCache`
- `Journal` records all operations for rollback
- Works correctly in single-threaded scenarios

**Limitation**: Does not support isolation between concurrent transactions. Uncommitted changes from transaction A are visible to transaction B.

## Option 1: Snapshot Isolation (Recommended)

### Design

```rust
pub struct Transaction {
    id: TransactionId,
    journal: Journal,
    snapshot: TableCache,  // Complete snapshot at transaction start
    state: TransactionState,
}

impl Transaction {
    pub fn begin(cache: &TableCache) -> Self {
        Self {
            id: NEXT_TX_ID.fetch_add(1, Ordering::SeqCst),
            journal: Journal::new(),
            snapshot: cache.clone(),  // Clone entire cache
            state: TransactionState::Active,
        }
    }

    // All operations execute on snapshot
    pub fn insert(&mut self, table: &str, row: Row) -> Result<RowId> {
        let store = self.snapshot.get_table_mut(table)?;
        let row_id = store.insert(row.clone())?;
        self.journal.record_insert(table, row);
        Ok(row_id)
    }

    // Merge back to main cache on commit
    pub fn commit(self, cache: &mut TableCache) -> Result<Vec<JournalEntry>> {
        // Conflict detection (optional)
        self.detect_conflicts(cache)?;

        // Apply changes to main cache
        for entry in self.journal.get_entries() {
            match entry {
                JournalEntry::Insert { table, row, .. } => {
                    cache.get_table_mut(table)?.insert(row.clone())?;
                }
                // ... update, delete
            }
        }
        Ok(self.journal.commit())
    }

    // Rollback just discards snapshot
    pub fn rollback(self) -> Result<()> {
        // snapshot auto-drops, no need to touch main cache
        Ok(())
    }
}
```

### Pros
- Simple implementation, ~100 lines of code
- Complete isolation, transactions don't affect each other
- Zero-cost rollback

### Cons
- Memory overhead: each transaction copies all data
- Not suitable for large datasets or long transactions

### Use Cases
- Small datasets (< 10MB)
- Short transactions
- In-browser memory databases

---

## Option 2: MVCC (Multi-Version Concurrency Control)

### Design

```rust
/// Versioned row
struct VersionedRow {
    row: Row,
    created_tx: TransactionId,
    deleted_tx: Option<TransactionId>,
}

/// MVCC row store
pub struct MvccRowStore {
    schema: Table,
    rows: BTreeMap<RowId, Vec<VersionedRow>>,  // Version chain
    // ...
}

impl MvccRowStore {
    /// Get visible version based on transaction ID
    pub fn get_visible(&self, row_id: RowId, tx_id: TransactionId) -> Option<&Row> {
        self.rows.get(&row_id)?
            .iter()
            .rev()
            .find(|v| v.created_tx <= tx_id && v.deleted_tx.map_or(true, |d| d > tx_id))
            .map(|v| &v.row)
    }

    /// Insert new version
    pub fn insert(&mut self, row: Row, tx_id: TransactionId) -> Result<RowId> {
        let row_id = row.id();
        let versioned = VersionedRow {
            row,
            created_tx: tx_id,
            deleted_tx: None,
        };
        self.rows.entry(row_id).or_default().push(versioned);
        Ok(row_id)
    }

    /// Delete = mark deleted_tx
    pub fn delete(&mut self, row_id: RowId, tx_id: TransactionId) -> Result<()> {
        let versions = self.rows.get_mut(&row_id)?;
        if let Some(v) = versions.last_mut() {
            v.deleted_tx = Some(tx_id);
        }
        Ok(())
    }
}

/// Garbage collection
impl MvccRowStore {
    pub fn gc(&mut self, min_active_tx: TransactionId) {
        for versions in self.rows.values_mut() {
            versions.retain(|v| {
                v.deleted_tx.map_or(true, |d| d >= min_active_tx)
            });
        }
    }
}
```

### Pros
- Memory efficient (only stores changes)
- Supports high-concurrency read/write
- Supports long transactions

### Cons
- Complex implementation, ~500+ lines of code
- Requires garbage collection mechanism
- Complex index maintenance (needs version awareness)
- Complex write conflict detection logic

### Use Cases
- Large datasets
- High-concurrency writes
- Long transaction support needed

---

## Recommendations

1. **Current stage**: Keep existing implementation, sufficient for single-threaded scenarios
2. **If isolation needed**: Implement snapshot isolation first (Option 1)
3. **MVCC**: Only consider when high-concurrency writes are confirmed necessary

Snapshot isolation has low implementation cost and is sufficient for typical in-browser memory database use cases (small data, short transactions).
