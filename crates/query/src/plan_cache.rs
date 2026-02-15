//! Query plan cache for avoiding repeated optimization.
//!
//! This module provides a simple LRU cache for compiled physical plans.
//! When the same logical plan is executed multiple times, the cached
//! physical plan can be reused, skipping the optimization phase.

use crate::ast::Expr;
use crate::planner::{LogicalPlan, PhysicalPlan};
use alloc::collections::BTreeMap;
use core::hash::Hasher;

/// A simple hasher for computing plan fingerprints.
/// Uses FNV-1a algorithm which is fast and has good distribution.
#[derive(Default)]
struct FnvHasher {
    state: u64,
}

impl FnvHasher {
    const FNV_OFFSET: u64 = 0xcbf29ce484222325;
    const FNV_PRIME: u64 = 0x100000001b3;

    fn new() -> Self {
        Self {
            state: Self::FNV_OFFSET,
        }
    }
}

impl Hasher for FnvHasher {
    fn finish(&self) -> u64 {
        self.state
    }

    fn write(&mut self, bytes: &[u8]) {
        for byte in bytes {
            self.state ^= *byte as u64;
            self.state = self.state.wrapping_mul(Self::FNV_PRIME);
        }
    }
}

/// Computes a fingerprint (hash) for a logical plan.
/// Plans with the same structure will have the same fingerprint.
pub fn compute_plan_fingerprint(plan: &LogicalPlan) -> u64 {
    let mut hasher = FnvHasher::new();
    hash_logical_plan(plan, &mut hasher);
    hasher.finish()
}

fn hash_logical_plan<H: Hasher>(plan: &LogicalPlan, hasher: &mut H) {
    match plan {
        LogicalPlan::Scan { table } => {
            hasher.write(b"scan");
            hasher.write(table.as_bytes());
        }
        LogicalPlan::IndexScan {
            table,
            index,
            range_start,
            range_end,
            include_start,
            include_end,
        } => {
            hasher.write(b"index_scan");
            hasher.write(table.as_bytes());
            hasher.write(index.as_bytes());
            if let Some(v) = range_start {
                hash_value(v, hasher);
            }
            if let Some(v) = range_end {
                hash_value(v, hasher);
            }
            hasher.write(&[*include_start as u8, *include_end as u8]);
        }
        LogicalPlan::IndexGet { table, index, key } => {
            hasher.write(b"index_get");
            hasher.write(table.as_bytes());
            hasher.write(index.as_bytes());
            hash_value(key, hasher);
        }
        LogicalPlan::IndexInGet { table, index, keys } => {
            hasher.write(b"index_in_get");
            hasher.write(table.as_bytes());
            hasher.write(index.as_bytes());
            for key in keys {
                hash_value(key, hasher);
            }
        }
        LogicalPlan::GinIndexScan {
            table,
            index,
            column,
            column_index,
            path,
            value,
            query_type,
        } => {
            hasher.write(b"gin_index_scan");
            hasher.write(table.as_bytes());
            hasher.write(index.as_bytes());
            hasher.write(column.as_bytes());
            hasher.write(&column_index.to_le_bytes());
            hasher.write(path.as_bytes());
            if let Some(v) = value {
                hash_value(v, hasher);
            }
            hasher.write(query_type.as_bytes());
        }
        LogicalPlan::GinIndexScanMulti {
            table,
            index,
            column,
            pairs,
        } => {
            hasher.write(b"gin_index_scan_multi");
            hasher.write(table.as_bytes());
            hasher.write(index.as_bytes());
            hasher.write(column.as_bytes());
            for (path, value) in pairs {
                hasher.write(path.as_bytes());
                hash_value(value, hasher);
            }
        }
        LogicalPlan::Filter { input, predicate } => {
            hasher.write(b"filter");
            hash_logical_plan(input, hasher);
            hash_expr(predicate, hasher);
        }
        LogicalPlan::Project { input, columns } => {
            hasher.write(b"project");
            hash_logical_plan(input, hasher);
            for col in columns {
                hash_expr(col, hasher);
            }
        }
        LogicalPlan::Join {
            left,
            right,
            condition,
            join_type,
        } => {
            hasher.write(b"join");
            hash_logical_plan(left, hasher);
            hash_logical_plan(right, hasher);
            hash_expr(condition, hasher);
            hasher.write(&[*join_type as u8]);
        }
        LogicalPlan::Aggregate {
            input,
            group_by,
            aggregates,
        } => {
            hasher.write(b"aggregate");
            hash_logical_plan(input, hasher);
            for col in group_by {
                hash_expr(col, hasher);
            }
            for (func, expr) in aggregates {
                hasher.write(&[*func as u8]);
                hash_expr(expr, hasher);
            }
        }
        LogicalPlan::Sort { input, order_by } => {
            hasher.write(b"sort");
            hash_logical_plan(input, hasher);
            for (expr, order) in order_by {
                hash_expr(expr, hasher);
                hasher.write(&[*order as u8]);
            }
        }
        LogicalPlan::Limit {
            input,
            limit,
            offset,
        } => {
            hasher.write(b"limit");
            hash_logical_plan(input, hasher);
            hasher.write(&limit.to_le_bytes());
            hasher.write(&offset.to_le_bytes());
        }
        LogicalPlan::CrossProduct { left, right } => {
            hasher.write(b"cross_product");
            hash_logical_plan(left, hasher);
            hash_logical_plan(right, hasher);
        }
        LogicalPlan::Union { left, right, .. } => {
            hasher.write(b"union");
            hash_logical_plan(left, hasher);
            hash_logical_plan(right, hasher);
        }
        LogicalPlan::Empty => {
            hasher.write(b"empty");
        }
    }
}

fn hash_expr<H: Hasher>(expr: &Expr, hasher: &mut H) {
    match expr {
        Expr::Column(col_ref) => {
            hasher.write(b"col");
            hasher.write(col_ref.table.as_bytes());
            hasher.write(col_ref.column.as_bytes());
            hasher.write(&col_ref.index.to_le_bytes());
        }
        Expr::Literal(v) => {
            hasher.write(b"lit");
            hash_value(v, hasher);
        }
        Expr::BinaryOp { left, op, right } => {
            hasher.write(b"binop");
            hasher.write(&[*op as u8]);
            hash_expr(left, hasher);
            hash_expr(right, hasher);
        }
        Expr::UnaryOp { op, expr } => {
            hasher.write(b"unop");
            hasher.write(&[*op as u8]);
            hash_expr(expr, hasher);
        }
        Expr::Function { name, args } => {
            hasher.write(b"func");
            hasher.write(name.as_bytes());
            for arg in args {
                hash_expr(arg, hasher);
            }
        }
        Expr::Aggregate {
            func,
            expr,
            distinct,
        } => {
            hasher.write(b"agg");
            hasher.write(&[*func as u8]);
            if let Some(e) = expr {
                hash_expr(e, hasher);
            }
            hasher.write(&[*distinct as u8]);
        }
        Expr::Between { expr, low, high } => {
            hasher.write(b"between");
            hash_expr(expr, hasher);
            hash_expr(low, hasher);
            hash_expr(high, hasher);
        }
        Expr::NotBetween { expr, low, high } => {
            hasher.write(b"not_between");
            hash_expr(expr, hasher);
            hash_expr(low, hasher);
            hash_expr(high, hasher);
        }
        Expr::In { expr, list } => {
            hasher.write(b"in");
            hash_expr(expr, hasher);
            for item in list {
                hash_expr(item, hasher);
            }
        }
        Expr::NotIn { expr, list } => {
            hasher.write(b"not_in");
            hash_expr(expr, hasher);
            for item in list {
                hash_expr(item, hasher);
            }
        }
        Expr::Like { expr, pattern } => {
            hasher.write(b"like");
            hash_expr(expr, hasher);
            hasher.write(pattern.as_bytes());
        }
        Expr::NotLike { expr, pattern } => {
            hasher.write(b"not_like");
            hash_expr(expr, hasher);
            hasher.write(pattern.as_bytes());
        }
        Expr::Match { expr, pattern } => {
            hasher.write(b"match");
            hash_expr(expr, hasher);
            hasher.write(pattern.as_bytes());
        }
        Expr::NotMatch { expr, pattern } => {
            hasher.write(b"not_match");
            hash_expr(expr, hasher);
            hasher.write(pattern.as_bytes());
        }
    }
}

fn hash_value<H: Hasher>(value: &cynos_core::Value, hasher: &mut H) {
    use cynos_core::Value;
    match value {
        Value::Null => hasher.write(b"null"),
        Value::Boolean(b) => {
            hasher.write(b"bool");
            hasher.write(&[*b as u8]);
        }
        Value::Int32(i) => {
            hasher.write(b"i32");
            hasher.write(&i.to_le_bytes());
        }
        Value::Int64(i) => {
            hasher.write(b"i64");
            hasher.write(&i.to_le_bytes());
        }
        Value::Float64(f) => {
            hasher.write(b"f64");
            hasher.write(&f.to_le_bytes());
        }
        Value::String(s) => {
            hasher.write(b"str");
            hasher.write(s.as_bytes());
        }
        Value::DateTime(dt) => {
            hasher.write(b"dt");
            hasher.write(&dt.to_le_bytes());
        }
        Value::Bytes(b) => {
            hasher.write(b"bytes");
            hasher.write(b);
        }
        Value::Jsonb(j) => {
            hasher.write(b"jsonb");
            // Hash the debug representation for simplicity
            use alloc::format;
            let s = format!("{:?}", j);
            hasher.write(s.as_bytes());
        }
    }
}

/// Cache entry with access tracking for LRU eviction.
struct CacheEntry {
    plan: PhysicalPlan,
    last_access: u64,
}

/// LRU cache for compiled physical plans.
///
/// The cache stores physical plans keyed by their logical plan fingerprint.
/// When the cache is full, the least recently used entry is evicted.
pub struct PlanCache {
    /// Cached plans indexed by fingerprint.
    cache: BTreeMap<u64, CacheEntry>,
    /// Maximum number of entries.
    max_size: usize,
    /// Global access counter for LRU tracking.
    access_counter: u64,
    /// Cache statistics.
    hits: u64,
    misses: u64,
}

impl PlanCache {
    /// Creates a new plan cache with the given maximum size.
    pub fn new(max_size: usize) -> Self {
        Self {
            cache: BTreeMap::new(),
            max_size,
            access_counter: 0,
            hits: 0,
            misses: 0,
        }
    }

    /// Creates a plan cache with default size (64 entries).
    pub fn default_size() -> Self {
        Self::new(64)
    }

    /// Gets a cached plan by fingerprint, or returns None if not cached.
    pub fn get(&mut self, fingerprint: u64) -> Option<&PhysicalPlan> {
        self.access_counter += 1;
        if let Some(entry) = self.cache.get_mut(&fingerprint) {
            entry.last_access = self.access_counter;
            self.hits += 1;
            Some(&entry.plan)
        } else {
            self.misses += 1;
            None
        }
    }

    /// Inserts a plan into the cache.
    /// If the cache is full, evicts the least recently used entry.
    pub fn insert(&mut self, fingerprint: u64, plan: PhysicalPlan) {
        // Evict if necessary
        if self.cache.len() >= self.max_size {
            self.evict_lru();
        }

        self.access_counter += 1;
        self.cache.insert(
            fingerprint,
            CacheEntry {
                plan,
                last_access: self.access_counter,
            },
        );
    }

    /// Gets a cached plan or compiles and caches a new one.
    pub fn get_or_insert_with<F>(&mut self, fingerprint: u64, compile: F) -> &PhysicalPlan
    where
        F: FnOnce() -> PhysicalPlan,
    {
        self.access_counter += 1;

        if self.cache.contains_key(&fingerprint) {
            let entry = self.cache.get_mut(&fingerprint).unwrap();
            entry.last_access = self.access_counter;
            self.hits += 1;
            &self.cache.get(&fingerprint).unwrap().plan
        } else {
            self.misses += 1;

            // Evict if necessary
            if self.cache.len() >= self.max_size {
                self.evict_lru();
            }

            let plan = compile();
            self.cache.insert(
                fingerprint,
                CacheEntry {
                    plan,
                    last_access: self.access_counter,
                },
            );
            &self.cache.get(&fingerprint).unwrap().plan
        }
    }

    /// Evicts the least recently used entry.
    fn evict_lru(&mut self) {
        if self.cache.is_empty() {
            return;
        }

        // Find the entry with the smallest last_access
        let lru_key = self
            .cache
            .iter()
            .min_by_key(|(_, entry)| entry.last_access)
            .map(|(k, _)| *k);

        if let Some(key) = lru_key {
            self.cache.remove(&key);
        }
    }

    /// Clears the cache.
    pub fn clear(&mut self) {
        self.cache.clear();
        self.hits = 0;
        self.misses = 0;
    }

    /// Returns the number of cached plans.
    pub fn len(&self) -> usize {
        self.cache.len()
    }

    /// Returns true if the cache is empty.
    pub fn is_empty(&self) -> bool {
        self.cache.is_empty()
    }

    /// Returns cache hit count.
    pub fn hits(&self) -> u64 {
        self.hits
    }

    /// Returns cache miss count.
    pub fn misses(&self) -> u64 {
        self.misses
    }

    /// Returns cache hit rate (0.0 to 1.0).
    pub fn hit_rate(&self) -> f64 {
        let total = self.hits + self.misses;
        if total == 0 {
            0.0
        } else {
            self.hits as f64 / total as f64
        }
    }

    /// Invalidates all cached plans for a specific table.
    /// Call this when table schema or data changes significantly.
    pub fn invalidate_table(&mut self, _table: &str) {
        // For simplicity, clear the entire cache.
        // A more sophisticated implementation could track which plans
        // reference which tables and only invalidate those.
        self.cache.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloc::boxed::Box;
    use alloc::string::String;

    #[test]
    fn test_plan_fingerprint_same_plan() {
        let plan1 = LogicalPlan::Scan {
            table: "users".into(),
        };
        let plan2 = LogicalPlan::Scan {
            table: "users".into(),
        };

        assert_eq!(
            compute_plan_fingerprint(&plan1),
            compute_plan_fingerprint(&plan2)
        );
    }

    #[test]
    fn test_plan_fingerprint_different_plans() {
        let plan1 = LogicalPlan::Scan {
            table: "users".into(),
        };
        let plan2 = LogicalPlan::Scan {
            table: "orders".into(),
        };

        assert_ne!(
            compute_plan_fingerprint(&plan1),
            compute_plan_fingerprint(&plan2)
        );
    }

    #[test]
    fn test_plan_fingerprint_with_filter() {
        let plan1 = LogicalPlan::Filter {
            input: Box::new(LogicalPlan::Scan {
                table: "users".into(),
            }),
            predicate: Expr::eq(
                Expr::column("users", "id", 0),
                Expr::literal(cynos_core::Value::Int64(42)),
            ),
        };
        let plan2 = LogicalPlan::Filter {
            input: Box::new(LogicalPlan::Scan {
                table: "users".into(),
            }),
            predicate: Expr::eq(
                Expr::column("users", "id", 0),
                Expr::literal(cynos_core::Value::Int64(42)),
            ),
        };

        assert_eq!(
            compute_plan_fingerprint(&plan1),
            compute_plan_fingerprint(&plan2)
        );
    }

    #[test]
    fn test_plan_fingerprint_different_values() {
        let plan1 = LogicalPlan::Filter {
            input: Box::new(LogicalPlan::Scan {
                table: "users".into(),
            }),
            predicate: Expr::eq(
                Expr::column("users", "id", 0),
                Expr::literal(cynos_core::Value::Int64(42)),
            ),
        };
        let plan2 = LogicalPlan::Filter {
            input: Box::new(LogicalPlan::Scan {
                table: "users".into(),
            }),
            predicate: Expr::eq(
                Expr::column("users", "id", 0),
                Expr::literal(cynos_core::Value::Int64(43)),
            ),
        };

        assert_ne!(
            compute_plan_fingerprint(&plan1),
            compute_plan_fingerprint(&plan2)
        );
    }

    #[test]
    fn test_cache_basic() {
        let mut cache = PlanCache::new(10);

        let table: String = "users".into();
        let plan = PhysicalPlan::table_scan(table);
        let fingerprint = 12345u64;

        cache.insert(fingerprint, plan);

        assert!(cache.get(fingerprint).is_some());
        assert!(cache.get(99999).is_none());
    }

    #[test]
    fn test_cache_lru_eviction() {
        let mut cache = PlanCache::new(2);

        let t1: String = "t1".into();
        let t2: String = "t2".into();
        let t3: String = "t3".into();

        cache.insert(1, PhysicalPlan::table_scan(t1));
        cache.insert(2, PhysicalPlan::table_scan(t2));

        // Access entry 1 to make it more recently used
        cache.get(1);

        // Insert entry 3, should evict entry 2 (LRU)
        cache.insert(3, PhysicalPlan::table_scan(t3));

        assert!(cache.get(1).is_some());
        assert!(cache.get(2).is_none()); // Evicted
        assert!(cache.get(3).is_some());
    }

    #[test]
    fn test_cache_stats() {
        let mut cache = PlanCache::new(10);

        let t1: String = "t1".into();
        cache.insert(1, PhysicalPlan::table_scan(t1));

        cache.get(1); // Hit
        cache.get(1); // Hit
        cache.get(2); // Miss

        assert_eq!(cache.hits(), 2);
        assert_eq!(cache.misses(), 1);
    }

    #[test]
    fn test_cache_get_or_insert() {
        let mut cache = PlanCache::new(10);

        let fingerprint = 12345u64;
        let mut compile_count = 0;

        // First call should compile
        let _ = cache.get_or_insert_with(fingerprint, || {
            compile_count += 1;
            let table: String = "users".into();
            PhysicalPlan::table_scan(table)
        });

        // Second call should use cache
        let _ = cache.get_or_insert_with(fingerprint, || {
            compile_count += 1;
            let table: String = "users".into();
            PhysicalPlan::table_scan(table)
        });

        assert_eq!(compile_count, 1);
        assert_eq!(cache.hits(), 1);
        assert_eq!(cache.misses(), 1);
    }
}
