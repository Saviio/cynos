//! SchemaLayout cache for avoiding repeated layout computation.
//!
//! Full-table layouts are cached by table name.
//! Projection layouts are created fresh each time (or could be cached by column signature).

use super::SchemaLayout;
use alloc::string::String;
use cynos_core::schema::Table;
use hashbrown::HashMap;

/// Cache for SchemaLayout instances.
///
/// - Full-table queries: cached by table name
/// - Projection queries: created fresh (column combinations are too varied to cache effectively)
#[derive(Default)]
pub struct SchemaLayoutCache {
    /// Cache for full-table layouts, keyed by table name
    full_table_layouts: HashMap<String, SchemaLayout>,
}

impl SchemaLayoutCache {
    /// Create a new empty cache
    pub fn new() -> Self {
        Self {
            full_table_layouts: HashMap::new(),
        }
    }

    /// Get or create a SchemaLayout for a full-table query.
    /// The layout is cached and reused for subsequent queries on the same table.
    #[inline]
    pub fn get_or_create_full(&mut self, table_name: &str, schema: &Table) -> &SchemaLayout {
        self.full_table_layouts
            .entry(table_name.into())
            .or_insert_with(|| SchemaLayout::from_schema(schema))
    }

    /// Create a SchemaLayout for a projection query.
    /// Projection layouts are not cached since column combinations vary too much.
    #[inline]
    pub fn create_projection(schema: &Table, column_names: &[String]) -> SchemaLayout {
        SchemaLayout::from_projection(schema, column_names)
    }

    /// Invalidate cache for a specific table (call when schema changes)
    pub fn invalidate(&mut self, table_name: &str) {
        self.full_table_layouts.remove(table_name);
    }

    /// Clear all cached layouts
    pub fn clear(&mut self) {
        self.full_table_layouts.clear();
    }
}
