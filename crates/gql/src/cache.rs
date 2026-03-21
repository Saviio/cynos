use alloc::string::String;
use cynos_storage::TableCache;

use crate::catalog::GraphqlCatalog;
use crate::schema::{render_schema_sdl, GraphqlSchema};

#[derive(Clone, Debug, Default)]
pub struct SchemaCache {
    epoch: Option<u64>,
    catalog: Option<GraphqlCatalog>,
    schema: Option<GraphqlSchema>,
    sdl: Option<String>,
}

impl SchemaCache {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn clear(&mut self) {
        self.epoch = None;
        self.catalog = None;
        self.schema = None;
        self.sdl = None;
    }

    pub fn catalog(&mut self, epoch: u64, cache: &TableCache) -> GraphqlCatalog {
        if self.epoch != Some(epoch) || self.catalog.is_none() {
            self.epoch = Some(epoch);
            self.catalog = Some(GraphqlCatalog::from_table_cache(cache));
            self.schema = None;
            self.sdl = None;
        }

        self.catalog
            .clone()
            .unwrap_or_else(|| GraphqlCatalog::from_table_cache(cache))
    }

    pub fn schema(&mut self, epoch: u64, cache: &TableCache) -> GraphqlSchema {
        if self.epoch != Some(epoch) {
            let schema = GraphqlSchema::from_table_cache(cache);
            let sdl = schema.to_sdl();
            self.epoch = Some(epoch);
            self.catalog = None;
            self.schema = Some(schema.clone());
            self.sdl = Some(sdl);
        }

        self.schema
            .clone()
            .unwrap_or_else(|| GraphqlSchema::from_table_cache(cache))
    }

    pub fn sdl(&mut self, epoch: u64, cache: &TableCache) -> String {
        if self.epoch != Some(epoch) {
            let schema = GraphqlSchema::from_table_cache(cache);
            let sdl = schema.to_sdl();
            self.epoch = Some(epoch);
            self.catalog = None;
            self.schema = Some(schema);
            self.sdl = Some(sdl.clone());
            return sdl;
        }

        self.sdl.clone().unwrap_or_else(|| render_schema_sdl(cache))
    }
}
