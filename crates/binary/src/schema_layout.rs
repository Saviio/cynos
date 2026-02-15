//! Schema layout for binary encoding.
//!
//! Pre-computes offsets and sizes for efficient binary encoding/decoding.

use super::BinaryDataType;
use alloc::string::{String, ToString};
use alloc::vec::Vec;
use cynos_core::schema::Table;
#[cfg(feature = "wasm")]
use wasm_bindgen::prelude::*;

/// Layout information for a single column
#[derive(Debug, Clone)]
pub struct ColumnLayout {
    /// Column name
    pub name: String,
    /// Binary data type
    pub data_type: BinaryDataType,
    /// Fixed size in bytes
    pub fixed_size: usize,
    /// Whether this column is nullable
    pub is_nullable: bool,
    /// Offset within the row (after null_mask)
    pub offset: usize,
}

/// Pre-computed layout for binary encoding/decoding
#[cfg_attr(feature = "wasm", wasm_bindgen)]
#[derive(Debug, Clone)]
pub struct SchemaLayout {
    columns: Vec<ColumnLayout>,
    /// Total bytes per row (null_mask + all columns)
    row_stride: usize,
    /// Size of null_mask in bytes: ceil(cols/8)
    null_mask_size: usize,
}

impl SchemaLayout {
    /// Create a SchemaLayout directly from components
    pub fn new(columns: Vec<ColumnLayout>, row_stride: usize, null_mask_size: usize) -> Self {
        Self {
            columns,
            row_stride,
            null_mask_size,
        }
    }

    /// Create a SchemaLayout from a table schema
    pub fn from_schema(schema: &Table) -> Self {
        let columns: Vec<ColumnLayout> = schema
            .columns()
            .iter()
            .scan(0usize, |offset, col| {
                let data_type = BinaryDataType::from(col.data_type());
                let fixed_size = data_type.fixed_size();
                let layout = ColumnLayout {
                    name: col.name().to_string(),
                    data_type,
                    fixed_size,
                    is_nullable: col.is_nullable(),
                    offset: *offset,
                };
                *offset += fixed_size;
                Some(layout)
            })
            .collect();

        let null_mask_size = (columns.len() + 7) / 8;
        let data_size: usize = columns.iter().map(|c| c.fixed_size).sum();
        let row_stride = null_mask_size + data_size;

        Self {
            columns,
            row_stride,
            null_mask_size,
        }
    }

    /// Create a SchemaLayout by merging multiple table schemas (for JOIN results).
    /// Columns are concatenated in order: left table columns, then right table columns, etc.
    /// Right-side columns are marked nullable since LEFT JOIN can produce NULLs.
    pub fn from_schemas(schemas: &[&Table]) -> Self {
        let columns: Vec<ColumnLayout> = schemas
            .iter()
            .enumerate()
            .flat_map(|(table_idx, schema)| {
                schema.columns().iter().map(move |col| (table_idx, col))
            })
            .scan(0usize, |offset, (table_idx, col)| {
                let data_type = BinaryDataType::from(col.data_type());
                let fixed_size = data_type.fixed_size();
                let layout = ColumnLayout {
                    name: col.name().to_string(),
                    data_type,
                    fixed_size,
                    // Right-side tables in a JOIN can have NULLs (LEFT JOIN)
                    is_nullable: table_idx > 0 || col.is_nullable(),
                    offset: *offset,
                };
                *offset += fixed_size;
                Some(layout)
            })
            .collect();

        let null_mask_size = (columns.len() + 7) / 8;
        let data_size: usize = columns.iter().map(|c| c.fixed_size).sum();
        let row_stride = null_mask_size + data_size;

        Self {
            columns,
            row_stride,
            null_mask_size,
        }
    }

    /// Create a SchemaLayout from projected columns
    pub fn from_projection(schema: &Table, column_names: &[String]) -> Self {
        let columns: Vec<ColumnLayout> = column_names
            .iter()
            .scan(0usize, |offset, name| {
                let col = schema.get_column(name)?;
                let data_type = BinaryDataType::from(col.data_type());
                let fixed_size = data_type.fixed_size();
                let layout = ColumnLayout {
                    name: col.name().to_string(),
                    data_type,
                    fixed_size,
                    is_nullable: col.is_nullable(),
                    offset: *offset,
                };
                *offset += fixed_size;
                Some(layout)
            })
            .collect();

        let null_mask_size = (columns.len() + 7) / 8;
        let data_size: usize = columns.iter().map(|c| c.fixed_size).sum();
        let row_stride = null_mask_size + data_size;

        Self {
            columns,
            row_stride,
            null_mask_size,
        }
    }

    /// Get the columns
    pub fn columns(&self) -> &[ColumnLayout] {
        &self.columns
    }

    /// Get row stride (total bytes per row)
    pub fn row_stride(&self) -> usize {
        self.row_stride
    }

    /// Get null mask size in bytes
    pub fn null_mask_size(&self) -> usize {
        self.null_mask_size
    }

    /// Calculate required buffer size for N rows (header + fixed section only)
    pub fn calculate_fixed_size(&self, row_count: usize) -> usize {
        super::HEADER_SIZE + self.row_stride * row_count
    }
}

// WASM bindings for JS access
#[cfg(feature = "wasm")]
#[wasm_bindgen]
impl SchemaLayout {
    /// Get the number of columns
    #[wasm_bindgen(js_name = columnCount)]
    pub fn column_count_js(&self) -> usize {
        self.columns.len()
    }

    /// Get column name by index
    #[wasm_bindgen(js_name = columnName)]
    pub fn column_name_js(&self, idx: usize) -> Option<String> {
        self.columns.get(idx).map(|c| c.name.clone())
    }

    /// Get column type by index (returns BinaryDataType as u8)
    #[wasm_bindgen(js_name = columnType)]
    pub fn column_type_js(&self, idx: usize) -> Option<u8> {
        self.columns.get(idx).map(|c| c.data_type as u8)
    }

    /// Get column offset by index (offset within row, after null_mask)
    #[wasm_bindgen(js_name = columnOffset)]
    pub fn column_offset_js(&self, idx: usize) -> Option<usize> {
        self.columns.get(idx).map(|c| c.offset)
    }

    /// Get column fixed size by index
    #[wasm_bindgen(js_name = columnFixedSize)]
    pub fn column_fixed_size_js(&self, idx: usize) -> Option<usize> {
        self.columns.get(idx).map(|c| c.fixed_size)
    }

    /// Check if column is nullable
    #[wasm_bindgen(js_name = columnNullable)]
    pub fn column_nullable_js(&self, idx: usize) -> Option<bool> {
        self.columns.get(idx).map(|c| c.is_nullable)
    }

    /// Get row stride (total bytes per row)
    #[wasm_bindgen(js_name = rowStride)]
    pub fn row_stride_js(&self) -> usize {
        self.row_stride
    }

    /// Get null mask size in bytes
    #[wasm_bindgen(js_name = nullMaskSize)]
    pub fn null_mask_size_js(&self) -> usize {
        self.null_mask_size
    }
}
