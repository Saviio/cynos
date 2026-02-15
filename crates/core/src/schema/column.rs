//! Column definition for Cynos database schema.

use crate::types::DataType;
use crate::value::Value;
use alloc::string::String;

/// A column definition in a table schema.
#[derive(Clone, Debug)]
pub struct Column {
    /// Column name.
    name: String,
    /// Data type of the column.
    data_type: DataType,
    /// Whether this column allows null values.
    nullable: bool,
    /// Whether values in this column must be unique.
    unique: bool,
    /// Default value for this column.
    default_value: Option<Value>,
    /// Column index in the table (0-based).
    index: usize,
}

impl Column {
    /// Creates a new column definition.
    pub fn new(name: impl Into<String>, data_type: DataType) -> Self {
        let name = name.into();
        let nullable = data_type.is_nullable_by_default();
        Self {
            name,
            data_type,
            nullable,
            unique: false,
            default_value: None,
            index: 0,
        }
    }

    /// Sets whether this column is nullable.
    pub fn nullable(mut self, nullable: bool) -> Self {
        self.nullable = nullable;
        self
    }

    /// Sets whether this column has unique values.
    pub fn unique(mut self, unique: bool) -> Self {
        self.unique = unique;
        self
    }

    /// Sets the default value for this column.
    pub fn default_value(mut self, value: Value) -> Self {
        self.default_value = Some(value);
        self
    }

    /// Sets the column index.
    pub(crate) fn with_index(mut self, index: usize) -> Self {
        self.index = index;
        self
    }

    /// Returns the column name.
    #[inline]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the data type.
    #[inline]
    pub fn data_type(&self) -> DataType {
        self.data_type
    }

    /// Returns whether this column is nullable.
    #[inline]
    pub fn is_nullable(&self) -> bool {
        self.nullable
    }

    /// Returns whether this column has unique values.
    #[inline]
    pub fn is_unique(&self) -> bool {
        self.unique
    }

    /// Returns the default value for this column.
    pub fn get_default_value(&self) -> Value {
        self.default_value
            .clone()
            .unwrap_or_else(|| {
                if self.nullable {
                    Value::Null
                } else {
                    Value::default_for_type(self.data_type)
                }
            })
    }

    /// Returns the column index.
    #[inline]
    pub fn index(&self) -> usize {
        self.index
    }

    /// Returns whether this column can be used as an index key.
    #[inline]
    pub fn is_indexable(&self) -> bool {
        self.data_type.is_indexable()
    }
}

impl PartialEq for Column {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name && self.data_type == other.data_type
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_column_new() {
        let col = Column::new("id", DataType::Int64);
        assert_eq!(col.name(), "id");
        assert_eq!(col.data_type(), DataType::Int64);
        assert!(!col.is_nullable());
        assert!(!col.is_unique());
    }

    #[test]
    fn test_column_builder() {
        let col = Column::new("name", DataType::String)
            .nullable(true)
            .unique(true)
            .default_value(Value::String("unknown".into()));

        assert!(col.is_nullable());
        assert!(col.is_unique());
        assert_eq!(col.get_default_value(), Value::String("unknown".into()));
    }

    #[test]
    fn test_column_default_nullable() {
        let bytes_col = Column::new("data", DataType::Bytes);
        assert!(bytes_col.is_nullable());

        let jsonb_col = Column::new("meta", DataType::Jsonb);
        assert!(jsonb_col.is_nullable());

        let int_col = Column::new("count", DataType::Int32);
        assert!(!int_col.is_nullable());
    }

    #[test]
    fn test_column_indexable() {
        assert!(Column::new("id", DataType::Int64).is_indexable());
        assert!(Column::new("name", DataType::String).is_indexable());
        assert!(!Column::new("data", DataType::Bytes).is_indexable());
        assert!(!Column::new("meta", DataType::Jsonb).is_indexable());
    }
}
