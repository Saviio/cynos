//! Constraint checking for Cynos database.
//!
//! This module provides constraint validation including primary key,
//! unique, not-null, and foreign key constraints.

use crate::cache::TableCache;
use crate::row_store::RowStore;
use alloc::format;
use cynos_core::schema::{ConstraintTiming, Table};
use cynos_core::{Error, Result, Row, RowId};

/// Constraint checker for validating database constraints.
pub struct ConstraintChecker;

impl ConstraintChecker {
    /// Checks the not-null constraint for a row.
    pub fn check_not_null(schema: &Table, row: &Row) -> Result<()> {
        let not_nullable = schema.constraints().get_not_nullable();

        for col_name in not_nullable {
            if schema.get_column(col_name).is_some() {
                let col_idx = schema.get_column_index(col_name).unwrap();
                if let Some(value) = row.get(col_idx) {
                    if value.is_null() {
                        return Err(Error::NullConstraint {
                            column: col_name.clone(),
                        });
                    }
                }
            }
        }

        Ok(())
    }

    /// Checks the not-null constraint for multiple rows.
    pub fn check_not_null_rows(schema: &Table, rows: &[Row]) -> Result<()> {
        for row in rows {
            Self::check_not_null(schema, row)?;
        }
        Ok(())
    }

    /// Checks foreign key constraints for insert.
    pub fn check_foreign_keys_for_insert(
        cache: &TableCache,
        schema: &Table,
        rows: &[Row],
        timing: ConstraintTiming,
    ) -> Result<()> {
        let foreign_keys = schema.constraints().get_foreign_keys();

        for fk in foreign_keys {
            if fk.timing != timing {
                continue;
            }

            let parent_store = cache.get_table(&fk.parent_table).ok_or_else(|| {
                Error::table_not_found(&fk.parent_table)
            })?;

            let child_col_idx = schema.get_column_index(&fk.child_column).ok_or_else(|| {
                Error::column_not_found(schema.name(), &fk.child_column)
            })?;

            for row in rows {
                if let Some(value) = row.get(child_col_idx) {
                    if !value.is_null() && !parent_store.pk_exists(value) {
                        return Err(Error::ForeignKeyViolation {
                            constraint: fk.name.clone(),
                            message: format!(
                                "Referenced key {:?} does not exist in {}",
                                value, fk.parent_table
                            ),
                        });
                    }
                }
            }
        }

        Ok(())
    }

    /// Checks foreign key constraints for delete.
    pub fn check_foreign_keys_for_delete(
        cache: &TableCache,
        schema: &Table,
        rows: &[Row],
        timing: ConstraintTiming,
    ) -> Result<()> {
        // Find all tables that reference this table
        for table_name in cache.table_names() {
            if let Some(child_store) = cache.get_table(table_name) {
                let child_schema = child_store.schema();
                let foreign_keys = child_schema.constraints().get_foreign_keys();

                for fk in foreign_keys {
                    if fk.parent_table != schema.name() || fk.timing != timing {
                        continue;
                    }

                    let parent_col_idx = schema.get_column_index(&fk.parent_column).ok_or_else(|| {
                        Error::column_not_found(schema.name(), &fk.parent_column)
                    })?;

                    for row in rows {
                        if let Some(pk_value) = row.get(parent_col_idx) {
                            // Check if any child rows reference this value
                            let child_rows = child_store.get_by_pk(pk_value);
                            if !child_rows.is_empty() {
                                return Err(Error::ForeignKeyViolation {
                                    constraint: fk.name.clone(),
                                    message: format!(
                                        "Cannot delete: referenced by {} rows in {}",
                                        child_rows.len(),
                                        child_schema.name()
                                    ),
                                });
                            }
                        }
                    }
                }
            }
        }

        Ok(())
    }

    /// Checks foreign key constraints for update.
    pub fn check_foreign_keys_for_update(
        cache: &TableCache,
        schema: &Table,
        modifications: &[(Row, Row)],
        timing: ConstraintTiming,
    ) -> Result<()> {
        // Check if updated values still satisfy FK constraints (as child)
        let foreign_keys = schema.constraints().get_foreign_keys();

        for fk in foreign_keys {
            if fk.timing != timing {
                continue;
            }

            let parent_store = cache.get_table(&fk.parent_table).ok_or_else(|| {
                Error::table_not_found(&fk.parent_table)
            })?;

            let child_col_idx = schema.get_column_index(&fk.child_column).ok_or_else(|| {
                Error::column_not_found(schema.name(), &fk.child_column)
            })?;

            for (_, new_row) in modifications {
                if let Some(value) = new_row.get(child_col_idx) {
                    if !value.is_null() && !parent_store.pk_exists(value) {
                        return Err(Error::ForeignKeyViolation {
                            constraint: fk.name.clone(),
                            message: format!(
                                "Referenced key {:?} does not exist in {}",
                                value, fk.parent_table
                            ),
                        });
                    }
                }
            }
        }

        // Check if updated values break FK constraints (as parent)
        for table_name in cache.table_names() {
            if let Some(child_store) = cache.get_table(table_name) {
                let child_schema = child_store.schema();
                let child_fks = child_schema.constraints().get_foreign_keys();

                for fk in child_fks {
                    if fk.parent_table != schema.name() || fk.timing != timing {
                        continue;
                    }

                    let parent_col_idx = schema.get_column_index(&fk.parent_column).ok_or_else(|| {
                        Error::column_not_found(schema.name(), &fk.parent_column)
                    })?;

                    for (old_row, new_row) in modifications {
                        let old_value = old_row.get(parent_col_idx);
                        let new_value = new_row.get(parent_col_idx);

                        // If the referenced column value changed
                        if old_value != new_value {
                            if let Some(old_val) = old_value {
                                let child_rows = child_store.get_by_pk(old_val);
                                if !child_rows.is_empty() {
                                    return Err(Error::ForeignKeyViolation {
                                        constraint: fk.name.clone(),
                                        message: format!(
                                            "Cannot update: referenced by {} rows in {}",
                                            child_rows.len(),
                                            child_schema.name()
                                        ),
                                    });
                                }
                            }
                        }
                    }
                }
            }
        }

        Ok(())
    }

    /// Finds existing row ID by primary key in the store.
    pub fn find_existing_row_id_in_pk_index(store: &RowStore, row: &Row) -> Option<RowId> {
        store.find_row_id_by_pk(row)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cynos_core::schema::TableBuilder;
    use cynos_core::{DataType, Value};
    use alloc::vec;

    fn test_schema_with_not_null() -> Table {
        TableBuilder::new("test")
            .unwrap()
            .add_column("id", DataType::Int64)
            .unwrap()
            .add_column("name", DataType::String)
            .unwrap()
            .add_primary_key(&["id"], false)
            .unwrap()
            .build()
            .unwrap()
    }

    #[test]
    fn test_check_not_null_valid() {
        let schema = test_schema_with_not_null();
        let row = Row::new(1, vec![Value::Int64(1), Value::String("test".into())]);

        let result = ConstraintChecker::check_not_null(&schema, &row);
        assert!(result.is_ok());
    }

    #[test]
    fn test_check_not_null_violation() {
        let schema = test_schema_with_not_null();
        // id column is not nullable (part of PK)
        let row = Row::new(1, vec![Value::Null, Value::String("test".into())]);

        let result = ConstraintChecker::check_not_null(&schema, &row);
        assert!(result.is_err());
    }

    #[test]
    fn test_find_existing_row_id() {
        let schema = test_schema_with_not_null();
        let mut store = RowStore::new(schema.clone());

        let row = Row::new(1, vec![Value::Int64(100), Value::String("test".into())]);
        store.insert(row).unwrap();

        let search_row = Row::new(2, vec![Value::Int64(100), Value::String("other".into())]);
        let found = ConstraintChecker::find_existing_row_id_in_pk_index(&store, &search_row);
        assert_eq!(found, Some(1));

        let not_found_row = Row::new(3, vec![Value::Int64(999), Value::String("other".into())]);
        let not_found = ConstraintChecker::find_existing_row_id_in_pk_index(&store, &not_found_row);
        assert!(not_found.is_none());
    }

    // === FK Constraint Tests ===

    fn create_users_table() -> Table {
        TableBuilder::new("users")
            .unwrap()
            .add_column("id", DataType::Int64)
            .unwrap()
            .add_column("name", DataType::String)
            .unwrap()
            .add_primary_key(&["id"], false)
            .unwrap()
            .build()
            .unwrap()
    }

    fn create_orders_table_with_fk() -> Table {
        TableBuilder::new("orders")
            .unwrap()
            .add_column("id", DataType::Int64)
            .unwrap()
            .add_column("user_id", DataType::Int64)
            .unwrap()
            .add_column("amount", DataType::Int64)
            .unwrap()
            .add_primary_key(&["id"], false)
            .unwrap()
            .add_foreign_key("fk_orders_user", "user_id", "users", "id")
            .unwrap()
            .build()
            .unwrap()
    }

    #[test]
    fn test_fk_insert_valid() {
        let mut cache = TableCache::new();
        cache.create_table(create_users_table()).unwrap();
        cache.create_table(create_orders_table_with_fk()).unwrap();

        // Insert parent row first
        let user = Row::new(1, vec![Value::Int64(1), Value::String("Alice".into())]);
        cache.get_table_mut("users").unwrap().insert(user).unwrap();

        // Insert child row referencing existing parent
        let order = Row::new(1, vec![Value::Int64(1), Value::Int64(1), Value::Int64(100)]);
        let orders_schema = cache.get_table("orders").unwrap().schema().clone();

        let result = ConstraintChecker::check_foreign_keys_for_insert(
            &cache,
            &orders_schema,
            &[order],
            ConstraintTiming::Immediate,
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_fk_insert_violation() {
        let mut cache = TableCache::new();
        cache.create_table(create_users_table()).unwrap();
        cache.create_table(create_orders_table_with_fk()).unwrap();

        // Try to insert child row without parent
        let order = Row::new(1, vec![Value::Int64(1), Value::Int64(999), Value::Int64(100)]);
        let orders_schema = cache.get_table("orders").unwrap().schema().clone();

        let result = ConstraintChecker::check_foreign_keys_for_insert(
            &cache,
            &orders_schema,
            &[order],
            ConstraintTiming::Immediate,
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_fk_insert_null_allowed() {
        let mut cache = TableCache::new();
        cache.create_table(create_users_table()).unwrap();
        cache.create_table(create_orders_table_with_fk()).unwrap();

        // Insert child row with NULL FK (should be allowed)
        let order = Row::new(1, vec![Value::Int64(1), Value::Null, Value::Int64(100)]);
        let orders_schema = cache.get_table("orders").unwrap().schema().clone();

        let result = ConstraintChecker::check_foreign_keys_for_insert(
            &cache,
            &orders_schema,
            &[order],
            ConstraintTiming::Immediate,
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_fk_delete_with_children() {
        let mut cache = TableCache::new();
        cache.create_table(create_users_table()).unwrap();
        cache.create_table(create_orders_table_with_fk()).unwrap();

        // Insert parent
        let user = Row::new(1, vec![Value::Int64(1), Value::String("Alice".into())]);
        cache.get_table_mut("users").unwrap().insert(user.clone()).unwrap();

        // Insert child referencing parent
        let order = Row::new(1, vec![Value::Int64(1), Value::Int64(1), Value::Int64(100)]);
        cache.get_table_mut("orders").unwrap().insert(order).unwrap();

        // Try to delete parent - should fail
        let users_schema = cache.get_table("users").unwrap().schema().clone();
        let result = ConstraintChecker::check_foreign_keys_for_delete(
            &cache,
            &users_schema,
            &[user],
            ConstraintTiming::Immediate,
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_fk_delete_no_children() {
        let mut cache = TableCache::new();
        cache.create_table(create_users_table()).unwrap();
        cache.create_table(create_orders_table_with_fk()).unwrap();

        // Insert parent only
        let user = Row::new(1, vec![Value::Int64(1), Value::String("Alice".into())]);
        cache.get_table_mut("users").unwrap().insert(user.clone()).unwrap();

        // Delete parent - should succeed (no children)
        let users_schema = cache.get_table("users").unwrap().schema().clone();
        let result = ConstraintChecker::check_foreign_keys_for_delete(
            &cache,
            &users_schema,
            &[user],
            ConstraintTiming::Immediate,
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_fk_update_child_valid() {
        let mut cache = TableCache::new();
        cache.create_table(create_users_table()).unwrap();
        cache.create_table(create_orders_table_with_fk()).unwrap();

        // Insert two users
        let user1 = Row::new(1, vec![Value::Int64(1), Value::String("Alice".into())]);
        let user2 = Row::new(2, vec![Value::Int64(2), Value::String("Bob".into())]);
        cache.get_table_mut("users").unwrap().insert(user1).unwrap();
        cache.get_table_mut("users").unwrap().insert(user2).unwrap();

        // Insert order referencing user1
        let order = Row::new(1, vec![Value::Int64(1), Value::Int64(1), Value::Int64(100)]);
        cache.get_table_mut("orders").unwrap().insert(order.clone()).unwrap();

        // Update order to reference user2 - should succeed
        let updated_order = Row::new(1, vec![Value::Int64(1), Value::Int64(2), Value::Int64(100)]);
        let orders_schema = cache.get_table("orders").unwrap().schema().clone();

        let result = ConstraintChecker::check_foreign_keys_for_update(
            &cache,
            &orders_schema,
            &[(order, updated_order)],
            ConstraintTiming::Immediate,
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_fk_update_child_violation() {
        let mut cache = TableCache::new();
        cache.create_table(create_users_table()).unwrap();
        cache.create_table(create_orders_table_with_fk()).unwrap();

        // Insert one user
        let user = Row::new(1, vec![Value::Int64(1), Value::String("Alice".into())]);
        cache.get_table_mut("users").unwrap().insert(user).unwrap();

        // Insert order referencing user
        let order = Row::new(1, vec![Value::Int64(1), Value::Int64(1), Value::Int64(100)]);
        cache.get_table_mut("orders").unwrap().insert(order.clone()).unwrap();

        // Update order to reference non-existent user - should fail
        let updated_order = Row::new(1, vec![Value::Int64(1), Value::Int64(999), Value::Int64(100)]);
        let orders_schema = cache.get_table("orders").unwrap().schema().clone();

        let result = ConstraintChecker::check_foreign_keys_for_update(
            &cache,
            &orders_schema,
            &[(order, updated_order)],
            ConstraintTiming::Immediate,
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_fk_update_parent_with_children() {
        let mut cache = TableCache::new();
        cache.create_table(create_users_table()).unwrap();
        cache.create_table(create_orders_table_with_fk()).unwrap();

        // Insert parent
        let user = Row::new(1, vec![Value::Int64(1), Value::String("Alice".into())]);
        cache.get_table_mut("users").unwrap().insert(user.clone()).unwrap();

        // Insert child referencing parent
        let order = Row::new(1, vec![Value::Int64(1), Value::Int64(1), Value::Int64(100)]);
        cache.get_table_mut("orders").unwrap().insert(order).unwrap();

        // Try to update parent PK - should fail (has children)
        let updated_user = Row::new(1, vec![Value::Int64(999), Value::String("Alice".into())]);
        let users_schema = cache.get_table("users").unwrap().schema().clone();

        let result = ConstraintChecker::check_foreign_keys_for_update(
            &cache,
            &users_schema,
            &[(user, updated_user)],
            ConstraintTiming::Immediate,
        );
        assert!(result.is_err());
    }
}
