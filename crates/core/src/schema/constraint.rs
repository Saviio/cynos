//! Constraint definitions for Cynos database schema.

use super::index::IndexDef;
use alloc::string::String;
use alloc::vec::Vec;

/// Foreign key action on update/delete.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Default)]
pub enum ConstraintAction {
    /// Reject the operation if it would violate the constraint.
    #[default]
    Restrict,
    /// Cascade the operation to related rows.
    Cascade,
}

/// Constraint timing for evaluation.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Default)]
pub enum ConstraintTiming {
    /// Evaluate constraint immediately.
    #[default]
    Immediate,
    /// Defer constraint evaluation until transaction commit.
    Deferrable,
}

/// Foreign key specification.
#[derive(Clone, Debug)]
pub struct ForeignKey {
    /// Constraint name.
    pub name: String,
    /// Child table name.
    pub child_table: String,
    /// Child column name.
    pub child_column: String,
    /// Parent table name.
    pub parent_table: String,
    /// Parent column name.
    pub parent_column: String,
    /// Action on parent row update/delete.
    pub action: ConstraintAction,
    /// When to evaluate the constraint.
    pub timing: ConstraintTiming,
}

impl ForeignKey {
    /// Creates a new foreign key specification.
    pub fn new(
        name: impl Into<String>,
        child_table: impl Into<String>,
        child_column: impl Into<String>,
        parent_table: impl Into<String>,
        parent_column: impl Into<String>,
    ) -> Self {
        Self {
            name: name.into(),
            child_table: child_table.into(),
            child_column: child_column.into(),
            parent_table: parent_table.into(),
            parent_column: parent_column.into(),
            action: ConstraintAction::Restrict,
            timing: ConstraintTiming::Immediate,
        }
    }

    /// Sets the constraint action.
    pub fn action(mut self, action: ConstraintAction) -> Self {
        self.action = action;
        self
    }

    /// Sets the constraint timing.
    pub fn timing(mut self, timing: ConstraintTiming) -> Self {
        self.timing = timing;
        self
    }
}

/// Table constraints container.
#[derive(Clone, Debug, Default)]
pub struct Constraints {
    /// Primary key index (if any).
    primary_key: Option<IndexDef>,
    /// Columns that cannot be null.
    not_nullable: Vec<String>,
    /// Foreign key constraints.
    foreign_keys: Vec<ForeignKey>,
}

impl Constraints {
    /// Creates a new empty constraints container.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the primary key.
    pub fn primary_key(mut self, pk: IndexDef) -> Self {
        self.primary_key = Some(pk);
        self
    }

    /// Adds a not-nullable column.
    pub fn add_not_nullable(mut self, column: impl Into<String>) -> Self {
        self.not_nullable.push(column.into());
        self
    }

    /// Sets the not-nullable columns.
    pub fn not_nullable(mut self, columns: Vec<String>) -> Self {
        self.not_nullable = columns;
        self
    }

    /// Adds a foreign key constraint.
    pub fn add_foreign_key(mut self, fk: ForeignKey) -> Self {
        self.foreign_keys.push(fk);
        self
    }

    /// Returns the primary key index.
    pub fn get_primary_key(&self) -> Option<&IndexDef> {
        self.primary_key.as_ref()
    }

    /// Returns the not-nullable columns.
    pub fn get_not_nullable(&self) -> &[String] {
        &self.not_nullable
    }

    /// Returns the foreign keys.
    pub fn get_foreign_keys(&self) -> &[ForeignKey] {
        &self.foreign_keys
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::index::IndexedColumn;
    use alloc::vec;

    #[test]
    fn test_foreign_key() {
        let fk = ForeignKey::new(
            "fk_order_user",
            "orders",
            "user_id",
            "users",
            "id",
        )
        .action(ConstraintAction::Cascade)
        .timing(ConstraintTiming::Immediate);

        assert_eq!(fk.name, "fk_order_user");
        assert_eq!(fk.child_table, "orders");
        assert_eq!(fk.child_column, "user_id");
        assert_eq!(fk.parent_table, "users");
        assert_eq!(fk.parent_column, "id");
        assert_eq!(fk.action, ConstraintAction::Cascade);
    }

    #[test]
    fn test_constraints() {
        let pk = IndexDef::new(
            "pk_users",
            "users",
            vec![IndexedColumn::new("id").auto_increment(true)],
        )
        .unique(true);

        let constraints = Constraints::new()
            .primary_key(pk)
            .add_not_nullable("name")
            .add_not_nullable("email");

        assert!(constraints.get_primary_key().is_some());
        assert_eq!(constraints.get_not_nullable().len(), 2);
    }
}
