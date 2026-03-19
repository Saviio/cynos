use core::hash::{Hash, Hasher};
use cynos_core::Value;

/// Borrowed `Value` wrapper that applies SQL-style equality/hash semantics.
#[derive(Clone, Copy, Debug)]
pub(crate) struct SqlValueRef<'a>(&'a Value);

impl<'a> SqlValueRef<'a> {
    #[inline]
    pub(crate) fn new(value: &'a Value) -> Self {
        Self(value)
    }
}

impl Hash for SqlValueRef<'_> {
    #[inline]
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.sql_hash(state);
    }
}

impl PartialEq for SqlValueRef<'_> {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        self.0.sql_eq(other.0)
    }
}

impl Eq for SqlValueRef<'_> {}
