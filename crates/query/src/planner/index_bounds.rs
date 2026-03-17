//! Index bounds shared by logical and physical index scans.

use alloc::vec::Vec;
use cynos_core::Value;
use cynos_index::KeyRange;

/// Bounds for an index scan.
#[derive(Clone, Debug, PartialEq)]
pub enum IndexBounds {
    /// Full index scan.
    Unbounded,
    /// Scalar key range, used by single-column indexes.
    Scalar(KeyRange<Value>),
    /// Tuple key range, used by composite indexes.
    Composite(KeyRange<Vec<Value>>),
}

impl Default for IndexBounds {
    fn default() -> Self {
        Self::Unbounded
    }
}

impl IndexBounds {
    /// Returns an unbounded full-index scan.
    pub fn all() -> Self {
        Self::Unbounded
    }

    /// Builds scalar bounds from start/end parameters.
    pub fn from_scalar_range(
        range_start: Option<Value>,
        range_end: Option<Value>,
        include_start: bool,
        include_end: bool,
    ) -> Self {
        match (range_start, range_end) {
            (Some(start), Some(end)) => Self::Scalar(KeyRange::bound(
                start,
                end,
                !include_start,
                !include_end,
            )),
            (Some(start), None) => Self::Scalar(KeyRange::lower_bound(start, !include_start)),
            (None, Some(end)) => Self::Scalar(KeyRange::upper_bound(end, !include_end)),
            (None, None) => Self::Unbounded,
        }
    }

    /// Returns the scalar range if these are scalar bounds.
    pub fn as_scalar(&self) -> Option<&KeyRange<Value>> {
        match self {
            Self::Scalar(range) => Some(range),
            _ => None,
        }
    }

    /// Returns the composite range if these are composite bounds.
    pub fn as_composite(&self) -> Option<&KeyRange<Vec<Value>>> {
        match self {
            Self::Composite(range) => Some(range),
            _ => None,
        }
    }

    /// Returns true when the scan is unbounded.
    pub fn is_unbounded(&self) -> bool {
        matches!(self, Self::Unbounded)
    }
}
