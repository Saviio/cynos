//! Incremental aggregate operators.

use crate::delta::Delta;
use cynos_core::{Row, Value};

/// Incremental COUNT aggregate.
///
/// Maintains a running count that is updated incrementally
/// as rows are inserted or deleted.
#[derive(Clone, Debug, Default)]
pub struct IncrementalCount {
    count: i64,
}

impl IncrementalCount {
    /// Creates a new incremental count starting at 0.
    pub fn new() -> Self {
        Self { count: 0 }
    }

    /// Creates a new incremental count with an initial value.
    pub fn with_initial(count: i64) -> Self {
        Self { count }
    }

    /// Applies a batch of deltas to update the count.
    pub fn apply<T>(&mut self, deltas: &[Delta<T>]) {
        for d in deltas {
            self.count += d.diff as i64;
        }
    }

    /// Returns the current count.
    #[inline]
    pub fn get(&self) -> i64 {
        self.count
    }

    /// Resets the count to 0.
    pub fn reset(&mut self) {
        self.count = 0;
    }
}

/// Incremental SUM aggregate.
///
/// Maintains a running sum that is updated incrementally.
#[derive(Clone, Debug)]
pub struct IncrementalSum {
    sum: f64,
    column: usize,
}

impl IncrementalSum {
    /// Creates a new incremental sum for the given column.
    pub fn new(column: usize) -> Self {
        Self { sum: 0.0, column }
    }

    /// Creates a new incremental sum with an initial value.
    pub fn with_initial(column: usize, sum: f64) -> Self {
        Self { sum, column }
    }

    /// Applies a batch of row deltas to update the sum.
    pub fn apply(&mut self, deltas: &[Delta<Row>]) {
        for d in deltas {
            if let Some(value) = d.data.get(self.column) {
                let num = extract_numeric(value);
                self.sum += num * d.diff as f64;
            }
        }
    }

    /// Returns the current sum.
    #[inline]
    pub fn get(&self) -> f64 {
        self.sum
    }

    /// Resets the sum to 0.
    pub fn reset(&mut self) {
        self.sum = 0.0;
    }
}

/// Incremental AVG aggregate.
///
/// Maintains both sum and count to compute average incrementally.
#[derive(Clone, Debug)]
pub struct IncrementalAvg {
    sum: f64,
    count: i64,
    column: usize,
}

impl IncrementalAvg {
    /// Creates a new incremental average for the given column.
    pub fn new(column: usize) -> Self {
        Self {
            sum: 0.0,
            count: 0,
            column,
        }
    }

    /// Applies a batch of row deltas to update the average.
    pub fn apply(&mut self, deltas: &[Delta<Row>]) {
        for d in deltas {
            if let Some(value) = d.data.get(self.column) {
                let num = extract_numeric(value);
                self.sum += num * d.diff as f64;
                self.count += d.diff as i64;
            }
        }
    }

    /// Returns the current average, or None if count is 0.
    pub fn get(&self) -> Option<f64> {
        if self.count == 0 {
            None
        } else {
            Some(self.sum / self.count as f64)
        }
    }

    /// Returns the current count.
    #[inline]
    pub fn count(&self) -> i64 {
        self.count
    }

    /// Returns the current sum.
    #[inline]
    pub fn sum(&self) -> f64 {
        self.sum
    }

    /// Resets the average.
    pub fn reset(&mut self) {
        self.sum = 0.0;
        self.count = 0;
    }
}

/// Incremental MIN aggregate.
///
/// Note: MIN is not fully incrementalizable - deletions may require
/// recomputation. This implementation tracks the current minimum
/// and a flag indicating if recomputation is needed.
#[derive(Clone, Debug)]
pub struct IncrementalMin {
    min: Option<Value>,
    column: usize,
    needs_recompute: bool,
}

impl IncrementalMin {
    /// Creates a new incremental min for the given column.
    pub fn new(column: usize) -> Self {
        Self {
            min: None,
            column,
            needs_recompute: false,
        }
    }

    /// Applies a batch of row deltas.
    ///
    /// For insertions, updates min if the new value is smaller.
    /// For deletions, marks for recomputation if the deleted value equals min.
    pub fn apply(&mut self, deltas: &[Delta<Row>]) {
        for d in deltas {
            if let Some(value) = d.data.get(self.column) {
                if d.is_insert() {
                    match &self.min {
                        None => self.min = Some(value.clone()),
                        Some(current) if value < current => {
                            self.min = Some(value.clone());
                        }
                        _ => {}
                    }
                } else if d.is_delete() {
                    if self.min.as_ref() == Some(value) {
                        self.needs_recompute = true;
                    }
                }
            }
        }
    }

    /// Returns the current minimum, or None if empty or needs recomputation.
    pub fn get(&self) -> Option<&Value> {
        if self.needs_recompute {
            None
        } else {
            self.min.as_ref()
        }
    }

    /// Returns true if the minimum needs to be recomputed from scratch.
    pub fn needs_recompute(&self) -> bool {
        self.needs_recompute
    }

    /// Recomputes the minimum from a full scan of values.
    pub fn recompute(&mut self, values: impl Iterator<Item = Value>) {
        self.min = values.min();
        self.needs_recompute = false;
    }

    /// Resets the min.
    pub fn reset(&mut self) {
        self.min = None;
        self.needs_recompute = false;
    }
}

/// Incremental MAX aggregate.
///
/// Similar to MIN, MAX is not fully incrementalizable for deletions.
#[derive(Clone, Debug)]
pub struct IncrementalMax {
    max: Option<Value>,
    column: usize,
    needs_recompute: bool,
}

impl IncrementalMax {
    /// Creates a new incremental max for the given column.
    pub fn new(column: usize) -> Self {
        Self {
            max: None,
            column,
            needs_recompute: false,
        }
    }

    /// Applies a batch of row deltas.
    pub fn apply(&mut self, deltas: &[Delta<Row>]) {
        for d in deltas {
            if let Some(value) = d.data.get(self.column) {
                if d.is_insert() {
                    match &self.max {
                        None => self.max = Some(value.clone()),
                        Some(current) if value > current => {
                            self.max = Some(value.clone());
                        }
                        _ => {}
                    }
                } else if d.is_delete() {
                    if self.max.as_ref() == Some(value) {
                        self.needs_recompute = true;
                    }
                }
            }
        }
    }

    /// Returns the current maximum, or None if empty or needs recomputation.
    pub fn get(&self) -> Option<&Value> {
        if self.needs_recompute {
            None
        } else {
            self.max.as_ref()
        }
    }

    /// Returns true if the maximum needs to be recomputed from scratch.
    pub fn needs_recompute(&self) -> bool {
        self.needs_recompute
    }

    /// Recomputes the maximum from a full scan of values.
    pub fn recompute(&mut self, values: impl Iterator<Item = Value>) {
        self.max = values.max();
        self.needs_recompute = false;
    }

    /// Resets the max.
    pub fn reset(&mut self) {
        self.max = None;
        self.needs_recompute = false;
    }
}

/// Extracts a numeric value from a Value for aggregation.
fn extract_numeric(value: &Value) -> f64 {
    match value {
        Value::Int32(v) => *v as f64,
        Value::Int64(v) => *v as f64,
        Value::Float64(v) => *v,
        _ => 0.0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloc::vec;

    fn make_row(id: u64, value: i64) -> Row {
        Row::new(id, vec![Value::Int64(value)])
    }

    #[test]
    fn test_incremental_count() {
        let mut count = IncrementalCount::new();

        count.apply(&[Delta::insert(1), Delta::insert(2)]);
        assert_eq!(count.get(), 2);

        count.apply(&[Delta::delete(1)]);
        assert_eq!(count.get(), 1);
    }

    #[test]
    fn test_incremental_sum() {
        let mut sum = IncrementalSum::new(0);

        sum.apply(&[
            Delta::insert(make_row(1, 10)),
            Delta::insert(make_row(2, 20)),
        ]);
        assert_eq!(sum.get(), 30.0);

        sum.apply(&[Delta::delete(make_row(1, 10))]);
        assert_eq!(sum.get(), 20.0);
    }

    #[test]
    fn test_incremental_avg() {
        let mut avg = IncrementalAvg::new(0);

        avg.apply(&[
            Delta::insert(make_row(1, 10)),
            Delta::insert(make_row(2, 20)),
            Delta::insert(make_row(3, 30)),
        ]);
        assert_eq!(avg.get(), Some(20.0));
        assert_eq!(avg.count(), 3);

        avg.apply(&[Delta::delete(make_row(3, 30))]);
        assert_eq!(avg.get(), Some(15.0));
        assert_eq!(avg.count(), 2);
    }

    #[test]
    fn test_incremental_avg_empty() {
        let avg = IncrementalAvg::new(0);
        assert_eq!(avg.get(), None);
    }

    #[test]
    fn test_incremental_min() {
        let mut min = IncrementalMin::new(0);

        min.apply(&[
            Delta::insert(make_row(1, 30)),
            Delta::insert(make_row(2, 10)),
            Delta::insert(make_row(3, 20)),
        ]);
        assert_eq!(min.get(), Some(&Value::Int64(10)));

        // Deleting non-min value doesn't trigger recompute
        min.apply(&[Delta::delete(make_row(1, 30))]);
        assert!(!min.needs_recompute());

        // Deleting min value triggers recompute
        min.apply(&[Delta::delete(make_row(2, 10))]);
        assert!(min.needs_recompute());
    }

    #[test]
    fn test_incremental_max() {
        let mut max = IncrementalMax::new(0);

        max.apply(&[
            Delta::insert(make_row(1, 10)),
            Delta::insert(make_row(2, 30)),
            Delta::insert(make_row(3, 20)),
        ]);
        assert_eq!(max.get(), Some(&Value::Int64(30)));

        // Deleting max value triggers recompute
        max.apply(&[Delta::delete(make_row(2, 30))]);
        assert!(max.needs_recompute());
    }
}
