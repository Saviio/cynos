//! Aggregate executor.

use crate::ast::AggregateFunc;
use crate::executor::{Relation, RelationEntry, SharedTables};
use alloc::collections::BTreeMap;
use alloc::rc::Rc;
use alloc::string::String;
use alloc::vec::Vec;
use cynos_core::{Row, Value};
use libm::{exp, log, sqrt};

/// Aggregate executor - computes aggregate functions.
pub struct AggregateExecutor {
    /// Group by column indices.
    group_by: Vec<usize>,
    /// Aggregates to compute: (function, column_index).
    aggregates: Vec<(AggregateFunc, Option<usize>)>,
}

impl AggregateExecutor {
    /// Creates a new aggregate executor.
    pub fn new(group_by: Vec<usize>, aggregates: Vec<(AggregateFunc, Option<usize>)>) -> Self {
        Self {
            group_by,
            aggregates,
        }
    }

    /// Creates an aggregate executor with no grouping.
    pub fn no_group(aggregates: Vec<(AggregateFunc, Option<usize>)>) -> Self {
        Self::new(Vec::new(), aggregates)
    }

    /// Executes the aggregation on the input relation.
    pub fn execute(&self, input: Relation) -> Relation {
        let tables = input.tables().to_vec();
        let shared_tables: SharedTables = tables.clone().into();
        // After aggregation, the result has a new column structure
        let result_column_count = self.group_by.len() + self.aggregates.len();

        if self.group_by.is_empty() {
            // No grouping - aggregate entire relation
            // Compute version as sum of all input row versions for change detection
            let version_sum: u64 = input.iter().map(|e| e.row.version()).sum();
            let values = self.compute_aggregates(input.iter());
            let entry = RelationEntry::new_combined(
                Rc::new(Row::dummy_with_version(version_sum, values)),
                shared_tables,
            );
            return Relation {
                entries: alloc::vec![entry],
                tables,
                table_column_counts: alloc::vec![result_column_count],
            };
        }

        // Group by specified columns
        let mut groups: BTreeMap<String, Vec<&RelationEntry>> = BTreeMap::new();

        for entry in input.iter() {
            let key = self.make_group_key(entry);
            groups.entry(key).or_default().push(entry);
        }

        let entries: Vec<RelationEntry> = groups
            .into_iter()
            .map(|(_, group_entries)| {
                let mut values = Vec::new();

                // Compute version as sum of group row versions for change detection
                let version_sum: u64 = group_entries.iter().map(|e| e.row.version()).sum();

                // Add group by values
                if let Some(first) = group_entries.first() {
                    for &idx in &self.group_by {
                        values.push(first.get_field(idx).cloned().unwrap_or(Value::Null));
                    }
                }

                // Add aggregate values
                let agg_values = self.compute_aggregates(group_entries.iter().copied());
                values.extend(agg_values);

                RelationEntry::new_combined(
                    Rc::new(Row::dummy_with_version(version_sum, values)),
                    shared_tables.clone(),
                )
            })
            .collect();

        Relation {
            entries,
            tables,
            table_column_counts: alloc::vec![result_column_count],
        }
    }

    fn make_group_key(&self, entry: &RelationEntry) -> String {
        self.group_by
            .iter()
            .map(|&idx| {
                entry
                    .get_field(idx)
                    .map(value_to_string)
                    .unwrap_or_else(|| String::from("null"))
            })
            .collect::<Vec<_>>()
            .join("|")
    }

    fn compute_aggregates<'a>(
        &self,
        entries: impl Iterator<Item = &'a RelationEntry>,
    ) -> Vec<Value> {
        let entries: Vec<_> = entries.collect();

        self.aggregates
            .iter()
            .map(|(func, col_idx)| self.compute_single_aggregate(*func, *col_idx, &entries))
            .collect()
    }

    fn compute_single_aggregate(
        &self,
        func: AggregateFunc,
        col_idx: Option<usize>,
        entries: &[&RelationEntry],
    ) -> Value {
        match func {
            AggregateFunc::Count => {
                if let Some(idx) = col_idx {
                    // COUNT(column) - count non-null values
                    let count = entries
                        .iter()
                        .filter(|e| {
                            e.get_field(idx)
                                .map(|v| !v.is_null())
                                .unwrap_or(false)
                        })
                        .count();
                    Value::Int64(count as i64)
                } else {
                    // COUNT(*) - count all rows
                    Value::Int64(entries.len() as i64)
                }
            }
            AggregateFunc::Sum => {
                let idx = col_idx.unwrap_or(0);
                let sum = entries
                    .iter()
                    .filter_map(|e| e.get_field(idx))
                    .filter(|v| !v.is_null())
                    .fold(0.0f64, |acc, v| {
                        acc + match v {
                            Value::Int32(i) => *i as f64,
                            Value::Int64(i) => *i as f64,
                            Value::Float64(f) => *f,
                            _ => 0.0,
                        }
                    });

                if entries.iter().all(|e| {
                    e.get_field(idx)
                        .map(|v| v.is_null() || matches!(v, Value::Int32(_) | Value::Int64(_)))
                        .unwrap_or(true)
                }) {
                    Value::Int64(sum as i64)
                } else {
                    Value::Float64(sum)
                }
            }
            AggregateFunc::Avg => {
                let idx = col_idx.unwrap_or(0);
                let values: Vec<f64> = entries
                    .iter()
                    .filter_map(|e| e.get_field(idx))
                    .filter(|v| !v.is_null())
                    .filter_map(|v| match v {
                        Value::Int32(i) => Some(*i as f64),
                        Value::Int64(i) => Some(*i as f64),
                        Value::Float64(f) => Some(*f),
                        _ => None,
                    })
                    .collect();

                if values.is_empty() {
                    Value::Null
                } else {
                    let sum: f64 = values.iter().sum();
                    Value::Float64(sum / values.len() as f64)
                }
            }
            AggregateFunc::Min => {
                let idx = col_idx.unwrap_or(0);
                entries
                    .iter()
                    .filter_map(|e| e.get_field(idx))
                    .filter(|v| !v.is_null())
                    .min()
                    .cloned()
                    .unwrap_or(Value::Null)
            }
            AggregateFunc::Max => {
                let idx = col_idx.unwrap_or(0);
                entries
                    .iter()
                    .filter_map(|e| e.get_field(idx))
                    .filter(|v| !v.is_null())
                    .max()
                    .cloned()
                    .unwrap_or(Value::Null)
            }
            AggregateFunc::Distinct => {
                let idx = col_idx.unwrap_or(0);
                let mut seen: BTreeMap<String, Value> = BTreeMap::new();
                for entry in entries {
                    if let Some(v) = entry.get_field(idx) {
                        let key = value_to_string(v);
                        seen.entry(key).or_insert_with(|| v.clone());
                    }
                }
                // Return count of distinct values for now
                Value::Int64(seen.len() as i64)
            }
            AggregateFunc::StdDev => {
                let idx = col_idx.unwrap_or(0);
                let values: Vec<f64> = entries
                    .iter()
                    .filter_map(|e| e.get_field(idx))
                    .filter(|v| !v.is_null())
                    .filter_map(|v| match v {
                        Value::Int32(i) => Some(*i as f64),
                        Value::Int64(i) => Some(*i as f64),
                        Value::Float64(f) => Some(*f),
                        _ => None,
                    })
                    .collect();

                if values.is_empty() {
                    Value::Null
                } else {
                    let mean: f64 = values.iter().sum::<f64>() / values.len() as f64;
                    let variance: f64 = values
                        .iter()
                        .map(|v| (v - mean) * (v - mean))
                        .sum::<f64>()
                        / values.len() as f64;
                    Value::Float64(sqrt(variance))
                }
            }
            AggregateFunc::GeoMean => {
                let idx = col_idx.unwrap_or(0);
                let values: Vec<f64> = entries
                    .iter()
                    .filter_map(|e| e.get_field(idx))
                    .filter(|v| !v.is_null())
                    .filter_map(|v| match v {
                        Value::Int32(i) => Some(*i as f64),
                        Value::Int64(i) => Some(*i as f64),
                        Value::Float64(f) => Some(*f),
                        _ => None,
                    })
                    .filter(|&v| v > 0.0)
                    .collect();

                if values.is_empty() {
                    Value::Null
                } else {
                    let log_sum: f64 = values.iter().map(|v| log(*v)).sum();
                    let geomean = exp(log_sum / values.len() as f64);
                    Value::Float64(geomean)
                }
            }
        }
    }
}

fn value_to_string(value: &Value) -> String {
    match value {
        Value::Null => String::from("null"),
        Value::Boolean(b) => alloc::format!("{}", b),
        Value::Int32(i) => alloc::format!("{}", i),
        Value::Int64(i) => alloc::format!("{}", i),
        Value::Float64(f) => alloc::format!("{}", f),
        Value::String(s) => s.clone(),
        Value::DateTime(d) => alloc::format!("{}", d),
        Value::Bytes(b) => alloc::format!("{:?}", b),
        Value::Jsonb(j) => alloc::format!("{:?}", j.0),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloc::vec;

    #[test]
    fn test_count_star() {
        let rows = vec![
            Row::new(0, vec![Value::Int64(1)]),
            Row::new(1, vec![Value::Int64(2)]),
            Row::new(2, vec![Value::Int64(3)]),
        ];
        let input = Relation::from_rows_owned(rows, vec!["t".into()]);

        let executor = AggregateExecutor::no_group(vec![(AggregateFunc::Count, None)]);
        let result = executor.execute(input);

        assert_eq!(result.len(), 1);
        assert_eq!(result.entries[0].get_field(0), Some(&Value::Int64(3)));
    }

    #[test]
    fn test_count_column() {
        let rows = vec![
            Row::new(0, vec![Value::Int64(1)]),
            Row::new(1, vec![Value::Null]),
            Row::new(2, vec![Value::Int64(3)]),
        ];
        let input = Relation::from_rows_owned(rows, vec!["t".into()]);

        let executor = AggregateExecutor::no_group(vec![(AggregateFunc::Count, Some(0))]);
        let result = executor.execute(input);

        assert_eq!(result.len(), 1);
        // Should count only non-null values
        assert_eq!(result.entries[0].get_field(0), Some(&Value::Int64(2)));
    }

    #[test]
    fn test_sum() {
        let rows = vec![
            Row::new(0, vec![Value::Int64(10)]),
            Row::new(1, vec![Value::Int64(20)]),
            Row::new(2, vec![Value::Int64(30)]),
        ];
        let input = Relation::from_rows_owned(rows, vec!["t".into()]);

        let executor = AggregateExecutor::no_group(vec![(AggregateFunc::Sum, Some(0))]);
        let result = executor.execute(input);

        assert_eq!(result.entries[0].get_field(0), Some(&Value::Int64(60)));
    }

    #[test]
    fn test_avg() {
        let rows = vec![
            Row::new(0, vec![Value::Int64(10)]),
            Row::new(1, vec![Value::Int64(20)]),
            Row::new(2, vec![Value::Int64(30)]),
        ];
        let input = Relation::from_rows_owned(rows, vec!["t".into()]);

        let executor = AggregateExecutor::no_group(vec![(AggregateFunc::Avg, Some(0))]);
        let result = executor.execute(input);

        assert_eq!(result.entries[0].get_field(0), Some(&Value::Float64(20.0)));
    }

    #[test]
    fn test_min_max() {
        let rows = vec![
            Row::new(0, vec![Value::Int64(30)]),
            Row::new(1, vec![Value::Int64(10)]),
            Row::new(2, vec![Value::Int64(20)]),
        ];
        let input = Relation::from_rows_owned(rows, vec!["t".into()]);

        let executor = AggregateExecutor::no_group(vec![
            (AggregateFunc::Min, Some(0)),
            (AggregateFunc::Max, Some(0)),
        ]);
        let result = executor.execute(input);

        assert_eq!(result.entries[0].get_field(0), Some(&Value::Int64(10)));
        assert_eq!(result.entries[0].get_field(1), Some(&Value::Int64(30)));
    }

    #[test]
    fn test_group_by() {
        let rows = vec![
            Row::new(0, vec![Value::String("A".into()), Value::Int64(10)]),
            Row::new(1, vec![Value::String("A".into()), Value::Int64(20)]),
            Row::new(2, vec![Value::String("B".into()), Value::Int64(30)]),
        ];
        let input = Relation::from_rows_owned(rows, vec!["t".into()]);

        let executor = AggregateExecutor::new(
            vec![0], // group by first column
            vec![(AggregateFunc::Sum, Some(1))],
        );
        let result = executor.execute(input);

        assert_eq!(result.len(), 2);
        // Verify actual group values - BTreeMap orders by key, so "A" comes before "B"
        // Group A: 10 + 20 = 30, Group B: 30
        let mut sums: Vec<i64> = result
            .entries
            .iter()
            .filter_map(|e| match e.get_field(1) {
                Some(Value::Int64(v)) => Some(*v),
                _ => None,
            })
            .collect();
        sums.sort();
        assert_eq!(sums, vec![30, 30]); // Both groups sum to 30
    }

    #[test]
    fn test_empty_relation() {
        let input = Relation::from_rows_owned(Vec::new(), vec!["t".into()]);

        let executor = AggregateExecutor::no_group(vec![
            (AggregateFunc::Count, None),
            (AggregateFunc::Sum, Some(0)),
            (AggregateFunc::Avg, Some(0)),
        ]);
        let result = executor.execute(input);

        assert_eq!(result.len(), 1);
        assert_eq!(result.entries[0].get_field(0), Some(&Value::Int64(0))); // COUNT
        assert_eq!(result.entries[0].get_field(1), Some(&Value::Int64(0))); // SUM
        assert_eq!(result.entries[0].get_field(2), Some(&Value::Null)); // AVG
    }

    #[test]
    fn test_stddev() {
        let rows = vec![
            Row::new(0, vec![Value::Float64(2.0)]),
            Row::new(1, vec![Value::Float64(4.0)]),
            Row::new(2, vec![Value::Float64(4.0)]),
            Row::new(3, vec![Value::Float64(4.0)]),
            Row::new(4, vec![Value::Float64(5.0)]),
            Row::new(5, vec![Value::Float64(5.0)]),
            Row::new(6, vec![Value::Float64(7.0)]),
            Row::new(7, vec![Value::Float64(9.0)]),
        ];
        let input = Relation::from_rows_owned(rows, vec!["t".into()]);

        let executor = AggregateExecutor::no_group(vec![(AggregateFunc::StdDev, Some(0))]);
        let result = executor.execute(input);

        // Standard deviation should be 2.0
        if let Some(Value::Float64(stddev)) = result.entries[0].get_field(0) {
            assert!((stddev - 2.0).abs() < 0.001);
        } else {
            panic!("Expected Float64 value");
        }
    }

    #[test]
    fn test_distinct() {
        let rows = vec![
            Row::new(0, vec![Value::Int64(1)]),
            Row::new(1, vec![Value::Int64(2)]),
            Row::new(2, vec![Value::Int64(1)]), // duplicate
            Row::new(3, vec![Value::Int64(3)]),
            Row::new(4, vec![Value::Int64(2)]), // duplicate
        ];
        let input = Relation::from_rows_owned(rows, vec!["t".into()]);

        let executor = AggregateExecutor::no_group(vec![(AggregateFunc::Distinct, Some(0))]);
        let result = executor.execute(input);

        assert_eq!(result.len(), 1);
        // Should return count of distinct values: 1, 2, 3 = 3
        assert_eq!(result.entries[0].get_field(0), Some(&Value::Int64(3)));
    }

    #[test]
    fn test_distinct_with_nulls() {
        let rows = vec![
            Row::new(0, vec![Value::Int64(1)]),
            Row::new(1, vec![Value::Null]),
            Row::new(2, vec![Value::Int64(1)]),
            Row::new(3, vec![Value::Null]),
        ];
        let input = Relation::from_rows_owned(rows, vec!["t".into()]);

        let executor = AggregateExecutor::no_group(vec![(AggregateFunc::Distinct, Some(0))]);
        let result = executor.execute(input);

        // Distinct values: 1, null = 2
        assert_eq!(result.entries[0].get_field(0), Some(&Value::Int64(2)));
    }

    #[test]
    fn test_geomean() {
        // Geometric mean of [2, 8] = sqrt(2 * 8) = sqrt(16) = 4
        let rows = vec![
            Row::new(0, vec![Value::Float64(2.0)]),
            Row::new(1, vec![Value::Float64(8.0)]),
        ];
        let input = Relation::from_rows_owned(rows, vec!["t".into()]);

        let executor = AggregateExecutor::no_group(vec![(AggregateFunc::GeoMean, Some(0))]);
        let result = executor.execute(input);

        if let Some(Value::Float64(geomean)) = result.entries[0].get_field(0) {
            assert!((geomean - 4.0).abs() < 0.001);
        } else {
            panic!("Expected Float64 value");
        }
    }

    #[test]
    fn test_geomean_single_value() {
        let rows = vec![Row::new(0, vec![Value::Float64(5.0)])];
        let input = Relation::from_rows_owned(rows, vec!["t".into()]);

        let executor = AggregateExecutor::no_group(vec![(AggregateFunc::GeoMean, Some(0))]);
        let result = executor.execute(input);

        if let Some(Value::Float64(geomean)) = result.entries[0].get_field(0) {
            assert!((geomean - 5.0).abs() < 0.001);
        } else {
            panic!("Expected Float64 value");
        }
    }

    #[test]
    fn test_geomean_with_zero_and_negative() {
        // GeoMean filters out non-positive values
        let rows = vec![
            Row::new(0, vec![Value::Float64(2.0)]),
            Row::new(1, vec![Value::Float64(0.0)]),  // filtered
            Row::new(2, vec![Value::Float64(-1.0)]), // filtered
            Row::new(3, vec![Value::Float64(8.0)]),
        ];
        let input = Relation::from_rows_owned(rows, vec!["t".into()]);

        let executor = AggregateExecutor::no_group(vec![(AggregateFunc::GeoMean, Some(0))]);
        let result = executor.execute(input);

        // Only [2, 8] are used, geomean = 4
        if let Some(Value::Float64(geomean)) = result.entries[0].get_field(0) {
            assert!((geomean - 4.0).abs() < 0.001);
        } else {
            panic!("Expected Float64 value");
        }
    }

    #[test]
    fn test_geomean_all_non_positive() {
        let rows = vec![
            Row::new(0, vec![Value::Float64(0.0)]),
            Row::new(1, vec![Value::Float64(-1.0)]),
        ];
        let input = Relation::from_rows_owned(rows, vec!["t".into()]);

        let executor = AggregateExecutor::no_group(vec![(AggregateFunc::GeoMean, Some(0))]);
        let result = executor.execute(input);

        // All values filtered, should return Null
        assert_eq!(result.entries[0].get_field(0), Some(&Value::Null));
    }

    #[test]
    fn test_sum_with_nulls() {
        let rows = vec![
            Row::new(0, vec![Value::Int64(10)]),
            Row::new(1, vec![Value::Null]),
            Row::new(2, vec![Value::Int64(20)]),
            Row::new(3, vec![Value::Null]),
        ];
        let input = Relation::from_rows_owned(rows, vec!["t".into()]);

        let executor = AggregateExecutor::no_group(vec![(AggregateFunc::Sum, Some(0))]);
        let result = executor.execute(input);

        // Sum should ignore nulls: 10 + 20 = 30
        assert_eq!(result.entries[0].get_field(0), Some(&Value::Int64(30)));
    }

    #[test]
    fn test_sum_mixed_types() {
        let rows = vec![
            Row::new(0, vec![Value::Int32(10)]),
            Row::new(1, vec![Value::Int64(20)]),
            Row::new(2, vec![Value::Float64(30.5)]),
        ];
        let input = Relation::from_rows_owned(rows, vec!["t".into()]);

        let executor = AggregateExecutor::no_group(vec![(AggregateFunc::Sum, Some(0))]);
        let result = executor.execute(input);

        // Mixed types should return Float64
        if let Some(Value::Float64(sum)) = result.entries[0].get_field(0) {
            assert!((sum - 60.5).abs() < 0.001);
        } else {
            panic!("Expected Float64 value for mixed types");
        }
    }

    #[test]
    fn test_min_max_with_nulls() {
        let rows = vec![
            Row::new(0, vec![Value::Int64(30)]),
            Row::new(1, vec![Value::Null]),
            Row::new(2, vec![Value::Int64(10)]),
            Row::new(3, vec![Value::Null]),
            Row::new(4, vec![Value::Int64(20)]),
        ];
        let input = Relation::from_rows_owned(rows, vec!["t".into()]);

        let executor = AggregateExecutor::no_group(vec![
            (AggregateFunc::Min, Some(0)),
            (AggregateFunc::Max, Some(0)),
        ]);
        let result = executor.execute(input);

        // Min/Max should ignore nulls
        assert_eq!(result.entries[0].get_field(0), Some(&Value::Int64(10)));
        assert_eq!(result.entries[0].get_field(1), Some(&Value::Int64(30)));
    }

    #[test]
    fn test_min_max_all_nulls() {
        let rows = vec![
            Row::new(0, vec![Value::Null]),
            Row::new(1, vec![Value::Null]),
        ];
        let input = Relation::from_rows_owned(rows, vec!["t".into()]);

        let executor = AggregateExecutor::no_group(vec![
            (AggregateFunc::Min, Some(0)),
            (AggregateFunc::Max, Some(0)),
        ]);
        let result = executor.execute(input);

        // All nulls should return Null
        assert_eq!(result.entries[0].get_field(0), Some(&Value::Null));
        assert_eq!(result.entries[0].get_field(1), Some(&Value::Null));
    }

    #[test]
    fn test_stddev_single_value() {
        let rows = vec![Row::new(0, vec![Value::Float64(5.0)])];
        let input = Relation::from_rows_owned(rows, vec!["t".into()]);

        let executor = AggregateExecutor::no_group(vec![(AggregateFunc::StdDev, Some(0))]);
        let result = executor.execute(input);

        // StdDev of single value is 0
        if let Some(Value::Float64(stddev)) = result.entries[0].get_field(0) {
            assert!((stddev - 0.0).abs() < 0.001);
        } else {
            panic!("Expected Float64 value");
        }
    }

    #[test]
    fn test_stddev_empty() {
        let input = Relation::from_rows_owned(Vec::new(), vec!["t".into()]);

        let executor = AggregateExecutor::no_group(vec![(AggregateFunc::StdDev, Some(0))]);
        let result = executor.execute(input);

        // Empty set should return Null
        assert_eq!(result.entries[0].get_field(0), Some(&Value::Null));
    }

    #[test]
    fn test_stddev_with_nulls() {
        let rows = vec![
            Row::new(0, vec![Value::Float64(2.0)]),
            Row::new(1, vec![Value::Null]),
            Row::new(2, vec![Value::Float64(4.0)]),
            Row::new(3, vec![Value::Null]),
            Row::new(4, vec![Value::Float64(6.0)]),
        ];
        let input = Relation::from_rows_owned(rows, vec!["t".into()]);

        let executor = AggregateExecutor::no_group(vec![(AggregateFunc::StdDev, Some(0))]);
        let result = executor.execute(input);

        // Values: [2, 4, 6], mean = 4, variance = ((2-4)^2 + (4-4)^2 + (6-4)^2) / 3 = 8/3
        // stddev = sqrt(8/3) ≈ 1.633
        if let Some(Value::Float64(stddev)) = result.entries[0].get_field(0) {
            assert!((stddev - 1.633).abs() < 0.01);
        } else {
            panic!("Expected Float64 value");
        }
    }

    #[test]
    fn test_avg_with_nulls() {
        let rows = vec![
            Row::new(0, vec![Value::Int64(10)]),
            Row::new(1, vec![Value::Null]),
            Row::new(2, vec![Value::Int64(20)]),
            Row::new(3, vec![Value::Null]),
        ];
        let input = Relation::from_rows_owned(rows, vec!["t".into()]);

        let executor = AggregateExecutor::no_group(vec![(AggregateFunc::Avg, Some(0))]);
        let result = executor.execute(input);

        // Avg should ignore nulls: (10 + 20) / 2 = 15
        assert_eq!(result.entries[0].get_field(0), Some(&Value::Float64(15.0)));
    }

    #[test]
    fn test_multiple_aggregates() {
        let rows = vec![
            Row::new(0, vec![Value::Int64(10)]),
            Row::new(1, vec![Value::Int64(20)]),
            Row::new(2, vec![Value::Int64(30)]),
            Row::new(3, vec![Value::Int64(40)]),
        ];
        let input = Relation::from_rows_owned(rows, vec!["t".into()]);

        let executor = AggregateExecutor::no_group(vec![
            (AggregateFunc::Count, None),
            (AggregateFunc::Sum, Some(0)),
            (AggregateFunc::Avg, Some(0)),
            (AggregateFunc::Min, Some(0)),
            (AggregateFunc::Max, Some(0)),
        ]);
        let result = executor.execute(input);

        assert_eq!(result.entries[0].get_field(0), Some(&Value::Int64(4)));    // COUNT
        assert_eq!(result.entries[0].get_field(1), Some(&Value::Int64(100)));  // SUM
        assert_eq!(result.entries[0].get_field(2), Some(&Value::Float64(25.0))); // AVG
        assert_eq!(result.entries[0].get_field(3), Some(&Value::Int64(10)));   // MIN
        assert_eq!(result.entries[0].get_field(4), Some(&Value::Int64(40)));   // MAX
    }

    #[test]
    fn test_group_by_with_multiple_aggregates() {
        let rows = vec![
            Row::new(0, vec![Value::String("A".into()), Value::Int64(10)]),
            Row::new(1, vec![Value::String("A".into()), Value::Int64(20)]),
            Row::new(2, vec![Value::String("A".into()), Value::Int64(30)]),
            Row::new(3, vec![Value::String("B".into()), Value::Int64(100)]),
        ];
        let input = Relation::from_rows_owned(rows, vec!["t".into()]);

        let executor = AggregateExecutor::new(
            vec![0],
            vec![
                (AggregateFunc::Count, None),
                (AggregateFunc::Sum, Some(1)),
                (AggregateFunc::Avg, Some(1)),
            ],
        );
        let result = executor.execute(input);

        assert_eq!(result.len(), 2);

        // Find group A and B by checking the group key
        for entry in &result.entries {
            let group_key = entry.get_field(0);
            match group_key {
                Some(Value::String(s)) if s == "A" => {
                    assert_eq!(entry.get_field(1), Some(&Value::Int64(3)));    // COUNT
                    assert_eq!(entry.get_field(2), Some(&Value::Int64(60)));   // SUM
                    assert_eq!(entry.get_field(3), Some(&Value::Float64(20.0))); // AVG
                }
                Some(Value::String(s)) if s == "B" => {
                    assert_eq!(entry.get_field(1), Some(&Value::Int64(1)));    // COUNT
                    assert_eq!(entry.get_field(2), Some(&Value::Int64(100)));  // SUM
                    assert_eq!(entry.get_field(3), Some(&Value::Float64(100.0))); // AVG
                }
                _ => panic!("Unexpected group key"),
            }
        }
    }
}
