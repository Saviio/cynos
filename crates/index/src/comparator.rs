//! Comparator implementations for index keys.
//!
//! This module provides comparators for ordering keys in indexes.

use alloc::vec::Vec;
use core::cmp::Ordering;

/// Sort order for index keys.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Order {
    /// Ascending order (smallest first)
    Asc,
    /// Descending order (largest first)
    Desc,
}

impl Order {
    /// Applies this order to a comparison result.
    #[inline]
    pub fn apply(&self, ord: Ordering) -> Ordering {
        match self {
            Order::Asc => ord,
            Order::Desc => ord.reverse(),
        }
    }
}

/// Trait for comparing index keys.
pub trait Comparator<K> {
    /// Compares two keys according to the comparator's ordering.
    fn compare(&self, a: &K, b: &K) -> Ordering;

    /// Returns true if a < b according to this comparator.
    fn is_less(&self, a: &K, b: &K) -> bool {
        self.compare(a, b) == Ordering::Less
    }

    /// Returns true if a <= b according to this comparator.
    fn is_less_or_equal(&self, a: &K, b: &K) -> bool {
        self.compare(a, b) != Ordering::Greater
    }

    /// Returns true if a > b according to this comparator.
    fn is_greater(&self, a: &K, b: &K) -> bool {
        self.compare(a, b) == Ordering::Greater
    }

    /// Returns true if a >= b according to this comparator.
    fn is_greater_or_equal(&self, a: &K, b: &K) -> bool {
        self.compare(a, b) != Ordering::Less
    }

    /// Returns true if a == b according to this comparator.
    fn is_equal(&self, a: &K, b: &K) -> bool {
        self.compare(a, b) == Ordering::Equal
    }
}

/// A simple comparator for single keys that implement Ord.
#[derive(Clone, Debug)]
pub struct SimpleComparator {
    order: Order,
}

impl SimpleComparator {
    /// Creates a new simple comparator with the given order.
    pub fn new(order: Order) -> Self {
        Self { order }
    }

    /// Creates an ascending comparator.
    pub fn asc() -> Self {
        Self::new(Order::Asc)
    }

    /// Creates a descending comparator.
    pub fn desc() -> Self {
        Self::new(Order::Desc)
    }

    /// Returns the order of this comparator.
    pub fn order(&self) -> Order {
        self.order
    }
}

impl<K: Ord> Comparator<K> for SimpleComparator {
    fn compare(&self, a: &K, b: &K) -> Ordering {
        self.order.apply(a.cmp(b))
    }
}

/// A comparator for multi-key indexes (composite keys).
#[derive(Clone, Debug)]
pub struct MultiKeyComparator {
    orders: Vec<Order>,
}

impl MultiKeyComparator {
    /// Creates a new multi-key comparator with the given orders.
    pub fn new(orders: Vec<Order>) -> Self {
        Self { orders }
    }

    /// Creates orders for n keys, all with the same order.
    pub fn create_orders(n: usize, order: Order) -> Vec<Order> {
        (0..n).map(|_| order).collect()
    }

    /// Returns the orders of this comparator.
    pub fn orders(&self) -> &[Order] {
        &self.orders
    }
}

impl<K: Ord> Comparator<Vec<K>> for MultiKeyComparator {
    fn compare(&self, a: &Vec<K>, b: &Vec<K>) -> Ordering {
        for (i, order) in self.orders.iter().enumerate() {
            let a_val = a.get(i);
            let b_val = b.get(i);

            let cmp = match (a_val, b_val) {
                (Some(av), Some(bv)) => order.apply(av.cmp(bv)),
                (None, Some(_)) => Ordering::Less,
                (Some(_), None) => Ordering::Greater,
                (None, None) => Ordering::Equal,
            };

            if cmp != Ordering::Equal {
                return cmp;
            }
        }
        Ordering::Equal
    }
}

/// A comparator for multi-key indexes that handles null values.
/// Null values are sorted before non-null values.
#[derive(Clone, Debug)]
pub struct MultiKeyComparatorWithNull {
    orders: Vec<Order>,
}

impl MultiKeyComparatorWithNull {
    /// Creates a new multi-key comparator with null handling.
    pub fn new(orders: Vec<Order>) -> Self {
        Self { orders }
    }

    /// Returns the orders of this comparator.
    pub fn orders(&self) -> &[Order] {
        &self.orders
    }
}

impl<K: Ord> Comparator<Vec<Option<K>>> for MultiKeyComparatorWithNull {
    fn compare(&self, a: &Vec<Option<K>>, b: &Vec<Option<K>>) -> Ordering {
        for (i, order) in self.orders.iter().enumerate() {
            let a_val = a.get(i).and_then(|v| v.as_ref());
            let b_val = b.get(i).and_then(|v| v.as_ref());

            let cmp = match (a_val, b_val) {
                (Some(av), Some(bv)) => order.apply(av.cmp(bv)),
                (None, Some(_)) => Ordering::Less,
                (Some(_), None) => Ordering::Greater,
                (None, None) => Ordering::Equal,
            };

            if cmp != Ordering::Equal {
                return cmp;
            }
        }
        Ordering::Equal
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloc::vec;

    #[test]
    fn test_order_apply() {
        assert_eq!(Order::Asc.apply(Ordering::Less), Ordering::Less);
        assert_eq!(Order::Asc.apply(Ordering::Greater), Ordering::Greater);
        assert_eq!(Order::Desc.apply(Ordering::Less), Ordering::Greater);
        assert_eq!(Order::Desc.apply(Ordering::Greater), Ordering::Less);
    }

    #[test]
    fn test_simple_comparator_asc() {
        let cmp = SimpleComparator::asc();
        assert_eq!(cmp.compare(&1, &2), Ordering::Less);
        assert_eq!(cmp.compare(&2, &1), Ordering::Greater);
        assert_eq!(cmp.compare(&1, &1), Ordering::Equal);
    }

    #[test]
    fn test_simple_comparator_desc() {
        let cmp = SimpleComparator::desc();
        assert_eq!(cmp.compare(&1, &2), Ordering::Greater);
        assert_eq!(cmp.compare(&2, &1), Ordering::Less);
        assert_eq!(cmp.compare(&1, &1), Ordering::Equal);
    }

    #[test]
    fn test_simple_comparator_helpers() {
        let cmp = SimpleComparator::asc();
        assert!(cmp.is_less(&1, &2));
        assert!(cmp.is_less_or_equal(&1, &2));
        assert!(cmp.is_less_or_equal(&1, &1));
        assert!(cmp.is_greater(&2, &1));
        assert!(cmp.is_greater_or_equal(&2, &1));
        assert!(cmp.is_greater_or_equal(&1, &1));
        assert!(cmp.is_equal(&1, &1));
    }

    #[test]
    fn test_multi_key_comparator() {
        let cmp = MultiKeyComparator::new(vec![Order::Asc, Order::Desc]);

        // First key determines order
        assert_eq!(cmp.compare(&vec![1, 10], &vec![2, 5]), Ordering::Less);
        assert_eq!(cmp.compare(&vec![2, 10], &vec![1, 5]), Ordering::Greater);

        // Same first key, second key (descending) determines order
        assert_eq!(cmp.compare(&vec![1, 10], &vec![1, 5]), Ordering::Less); // 10 > 5, but DESC
        assert_eq!(cmp.compare(&vec![1, 5], &vec![1, 10]), Ordering::Greater);

        // Equal
        assert_eq!(cmp.compare(&vec![1, 5], &vec![1, 5]), Ordering::Equal);
    }

    #[test]
    fn test_multi_key_comparator_with_null() {
        let cmp = MultiKeyComparatorWithNull::new(vec![Order::Asc, Order::Asc]);

        // Null is less than non-null
        assert_eq!(
            cmp.compare(&vec![None, Some(1)], &vec![Some(1), Some(1)]),
            Ordering::Less
        );
        assert_eq!(
            cmp.compare(&vec![Some(1), None], &vec![Some(1), Some(1)]),
            Ordering::Less
        );

        // Both null
        assert_eq!(
            cmp.compare(&vec![None::<i32>, None], &vec![None, None]),
            Ordering::Equal
        );

        // Normal comparison
        assert_eq!(
            cmp.compare(&vec![Some(1), Some(2)], &vec![Some(1), Some(3)]),
            Ordering::Less
        );
    }

    #[test]
    fn test_create_orders() {
        let orders = MultiKeyComparator::create_orders(3, Order::Asc);
        assert_eq!(orders, vec![Order::Asc, Order::Asc, Order::Asc]);

        let orders = MultiKeyComparator::create_orders(2, Order::Desc);
        assert_eq!(orders, vec![Order::Desc, Order::Desc]);
    }

    // ==================== Additional Multi-Key Tests ====================

    /// Test multi-key comparator with mixed orders
    #[test]
    fn test_multi_key_mixed_orders() {
        // First key ASC, second key DESC
        let cmp = MultiKeyComparator::new(vec![Order::Asc, Order::Desc]);

        // Same first key, different second key
        let a = vec![1, 100];
        let b = vec![1, 50];
        // DESC on second key means 100 < 50 in comparison
        assert_eq!(cmp.compare(&a, &b), Ordering::Less);

        // Different first key
        let c = vec![2, 100];
        assert_eq!(cmp.compare(&a, &c), Ordering::Less); // 1 < 2
    }

    /// Test multi-key comparator with all DESC
    #[test]
    fn test_multi_key_all_desc() {
        let cmp = MultiKeyComparator::new(vec![Order::Desc, Order::Desc]);

        assert_eq!(cmp.compare(&vec![1, 1], &vec![2, 2]), Ordering::Greater);
        assert_eq!(cmp.compare(&vec![2, 1], &vec![2, 2]), Ordering::Greater);
        assert_eq!(cmp.compare(&vec![2, 2], &vec![2, 2]), Ordering::Equal);
    }

    /// Test multi-key comparator with string keys
    #[test]
    fn test_multi_key_string() {
        let cmp = MultiKeyComparator::new(vec![Order::Asc, Order::Asc]);

        let a = vec!["apple", "red"];
        let b = vec!["apple", "green"];
        let c = vec!["banana", "yellow"];

        assert_eq!(cmp.compare(&a, &b), Ordering::Greater); // "red" > "green"
        assert_eq!(cmp.compare(&a, &c), Ordering::Less); // "apple" < "banana"
    }

    /// Test multi-key comparator with unequal length vectors
    #[test]
    fn test_multi_key_unequal_length() {
        let cmp = MultiKeyComparator::new(vec![Order::Asc, Order::Asc, Order::Asc]);

        let a = vec![1, 2];
        let b = vec![1, 2, 3];

        // Missing element treated as less
        assert_eq!(cmp.compare(&a, &b), Ordering::Less);
        assert_eq!(cmp.compare(&b, &a), Ordering::Greater);
    }

    /// Test multi-key with null - null sorting behcynos
    #[test]
    fn test_multi_key_null_sorting() {
        let cmp = MultiKeyComparatorWithNull::new(vec![Order::Asc, Order::Asc]);

        // Null values should sort before non-null
        let null_first = vec![None, Some(1)];
        let non_null = vec![Some(1), Some(1)];
        assert_eq!(cmp.compare(&null_first, &non_null), Ordering::Less);

        // Null in second position
        let a = vec![Some(1), None];
        let b = vec![Some(1), Some(1)];
        assert_eq!(cmp.compare(&a, &b), Ordering::Less);
    }

    /// Test multi-key with null - all null comparison
    #[test]
    fn test_multi_key_all_null() {
        let cmp = MultiKeyComparatorWithNull::new(vec![Order::Asc, Order::Asc]);

        let a: Vec<Option<i32>> = vec![None, None];
        let b: Vec<Option<i32>> = vec![None, None];
        assert_eq!(cmp.compare(&a, &b), Ordering::Equal);
    }

    /// Test multi-key with null - mixed null and values
    #[test]
    fn test_multi_key_mixed_null() {
        let cmp = MultiKeyComparatorWithNull::new(vec![Order::Asc, Order::Asc]);

        // First key null vs non-null
        let a: Vec<Option<i32>> = vec![None, Some(100)];
        let b: Vec<Option<i32>> = vec![Some(1), Some(1)];
        assert_eq!(cmp.compare(&a, &b), Ordering::Less);

        // Same first key (both null), compare second
        let c: Vec<Option<i32>> = vec![None, Some(50)];
        let d: Vec<Option<i32>> = vec![None, Some(100)];
        assert_eq!(cmp.compare(&c, &d), Ordering::Less);
    }

    /// Test multi-key with null - DESC order
    #[test]
    fn test_multi_key_null_desc() {
        let cmp = MultiKeyComparatorWithNull::new(vec![Order::Desc, Order::Desc]);

        // With DESC, larger values come first, but null still comes before non-null
        let a: Vec<Option<i32>> = vec![Some(10), Some(20)];
        let b: Vec<Option<i32>> = vec![Some(5), Some(10)];
        assert_eq!(cmp.compare(&a, &b), Ordering::Less); // DESC: 10 < 5 means 10 comes after 5

        // Null handling remains the same (null < non-null)
        let c: Vec<Option<i32>> = vec![None, Some(1)];
        let d: Vec<Option<i32>> = vec![Some(1), Some(1)];
        assert_eq!(cmp.compare(&c, &d), Ordering::Less);
    }

    /// Test simple comparator with strings
    #[test]
    fn test_simple_comparator_strings() {
        let cmp = SimpleComparator::asc();
        assert_eq!(cmp.compare(&"apple", &"banana"), Ordering::Less);
        assert_eq!(cmp.compare(&"zebra", &"apple"), Ordering::Greater);
        assert_eq!(cmp.compare(&"test", &"test"), Ordering::Equal);

        let cmp_desc = SimpleComparator::desc();
        assert_eq!(cmp_desc.compare(&"apple", &"banana"), Ordering::Greater);
    }

    /// Test comparator trait helper methods
    #[test]
    fn test_comparator_helpers_comprehensive() {
        let cmp = SimpleComparator::asc();

        // is_less
        assert!(cmp.is_less(&1, &2));
        assert!(!cmp.is_less(&2, &1));
        assert!(!cmp.is_less(&1, &1));

        // is_less_or_equal
        assert!(cmp.is_less_or_equal(&1, &2));
        assert!(cmp.is_less_or_equal(&1, &1));
        assert!(!cmp.is_less_or_equal(&2, &1));

        // is_greater
        assert!(cmp.is_greater(&2, &1));
        assert!(!cmp.is_greater(&1, &2));
        assert!(!cmp.is_greater(&1, &1));

        // is_greater_or_equal
        assert!(cmp.is_greater_or_equal(&2, &1));
        assert!(cmp.is_greater_or_equal(&1, &1));
        assert!(!cmp.is_greater_or_equal(&1, &2));

        // is_equal
        assert!(cmp.is_equal(&1, &1));
        assert!(!cmp.is_equal(&1, &2));
    }

    /// Test multi-key comparator with 3 keys
    #[test]
    fn test_multi_key_three_keys() {
        let cmp = MultiKeyComparator::new(vec![Order::Asc, Order::Asc, Order::Desc]);

        let a = vec![1, 2, 100];
        let b = vec![1, 2, 50];
        // Third key is DESC, so 100 < 50
        assert_eq!(cmp.compare(&a, &b), Ordering::Less);

        let c = vec![1, 3, 100];
        // Second key differs: 2 < 3
        assert_eq!(cmp.compare(&a, &c), Ordering::Less);
    }
}
