//! Index performance benchmarks

use crate::report::Report;
use crate::utils::*;
use cynos_index::{BTreeIndex, HashIndex, Index, KeyRange, RangeIndex};

pub fn run(report: &mut Report) {
    btree_insert(report);
    btree_get(report);
    btree_range(report);
    hash_insert(report);
    hash_get(report);
    btree_vs_hash(report);
}

fn btree_insert(report: &mut Report) {
    println!("  BTree Insert:");
    for &size in &SIZES {
        let result = measure(ITERATIONS, || {
            let mut tree = BTreeIndex::new(64, false);
            for i in 0..size as i64 {
                tree.add(i, i as u64).unwrap();
            }
            tree
        });

        let throughput = result.throughput(size);
        println!(
            "    {:>7} rows: {:>10} ({:>12})",
            size,
            format_duration(result.mean),
            format_throughput(throughput)
        );
        report.add_result("Index/BTree", "insert", Some(size), result, Some(throughput));
    }
}

fn btree_get(report: &mut Report) {
    println!("  BTree Point Lookup (random order):");
    for &size in &SIZES {
        // Setup
        let mut tree = BTreeIndex::new(64, true);
        for i in 0..size as i64 {
            tree.add(i, i as u64).unwrap();
        }

        // Use random order to avoid CPU prefetch optimization
        let lookup_keys: Vec<i64> = shuffle_indices(size, 12345)
            .into_iter()
            .take(1000.min(size))
            .map(|i| i as i64)
            .collect();
        let lookup_count = lookup_keys.len();

        let result = measure(ITERATIONS, || {
            let mut found = 0;
            for &key in &lookup_keys {
                if !tree.get(&key).is_empty() {
                    found += 1;
                }
            }
            found
        });

        let throughput = lookup_count as f64 / result.mean.as_secs_f64();
        println!(
            "    {:>7} rows ({} lookups): {:>10} ({:>12})",
            size,
            lookup_count,
            format_duration(result.mean),
            format_throughput(throughput)
        );
        report.add_result("Index/BTree", "get", Some(size), result, Some(throughput));
    }
}

fn btree_range(report: &mut Report) {
    println!("  BTree Range Scan:");
    // Setup large tree
    let mut tree = BTreeIndex::new(64, true);
    for i in 0..100_000i64 {
        tree.add(i, i as u64).unwrap();
    }

    for &range_size in &[100, 1000, 10000] {
        let range = KeyRange::bound(1000i64, 1000 + range_size as i64, false, false);
        let result = measure(ITERATIONS, || {
            tree.get_range(Some(&range), false, None, 0)
        });

        let throughput = range_size as f64 / result.mean.as_secs_f64();
        println!(
            "    {:>7} rows: {:>10} ({:>12})",
            range_size,
            format_duration(result.mean),
            format_throughput(throughput)
        );
        report.add_result("Index/BTree", "range", Some(range_size), result, Some(throughput));
    }
}

fn hash_insert(report: &mut Report) {
    println!("  Hash Insert:");
    for &size in &SIZES {
        let result = measure(ITERATIONS, || {
            let mut index = HashIndex::new(false);
            for i in 0..size as i64 {
                index.add(i, i as u64).unwrap();
            }
            index
        });

        let throughput = result.throughput(size);
        println!(
            "    {:>7} rows: {:>10} ({:>12})",
            size,
            format_duration(result.mean),
            format_throughput(throughput)
        );
        report.add_result("Index/Hash", "insert", Some(size), result, Some(throughput));
    }
}

fn hash_get(report: &mut Report) {
    println!("  Hash Point Lookup (random order):");
    for &size in &SIZES {
        // Setup
        let mut index = HashIndex::new(true);
        for i in 0..size as i64 {
            index.add(i, i as u64).unwrap();
        }

        // Use random order to avoid CPU prefetch optimization
        let lookup_keys: Vec<i64> = shuffle_indices(size, 54321)
            .into_iter()
            .take(1000.min(size))
            .map(|i| i as i64)
            .collect();
        let lookup_count = lookup_keys.len();

        let result = measure(ITERATIONS, || {
            let mut found = 0;
            for &key in &lookup_keys {
                if !index.get(&key).is_empty() {
                    found += 1;
                }
            }
            found
        });

        let throughput = lookup_count as f64 / result.mean.as_secs_f64();
        println!(
            "    {:>7} rows ({} lookups): {:>10} ({:>12})",
            size,
            lookup_count,
            format_duration(result.mean),
            format_throughput(throughput)
        );
        report.add_result("Index/Hash", "get", Some(size), result, Some(throughput));
    }
}

fn btree_vs_hash(report: &mut Report) {
    println!("  BTree vs Hash (10K rows, 1000 random lookups):");
    let size = 10_000i64;

    // Setup both
    let mut btree = BTreeIndex::new(64, true);
    let mut hash = HashIndex::new(true);
    for i in 0..size {
        btree.add(i, i as u64).unwrap();
        hash.add(i, i as u64).unwrap();
    }

    // Random lookup keys
    let lookup_keys: Vec<i64> = shuffle_indices(size as usize, 99999)
        .into_iter()
        .take(1000)
        .map(|i| i as i64)
        .collect();

    let btree_result = measure(ITERATIONS, || {
        let mut found = 0;
        for &key in &lookup_keys {
            if !btree.get(&key).is_empty() {
                found += 1;
            }
        }
        found
    });

    let hash_result = measure(ITERATIONS, || {
        let mut found = 0;
        for &key in &lookup_keys {
            if !hash.get(&key).is_empty() {
                found += 1;
            }
        }
        found
    });

    println!(
        "    BTree: {:>10}  |  Hash: {:>10}",
        format_duration(btree_result.mean),
        format_duration(hash_result.mean)
    );

    report.add_result("Index/Comparison", "btree_lookup", Some(10_000), btree_result, None);
    report.add_result("Index/Comparison", "hash_lookup", Some(10_000), hash_result, None);
}
