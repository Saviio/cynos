//! Benchmarks for GIN JSONB_CONTAINS prefilter selectivity.

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use cynos_core::schema::TableBuilder;
use cynos_core::{DataType, Row, Value};
use cynos_storage::RowStore;
use std::collections::BTreeSet;

const INDEX_NAME: &str = "idx_data_gin";
const PATH_KEY: &str = "tags";
const NEEDLE: &str = "portable";
const PORTABLE_EVERY: u64 = 200;

fn create_gin_schema() -> cynos_core::schema::Table {
    TableBuilder::new("products")
        .unwrap()
        .add_column("id", DataType::Int64)
        .unwrap()
        .add_column("data", DataType::Jsonb)
        .unwrap()
        .add_primary_key(&["id"], false)
        .unwrap()
        .add_index(INDEX_NAME, &["data"], false)
        .unwrap()
        .build()
        .unwrap()
}

fn make_jsonb(json_str: &str) -> Value {
    Value::Jsonb(cynos_core::JsonbValue(json_str.as_bytes().to_vec()))
}

fn build_row(id: u64) -> Row {
    let tags_json = if id % PORTABLE_EVERY == 0 {
        r#"["portable","travel"]"#.to_string()
    } else {
        match id % 4 {
            0 => r#"["desktop","office"]"#.to_string(),
            1 => r#"["camera","travel"]"#.to_string(),
            2 => r#"["speaker","studio"]"#.to_string(),
            _ => r#"["printer","office"]"#.to_string(),
        }
    };

    Row::new(
        id,
        vec![
            Value::Int64(id as i64),
            make_jsonb(&format!(
                r#"{{"tags":{tags_json},"category":"device","sku":"SKU{id}"}}"#
            )),
        ],
    )
}

fn build_store(row_count: u64) -> RowStore {
    let mut store = RowStore::new(create_gin_schema());
    for row_id in 1..=row_count {
        store.insert(build_row(row_id)).unwrap();
    }
    store
}

fn contains_trigram_key(path: &str) -> String {
    format!("__cynos_contains3__:{path}")
}

fn contains_trigrams(value: &str) -> Vec<String> {
    let chars: Vec<char> = value.chars().collect();
    if chars.len() < 3 {
        return Vec::new();
    }

    let mut grams = BTreeSet::new();
    for window in chars.windows(3) {
        let gram: String = window.iter().collect();
        grams.insert(gram);
    }

    grams.into_iter().collect()
}

fn contains_pairs(path: &str, needle: &str) -> Vec<(String, String)> {
    let contains_key = contains_trigram_key(path);
    contains_trigrams(needle)
        .into_iter()
        .map(|gram| (contains_key.clone(), gram))
        .collect()
}

fn gin_contains_prefilter_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("gin_contains_prefilter");

    for row_count in [10_000u64, 50_000u64] {
        let store = build_store(row_count);
        let pair_strings = contains_pairs(PATH_KEY, NEEDLE);
        let pair_refs: Vec<(&str, &str)> = pair_strings
            .iter()
            .map(|(key, value)| (key.as_str(), value.as_str()))
            .collect();
        let expected_matches = row_count / PORTABLE_EVERY;

        let legacy_rows = store.gin_index_get_by_key(INDEX_NAME, PATH_KEY);
        let optimized_rows = store.gin_index_get_by_key_values_all(INDEX_NAME, &pair_refs);
        assert_eq!(legacy_rows.len(), row_count as usize);
        assert_eq!(optimized_rows.len(), expected_matches as usize);

        group.bench_with_input(BenchmarkId::new("legacy", row_count), &row_count, |b, _| {
            b.iter(|| black_box(store.gin_index_get_by_key(INDEX_NAME, PATH_KEY)));
        });

        group.bench_with_input(
            BenchmarkId::new("optimized", row_count),
            &row_count,
            |b, _| {
                b.iter(|| black_box(store.gin_index_get_by_key_values_all(INDEX_NAME, &pair_refs)));
            },
        );
    }

    group.finish();
}

criterion_group!(benches, gin_contains_prefilter_benchmark);
criterion_main!(benches);
