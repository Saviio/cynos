//! JSONB performance benchmarks

use crate::report::Report;
use crate::utils::*;
use cynos_jsonb::{JsonbBinary, JsonbObject, JsonbValue, JsonPath};

pub fn run(report: &mut Report) {
    parse(report);
    encode_decode(report);
    query(report);
    operations(report);
}

fn create_simple_json() -> JsonbValue {
    let mut obj = JsonbObject::new();
    obj.insert("name".into(), JsonbValue::String("Alice".into()));
    obj.insert("age".into(), JsonbValue::Number(25.0));
    obj.insert("active".into(), JsonbValue::Bool(true));
    JsonbValue::Object(obj)
}

fn create_nested_json(depth: usize) -> JsonbValue {
    let mut current = JsonbValue::String("leaf".into());
    for i in 0..depth {
        let mut obj = JsonbObject::new();
        obj.insert(format!("level_{}", i).into(), current);
        obj.insert("index".into(), JsonbValue::Number(i as f64));
        current = JsonbValue::Object(obj);
    }
    current
}

fn create_array_json(size: usize) -> JsonbValue {
    // Create {items: [...]} structure to match $.items[*].name path
    let items: Vec<JsonbValue> = (0..size)
        .map(|i| {
            let mut obj = JsonbObject::new();
            obj.insert("id".into(), JsonbValue::Number(i as f64));
            obj.insert("name".into(), JsonbValue::String(format!("item_{}", i).into()));
            obj.insert("value".into(), JsonbValue::Number((i * 100) as f64));
            JsonbValue::Object(obj)
        })
        .collect();
    let mut root = JsonbObject::new();
    root.insert("items".into(), JsonbValue::Array(items.into()));
    JsonbValue::Object(root)
}

fn parse(report: &mut Report) {
    println!("  JSONPath Parse:");

    let paths = [
        ("simple", "$.name"),
        ("nested", "$.user.profile.name"),
        ("array", "$.items[0].name"),
        ("wildcard", "$.items[*].name"),
        ("filter", "$.items[?(@.age > 18)].name"),
    ];

    for (name, path_str) in paths {
        let result = measure(ITERATIONS * 100, || JsonPath::parse(path_str));

        println!(
            "    {:<12}: {:>10}",
            name,
            format_duration(result.mean)
        );
        report.add_result("JSONB/Parse", name, None, result, None);
    }
}

fn encode_decode(report: &mut Report) {
    println!("  JSONB Encode/Decode:");

    // Simple object
    let simple = create_simple_json();
    let result = measure(ITERATIONS * 100, || {
        let binary = JsonbBinary::encode(&simple);
        binary.decode()
    });
    println!(
        "    simple:      {:>10}",
        format_duration(result.mean)
    );
    report.add_result("JSONB/Codec", "simple", None, result, None);

    // Nested object
    for &depth in &[5, 10, 20] {
        let nested = create_nested_json(depth);
        let result = measure(ITERATIONS * 10, || {
            let binary = JsonbBinary::encode(&nested);
            binary.decode()
        });
        println!(
            "    nested({}):  {:>10}",
            depth,
            format_duration(result.mean)
        );
        report.add_result("JSONB/Codec", &format!("nested_{}", depth), None, result, None);
    }

    // Array
    for &size in &[10, 100, 1000] {
        let array = create_array_json(size);
        let result = measure(ITERATIONS, || {
            let binary = JsonbBinary::encode(&array);
            binary.decode()
        });
        let throughput = result.throughput(size);
        println!(
            "    array({}): {:>10} ({:>12})",
            size,
            format_duration(result.mean),
            format_throughput(throughput)
        );
        report.add_result("JSONB/Codec", &format!("array_{}", size), Some(size), result, Some(throughput));
    }
}

fn query(report: &mut Report) {
    println!("  JSONPath Query:");

    // Simple query
    let simple = create_simple_json();
    let path = JsonPath::parse("$.name").unwrap();
    let result = measure(ITERATIONS * 100, || simple.query(&path));
    println!(
        "    simple:      {:>10}",
        format_duration(result.mean)
    );
    report.add_result("JSONB/Query", "simple", None, result, None);

    // Nested query
    let nested = create_nested_json(10);
    let path = JsonPath::parse("$.level_9.level_8.level_7.index").unwrap();
    let result = measure(ITERATIONS * 100, || nested.query(&path));
    println!(
        "    nested:      {:>10}",
        format_duration(result.mean)
    );
    report.add_result("JSONB/Query", "nested", None, result, None);

    // Array query - now correctly matches $.items[*].name
    for &size in &[10, 100, 1000] {
        let array = create_array_json(size);
        let path = JsonPath::parse("$.items[*].name").unwrap();

        // Verify the query actually returns results
        let test_result = array.query(&path);
        let result_count = test_result.len();

        let result = measure(ITERATIONS, || {
            let query_result = array.query(&path);
            // Force materialization of results
            std::hint::black_box(&query_result);
            query_result.len()
        });
        let throughput = result.throughput(size);
        println!(
            "    array({}, {} results): {:>10} ({:>12})",
            size,
            result_count,
            format_duration(result.mean),
            format_throughput(throughput)
        );
        report.add_result("JSONB/Query", &format!("array_{}", size), Some(size), result, Some(throughput));
    }
}

fn operations(report: &mut Report) {
    println!("  JSONB Operations:");

    // Create a larger object for more realistic testing
    let mut large_obj = JsonbObject::new();
    for i in 0..100 {
        large_obj.insert(format!("key_{}", i).into(), JsonbValue::String(format!("value_{}", i).into()));
    }
    let large_json = JsonbValue::Object(large_obj.clone());

    // Get by key (from larger object)
    let result = measure(ITERATIONS, || {
        let mut found = 0;
        for i in 0..100 {
            if let JsonbValue::Object(ref obj) = large_json {
                if obj.get(&format!("key_{}", i)).is_some() {
                    found += 1;
                }
            }
        }
        found
    });
    println!(
        "    get_key (100 lookups): {:>10}",
        format_duration(result.mean)
    );
    report.add_result("JSONB/Ops", "get_key_100", None, result, None);

    // Contains key
    let result = measure(ITERATIONS, || {
        let mut found = 0;
        for i in 0..100 {
            if let JsonbValue::Object(ref obj) = large_json {
                if obj.contains_key(&format!("key_{}", i)) {
                    found += 1;
                }
            }
        }
        found
    });
    println!(
        "    contains (100 checks): {:>10}",
        format_duration(result.mean)
    );
    report.add_result("JSONB/Ops", "contains_key_100", None, result, None);

    // Insert key (build object from scratch)
    let result = measure(ITERATIONS, || {
        let mut obj = JsonbObject::new();
        for i in 0..100 {
            obj.insert(format!("key_{}", i).into(), JsonbValue::String(format!("value_{}", i).into()));
        }
        obj
    });
    println!(
        "    insert (100 keys):     {:>10}",
        format_duration(result.mean)
    );
    report.add_result("JSONB/Ops", "insert_100", None, result, None);

    // Full encode/decode cycle with larger object
    let result = measure(ITERATIONS, || {
        let binary = JsonbBinary::encode(&large_json);
        let decoded = binary.decode();
        decoded
    });
    println!(
        "    encode+decode (100 keys): {:>10}",
        format_duration(result.mean)
    );
    report.add_result("JSONB/Ops", "encode_decode_100", None, result, None);
}
