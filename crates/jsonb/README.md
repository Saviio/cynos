# cynos-jsonb

JSONB data type implementation for Cynos database.

## Overview

This crate provides a complete JSONB implementation including:

- `JsonbValue`: The core JSON value type with sorted object keys
- `JsonbBinary`: Binary encoding/decoding for efficient storage
- `JsonPath`: JSONPath query language support
- `JsonbOp`: PostgreSQL-compatible JSONB operators
- GIN index support for efficient querying

## Features

- `#![no_std]` compatible
- Compact binary encoding for storage efficiency
- Full JSONPath query support
- PostgreSQL-compatible operators (@>, <@, ?, ?|, ?&)

## Usage

```rust
use cynos_jsonb::{JsonbValue, JsonbObject, JsonPath, JsonbBinary};

// Create a JSON object
let mut obj = JsonbObject::new();
obj.insert("name".into(), JsonbValue::String("Alice".into()));
obj.insert("age".into(), JsonbValue::Number(25.0));

let json = JsonbValue::Object(obj);

// Query with JSONPath
let path = JsonPath::parse("$.name").unwrap();
let results = json.query(&path);
assert_eq!(results[0], &JsonbValue::String("Alice".into()));

// Binary encoding
let binary = JsonbBinary::encode(&json);
let decoded = binary.decode();
assert_eq!(json, decoded);
```

## JSONPath Support

- `$` - Root element
- `.key` - Object member access
- `[n]` - Array index access
- `[*]` - Wildcard array access
- `..key` - Recursive descent
- `[?(@.key > value)]` - Filter expressions

## License

Apache-2.0
