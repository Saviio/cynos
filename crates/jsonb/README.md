# cynos-jsonb

JSONB value model, binary codec, JSONPath subset, and JSONB operators for Cynos.

## Overview

This crate provides the structured JSONB pieces used by storage, query planning, and indexing:

- `JsonbValue` / `JsonbObject`: owned JSONB value types with sorted object keys.
- `JsonbBinary`: compact binary encode/decode support.
- `JsonPath`: parser for a practical JSONPath subset.
- `JsonbOp`: JSONB-style operators such as field access, containment, and key existence.
- Extraction helpers (`extract_keys`, `extract_key_values`, `extract_paths`, `extract_scalars`) used for GIN indexing.

## What This Crate Does Not Do

- It does not expose a full generic JSON text parser like `serde_json`.
- In practice you build values programmatically, decode them from `JsonbBinary`, or receive them through higher-level database APIs.

## Supported JSONPath Syntax

- `$` root
- `.field` or `['field']`
- `[index]`
- `[start:end]`
- `[*]` and `.*`
- `..field`
- `[?(@.field <op> value)]`

## Example

```rust
use cynos_jsonb::{JsonPath, JsonbBinary, JsonbObject, JsonbOp, JsonbValue};

fn main() -> Result<(), cynos_jsonb::ParseError> {
    let mut obj = JsonbObject::new();
    obj.insert("name".into(), JsonbValue::String("Alice".into()));
    obj.insert("age".into(), JsonbValue::Number(25.0));

    let json = JsonbValue::Object(obj.clone());

    let path = JsonPath::parse("$.name")?;
    let matches = json.query(&path);
    assert_eq!(matches[0], &JsonbValue::String("Alice".into()));

    let encoded = JsonbBinary::encode(&json);
    let decoded = encoded.decode();
    assert_eq!(decoded, JsonbValue::Object(obj));

    assert_eq!(
        json.apply_op(&JsonbOp::HasKey("name".into())),
        Some(JsonbValue::Bool(true)),
    );
    Ok(())
}
```

## Notes

- Objects keep keys sorted so lookup is efficient and deterministic.
- `contains()` and related operators recurse structurally for objects and arrays.
- The GIN helpers intentionally work on extracted tokens rather than the original textual JSON representation.

## License

Apache-2.0
