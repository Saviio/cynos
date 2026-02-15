//! Binary encoding and decoding for JSONB values.
//!
//! This module provides efficient binary serialization for JsonbValue.
//! The format is designed for compact storage and fast decoding.
//!
//! ## Encoding Format
//!
//! Each value is encoded as: `[type_tag: u8] [data...]`
//!
//! Type tags:
//! - 0x00: null
//! - 0x01: false
//! - 0x02: true
//! - 0x03: number (8 bytes, f64 little-endian)
//! - 0x04: string (varint length + UTF-8 bytes)
//! - 0x05: array (varint count + encoded elements)
//! - 0x06: object (varint count + sorted key-value pairs)

use crate::value::{JsonbObject, JsonbValue};
use alloc::string::String;
use alloc::vec::Vec;

const TAG_NULL: u8 = 0x00;
const TAG_FALSE: u8 = 0x01;
const TAG_TRUE: u8 = 0x02;
const TAG_NUMBER: u8 = 0x03;
const TAG_STRING: u8 = 0x04;
const TAG_ARRAY: u8 = 0x05;
const TAG_OBJECT: u8 = 0x06;

/// Binary representation of a JSONB value.
#[derive(Clone, Debug, PartialEq)]
pub struct JsonbBinary {
    data: Vec<u8>,
}

impl JsonbBinary {
    /// Creates a new JsonbBinary from raw bytes.
    pub fn from_bytes(data: Vec<u8>) -> Self {
        Self { data }
    }

    /// Returns the underlying bytes.
    pub fn as_bytes(&self) -> &[u8] {
        &self.data
    }

    /// Consumes self and returns the underlying bytes.
    pub fn into_bytes(self) -> Vec<u8> {
        self.data
    }

    /// Returns the size in bytes.
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Returns true if empty.
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Encodes a JsonbValue into binary format.
    pub fn encode(value: &JsonbValue) -> Self {
        let mut data = Vec::new();
        encode_value(value, &mut data);
        Self { data }
    }

    /// Decodes binary data into a JsonbValue.
    pub fn decode(&self) -> JsonbValue {
        let mut pos = 0;
        decode_value(&self.data, &mut pos)
    }
}

/// Encodes a varint (variable-length integer).
fn encode_varint(value: usize, out: &mut Vec<u8>) {
    let mut v = value;
    loop {
        let mut byte = (v & 0x7F) as u8;
        v >>= 7;
        if v != 0 {
            byte |= 0x80;
        }
        out.push(byte);
        if v == 0 {
            break;
        }
    }
}

/// Decodes a varint from the buffer.
fn decode_varint(data: &[u8], pos: &mut usize) -> usize {
    let mut result = 0usize;
    let mut shift = 0;
    loop {
        if *pos >= data.len() {
            break;
        }
        let byte = data[*pos];
        *pos += 1;
        result |= ((byte & 0x7F) as usize) << shift;
        if byte & 0x80 == 0 {
            break;
        }
        shift += 7;
    }
    result
}

/// Encodes a JsonbValue into the output buffer.
fn encode_value(value: &JsonbValue, out: &mut Vec<u8>) {
    match value {
        JsonbValue::Null => {
            out.push(TAG_NULL);
        }
        JsonbValue::Bool(false) => {
            out.push(TAG_FALSE);
        }
        JsonbValue::Bool(true) => {
            out.push(TAG_TRUE);
        }
        JsonbValue::Number(n) => {
            out.push(TAG_NUMBER);
            out.extend_from_slice(&n.to_le_bytes());
        }
        JsonbValue::String(s) => {
            out.push(TAG_STRING);
            encode_varint(s.len(), out);
            out.extend_from_slice(s.as_bytes());
        }
        JsonbValue::Array(arr) => {
            out.push(TAG_ARRAY);
            encode_varint(arr.len(), out);
            for item in arr {
                encode_value(item, out);
            }
        }
        JsonbValue::Object(obj) => {
            out.push(TAG_OBJECT);
            encode_varint(obj.len(), out);
            for (key, val) in obj.iter() {
                encode_varint(key.len(), out);
                out.extend_from_slice(key.as_bytes());
                encode_value(val, out);
            }
        }
    }
}

/// Decodes a JsonbValue from the buffer.
fn decode_value(data: &[u8], pos: &mut usize) -> JsonbValue {
    if *pos >= data.len() {
        return JsonbValue::Null;
    }

    let tag = data[*pos];
    *pos += 1;

    match tag {
        TAG_NULL => JsonbValue::Null,
        TAG_FALSE => JsonbValue::Bool(false),
        TAG_TRUE => JsonbValue::Bool(true),
        TAG_NUMBER => {
            if *pos + 8 > data.len() {
                return JsonbValue::Null;
            }
            let bytes: [u8; 8] = data[*pos..*pos + 8].try_into().unwrap_or([0; 8]);
            *pos += 8;
            JsonbValue::Number(f64::from_le_bytes(bytes))
        }
        TAG_STRING => {
            let len = decode_varint(data, pos);
            if *pos + len > data.len() {
                return JsonbValue::Null;
            }
            let s = String::from_utf8_lossy(&data[*pos..*pos + len]).into_owned();
            *pos += len;
            JsonbValue::String(s)
        }
        TAG_ARRAY => {
            let count = decode_varint(data, pos);
            let mut arr = Vec::with_capacity(count);
            for _ in 0..count {
                arr.push(decode_value(data, pos));
            }
            JsonbValue::Array(arr)
        }
        TAG_OBJECT => {
            let count = decode_varint(data, pos);
            let mut obj = JsonbObject::with_capacity(count);
            for _ in 0..count {
                let key_len = decode_varint(data, pos);
                if *pos + key_len > data.len() {
                    break;
                }
                let key = String::from_utf8_lossy(&data[*pos..*pos + key_len]).into_owned();
                *pos += key_len;
                let val = decode_value(data, pos);
                obj.insert(key, val);
            }
            JsonbValue::Object(obj)
        }
        _ => JsonbValue::Null,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloc::vec;

    #[test]
    fn test_encode_decode_null() {
        let value = JsonbValue::Null;
        let binary = JsonbBinary::encode(&value);
        let decoded = binary.decode();
        assert_eq!(value, decoded);
    }

    #[test]
    fn test_encode_decode_bool() {
        let value = JsonbValue::Bool(true);
        let binary = JsonbBinary::encode(&value);
        let decoded = binary.decode();
        assert_eq!(value, decoded);

        let value = JsonbValue::Bool(false);
        let binary = JsonbBinary::encode(&value);
        let decoded = binary.decode();
        assert_eq!(value, decoded);
    }

    #[test]
    fn test_encode_decode_number() {
        let value = JsonbValue::Number(42.5);
        let binary = JsonbBinary::encode(&value);
        let decoded = binary.decode();
        assert_eq!(value, decoded);

        let value = JsonbValue::Number(-123.456);
        let binary = JsonbBinary::encode(&value);
        let decoded = binary.decode();
        assert_eq!(value, decoded);
    }

    #[test]
    fn test_encode_decode_string() {
        let value = JsonbValue::String("hello world".into());
        let binary = JsonbBinary::encode(&value);
        let decoded = binary.decode();
        assert_eq!(value, decoded);

        let value = JsonbValue::String("".into());
        let binary = JsonbBinary::encode(&value);
        let decoded = binary.decode();
        assert_eq!(value, decoded);
    }

    #[test]
    fn test_encode_decode_array() {
        let value = JsonbValue::Array(vec![
            JsonbValue::Number(1.0),
            JsonbValue::String("two".into()),
            JsonbValue::Bool(true),
        ]);
        let binary = JsonbBinary::encode(&value);
        let decoded = binary.decode();
        assert_eq!(value, decoded);
    }

    #[test]
    fn test_encode_decode_object() {
        let mut obj = JsonbObject::new();
        obj.insert("name".into(), JsonbValue::String("Alice".into()));
        obj.insert("age".into(), JsonbValue::Number(25.0));
        obj.insert("active".into(), JsonbValue::Bool(true));

        let value = JsonbValue::Object(obj);
        let binary = JsonbBinary::encode(&value);
        let decoded = binary.decode();
        assert_eq!(value, decoded);
    }

    #[test]
    fn test_encode_decode_nested() {
        let mut inner_obj = JsonbObject::new();
        inner_obj.insert("city".into(), JsonbValue::String("NYC".into()));

        let mut obj = JsonbObject::new();
        obj.insert("name".into(), JsonbValue::String("Alice".into()));
        obj.insert("address".into(), JsonbValue::Object(inner_obj));
        obj.insert(
            "tags".into(),
            JsonbValue::Array(vec![
                JsonbValue::String("admin".into()),
                JsonbValue::String("developer".into()),
            ]),
        );

        let value = JsonbValue::Object(obj);
        let binary = JsonbBinary::encode(&value);
        let decoded = binary.decode();
        assert_eq!(value, decoded);
    }

    #[test]
    fn test_varint_encoding() {
        let mut buf = Vec::new();

        // Small number
        encode_varint(127, &mut buf);
        let mut pos = 0;
        assert_eq!(decode_varint(&buf, &mut pos), 127);

        // Larger number
        buf.clear();
        encode_varint(300, &mut buf);
        pos = 0;
        assert_eq!(decode_varint(&buf, &mut pos), 300);

        // Large number
        buf.clear();
        encode_varint(100000, &mut buf);
        pos = 0;
        assert_eq!(decode_varint(&buf, &mut pos), 100000);
    }

    #[test]
    fn test_binary_roundtrip_complex() {
        let mut obj = JsonbObject::new();
        obj.insert("string".into(), JsonbValue::String("hello".into()));
        obj.insert("number".into(), JsonbValue::Number(42.5));
        obj.insert("bool".into(), JsonbValue::Bool(true));
        obj.insert("null".into(), JsonbValue::Null);
        obj.insert(
            "array".into(),
            JsonbValue::Array(vec![
                JsonbValue::Number(1.0),
                JsonbValue::Number(2.0),
                JsonbValue::Number(3.0),
            ]),
        );

        let mut nested = JsonbObject::new();
        nested.insert("a".into(), JsonbValue::Number(1.0));
        obj.insert("nested".into(), JsonbValue::Object(nested));

        let original = JsonbValue::Object(obj);
        let binary = JsonbBinary::encode(&original);
        let decoded = binary.decode();

        assert_eq!(original, decoded);
    }

    // Edge case tests
    #[test]
    fn test_encode_decode_empty_array() {
        let value = JsonbValue::Array(vec![]);
        let binary = JsonbBinary::encode(&value);
        let decoded = binary.decode();
        assert_eq!(value, decoded);
    }

    #[test]
    fn test_encode_decode_empty_object() {
        let obj = JsonbObject::new();
        let value = JsonbValue::Object(obj);
        let binary = JsonbBinary::encode(&value);
        let decoded = binary.decode();
        assert_eq!(value, decoded);
    }

    #[test]
    fn test_encode_decode_unicode_string() {
        let value = JsonbValue::String("你好世界 🌍 émojis".into());
        let binary = JsonbBinary::encode(&value);
        let decoded = binary.decode();
        assert_eq!(value, decoded);
    }

    #[test]
    fn test_encode_decode_unicode_keys() {
        let mut obj = JsonbObject::new();
        obj.insert("名前".into(), JsonbValue::String("田中".into()));
        obj.insert("年齢".into(), JsonbValue::Number(25.0));
        obj.insert("🔑".into(), JsonbValue::String("emoji key".into()));

        let value = JsonbValue::Object(obj);
        let binary = JsonbBinary::encode(&value);
        let decoded = binary.decode();
        assert_eq!(value, decoded);
    }

    #[test]
    fn test_encode_decode_special_numbers() {
        // Zero
        let value = JsonbValue::Number(0.0);
        let binary = JsonbBinary::encode(&value);
        assert_eq!(value, binary.decode());

        // Negative zero
        let value = JsonbValue::Number(-0.0);
        let binary = JsonbBinary::encode(&value);
        let decoded = binary.decode();
        // -0.0 and 0.0 are equal in f64 comparison
        assert_eq!(decoded.as_f64(), Some(0.0));

        // Very large number
        let value = JsonbValue::Number(1e308);
        let binary = JsonbBinary::encode(&value);
        assert_eq!(value, binary.decode());

        // Very small number
        let value = JsonbValue::Number(1e-308);
        let binary = JsonbBinary::encode(&value);
        assert_eq!(value, binary.decode());

        // Infinity
        let value = JsonbValue::Number(f64::INFINITY);
        let binary = JsonbBinary::encode(&value);
        assert_eq!(value, binary.decode());

        // NaN - special case, NaN != NaN
        let value = JsonbValue::Number(f64::NAN);
        let binary = JsonbBinary::encode(&value);
        let decoded = binary.decode();
        assert!(decoded.as_f64().unwrap().is_nan());
    }

    #[test]
    fn test_encode_decode_special_strings() {
        // Empty string
        let value = JsonbValue::String("".into());
        let binary = JsonbBinary::encode(&value);
        assert_eq!(value, binary.decode());

        // String with null bytes
        let value = JsonbValue::String("hello\0world".into());
        let binary = JsonbBinary::encode(&value);
        assert_eq!(value, binary.decode());

        // String with newlines and tabs
        let value = JsonbValue::String("line1\nline2\ttab".into());
        let binary = JsonbBinary::encode(&value);
        assert_eq!(value, binary.decode());

        // String with quotes
        let value = JsonbValue::String("say \"hello\"".into());
        let binary = JsonbBinary::encode(&value);
        assert_eq!(value, binary.decode());
    }

    #[test]
    fn test_decode_malformed_truncated_number() {
        // Number tag but not enough bytes
        let data = vec![TAG_NUMBER, 0x00, 0x00]; // Only 3 bytes instead of 8
        let binary = JsonbBinary::from_bytes(data);
        let decoded = binary.decode();
        assert_eq!(decoded, JsonbValue::Null);
    }

    #[test]
    fn test_decode_malformed_truncated_string() {
        // String tag with length 10 but only 3 bytes of data
        let data = vec![TAG_STRING, 10, b'a', b'b', b'c'];
        let binary = JsonbBinary::from_bytes(data);
        let decoded = binary.decode();
        assert_eq!(decoded, JsonbValue::Null);
    }

    #[test]
    fn test_decode_unknown_tag() {
        let data = vec![0xFF]; // Unknown tag
        let binary = JsonbBinary::from_bytes(data);
        let decoded = binary.decode();
        assert_eq!(decoded, JsonbValue::Null);
    }

    #[test]
    fn test_decode_empty_data() {
        let binary = JsonbBinary::from_bytes(vec![]);
        let decoded = binary.decode();
        assert_eq!(decoded, JsonbValue::Null);
    }

    #[test]
    fn test_deeply_nested_structure() {
        // Create a deeply nested structure: [[[[[[1]]]]]]
        let mut value = JsonbValue::Number(1.0);
        for _ in 0..10 {
            value = JsonbValue::Array(vec![value]);
        }
        let binary = JsonbBinary::encode(&value);
        let decoded = binary.decode();
        assert_eq!(value, decoded);
    }

    #[test]
    fn test_varint_edge_cases() {
        let mut buf = Vec::new();

        // Zero
        encode_varint(0, &mut buf);
        let mut pos = 0;
        assert_eq!(decode_varint(&buf, &mut pos), 0);

        // Max single byte (127)
        buf.clear();
        encode_varint(127, &mut buf);
        assert_eq!(buf.len(), 1);
        pos = 0;
        assert_eq!(decode_varint(&buf, &mut pos), 127);

        // Min two bytes (128)
        buf.clear();
        encode_varint(128, &mut buf);
        assert_eq!(buf.len(), 2);
        pos = 0;
        assert_eq!(decode_varint(&buf, &mut pos), 128);

        // Large value
        buf.clear();
        encode_varint(usize::MAX >> 1, &mut buf);
        pos = 0;
        assert_eq!(decode_varint(&buf, &mut pos), usize::MAX >> 1);
    }
}
