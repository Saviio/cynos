use alloc::string::String;
use alloc::vec::Vec;
use cynos_core::Value;

#[derive(Clone, Debug, PartialEq)]
pub struct GraphqlResponse {
    pub data: ResponseValue,
}

impl GraphqlResponse {
    pub fn new(data: ResponseValue) -> Self {
        Self { data }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum ResponseValue {
    Null,
    Scalar(Value),
    Object(Vec<ResponseField>),
    List(Vec<ResponseValue>),
}

impl ResponseValue {
    pub fn object(fields: Vec<ResponseField>) -> Self {
        Self::Object(fields)
    }

    pub fn list(items: Vec<ResponseValue>) -> Self {
        Self::List(items)
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct ResponseField {
    pub name: String,
    pub value: ResponseValue,
}

impl ResponseField {
    pub fn new(name: impl Into<String>, value: ResponseValue) -> Self {
        Self {
            name: name.into(),
            value,
        }
    }
}
