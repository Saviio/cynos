use alloc::boxed::Box;
use alloc::string::String;
use alloc::vec::Vec;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Document {
    pub operations: Vec<OperationDefinition>,
}

impl Document {
    pub fn new(operations: Vec<OperationDefinition>) -> Self {
        Self { operations }
    }

    pub fn is_empty(&self) -> bool {
        self.operations.is_empty()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OperationType {
    Query,
    Mutation,
    Subscription,
}

impl OperationType {
    pub fn root_typename(self) -> &'static str {
        match self {
            Self::Query => "Query",
            Self::Mutation => "Mutation",
            Self::Subscription => "Subscription",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OperationDefinition {
    pub kind: OperationType,
    pub name: Option<String>,
    pub variable_definitions: Vec<VariableDefinition>,
    pub selection_set: SelectionSet,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VariableDefinition {
    pub name: String,
    pub type_ref: TypeReference,
    pub default_value: Option<InputValue>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SelectionSet {
    pub fields: Vec<Field>,
}

impl SelectionSet {
    pub fn new(fields: Vec<Field>) -> Self {
        Self { fields }
    }

    pub fn is_empty(&self) -> bool {
        self.fields.is_empty()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Field {
    pub alias: Option<String>,
    pub name: String,
    pub arguments: Vec<Argument>,
    pub directives: Vec<Directive>,
    pub selection_set: Option<SelectionSet>,
}

impl Field {
    pub fn response_key(&self) -> &str {
        self.alias.as_deref().unwrap_or(&self.name)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Argument {
    pub name: String,
    pub value: InputValue,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Directive {
    pub name: String,
    pub arguments: Vec<Argument>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum InputValue {
    Null,
    Boolean(bool),
    Int(i64),
    Float(FloatValue),
    String(String),
    Enum(String),
    List(Vec<InputValue>),
    Object(Vec<ObjectField>),
    Variable(String),
}

impl InputValue {
    pub fn as_object(&self) -> Option<&[ObjectField]> {
        match self {
            Self::Object(fields) => Some(fields),
            _ => None,
        }
    }

    pub fn as_list(&self) -> Option<&[InputValue]> {
        match self {
            Self::List(values) => Some(values),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FloatValue(u64);

impl FloatValue {
    pub fn new(value: f64) -> Self {
        Self(value.to_bits())
    }

    pub fn as_f64(self) -> f64 {
        f64::from_bits(self.0)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ObjectField {
    pub name: String,
    pub value: InputValue,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TypeReference {
    Named(String),
    List(Box<TypeReference>),
    NonNull(Box<TypeReference>),
}
