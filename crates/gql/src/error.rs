use alloc::string::String;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GqlErrorKind {
    Syntax,
    Validation,
    Binding,
    Execution,
    Unsupported,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GqlError {
    kind: GqlErrorKind,
    message: String,
}

impl GqlError {
    pub fn new(kind: GqlErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }

    pub fn kind(&self) -> GqlErrorKind {
        self.kind
    }

    pub fn message(&self) -> &str {
        &self.message
    }
}

pub type GqlResult<T> = core::result::Result<T, GqlError>;
