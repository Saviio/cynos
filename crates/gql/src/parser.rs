use alloc::boxed::Box;
use alloc::format;
use alloc::string::{String, ToString};
use alloc::vec::Vec;

use crate::ast::{
    Argument, Directive, Document, Field, FloatValue, InputValue, ObjectField, OperationDefinition,
    OperationType, SelectionSet, TypeReference, VariableDefinition,
};
use crate::error::{GqlError, GqlErrorKind, GqlResult};

pub fn parse_document(input: &str) -> GqlResult<Document> {
    let mut parser = Parser::new(input);
    parser.parse_document()
}

struct Parser<'a> {
    input: &'a str,
    pos: usize,
}

impl<'a> Parser<'a> {
    fn new(input: &'a str) -> Self {
        Self { input, pos: 0 }
    }

    fn parse_document(&mut self) -> GqlResult<Document> {
        self.skip_ignored();
        let mut operations = Vec::new();
        while !self.is_eof() {
            operations.push(self.parse_operation_definition()?);
            self.skip_ignored();
        }

        if operations.is_empty() {
            return self.syntax_error("expected GraphQL operation");
        }

        Ok(Document::new(operations))
    }

    fn parse_operation_definition(&mut self) -> GqlResult<OperationDefinition> {
        self.skip_ignored();
        if self.peek_char() == Some('{') {
            return Ok(OperationDefinition {
                kind: OperationType::Query,
                name: None,
                variable_definitions: Vec::new(),
                selection_set: self.parse_selection_set()?,
            });
        }

        let keyword = self.parse_name()?;
        let kind = match keyword.as_str() {
            "query" => OperationType::Query,
            "mutation" => OperationType::Mutation,
            "subscription" => OperationType::Subscription,
            _ => {
                return Err(GqlError::new(
                    GqlErrorKind::Syntax,
                    format!("unexpected token `{}`", keyword),
                ))
            }
        };

        self.skip_ignored();
        let name = if matches!(self.peek_char(), Some('$' | '{')) {
            None
        } else if self.peek_name_start() {
            Some(self.parse_name()?)
        } else {
            None
        };

        self.skip_ignored();
        let variable_definitions = if self.peek_char() == Some('(') {
            self.parse_variable_definitions()?
        } else {
            Vec::new()
        };

        if self.peek_char() == Some('@') {
            return Err(GqlError::new(
                GqlErrorKind::Unsupported,
                "directives are only supported on fields",
            ));
        }

        let selection_set = self.parse_selection_set()?;
        Ok(OperationDefinition {
            kind,
            name,
            variable_definitions,
            selection_set,
        })
    }

    fn parse_variable_definitions(&mut self) -> GqlResult<Vec<VariableDefinition>> {
        self.expect_char('(')?;
        let mut definitions = Vec::new();
        loop {
            self.skip_ignored();
            if self.peek_char() == Some(')') {
                self.advance_char();
                break;
            }

            self.expect_char('$')?;
            let name = self.parse_name()?;
            self.expect_char(':')?;
            let type_ref = self.parse_type_reference()?;
            self.skip_ignored();
            let default_value = if self.peek_char() == Some('=') {
                self.advance_char();
                Some(self.parse_input_value()?)
            } else {
                None
            };
            definitions.push(VariableDefinition {
                name,
                type_ref,
                default_value,
            });
            self.skip_ignored();
        }
        Ok(definitions)
    }

    fn parse_type_reference(&mut self) -> GqlResult<TypeReference> {
        self.skip_ignored();
        let mut ty = if self.peek_char() == Some('[') {
            self.advance_char();
            let inner = self.parse_type_reference()?;
            self.expect_char(']')?;
            TypeReference::List(Box::new(inner))
        } else {
            TypeReference::Named(self.parse_name()?)
        };

        self.skip_ignored();
        if self.peek_char() == Some('!') {
            self.advance_char();
            ty = TypeReference::NonNull(Box::new(ty));
        }
        Ok(ty)
    }

    fn parse_selection_set(&mut self) -> GqlResult<SelectionSet> {
        self.expect_char('{')?;
        let mut fields = Vec::new();
        loop {
            self.skip_ignored();
            match self.peek_char() {
                Some('}') => {
                    self.advance_char();
                    break;
                }
                Some('.') => {
                    return Err(GqlError::new(
                        GqlErrorKind::Unsupported,
                        "fragments are not supported yet",
                    ))
                }
                Some(_) => fields.push(self.parse_field()?),
                None => return self.syntax_error("unterminated selection set"),
            }
        }

        if fields.is_empty() {
            return self.syntax_error("selection set cannot be empty");
        }

        Ok(SelectionSet::new(fields))
    }

    fn parse_field(&mut self) -> GqlResult<Field> {
        self.skip_ignored();
        let first_name = self.parse_name()?;
        self.skip_ignored();

        let (alias, name) = if self.peek_char() == Some(':') {
            self.advance_char();
            (Some(first_name), self.parse_name()?)
        } else {
            (None, first_name)
        };

        self.skip_ignored();
        let arguments = if self.peek_char() == Some('(') {
            self.parse_arguments()?
        } else {
            Vec::new()
        };

        let directives = self.parse_directives()?;

        self.skip_ignored();
        let selection_set = if self.peek_char() == Some('{') {
            Some(self.parse_selection_set()?)
        } else {
            None
        };

        Ok(Field {
            alias,
            name,
            arguments,
            directives,
            selection_set,
        })
    }

    fn parse_directives(&mut self) -> GqlResult<Vec<Directive>> {
        let mut directives = Vec::new();
        loop {
            self.skip_ignored();
            if self.peek_char() != Some('@') {
                break;
            }

            self.advance_char();
            let name = self.parse_name()?;
            self.skip_ignored();
            let arguments = if self.peek_char() == Some('(') {
                self.parse_arguments()?
            } else {
                Vec::new()
            };
            directives.push(Directive { name, arguments });
        }
        Ok(directives)
    }

    fn parse_arguments(&mut self) -> GqlResult<Vec<Argument>> {
        self.expect_char('(')?;
        let mut arguments = Vec::new();
        loop {
            self.skip_ignored();
            if self.peek_char() == Some(')') {
                self.advance_char();
                break;
            }

            let name = self.parse_name()?;
            self.expect_char(':')?;
            let value = self.parse_input_value()?;
            arguments.push(Argument { name, value });
            self.skip_ignored();
        }
        Ok(arguments)
    }

    fn parse_input_value(&mut self) -> GqlResult<InputValue> {
        self.skip_ignored();
        match self.peek_char() {
            Some('"') => self.parse_string().map(InputValue::String),
            Some('[') => self.parse_list_value(),
            Some('{') => self.parse_object_value(),
            Some('$') => {
                self.advance_char();
                self.parse_name().map(InputValue::Variable)
            }
            Some('-') | Some('0'..='9') => self.parse_number_value(),
            Some(_) => {
                let name = self.parse_name()?;
                match name.as_str() {
                    "true" => Ok(InputValue::Boolean(true)),
                    "false" => Ok(InputValue::Boolean(false)),
                    "null" => Ok(InputValue::Null),
                    _ => Ok(InputValue::Enum(name)),
                }
            }
            None => self.syntax_error("expected input value"),
        }
    }

    fn parse_list_value(&mut self) -> GqlResult<InputValue> {
        self.expect_char('[')?;
        let mut values = Vec::new();
        loop {
            self.skip_ignored();
            if self.peek_char() == Some(']') {
                self.advance_char();
                break;
            }
            values.push(self.parse_input_value()?);
            self.skip_ignored();
        }
        Ok(InputValue::List(values))
    }

    fn parse_object_value(&mut self) -> GqlResult<InputValue> {
        self.expect_char('{')?;
        let mut fields = Vec::new();
        loop {
            self.skip_ignored();
            if self.peek_char() == Some('}') {
                self.advance_char();
                break;
            }
            let name = self.parse_name()?;
            self.expect_char(':')?;
            let value = self.parse_input_value()?;
            fields.push(ObjectField { name, value });
            self.skip_ignored();
        }
        Ok(InputValue::Object(fields))
    }

    fn parse_number_value(&mut self) -> GqlResult<InputValue> {
        let start = self.pos;
        if self.peek_char() == Some('-') {
            self.advance_char();
        }

        let mut has_digits = false;
        while matches!(self.peek_char(), Some('0'..='9')) {
            has_digits = true;
            self.advance_char();
        }

        let mut is_float = false;
        if self.peek_char() == Some('.') {
            is_float = true;
            self.advance_char();
            let mut fraction_digits = false;
            while matches!(self.peek_char(), Some('0'..='9')) {
                fraction_digits = true;
                self.advance_char();
            }
            if !fraction_digits {
                return self.syntax_error("expected digits after decimal point");
            }
        }

        if matches!(self.peek_char(), Some('e' | 'E')) {
            is_float = true;
            self.advance_char();
            if matches!(self.peek_char(), Some('+' | '-')) {
                self.advance_char();
            }
            let mut exp_digits = false;
            while matches!(self.peek_char(), Some('0'..='9')) {
                exp_digits = true;
                self.advance_char();
            }
            if !exp_digits {
                return self.syntax_error("expected exponent digits");
            }
        }

        if !has_digits {
            return self.syntax_error("expected numeric literal");
        }

        let literal = &self.input[start..self.pos];
        if is_float {
            let value = literal
                .parse::<f64>()
                .map_err(|_| GqlError::new(GqlErrorKind::Syntax, "invalid float literal"))?;
            Ok(InputValue::Float(FloatValue::new(value)))
        } else {
            let value = literal
                .parse::<i64>()
                .map_err(|_| GqlError::new(GqlErrorKind::Syntax, "invalid integer literal"))?;
            Ok(InputValue::Int(value))
        }
    }

    fn parse_string(&mut self) -> GqlResult<String> {
        self.expect_char('"')?;
        let mut out = String::new();
        loop {
            let ch = self.advance_char().ok_or_else(|| {
                GqlError::new(GqlErrorKind::Syntax, "unterminated string literal")
            })?;
            match ch {
                '"' => break,
                '\\' => {
                    let escaped = self.advance_char().ok_or_else(|| {
                        GqlError::new(GqlErrorKind::Syntax, "unterminated escape sequence")
                    })?;
                    match escaped {
                        '"' => out.push('"'),
                        '\\' => out.push('\\'),
                        '/' => out.push('/'),
                        'b' => out.push('\u{0008}'),
                        'f' => out.push('\u{000C}'),
                        'n' => out.push('\n'),
                        'r' => out.push('\r'),
                        't' => out.push('\t'),
                        'u' => out.push(self.parse_unicode_escape()?),
                        other => {
                            return Err(GqlError::new(
                                GqlErrorKind::Syntax,
                                format!("unsupported escape sequence `\\{}`", other),
                            ))
                        }
                    }
                }
                _ => out.push(ch),
            }
        }
        Ok(out)
    }

    fn parse_unicode_escape(&mut self) -> GqlResult<char> {
        let start = self.pos;
        for _ in 0..4 {
            match self.advance_char() {
                Some(ch) if ch.is_ascii_hexdigit() => {}
                _ => return self.syntax_error("invalid unicode escape"),
            }
        }
        let hex = &self.input[start..self.pos];
        let value = u32::from_str_radix(hex, 16)
            .map_err(|_| GqlError::new(GqlErrorKind::Syntax, "invalid unicode escape"))?;
        char::from_u32(value)
            .ok_or_else(|| GqlError::new(GqlErrorKind::Syntax, "invalid unicode code point"))
    }

    fn parse_name(&mut self) -> GqlResult<String> {
        self.skip_ignored();
        let start = self.pos;
        match self.peek_char() {
            Some(ch) if is_name_start(ch) => self.advance_char(),
            Some(_) => return self.syntax_error("expected name"),
            None => return self.syntax_error("unexpected end of input"),
        };
        while matches!(self.peek_char(), Some(ch) if is_name_continue(ch)) {
            self.advance_char();
        }
        Ok(self.input[start..self.pos].to_string())
    }

    fn skip_ignored(&mut self) {
        loop {
            while matches!(self.peek_char(), Some(ch) if ch.is_whitespace() || ch == ',') {
                self.advance_char();
            }
            if self.peek_char() == Some('#') {
                while let Some(ch) = self.advance_char() {
                    if ch == '\n' {
                        break;
                    }
                }
                continue;
            }
            break;
        }
    }

    fn expect_char(&mut self, expected: char) -> GqlResult<()> {
        self.skip_ignored();
        match self.advance_char() {
            Some(ch) if ch == expected => Ok(()),
            Some(ch) => Err(GqlError::new(
                GqlErrorKind::Syntax,
                format!(
                    "expected `{}`, found `{}` at offset {}",
                    expected, ch, self.pos
                ),
            )),
            None => Err(GqlError::new(
                GqlErrorKind::Syntax,
                format!("expected `{}`, found end of input", expected),
            )),
        }
    }

    fn syntax_error<T>(&self, message: &str) -> GqlResult<T> {
        Err(GqlError::new(
            GqlErrorKind::Syntax,
            format!("{} at offset {}", message, self.pos),
        ))
    }

    fn is_eof(&self) -> bool {
        self.pos >= self.input.len()
    }

    fn peek_char(&self) -> Option<char> {
        self.input[self.pos..].chars().next()
    }

    fn advance_char(&mut self) -> Option<char> {
        let ch = self.peek_char()?;
        self.pos += ch.len_utf8();
        Some(ch)
    }

    fn peek_name_start(&self) -> bool {
        matches!(self.peek_char(), Some(ch) if is_name_start(ch))
    }
}

fn is_name_start(ch: char) -> bool {
    ch == '_' || ch.is_ascii_alphabetic()
}

fn is_name_continue(ch: char) -> bool {
    is_name_start(ch) || ch.is_ascii_digit()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::InputValue;

    #[test]
    fn parse_query_with_aliases_and_variables() {
        let doc = parse_document(
            "query GetOrders($min: Float!, $limit: Int = 2) { orders(limit: $limit, where: { total: { gte: $min } }) { nodeId: id total } }",
        )
        .unwrap();

        assert_eq!(doc.operations.len(), 1);
        let operation = &doc.operations[0];
        assert_eq!(operation.name.as_deref(), Some("GetOrders"));
        assert_eq!(operation.variable_definitions.len(), 2);
        let field = &operation.selection_set.fields[0];
        assert_eq!(field.name, "orders");
        assert_eq!(
            field.selection_set.as_ref().unwrap().fields[0]
                .alias
                .as_deref(),
            Some("nodeId")
        );
    }

    #[test]
    fn parse_input_value_kinds() {
        let doc = parse_document("{ users(where: { active: { eq: true }, name: { like: \"A%\" }, score: { between: [1, 10] } }) { id } }").unwrap();
        let args = &doc.operations[0].selection_set.fields[0].arguments;
        let where_arg = args.iter().find(|arg| arg.name == "where").unwrap();
        match &where_arg.value {
            InputValue::Object(fields) => assert_eq!(fields.len(), 3),
            other => panic!("unexpected where value: {other:?}"),
        }
    }

    #[test]
    fn parse_field_directives_with_arguments() {
        let doc = parse_document(
            "query Feed($showUsers: Boolean!, $withPosts: Boolean!) { users @include(if: $showUsers) { id posts @skip(if: $withPosts) { id } } }",
        )
        .unwrap();

        let root_field = &doc.operations[0].selection_set.fields[0];
        assert_eq!(root_field.name, "users");
        assert_eq!(root_field.directives.len(), 1);
        assert_eq!(root_field.directives[0].name, "include");
        assert_eq!(root_field.directives[0].arguments[0].name, "if");

        let nested_field = &root_field.selection_set.as_ref().unwrap().fields[1];
        assert_eq!(nested_field.name, "posts");
        assert_eq!(nested_field.directives.len(), 1);
        assert_eq!(nested_field.directives[0].name, "skip");
    }
}
