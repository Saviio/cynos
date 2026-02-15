//! JSONPath parser for JSONB queries.
//!
//! Supports a subset of JSONPath syntax:
//! - `$` - root element
//! - `.field` or `['field']` - object field access
//! - `[0]` - array index access
//! - `[0:10]` - array slice
//! - `[*]` - wildcard (all elements)
//! - `..field` - recursive descent
//! - `[?(@.price < 10)]` - filter expressions

use alloc::boxed::Box;
use alloc::string::{String, ToString};

/// A parsed JSONPath expression.
#[derive(Clone, Debug, PartialEq)]
pub enum JsonPath {
    /// Root element ($)
    Root,
    /// Field access ($.field)
    Field(Box<JsonPath>, String),
    /// Array index access ($[0])
    Index(Box<JsonPath>, usize),
    /// Array slice ($[start:end])
    Slice(Box<JsonPath>, Option<usize>, Option<usize>),
    /// Recursive field access ($..field)
    RecursiveField(Box<JsonPath>, String),
    /// Wildcard ($[*] or $.*)
    Wildcard(Box<JsonPath>),
    /// Filter expression ($[?(...)])
    Filter(Box<JsonPath>, Box<JsonPathPredicate>),
}

/// A predicate for filter expressions.
#[derive(Clone, Debug, PartialEq)]
pub enum JsonPathPredicate {
    /// Comparison: @.field op value
    Compare(String, CompareOp, PredicateValue),
    /// Existence check: @.field
    Exists(String),
    /// Logical AND
    And(Box<JsonPathPredicate>, Box<JsonPathPredicate>),
    /// Logical OR
    Or(Box<JsonPathPredicate>, Box<JsonPathPredicate>),
    /// Logical NOT
    Not(Box<JsonPathPredicate>),
}

/// Comparison operators for predicates.
#[derive(Clone, Debug, PartialEq)]
pub enum CompareOp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

/// Values that can appear in predicates.
#[derive(Clone, Debug, PartialEq)]
pub enum PredicateValue {
    Null,
    Bool(bool),
    Number(f64),
    String(String),
}

/// Error type for JSONPath parsing.
#[derive(Clone, Debug, PartialEq)]
pub struct ParseError {
    pub message: String,
    pub position: usize,
}

impl ParseError {
    fn new(message: impl Into<String>, position: usize) -> Self {
        Self {
            message: message.into(),
            position,
        }
    }
}

/// Parser state.
struct Parser<'a> {
    input: &'a str,
    pos: usize,
}

impl<'a> Parser<'a> {
    fn new(input: &'a str) -> Self {
        Self { input, pos: 0 }
    }

    fn peek(&self) -> Option<char> {
        self.input[self.pos..].chars().next()
    }

    fn advance(&mut self) {
        if let Some(c) = self.peek() {
            self.pos += c.len_utf8();
        }
    }

    fn skip_whitespace(&mut self) {
        while let Some(c) = self.peek() {
            if c.is_whitespace() {
                self.advance();
            } else {
                break;
            }
        }
    }

    fn expect(&mut self, expected: char) -> Result<(), ParseError> {
        self.skip_whitespace();
        match self.peek() {
            Some(c) if c == expected => {
                self.advance();
                Ok(())
            }
            Some(c) => Err(ParseError::new(
                alloc::format!("Expected '{}', found '{}'", expected, c),
                self.pos,
            )),
            None => Err(ParseError::new(
                alloc::format!("Expected '{}', found end of input", expected),
                self.pos,
            )),
        }
    }

    fn parse_identifier(&mut self) -> Result<String, ParseError> {
        self.skip_whitespace();
        let start = self.pos;
        while let Some(c) = self.peek() {
            if c.is_alphanumeric() || c == '_' {
                self.advance();
            } else {
                break;
            }
        }
        if self.pos == start {
            return Err(ParseError::new("Expected identifier", self.pos));
        }
        Ok(self.input[start..self.pos].to_string())
    }

    fn parse_number(&mut self) -> Result<f64, ParseError> {
        self.skip_whitespace();
        let start = self.pos;
        if self.peek() == Some('-') {
            self.advance();
        }
        while let Some(c) = self.peek() {
            if c.is_ascii_digit() || c == '.' {
                self.advance();
            } else {
                break;
            }
        }
        if self.pos == start {
            return Err(ParseError::new("Expected number", self.pos));
        }
        self.input[start..self.pos]
            .parse()
            .map_err(|_| ParseError::new("Invalid number", start))
    }

    fn parse_string_literal(&mut self) -> Result<String, ParseError> {
        self.skip_whitespace();
        let quote = self.peek();
        if quote != Some('\'') && quote != Some('"') {
            return Err(ParseError::new("Expected string literal", self.pos));
        }
        let quote = quote.unwrap();
        self.advance();

        let start = self.pos;
        while let Some(c) = self.peek() {
            if c == quote {
                let result = self.input[start..self.pos].to_string();
                self.advance();
                return Ok(result);
            }
            if c == '\\' {
                self.advance();
            }
            self.advance();
        }
        Err(ParseError::new("Unterminated string", start))
    }
}

impl JsonPath {
    /// Parses a JSONPath expression from a string.
    pub fn parse(input: &str) -> Result<Self, ParseError> {
        let mut parser = Parser::new(input);
        parser.skip_whitespace();

        parser.expect('$')?;
        let mut path = JsonPath::Root;

        loop {
            parser.skip_whitespace();
            match parser.peek() {
                Some('.') => {
                    parser.advance();
                    if parser.peek() == Some('.') {
                        parser.advance();
                        let field = parser.parse_identifier()?;
                        path = JsonPath::RecursiveField(Box::new(path), field);
                    } else if parser.peek() == Some('*') {
                        parser.advance();
                        path = JsonPath::Wildcard(Box::new(path));
                    } else {
                        let field = parser.parse_identifier()?;
                        path = JsonPath::Field(Box::new(path), field);
                    }
                }
                Some('[') => {
                    parser.advance();
                    parser.skip_whitespace();

                    match parser.peek() {
                        Some('*') => {
                            parser.advance();
                            parser.expect(']')?;
                            path = JsonPath::Wildcard(Box::new(path));
                        }
                        Some('?') => {
                            parser.advance();
                            parser.expect('(')?;
                            let predicate = parse_predicate(&mut parser)?;
                            parser.expect(')')?;
                            parser.expect(']')?;
                            path = JsonPath::Filter(Box::new(path), Box::new(predicate));
                        }
                        Some('\'') | Some('"') => {
                            let field = parser.parse_string_literal()?;
                            parser.expect(']')?;
                            path = JsonPath::Field(Box::new(path), field);
                        }
                        Some(c) if c.is_ascii_digit() || c == ':' || c == '-' => {
                            let (start, end) = parse_index_or_slice(&mut parser)?;
                            parser.expect(']')?;
                            if start.is_some() && end.is_none() && !parser.input[..parser.pos].contains(':') {
                                path = JsonPath::Index(Box::new(path), start.unwrap());
                            } else {
                                path = JsonPath::Slice(Box::new(path), start, end);
                            }
                        }
                        _ => {
                            return Err(ParseError::new("Invalid bracket expression", parser.pos));
                        }
                    }
                }
                None => break,
                _ => break,
            }
        }

        Ok(path)
    }
}

fn parse_index_or_slice(parser: &mut Parser) -> Result<(Option<usize>, Option<usize>), ParseError> {
    parser.skip_whitespace();

    let start = if parser.peek() == Some(':') {
        None
    } else if parser.peek().map(|c| c.is_ascii_digit()).unwrap_or(false) {
        Some(parser.parse_number()? as usize)
    } else {
        None
    };

    parser.skip_whitespace();
    if parser.peek() == Some(':') {
        parser.advance();
        parser.skip_whitespace();

        let end = if parser.peek().map(|c| c.is_ascii_digit()).unwrap_or(false) {
            Some(parser.parse_number()? as usize)
        } else {
            None
        };

        Ok((start, end))
    } else {
        Ok((start, None))
    }
}

fn parse_predicate(parser: &mut Parser) -> Result<JsonPathPredicate, ParseError> {
    parser.skip_whitespace();
    parser.expect('@')?;
    parser.expect('.')?;

    let field = parser.parse_identifier()?;
    parser.skip_whitespace();

    match parser.peek() {
        Some('=') => {
            parser.advance();
            let op = if parser.peek() == Some('=') {
                parser.advance();
                CompareOp::Eq
            } else {
                CompareOp::Eq
            };
            let value = parse_predicate_value(parser)?;
            Ok(JsonPathPredicate::Compare(field, op, value))
        }
        Some('!') => {
            parser.advance();
            parser.expect('=')?;
            let value = parse_predicate_value(parser)?;
            Ok(JsonPathPredicate::Compare(field, CompareOp::Ne, value))
        }
        Some('<') => {
            parser.advance();
            let op = if parser.peek() == Some('=') {
                parser.advance();
                CompareOp::Le
            } else {
                CompareOp::Lt
            };
            let value = parse_predicate_value(parser)?;
            Ok(JsonPathPredicate::Compare(field, op, value))
        }
        Some('>') => {
            parser.advance();
            let op = if parser.peek() == Some('=') {
                parser.advance();
                CompareOp::Ge
            } else {
                CompareOp::Gt
            };
            let value = parse_predicate_value(parser)?;
            Ok(JsonPathPredicate::Compare(field, op, value))
        }
        _ => Ok(JsonPathPredicate::Exists(field)),
    }
}

fn parse_predicate_value(parser: &mut Parser) -> Result<PredicateValue, ParseError> {
    parser.skip_whitespace();

    match parser.peek() {
        Some('\'') | Some('"') => {
            let s = parser.parse_string_literal()?;
            Ok(PredicateValue::String(s))
        }
        Some('t') => {
            let id = parser.parse_identifier()?;
            if id == "true" {
                Ok(PredicateValue::Bool(true))
            } else {
                Err(ParseError::new("Invalid value", parser.pos))
            }
        }
        Some('f') => {
            let id = parser.parse_identifier()?;
            if id == "false" {
                Ok(PredicateValue::Bool(false))
            } else {
                Err(ParseError::new("Invalid value", parser.pos))
            }
        }
        Some('n') => {
            let id = parser.parse_identifier()?;
            if id == "null" {
                Ok(PredicateValue::Null)
            } else {
                Err(ParseError::new("Invalid value", parser.pos))
            }
        }
        Some(c) if c.is_ascii_digit() || c == '-' => {
            let n = parser.parse_number()?;
            Ok(PredicateValue::Number(n))
        }
        _ => Err(ParseError::new("Expected value", parser.pos)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_root() {
        let path = JsonPath::parse("$").unwrap();
        assert_eq!(path, JsonPath::Root);
    }

    #[test]
    fn test_parse_field() {
        let path = JsonPath::parse("$.name").unwrap();
        assert_eq!(
            path,
            JsonPath::Field(Box::new(JsonPath::Root), "name".into())
        );
    }

    #[test]
    fn test_parse_nested_fields() {
        let path = JsonPath::parse("$.user.name").unwrap();
        assert_eq!(
            path,
            JsonPath::Field(
                Box::new(JsonPath::Field(Box::new(JsonPath::Root), "user".into())),
                "name".into()
            )
        );
    }

    #[test]
    fn test_parse_index() {
        let path = JsonPath::parse("$[0]").unwrap();
        assert_eq!(path, JsonPath::Index(Box::new(JsonPath::Root), 0));
    }

    #[test]
    fn test_parse_field_and_index() {
        let path = JsonPath::parse("$.items[0]").unwrap();
        assert_eq!(
            path,
            JsonPath::Index(
                Box::new(JsonPath::Field(Box::new(JsonPath::Root), "items".into())),
                0
            )
        );
    }

    #[test]
    fn test_parse_wildcard() {
        let path = JsonPath::parse("$[*]").unwrap();
        assert_eq!(path, JsonPath::Wildcard(Box::new(JsonPath::Root)));

        let path = JsonPath::parse("$.*").unwrap();
        assert_eq!(path, JsonPath::Wildcard(Box::new(JsonPath::Root)));
    }

    #[test]
    fn test_parse_recursive() {
        let path = JsonPath::parse("$..name").unwrap();
        assert_eq!(
            path,
            JsonPath::RecursiveField(Box::new(JsonPath::Root), "name".into())
        );
    }

    #[test]
    fn test_parse_slice() {
        let path = JsonPath::parse("$[0:10]").unwrap();
        assert_eq!(
            path,
            JsonPath::Slice(Box::new(JsonPath::Root), Some(0), Some(10))
        );

        let path = JsonPath::parse("$[:5]").unwrap();
        assert_eq!(
            path,
            JsonPath::Slice(Box::new(JsonPath::Root), None, Some(5))
        );

        let path = JsonPath::parse("$[5:]").unwrap();
        assert_eq!(
            path,
            JsonPath::Slice(Box::new(JsonPath::Root), Some(5), None)
        );
    }

    #[test]
    fn test_parse_bracket_field() {
        let path = JsonPath::parse("$['field-name']").unwrap();
        assert_eq!(
            path,
            JsonPath::Field(Box::new(JsonPath::Root), "field-name".into())
        );
    }

    #[test]
    fn test_parse_filter() {
        let path = JsonPath::parse("$[?(@.price < 10)]").unwrap();
        assert_eq!(
            path,
            JsonPath::Filter(
                Box::new(JsonPath::Root),
                Box::new(JsonPathPredicate::Compare(
                    "price".into(),
                    CompareOp::Lt,
                    PredicateValue::Number(10.0)
                ))
            )
        );
    }

    #[test]
    fn test_parse_filter_string() {
        let path = JsonPath::parse("$[?(@.name == 'Alice')]").unwrap();
        assert_eq!(
            path,
            JsonPath::Filter(
                Box::new(JsonPath::Root),
                Box::new(JsonPathPredicate::Compare(
                    "name".into(),
                    CompareOp::Eq,
                    PredicateValue::String("Alice".into())
                ))
            )
        );
    }

    #[test]
    fn test_parse_complex() {
        let path = JsonPath::parse("$.store.books[0].title").unwrap();
        assert_eq!(
            path,
            JsonPath::Field(
                Box::new(JsonPath::Index(
                    Box::new(JsonPath::Field(
                        Box::new(JsonPath::Field(Box::new(JsonPath::Root), "store".into())),
                        "books".into()
                    )),
                    0
                )),
                "title".into()
            )
        );
    }

    // Error handling tests
    #[test]
    fn test_parse_error_missing_root() {
        let result = JsonPath::parse("name");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_error_invalid_bracket() {
        let result = JsonPath::parse("$[abc]");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_error_unclosed_bracket() {
        let result = JsonPath::parse("$[0");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_error_invalid_filter() {
        let result = JsonPath::parse("$[?(@.price xyz)]");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_error_empty_string() {
        let result = JsonPath::parse("");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_whitespace_handling() {
        let path = JsonPath::parse("  $  .  name  ").unwrap();
        assert_eq!(
            path,
            JsonPath::Field(Box::new(JsonPath::Root), "name".into())
        );

        let path = JsonPath::parse("$ [ 0 ]").unwrap();
        assert_eq!(path, JsonPath::Index(Box::new(JsonPath::Root), 0));
    }

    #[test]
    fn test_parse_filter_all_operators() {
        // Equality
        let path = JsonPath::parse("$[?(@.x == 1)]").unwrap();
        assert!(matches!(path, JsonPath::Filter(_, _)));

        // Not equal
        let path = JsonPath::parse("$[?(@.x != 1)]").unwrap();
        assert!(matches!(path, JsonPath::Filter(_, _)));

        // Less than
        let path = JsonPath::parse("$[?(@.x < 1)]").unwrap();
        assert!(matches!(path, JsonPath::Filter(_, _)));

        // Less than or equal
        let path = JsonPath::parse("$[?(@.x <= 1)]").unwrap();
        assert!(matches!(path, JsonPath::Filter(_, _)));

        // Greater than
        let path = JsonPath::parse("$[?(@.x > 1)]").unwrap();
        assert!(matches!(path, JsonPath::Filter(_, _)));

        // Greater than or equal
        let path = JsonPath::parse("$[?(@.x >= 1)]").unwrap();
        assert!(matches!(path, JsonPath::Filter(_, _)));

        // Exists
        let path = JsonPath::parse("$[?(@.x)]").unwrap();
        assert!(matches!(path, JsonPath::Filter(_, _)));
    }

    #[test]
    fn test_parse_filter_value_types() {
        // Boolean true
        let path = JsonPath::parse("$[?(@.active == true)]").unwrap();
        assert!(matches!(path, JsonPath::Filter(_, _)));

        // Boolean false
        let path = JsonPath::parse("$[?(@.active == false)]").unwrap();
        assert!(matches!(path, JsonPath::Filter(_, _)));

        // Null
        let path = JsonPath::parse("$[?(@.value == null)]").unwrap();
        assert!(matches!(path, JsonPath::Filter(_, _)));

        // Negative number
        let path = JsonPath::parse("$[?(@.temp < -10)]").unwrap();
        assert!(matches!(path, JsonPath::Filter(_, _)));

        // Double quoted string
        let path = JsonPath::parse("$[?(@.name == \"Alice\")]").unwrap();
        assert!(matches!(path, JsonPath::Filter(_, _)));
    }

    #[test]
    fn test_parse_double_quoted_bracket_field() {
        let path = JsonPath::parse("$[\"field-name\"]").unwrap();
        assert_eq!(
            path,
            JsonPath::Field(Box::new(JsonPath::Root), "field-name".into())
        );
    }

    #[test]
    fn test_parse_large_index() {
        let path = JsonPath::parse("$[999999]").unwrap();
        assert_eq!(path, JsonPath::Index(Box::new(JsonPath::Root), 999999));
    }
}
