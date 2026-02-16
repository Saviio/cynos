//! Pattern matching utilities for LIKE and regex expressions.
//!
//! Provides a single, canonical implementation used by both the
//! PhysicalPlanRunner (re-query path) and the DataflowNode evaluator
//! (IVM path), ensuring identical semantics across both query strategies.
//!
//! # LIKE patterns
//!
//! SQL LIKE with two wildcards:
//! - `%` matches zero or more characters
//! - `_` matches exactly one character
//!
//! Matching is **case-sensitive** and operates on Unicode scalar values.
//!
//! # Regex patterns
//!
//! A compact, `no_std`-compatible regex engine supporting:
//! - `.`  — any character
//! - `*`  — zero or more (greedy)
//! - `+`  — one or more (greedy)
//! - `?`  — zero or one
//! - `^` / `$` — anchors
//! - `\d` `\D` `\w` `\W` `\s` `\S` — character classes
//! - `[abc]` `[a-z]` `[^abc]` — bracket classes
//! - `\.` `\\` etc. — literal escapes

use alloc::boxed::Box;
use alloc::vec::Vec;

// =========================================================================
// LIKE
// =========================================================================

/// SQL LIKE pattern matching.
///
/// `%` matches any sequence of zero or more characters.
/// `_` matches exactly one character.
///
/// ```
/// use cynos_core::pattern_match::like;
/// assert!(like("hello", "h%o"));
/// assert!(like("hello", "_ello"));
/// assert!(!like("hello", "world"));
/// ```
pub fn like(value: &str, pattern: &str) -> bool {
    let v: Vec<char> = value.chars().collect();
    let p: Vec<char> = pattern.chars().collect();
    like_recursive(&v, &p, 0, 0)
}

fn like_recursive(v: &[char], p: &[char], vi: usize, pi: usize) -> bool {
    if pi == p.len() {
        return vi == v.len();
    }
    match p[pi] {
        '%' => {
            // % matches zero or more characters
            for skip in vi..=v.len() {
                if like_recursive(v, p, skip, pi + 1) {
                    return true;
                }
            }
            false
        }
        '_' => {
            // _ matches exactly one character
            vi < v.len() && like_recursive(v, p, vi + 1, pi + 1)
        }
        ch => vi < v.len() && v[vi] == ch && like_recursive(v, p, vi + 1, pi + 1),
    }
}

// =========================================================================
// Regex
// =========================================================================

/// Regex pattern matching (subset suitable for SQL MATCH / `~` operator).
///
/// Supports: `.` `*` `+` `?` `^` `$` `\d` `\D` `\w` `\W` `\s` `\S`
/// `[abc]` `[a-z]` `[^abc]` and literal escapes.
///
/// By default the match is **unanchored** — it succeeds if the pattern
/// matches any substring.  Use `^` and/or `$` to anchor.
///
/// ```
/// use cynos_core::pattern_match::regex;
/// assert!(regex("abc123", "\\d+"));
/// assert!(!regex("abc123", "^\\d+$"));
/// assert!(regex("abc123", "^[a-z]+\\d+$"));
/// ```
pub fn regex(value: &str, pattern: &str) -> bool {
    let (pat, anchored_start, anchored_end) = parse_anchors(pattern);
    let chars: Vec<char> = value.chars().collect();
    let pat_chars: Vec<char> = pat.chars().collect();

    if anchored_start && anchored_end {
        regex_match_at(&chars, &pat_chars, 0, 0) == Some(chars.len())
    } else if anchored_start {
        regex_match_at(&chars, &pat_chars, 0, 0).is_some()
    } else {
        for start in 0..=chars.len() {
            if let Some(end) = regex_match_at(&chars, &pat_chars, start, 0) {
                if !anchored_end || end == chars.len() {
                    return true;
                }
            }
        }
        false
    }
}

fn parse_anchors(pattern: &str) -> (&str, bool, bool) {
    let start = pattern.starts_with('^');
    let end = pattern.ends_with('$') && !pattern.ends_with("\\$");
    let p = if start { &pattern[1..] } else { pattern };
    let p = if end && !p.is_empty() {
        &p[..p.len() - 1]
    } else {
        p
    };
    (p, start, end)
}

/// Returns `Some(end_position)` if pattern matches starting at
/// `chars[ci]`, `pattern[pi]`.
fn regex_match_at(chars: &[char], pat: &[char], ci: usize, pi: usize) -> Option<usize> {
    if pi >= pat.len() {
        return Some(ci);
    }

    let (token_len, matcher) = parse_token(pat, pi)?;
    let next_pi = pi + token_len;
    let has_quantifier = next_pi < pat.len();

    if has_quantifier && pat[next_pi] == '*' {
        return regex_match_star(chars, pat, ci, next_pi + 1, &matcher);
    }
    if has_quantifier && pat[next_pi] == '+' {
        if ci < chars.len() && matcher(chars[ci]) {
            return regex_match_star(chars, pat, ci + 1, next_pi + 1, &matcher);
        }
        return None;
    }
    if has_quantifier && pat[next_pi] == '?' {
        if ci < chars.len() && matcher(chars[ci]) {
            if let Some(r) = regex_match_at(chars, pat, ci + 1, next_pi + 1) {
                return Some(r);
            }
        }
        return regex_match_at(chars, pat, ci, next_pi + 1);
    }

    // No quantifier — match exactly one
    if ci < chars.len() && matcher(chars[ci]) {
        regex_match_at(chars, pat, ci + 1, next_pi)
    } else {
        None
    }
}

fn regex_match_star(
    chars: &[char],
    pat: &[char],
    ci: usize,
    next_pi: usize,
    matcher: &dyn Fn(char) -> bool,
) -> Option<usize> {
    // Greedy: consume as many as possible, then backtrack
    let mut end = ci;
    while end < chars.len() && matcher(chars[end]) {
        end += 1;
    }
    for try_ci in (ci..=end).rev() {
        if let Some(r) = regex_match_at(chars, pat, try_ci, next_pi) {
            return Some(r);
        }
    }
    None
}

/// Parse one regex token at `pat[pi]`.
///
/// Returns `(token_length, char_matcher)`.
fn parse_token(pat: &[char], pi: usize) -> Option<(usize, Box<dyn Fn(char) -> bool>)> {
    if pi >= pat.len() {
        return None;
    }
    match pat[pi] {
        '.' => Some((1, Box::new(|_| true))),
        '\\' if pi + 1 < pat.len() => {
            let next = pat[pi + 1];
            match next {
                'd' => Some((2, Box::new(|c: char| c.is_ascii_digit()))),
                'w' => Some((2, Box::new(|c: char| c.is_alphanumeric() || c == '_'))),
                's' => Some((2, Box::new(|c: char| c.is_whitespace()))),
                'D' => Some((2, Box::new(|c: char| !c.is_ascii_digit()))),
                'W' => Some((2, Box::new(|c: char| !c.is_alphanumeric() && c != '_'))),
                'S' => Some((2, Box::new(|c: char| !c.is_whitespace()))),
                _ => Some((2, Box::new(move |c: char| c == next))),
            }
        }
        '[' => parse_bracket_class(pat, pi),
        ch => Some((1, Box::new(move |c: char| c == ch))),
    }
}

/// Parse a bracket character class: `[abc]`, `[a-z]`, `[^abc]`.
fn parse_bracket_class(
    pat: &[char],
    pi: usize,
) -> Option<(usize, Box<dyn Fn(char) -> bool>)> {
    let negate = pi + 1 < pat.len() && pat[pi + 1] == '^';
    let start = if negate { pi + 2 } else { pi + 1 };
    let mut end = start;
    while end < pat.len() && pat[end] != ']' {
        end += 1;
    }
    if end >= pat.len() {
        return None; // unclosed bracket
    }
    let class_chars: Vec<char> = pat[start..end].to_vec();
    let token_len = end - pi + 1; // includes ']'
    Some((
        token_len,
        Box::new(move |c: char| {
            let mut matched = false;
            let mut i = 0;
            while i < class_chars.len() {
                if i + 2 < class_chars.len() && class_chars[i + 1] == '-' {
                    if c >= class_chars[i] && c <= class_chars[i + 2] {
                        matched = true;
                        break;
                    }
                    i += 3;
                } else {
                    if c == class_chars[i] {
                        matched = true;
                        break;
                    }
                    i += 1;
                }
            }
            if negate { !matched } else { matched }
        }),
    ))
}

// =========================================================================
// Tests
// =========================================================================

#[cfg(test)]
mod tests {
    use super::*;

    // ----- LIKE -----------------------------------------------------------

    #[test]
    fn like_exact() {
        assert!(like("hello", "hello"));
        assert!(!like("hello", "world"));
    }

    #[test]
    fn like_percent() {
        assert!(like("hello", "%"));
        assert!(like("hello", "h%"));
        assert!(like("hello", "%o"));
        assert!(like("hello", "h%o"));
        assert!(like("hello", "%ell%"));
        assert!(!like("hello", "x%"));
    }

    #[test]
    fn like_underscore() {
        assert!(like("hello", "_ello"));
        assert!(like("hello", "h_llo"));
        assert!(like("hello", "hell_"));
        assert!(like("hello", "_____"));
        assert!(!like("hello", "______"));
    }

    #[test]
    fn like_combined() {
        assert!(like("hello", "h%_o"));
        assert!(like("hello world", "hello%"));
        assert!(like("hello world", "%world"));
    }

    #[test]
    fn like_empty() {
        assert!(like("", ""));
        assert!(like("", "%"));
        assert!(!like("", "_"));
        assert!(!like("", "a"));
    }

    // ----- Regex ----------------------------------------------------------

    #[test]
    fn regex_digit_class() {
        assert!(regex("abc123", "\\d+"));
        assert!(!regex("abcdef", "\\d+"));
    }

    #[test]
    fn regex_anchored() {
        assert!(!regex("abc123", "^\\d+$"));
        assert!(regex("abc123", "^[a-z]+\\d+$"));
        assert!(regex("123", "^\\d+$"));
    }

    #[test]
    fn regex_unanchored_substring() {
        assert!(regex("hello world", "wor"));
        assert!(regex("hello world", "^hello"));
        assert!(regex("hello world", "world$"));
    }

    #[test]
    fn regex_dot_star() {
        assert!(regex("anything", ".*"));
        assert!(regex("abc", "a.c"));
        assert!(!regex("ac", "a.c"));
    }

    #[test]
    fn regex_question_mark() {
        assert!(regex("ac", "ab?c"));
        assert!(regex("abc", "ab?c"));
        assert!(!regex("abbc", "^ab?c$"));
    }

    #[test]
    fn regex_bracket_class() {
        assert!(regex("cat", "^[cb]at$"));
        assert!(regex("bat", "^[cb]at$"));
        assert!(!regex("hat", "^[cb]at$"));
    }

    #[test]
    fn regex_negated_bracket() {
        assert!(regex("hat", "^[^cb]at$"));
        assert!(!regex("cat", "^[^cb]at$"));
    }

    #[test]
    fn regex_range() {
        assert!(regex("m", "^[a-z]$"));
        assert!(!regex("M", "^[a-z]$"));
        assert!(regex("5", "^[0-9]$"));
    }

    #[test]
    fn regex_word_class() {
        assert!(regex("hello_world", "^\\w+$"));
        assert!(!regex("hello world", "^\\w+$"));
    }

    #[test]
    fn regex_escaped_literal() {
        assert!(regex("a.b", "a\\.b"));
        assert!(!regex("axb", "^a\\.b$"));
    }

    #[test]
    fn regex_plus_quantifier() {
        assert!(regex("aaa", "^a+$"));
        assert!(!regex("", "^a+$"));
        assert!(regex("a", "^a+$"));
    }

    #[test]
    fn regex_empty() {
        assert!(regex("", ""));
        assert!(regex("", "^$"));
        assert!(!regex("", "^a$"));
    }
}
