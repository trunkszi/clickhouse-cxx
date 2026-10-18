//! SQL token scanning for the terminal output format clause.

use crate::{Error, Result};

/// Preserve compatible terminal FORMAT clauses and append absent clauses safely.
pub(super) fn ensure_format(sql: &str, format: &str) -> Result<String> {
    let tokens = tokens(sql)?;
    let mut end = tokens.len();
    while end > 0 && tokens[end - 1].text == ";" {
        end -= 1;
    }
    if let Some(index) = tokens[..end]
        .iter()
        .rposition(|token| token.text.eq_ignore_ascii_case("FORMAT"))
        && index + 2 == end
        && tokens[index + 1].identifier
    {
        if !tokens[index + 1].text.eq_ignore_ascii_case(format) {
            return Err(Error::Invalid(
                format!(
                    "output FORMAT {} is incompatible with {format}",
                    tokens[index + 1].text
                )
                .into(),
            ));
        }
        return Ok(sql[..tokens[end - 1].end].to_owned());
    }
    // Drop only trailing comments, whitespace, and statement delimiters. Interior
    // comments stay intact; the newline also terminates any final line comment.
    let meaningful_end = tokens[..end].last().map_or(0, |token| token.end);
    Ok(format!("{}\nFORMAT {format}", &sql[..meaningful_end]))
}

struct Token<'a> {
    text: &'a str,
    end: usize,
    identifier: bool,
}

/// Ignore quoted text and comments while retaining their statement boundaries.
fn tokens(sql: &str) -> Result<Vec<Token<'_>>> {
    let bytes = sql.as_bytes();
    let mut out = Vec::new();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i].is_ascii_whitespace() {
            i += 1;
            continue;
        }
        if bytes[i..].starts_with(b"--") {
            while i < bytes.len() && bytes[i] != b'\n' {
                i += 1;
            }
            continue;
        }
        if bytes[i..].starts_with(b"/*") {
            i += 2;
            let mut depth = 1;
            while i < bytes.len() && depth > 0 {
                if bytes[i..].starts_with(b"/*") {
                    depth += 1;
                    i += 2;
                } else if bytes[i..].starts_with(b"*/") {
                    depth -= 1;
                    i += 2;
                } else {
                    i += 1;
                }
            }
            if depth != 0 {
                return Err(Error::Invalid("unterminated SQL block comment".into()));
            }
            continue;
        }
        let start = i;
        let quote = bytes[i];
        if matches!(quote, b'\'' | b'"' | b'`') {
            i += 1;
            let mut closed = false;
            while i < bytes.len() {
                if bytes[i] == b'\\' {
                    i = (i + 2).min(bytes.len());
                } else if bytes[i] == quote {
                    i += 1;
                    if i < bytes.len() && bytes[i] == quote {
                        i += 1;
                    } else {
                        closed = true;
                        break;
                    }
                } else {
                    i += 1;
                }
            }
            if !closed {
                return Err(Error::Invalid("unterminated SQL quoted token".into()));
            }
            out.push(Token {
                text: &sql[start..i],
                end: i,
                identifier: false,
            });
            continue;
        }
        let identifier = bytes[i].is_ascii_alphabetic() || bytes[i] == b'_';
        if identifier {
            i += 1;
            while i < bytes.len() && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_') {
                i += 1;
            }
        } else {
            // Consume complete UTF-8 tokens so all slices end on character boundaries.
            i += sql[i..].chars().next().map_or(1, char::len_utf8);
        }
        out.push(Token {
            text: &sql[start..i],
            end: i,
            identifier,
        });
    }
    Ok(out)
}

#[cfg(test)]
mod unit {
    use super::ensure_format;

    /// Literal/comment FORMAT text cannot suppress the required terminal clause.
    #[test]
    fn literals_comments_and_delimiters() {
        for sql in [
            "SELECT ' format ' AS x",
            "SELECT 1;",
            "SELECT 1; -- FORMAT JSON",
            "SELECT \"format\" /* FORMAT JSON */",
            "SELECT 'it\\'s format JSON'",
            "SELECT 1 /* outer /* nested */ done */",
        ] {
            let result = ensure_format(sql, "Native").unwrap();
            assert!(result.ends_with("\nFORMAT Native"));
            assert!(!result.contains(";\nFORMAT"));
        }
    }

    /// Existing incompatible formats fail before issuing any request.
    #[test]
    fn terminal_format_is_validated() {
        assert_eq!(
            ensure_format("SELECT 1 FORMAT Native; -- done", "native").unwrap(),
            "SELECT 1 FORMAT Native"
        );
        assert!(ensure_format("SELECT 1 FORMAT JSON", "Native").is_err());
        assert!(ensure_format("SELECT 'unterminated", "Native").is_err());
        assert!(ensure_format("SELECT 1 /* unclosed", "Native").is_err());
    }
}
