//! ClickHouse type-name classification for Native column payloads.

use crate::error::Error;

/// How a column payload is framed for a known row count.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Layout<'a> {
    /// `width` bytes per row, little-endian when the type is an integer.
    Fixed(usize),
    /// Per-value unsigned LEB128 length plus bytes.
    String,
    /// One null byte per row (`1` means NULL) followed by nested values.
    Nullable(&'a str),
    /// Cumulative `u64` end offsets followed by the nested payload.
    Array(&'a str),
    /// Shared offsets followed by complete key and value columns.
    Map(&'a str, &'a str),
}

/// Classifies `type_name` so a reader can consume the matching payload.
///
/// # Errors
///
/// Returns [`Error::Unsupported`] for nested types this codec does not frame,
/// including `LowCardinality` and `Tuple`.
pub(crate) fn layout(type_name: &str) -> Result<Layout<'_>, Error> {
    let name = type_name.trim();
    if name.bytes().filter(|c| *c == b'(').count() > 64 {
        return Err(Error::unsupported("column nesting exceeds limit"));
    }
    if let Some(inner) = strip_wrap(name, "Nullable") {
        return Ok(Layout::Nullable(inner.trim()));
    }
    if let Some(inner) = strip_wrap(name, "Array") {
        return Ok(Layout::Array(inner.trim()));
    }
    if let Some(inner) = strip_wrap(name, "Map") {
        let mut depth = 0usize;
        for (i, c) in inner.char_indices() {
            match c {
                '(' => depth += 1,
                ')' => {
                    depth = depth
                        .checked_sub(1)
                        .ok_or_else(|| Error::protocol("unbalanced Map type"))?
                }
                ',' if depth == 0 => {
                    return Ok(Layout::Map(inner[..i].trim(), inner[i + 1..].trim()));
                }
                _ => {}
            }
        }
        return Err(Error::protocol("Map requires key and value types"));
    }
    if name == "String" {
        return Ok(Layout::String);
    }
    if let Some(width) = fixed_width(name) {
        return Ok(Layout::Fixed(width));
    }
    Err(Error::unsupported(format!("column type {name}")))
}

/// Byte width of one value, when the column is a single fixed-width type.
#[must_use]
pub(crate) fn fixed_width(type_name: &str) -> Option<usize> {
    let name = type_name.trim();
    for (family, width) in [
        ("DateTime64", 8),
        ("Time64", 8),
        ("Decimal32", 4),
        ("Decimal64", 8),
        ("Decimal128", 16),
        ("Decimal256", 32),
        ("Enum8", 1),
        ("Enum16", 2),
        ("DateTime", 4),
    ] {
        if strip_wrap(name, family).is_some() {
            return Some(width);
        }
    }
    if let Some(inner) = strip_wrap(name, "FixedString") {
        return inner
            .trim()
            .parse::<usize>()
            .ok()
            .filter(|width| *width > 0);
    }
    if let Some(inner) = strip_wrap(name, "Decimal") {
        return inner
            .split_once(',')?
            .0
            .trim()
            .parse()
            .ok()
            .and_then(decimal_width);
    }
    Some(match name {
        "UInt8" | "Int8" | "Bool" => 1,
        "UInt16" | "Int16" | "Date" => 2,
        "UInt32" | "Int32" | "Float32" | "Date32" | "IPv4" => 4,
        "DateTime" | "Time" => 4,
        "UInt64" | "Int64" | "Float64" => 8,
        "UInt128" | "Int128" | "UUID" | "IPv6" => 16,
        "UInt256" | "Int256" => 32,
        _ => return None,
    })
}

pub(crate) fn strip_wrap<'a>(name: &'a str, prefix: &str) -> Option<&'a str> {
    let rest = name
        .trim()
        .strip_prefix(prefix)?
        .trim_start()
        .strip_prefix('(')?;
    rest.strip_suffix(')').map(str::trim)
}

/// Compare type tokens while retaining whitespace and escapes in quoted literals.
pub(crate) fn same_type(left: &str, right: &str) -> bool {
    fn next_token<'a>(text: &mut &'a str) -> Option<&'a str> {
        *text = text.trim_start();
        let input = *text;
        let first = input.chars().next()?;
        let end = if matches!(first, '\'' | '"' | '`') {
            let mut escaped = false;
            let mut end = input.len();
            for (index, ch) in input.char_indices().skip(1) {
                if escaped {
                    escaped = false;
                } else if ch == '\\' {
                    escaped = true;
                } else if ch == first {
                    end = index + ch.len_utf8();
                    break;
                }
            }
            end
        } else if matches!(first, '(' | ')' | ',' | '=') {
            first.len_utf8()
        } else {
            input
                .char_indices()
                .skip(1)
                .find(|(_, ch)| {
                    ch.is_whitespace() || matches!(ch, '(' | ')' | ',' | '=' | '\'' | '"' | '`')
                })
                .map_or(input.len(), |(index, _)| index)
        };
        *text = &input[end..];
        Some(&input[..end])
    }
    let (mut left, mut right) = (left, right);
    loop {
        match (next_token(&mut left), next_token(&mut right)) {
            (None, None) => return true,
            (Some(left), Some(right)) if left == right => {}
            _ => return false,
        }
    }
}

/// Native fixed-width column family, shared by row conversion and projections.
#[cfg(feature = "tcp")]
pub(crate) fn fixed_kind(type_name: &str) -> Option<crate::FixedKind> {
    use crate::FixedKind as K;
    let name = type_name.trim();
    Some(match name {
        "UInt8" => K::UInt8,
        "UInt16" => K::UInt16,
        "UInt32" => K::UInt32,
        "UInt64" => K::UInt64,
        "UInt128" => K::UInt128,
        "Int8" => K::Int8,
        "Int16" => K::Int16,
        "Int32" => K::Int32,
        "Int64" => K::Int64,
        "Int128" => K::Int128,
        "Float32" => K::Float32,
        "Float64" => K::Float64,
        "Bool" => K::Bool,
        "Date" => K::Date,
        "Date32" => K::Date32,
        "DateTime" => K::DateTime,
        "Time" => K::Time,
        "UUID" => K::Uuid,
        "IPv4" => K::Ipv4,
        "IPv6" => K::Ipv6,
        _ => {
            for (family, kind) in [
                ("DateTime64", K::DateTime64),
                ("DateTime", K::DateTime),
                ("Time64", K::Time64),
                ("Enum8", K::Enum8),
                ("Enum16", K::Enum16),
                ("Decimal32", K::Decimal32),
                ("Decimal64", K::Decimal64),
                ("Decimal128", K::Decimal128),
            ] {
                if strip_wrap(name, family).is_some() {
                    return Some(kind);
                }
            }
            if strip_wrap(name, "Decimal").is_some() {
                match fixed_width(name)? {
                    4 => K::Decimal32,
                    8 => K::Decimal64,
                    16 => K::Decimal128,
                    _ => return None,
                }
            } else {
                return None;
            }
        }
    })
}

fn decimal_width(precision: usize) -> Option<usize> {
    if precision == 0 || precision > 76 {
        None
    } else if precision <= 9 {
        Some(4)
    } else if precision <= 18 {
        Some(8)
    } else if precision <= 38 {
        Some(16)
    } else {
        Some(32)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parameter_whitespace_preserves_layout() {
        assert_eq!(
            layout(" Map ( String , Decimal ( 18 , 4 ) ) ").unwrap(),
            Layout::Map("String", "Decimal ( 18 , 4 )")
        );
        for (name, width) in [
            ("FixedString ( 12 )", 12),
            ("Decimal ( 18 , 4 )", 8),
            ("DateTime64 ( 6 , 'UTC' )", 8),
            ("Time", 4),
            ("Time64 ( 9 )", 8),
        ] {
            assert_eq!(fixed_width(name), Some(width), "{name}");
        }
        assert_eq!(fixed_width("FixedString ( 0 )"), None);
        assert_eq!(fixed_width("Tuple ( UInt8 , UInt8 )"), None);
    }

    #[test]
    fn type_equality_preserves_quoted_literals_and_word_boundaries() {
        assert!(same_type(
            "Map ( String , Decimal(18,4) )",
            "Map(String,Decimal ( 18 , 4 ))"
        ));
        assert!(same_type(
            "Tuple ( a UInt8 , b String )",
            "Tuple(a UInt8,b String)"
        ));
        assert!(!same_type("Tuple(a UInt8)", "Tuple(aUInt8)"));
        assert!(!same_type("DateTime('A B')", "DateTime('AB')"));
        assert!(!same_type("UInt 8", "UInt8"));
    }

    #[cfg(feature = "tcp")]
    #[test]
    fn fixed_kind_includes_native_scalar_families() {
        assert_eq!(
            fixed_kind("DateTime64 ( 6 )"),
            Some(crate::FixedKind::DateTime64)
        );
        assert_eq!(
            fixed_kind("Decimal ( 18 , 4 )"),
            Some(crate::FixedKind::Decimal64)
        );
        assert_eq!(fixed_kind("Decimal(76,4)"), None);
        assert_eq!(fixed_kind("Array(UInt8)"), None);
    }
}
