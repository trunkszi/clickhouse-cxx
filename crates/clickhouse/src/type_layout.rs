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
    if name.starts_with("Nullable(") || name.starts_with("Array(") || name == "String" {
        return None;
    }
    if name.starts_with("LowCardinality(") || name.starts_with("Tuple(") || name.starts_with("Map(")
    {
        return None;
    }
    if name.starts_with("DateTime64(") {
        return Some(8);
    }
    if name.starts_with("FixedString(") {
        return first_int(name);
    }
    if name.starts_with("Decimal32(") {
        return Some(4);
    }
    if name.starts_with("Decimal64(") {
        return Some(8);
    }
    if name.starts_with("Decimal128(") {
        return Some(16);
    }
    if name.starts_with("Decimal256(") {
        return Some(32);
    }
    if name.starts_with("Decimal(") {
        return first_int(name).and_then(decimal_width);
    }
    if name.starts_with("Enum8(") {
        return Some(1);
    }
    if name.starts_with("Enum16(") {
        return Some(2);
    }
    if name.starts_with("DateTime(") {
        return Some(4);
    }
    Some(match name {
        "UInt8" | "Int8" | "Bool" => 1,
        "UInt16" | "Int16" | "Date" => 2,
        "UInt32" | "Int32" | "Float32" | "Date32" | "IPv4" => 4,
        "DateTime" => 4,
        "UInt64" | "Int64" | "Float64" => 8,
        "UInt128" | "Int128" | "UUID" | "IPv6" => 16,
        "UInt256" | "Int256" => 32,
        _ => return None,
    })
}

fn strip_wrap<'a>(name: &'a str, prefix: &str) -> Option<&'a str> {
    let rest = name.strip_prefix(prefix)?.strip_prefix('(')?;
    rest.strip_suffix(')')
}

fn first_int(text: &str) -> Option<usize> {
    let start = text.find(|ch: char| ch.is_ascii_digit())?;
    let digits = text[start..]
        .chars()
        .take_while(|ch| ch.is_ascii_digit())
        .collect::<String>();
    digits.parse().ok()
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
