//! Amortized column builders for row-oriented HTTP responses.
use crate::{
    Column, Error, Result,
    column::{AlignedBytes, ColumnData},
    type_layout::{Layout, layout},
    varint::read_varu64_slice,
};
const MAX_ROWS: usize = 16 * 1024 * 1024;
const MAX_BYTES: usize = 256 * 1024 * 1024;
/// Private row-to-column transposition buffers keep each allocation amortized.
pub(crate) struct ColumnBuffer {
    name: String,
    ty: String,
    rows: usize,
    data: Storage,
}
enum Storage {
    Map {
        offsets: Vec<u64>,
        keys: Box<ColumnBuffer>,
        values: Box<ColumnBuffer>,
    },
    Fixed {
        width: usize,
        bytes: Vec<u8>,
    },
    Strings {
        offsets: Vec<u64>,
        bytes: Vec<u8>,
    },
    Nullable {
        null_map: Vec<u8>,
        values: Box<ColumnBuffer>,
    },
    Array {
        offsets: Vec<u64>,
        values: Box<ColumnBuffer>,
    },
}
fn take<'a>(input: &mut &'a [u8], n: usize) -> Result<&'a [u8]> {
    let b = input
        .get(..n)
        .ok_or_else(|| Error::protocol("truncated RowBinary field"))?;
    *input = &input[n..];
    Ok(b)
}
fn length(input: &mut &[u8]) -> Result<usize> {
    usize::try_from(read_varu64_slice(input)?).map_err(|_| Error::protocol("field length overflow"))
}
impl ColumnBuffer {
    pub(crate) fn new(name: String, ty: String) -> Result<Self> {
        let data = match layout(&ty)? {
            Layout::Map(key, value) => Storage::Map {
                offsets: Vec::new(),
                keys: Box::new(Self::new("keys".into(), key.into())?),
                values: Box::new(Self::new("values".into(), value.into())?),
            },
            Layout::Fixed(width) => Storage::Fixed {
                width,
                bytes: Vec::new(),
            },
            Layout::String => Storage::Strings {
                offsets: Vec::new(),
                bytes: Vec::new(),
            },
            Layout::Nullable(inner) => Storage::Nullable {
                null_map: Vec::new(),
                values: Box::new(Self::new("values".into(), inner.into())?),
            },
            Layout::Array(inner) => Storage::Array {
                offsets: Vec::new(),
                values: Box::new(Self::new("values".into(), inner.into())?),
            },
        };
        Ok(Self {
            name,
            ty,
            rows: 0,
            data,
        })
    }
    pub(crate) fn push(&mut self, input: &mut &[u8]) -> Result<()> {
        self.check_rows()?;
        match &mut self.data {
            Storage::Map {
                offsets,
                keys,
                values,
            } => {
                let n = length(input)?;
                if keys.rows.saturating_add(n) > MAX_ROWS {
                    return Err(Error::protocol("map exceeds row limit"));
                }
                for _ in 0..n {
                    keys.push(input)?;
                    values.push(input)?;
                }
                offsets.push(keys.rows as u64);
            }
            Storage::Fixed { width, bytes } => {
                if bytes.len().saturating_add(*width) > MAX_BYTES {
                    return Err(Error::protocol("fixed column exceeds byte limit"));
                }
                bytes.extend_from_slice(take(input, *width)?);
            }
            Storage::Strings { offsets, bytes } => {
                let n = length(input)?;
                if bytes.len().saturating_add(n) > MAX_BYTES {
                    return Err(Error::protocol("string column exceeds byte limit"));
                }
                bytes.extend_from_slice(take(input, n)?);
                offsets.push(bytes.len() as u64);
            }
            Storage::Nullable { null_map, values } => {
                let flag = take(input, 1)?[0];
                if flag > 1 {
                    return Err(Error::protocol("invalid null flag"));
                }
                null_map.push(flag);
                if flag == 0 {
                    values.push(input)?;
                } else {
                    values.push_default()?;
                }
            }
            Storage::Array { offsets, values } => {
                let n = length(input)?;
                if values.rows.saturating_add(n) > MAX_ROWS {
                    return Err(Error::protocol("array exceeds row limit"));
                }
                for _ in 0..n {
                    values.push(input)?;
                }
                offsets.push(values.rows as u64);
            }
        }
        self.rows += 1;
        Ok(())
    }
    fn check_rows(&self) -> Result<()> {
        if self.rows >= MAX_ROWS {
            return Err(Error::protocol("column exceeds row limit"));
        }
        Ok(())
    }
    fn push_default(&mut self) -> Result<()> {
        self.check_rows()?;
        match &mut self.data {
            Storage::Map { offsets, keys, .. } => offsets.push(keys.rows as u64),
            Storage::Fixed { width, bytes } => {
                let len = bytes
                    .len()
                    .checked_add(*width)
                    .filter(|n| *n <= MAX_BYTES)
                    .ok_or_else(|| Error::protocol("fixed column exceeds byte limit"))?;
                bytes.resize(len, 0);
            }
            Storage::Strings { offsets, bytes } => offsets.push(bytes.len() as u64),
            Storage::Nullable { null_map, values } => {
                null_map.push(1);
                values.push_default()?;
            }
            Storage::Array { offsets, values } => offsets.push(values.rows as u64),
        }
        self.rows += 1;
        Ok(())
    }
    pub(crate) fn finish(self) -> Column {
        let data = match self.data {
            Storage::Map {
                offsets,
                keys,
                values,
            } => ColumnData::Map {
                offsets,
                keys: Box::new(keys.finish()),
                values: Box::new(values.finish()),
            },
            Storage::Fixed { bytes, .. } => ColumnData::Fixed(AlignedBytes::from_bytes(&bytes)),
            Storage::Strings { offsets, bytes } => ColumnData::Strings { offsets, bytes },
            Storage::Nullable { null_map, values } => ColumnData::Nullable {
                null_map,
                values: Box::new(values.finish()),
            },
            Storage::Array { offsets, values } => ColumnData::Array {
                offsets,
                values: Box::new(values.finish()),
            },
        };
        Column {
            name: self.name.into(),
            type_name: self.ty.into(),
            rows: self.rows,
            data,
        }
    }
}
