//! Column-oriented owned blocks with checked typed projections.
use std::borrow::Cow;

use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

use crate::{
    BufferPolicy, Column, Error, MemoryBuffer, Result,
    column::{AlignedBytes, ColumnData, validate_offsets},
    error::schema,
};
/// A collection of equally sized named columns.
#[derive(Default)]
pub struct Block {
    columns: Vec<Column>,
    rows: usize,
}
impl Block {
    /// Refill matching schema columns in place, reusing nested Vec capacity.
    /// # Errors
    /// Any schema or row-count mismatch rejects the entire refill before mutation.
    pub fn refill_from(&mut self, source: &Self) -> Result<()> {
        if self.columns.len() != source.columns.len()
            || self
                .columns
                .iter()
                .zip(&source.columns)
                .any(|(a, b)| !a.same_storage(b) || b.len() != source.rows)
        {
            return Err(schema("block refill schema or row count mismatch"));
        }
        for (target, input) in self.columns.iter_mut().zip(&source.columns) {
            target.copy_values(input);
        }
        self.rows = source.rows;
        Ok(())
    }

    /// Copy supported row columns into a native transport block.
    /// # Errors
    /// Unsupported column types or native conversion failures return an error.
    #[cfg(feature = "tcp")]
    pub fn to_native(&self) -> Result<crate::NativeBlock> {
        crate::row::native::to_native_block(self)
    }
    /// Copy supported columns from a native transport block into aligned buffers.
    /// # Errors
    /// Unsupported kinds or invalid native projections return an error.
    #[cfg(feature = "tcp")]
    pub fn from_native(native: &crate::NativeBlock) -> Result<Self> {
        crate::row::native::from_native(native)
    }
    pub fn new() -> Self {
        Self::default()
    }
    pub fn with_capacity(cols: usize, _rows: usize) -> Self {
        Self {
            columns: Vec::with_capacity(cols),
            rows: 0,
        }
    }
    pub fn row_count(&self) -> usize {
        self.rows
    }
    pub fn column_count(&self) -> usize {
        self.columns.len()
    }
    /// Exact retained capacity of the column Vec and nested value Vecs in bytes.
    pub fn allocated_bytes(&self) -> usize {
        self.columns.allocated_bytes()
            + self
                .columns
                .iter()
                .map(Column::allocated_bytes)
                .sum::<usize>()
    }
    /// Clear lengths using the balanced default aggregate budget.
    pub fn clear(&mut self) {
        self.clear_with_policy(BufferPolicy::default());
    }
    /// Clear lengths and trim burst capacity across all columns.
    /// Budgets smaller than the schema Vec release the schema as well; otherwise
    /// column names and types remain available for in-place refill.
    pub fn clear_with_policy(&mut self, policy: BufferPolicy) {
        if self.columns.allocated_bytes() > policy.max_retained_bytes {
            self.columns.clear();
            self.columns.shrink_to_fit();
        } else {
            let bytes = self.columns.iter().map(Column::allocated_bytes).sum();
            let rows = self
                .columns
                .iter()
                .map(MemoryBuffer::row_capacity)
                .max()
                .unwrap_or(0);
            let budget = BufferPolicy {
                max_retained_bytes: policy.max_retained_bytes - self.columns.allocated_bytes(),
                ..policy
            };
            let retention = budget.retention(rows, bytes);
            for column in &mut self.columns {
                column.clear_retained(retention);
            }
        }
        self.rows = 0;
    }
    /// Clear lengths without reclaiming warmed capacity.
    pub fn clear_unbounded(&mut self) {
        self.clear_with_policy(BufferPolicy::unbounded());
    }
    /// Release unused capacity while preserving current rows and schema.
    pub fn shrink_to_fit(&mut self) {
        for column in &mut self.columns {
            column.shrink_to_fit();
        }
        self.columns.shrink_to_fit();
    }
    pub fn column_index(&self, name: &str) -> Option<usize> {
        self.columns.iter().position(|c| c.name == name)
    }
    /// Look up a column by its schema name.
    /// # Errors
    /// An unknown name returns ColumnNotFound.
    pub fn column_by_name(&self, name: &str) -> Result<&Column> {
        self.column_index(name)
            .map(|i| &self.columns[i])
            .ok_or_else(|| Error::ColumnNotFound {
                col_name: Cow::Owned(String::from(name)),
            })
    }
    /// Look up a column by its index.
    /// # Errors
    /// An out-of-range index returns a schema error.
    pub fn column(&self, idx: usize) -> Result<&Column> {
        self.columns
            .get(idx)
            .ok_or_else(|| schema("column index out of bounds"))
    }
    /// Borrow a fixed-width slice directly from a column.
    /// # Errors
    /// Missing columns, wrong storage, width, or alignment reject the projection.
    pub fn column_slice<T: FromBytes + Immutable + KnownLayout>(
        &self,
        col_idx: usize,
    ) -> Result<&[T]> {
        let col = self.column(col_idx)?;
        col.slice().map_err(|e| Error::ZeroCopyCast {
            col_idx,
            col_name: col.name.clone(),
            reason: format!("{e}").into(),
        })
    }
    /// Borrow a fixed-width slice using the column name.
    /// # Errors
    /// A missing column or invalid cast returns an error.
    pub fn column_slice_by_name<T: FromBytes + Immutable + KnownLayout>(
        &self,
        name: &str,
    ) -> Result<&[T]> {
        self.column_slice(
            self.column_index(name)
                .ok_or_else(|| Error::ColumnNotFound {
                    col_name: Cow::Owned(String::from(name)),
                })?,
        )
    }
    /// Borrow bytes of a string column row.
    /// # Errors
    /// Wrong column storage, offsets, or row index reject the read.
    pub fn column_bytes(&self, col_idx: usize, row_idx: usize) -> Result<&[u8]> {
        let c = self.column(col_idx)?;
        let ColumnData::Strings { offsets, bytes } = &c.data else {
            return Err(schema("expected string storage"));
        };
        validate_offsets(offsets, bytes.len())?;
        let end = *offsets.get(row_idx).ok_or(Error::RowIndexOutOfBounds {
            row_idx,
            row_count: c.len(),
        })? as usize;
        let start = if row_idx == 0 {
            0
        } else {
            offsets[row_idx - 1] as usize
        };
        bytes
            .get(start..end)
            .ok_or_else(|| schema("invalid string row bounds"))
    }
    /// Borrow a UTF-8 string row.
    /// # Errors
    /// Invalid string storage, bounds, or UTF-8 returns an error.
    pub fn column_str(&self, col_idx: usize, row_idx: usize) -> Result<&str> {
        std::str::from_utf8(self.column_bytes(col_idx, row_idx)?)
            .map_err(|_| schema("invalid UTF-8 string"))
    }
    /// Append one owned column; reject duplicate column names.
    /// # Errors
    /// Row-count or column-name mismatches reject the insertion.
    pub fn append_column(&mut self, column: Column) -> Result<()> {
        if self.rows != 0 && self.rows != column.len() {
            return Err(schema("block column row count mismatch"));
        }
        if self.column_index(&column.name).is_some() {
            return Err(schema("duplicate block column"));
        }
        if !self.columns.is_empty() && self.columns[0].len() != column.len() {
            return Err(schema("block column row count mismatch"));
        }
        self.rows = column.len();
        self.columns.push(column);
        Ok(())
    }
    /// Copy a fixed-width column into aligned owned storage.
    /// # Errors
    /// Duplicate names or inconsistent row counts reject insertion.
    pub fn append_fixed_slice<T: IntoBytes + Immutable>(
        &mut self,
        name: impl Into<Cow<'static, str>>,
        type_name: &'static str,
        values: &[T],
    ) -> Result<()> {
        self.append_column(Column {
            name: name.into(),
            type_name: type_name.into(),
            rows: values.len(),
            data: ColumnData::Fixed(AlignedBytes::from_bytes(values.as_bytes())),
        })
    }
    /// Copy a flat string column after validating all offsets.
    /// # Errors
    /// Invalid offsets or a block schema mismatch reject insertion.
    pub fn append_strings_flat(
        &mut self,
        name: impl Into<Cow<'static, str>>,
        offsets: &[u64],
        flat_bytes: &[u8],
    ) -> Result<()> {
        validate_offsets(offsets, flat_bytes.len())?;
        self.append_column(Column {
            name: name.into(),
            type_name: "String".into(),
            rows: offsets.len(),
            data: ColumnData::Strings {
                offsets: offsets.to_vec(),
                bytes: flat_bytes.to_vec(),
            },
        })
    }
    /// Copy a flat primitive array column.
    /// # Errors
    /// Invalid offsets or inconsistent row counts reject insertion.
    pub fn append_array_fixed_flat<T: IntoBytes + Immutable>(
        &mut self,
        name: impl Into<Cow<'static, str>>,
        elem_type_name: &'static str,
        offsets: &[u64],
        flat_values: &[T],
    ) -> Result<()> {
        validate_offsets(offsets, flat_values.len())?;
        let mut inner = Block::new();
        inner.append_fixed_slice("values", elem_type_name, flat_values)?;
        let values = inner
            .columns
            .pop()
            .ok_or_else(|| schema("missing array values"))?;
        self.append_column(Column {
            name: name.into(),
            type_name: format!("Array({elem_type_name})").into(),
            rows: offsets.len(),
            data: ColumnData::Array {
                offsets: offsets.to_vec(),
                values: Box::new(values),
            },
        })
    }
    /// Copy a nullable primitive column after validating its null map.
    /// # Errors
    /// Invalid null flags, lengths, or block schema reject insertion.
    pub fn append_nullable_fixed<T: IntoBytes + Immutable>(
        &mut self,
        name: impl Into<Cow<'static, str>>,
        inner_type_name: &'static str,
        values: &[T],
        null_map: &[u8],
    ) -> Result<()> {
        if values.len() != null_map.len() || null_map.iter().any(|v| *v > 1) {
            return Err(schema("invalid nullable null map"));
        }
        let mut inner = Block::new();
        inner.append_fixed_slice("values", inner_type_name, values)?;
        let values = inner
            .columns
            .pop()
            .ok_or_else(|| schema("missing nullable values"))?;
        self.append_column(Column {
            name: name.into(),
            type_name: format!("Nullable({inner_type_name})").into(),
            rows: null_map.len(),
            data: ColumnData::Nullable {
                null_map: null_map.to_vec(),
                values: Box::new(values),
            },
        })
    }
}
