//! Nullable, Array, LowCardinality, Tuple, Map, Geo, Nothing, and Decimal256 columns.

use clickhouse_sys as sys;
use cxx::{SharedPtr, UniquePtr};
use zerocopy::IntoBytes;

use super::{FixedType, WireColumn};
use crate::wire_error::{WireError, WireResult};

/// A Nullable wrapper around a nested column.
pub struct NullableColumn {
    handle: UniquePtr<sys::ColumnHandle>,
    nested_type: String,
}

impl NullableColumn {
    /// Create an empty Nullable column.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying operation fails.
    pub fn new(inner: &WireColumn) -> WireResult<Self> {
        let nested_type = inner.type_name();
        let name = format!("Nullable({nested_type})");
        let handle = sys::new_column(&name)?;
        Ok(Self {
            handle,
            nested_type,
        })
    }

    /// Append one null row.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying operation fails.
    pub fn append_null(&mut self) -> WireResult<()> {
        if let Some(nested) = self.nested().as_ref().and_then(|handle| handle.as_ref()) {
            nested.append_default().map_err(WireError::from)?;
        }
        self.handle
            .as_ref()
            .ok_or_else(|| WireError::Unsupported("Nullable column handle is null".to_owned()))?
            .append_nullable_flag(true)
            .map_err(WireError::from)
    }

    /// Append one or more non-null fixed-width values.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying operation fails.
    pub fn append_fixed<T: FixedType>(&mut self, values: &[T]) -> WireResult<()> {
        let nested = self
            .nested()
            .ok_or_else(|| WireError::Unsupported("Nullable has no nested column".to_owned()))?;
        let nested = nested
            .as_ref()
            .ok_or_else(|| WireError::Unsupported("Nullable nested handle is null".to_owned()))?;
        nested
            .append_fixed(values.as_bytes())
            .map_err(WireError::from)?;
        for _ in values {
            self.append_flag(false)?;
        }
        Ok(())
    }

    /// Append one non-null string value.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying operation fails.
    pub fn append_str(&mut self, value: &str) -> WireResult<()> {
        let nested = self
            .nested()
            .ok_or_else(|| WireError::Unsupported("Nullable has no nested column".to_owned()))?;
        let nested = nested
            .as_ref()
            .ok_or_else(|| WireError::Unsupported("Nullable nested handle is null".to_owned()))?;
        nested
            .append_string(value.as_bytes())
            .map_err(WireError::from)?;
        self.append_flag(false)
    }

    fn append_flag(&mut self, is_null: bool) -> WireResult<()> {
        self.handle
            .as_ref()
            .ok_or_else(|| WireError::Unsupported("Nullable column handle is null".to_owned()))?
            .append_nullable_flag(is_null)
            .map_err(WireError::from)
    }

    /// Append a whole nested column.
    ///
    /// # Errors
    ///
    /// Returns an error when the nullable or nested handle is missing, or the
    /// underlying native append fails.
    pub fn append_nested_column(&mut self, nested: &WireColumn) -> WireResult<()> {
        let inner = self
            .nested()
            .ok_or_else(|| WireError::Unsupported("Nullable has no nested column".to_owned()))?;
        let inner = inner
            .as_ref()
            .ok_or_else(|| WireError::Unsupported("Nullable nested handle is null".to_owned()))?;
        let other = nested
            .wire_handle()
            .ok_or_else(|| WireError::Unsupported("nested column has no C++ handle".to_owned()))?;
        inner.append_column(other).map_err(WireError::from)
    }

    /// Append one null flag without changing the nested column.
    ///
    /// # Errors
    ///
    /// Returns an error when the nullable handle is missing or the native
    /// append fails.
    pub fn append_null_flag(&mut self, is_null: bool) -> WireResult<()> {
        self.append_flag(is_null)
    }

    /// Nested column handle.
    pub fn nested(&self) -> Option<SharedPtr<sys::ColumnHandle>> {
        self.handle.as_ref().map(|handle| handle.nested())
    }

    /// Null bitmap.
    pub fn nulls(&self) -> &[u8] {
        self.handle.as_ref().map_or(&[], |handle| handle.nulls())
    }

    /// Number of rows.
    pub fn len(&self) -> usize {
        self.handle.as_ref().map_or(0, |handle| handle.size())
    }

    /// Whether the column has no rows.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Reset the column while retaining capacity.
    pub fn clear(&mut self) {
        if let Some(handle) = self.handle.as_ref() {
            handle.clear();
        }
    }

    /// ClickHouse type name.
    pub fn type_name(&self) -> String {
        self.handle
            .as_ref()
            .map_or_else(String::new, |handle| handle.type_name())
    }

    /// Nested ClickHouse type name.
    pub fn nested_type(&self) -> &str {
        &self.nested_type
    }

    pub(crate) fn wire_handle(&self) -> &sys::ColumnHandle {
        self.handle.as_ref().expect("Nullable column handle")
    }
}

/// An Array wrapper around a nested column.
pub struct ArrayColumn {
    handle: UniquePtr<sys::ColumnHandle>,
    item_type: String,
}

impl ArrayColumn {
    /// Create an empty Array column.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying operation fails.
    pub fn new(item_type: &str) -> WireResult<Self> {
        let name = format!("Array({item_type})");
        let handle = sys::new_column(&name)?;
        Ok(Self {
            handle,
            item_type: item_type.to_owned(),
        })
    }

    /// Append one array row from a column containing the row's values.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying operation fails.
    pub fn append_row(&mut self, values: &WireColumn) -> WireResult<()> {
        let values = values.wire_handle().ok_or_else(|| {
            WireError::Unsupported("cannot append Decimal256 to an Array".to_owned())
        })?;
        self.handle
            .as_ref()
            .ok_or_else(|| WireError::Unsupported("Array column handle is null".to_owned()))?
            .array_append_row(values)
            .map_err(WireError::from)
    }

    /// Nested values column handle.
    pub fn values(&self) -> Option<SharedPtr<sys::ColumnHandle>> {
        self.handle.as_ref().map(|handle| handle.nested())
    }

    /// Array offsets.
    pub fn offsets(&self) -> &[u64] {
        self.handle
            .as_ref()
            .map_or(&[], |handle| handle.array_offsets())
    }

    /// Number of array rows.
    pub fn len(&self) -> usize {
        self.handle.as_ref().map_or(0, |handle| handle.size())
    }

    /// Whether the column has no rows.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Reset the column while retaining capacity.
    pub fn clear(&mut self) {
        if let Some(handle) = self.handle.as_ref() {
            handle.clear();
        }
    }

    /// ClickHouse type name.
    pub fn type_name(&self) -> String {
        self.handle
            .as_ref()
            .map_or_else(String::new, |handle| handle.type_name())
    }

    /// Nested item type name.
    pub fn item_type(&self) -> &str {
        &self.item_type
    }

    pub(crate) fn wire_handle(&self) -> &sys::ColumnHandle {
        self.handle.as_ref().expect("Array column handle")
    }
}

/// A LowCardinality wrapper.
pub struct LowCardinalityColumn {
    handle: UniquePtr<sys::ColumnHandle>,
}

impl LowCardinalityColumn {
    /// Create an empty LowCardinality column.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying operation fails.
    pub fn new(inner_type: &str) -> WireResult<Self> {
        let name = format!("LowCardinality({inner_type})");
        Ok(Self {
            handle: sys::new_column(&name)?,
        })
    }

    /// Append a string dictionary value.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying operation fails.
    pub fn append_str(&mut self, value: &str) -> WireResult<()> {
        self.handle
            .as_ref()
            .ok_or_else(|| WireError::Unsupported("LowCardinality handle is null".to_owned()))?
            .low_cardinality_append_string(value.as_bytes())
            .map_err(WireError::from)
    }

    /// Append a signed integer dictionary value.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying operation fails.
    pub fn append_i64(&mut self, value: i64) -> WireResult<()> {
        self.handle
            .as_ref()
            .ok_or_else(|| WireError::Unsupported("LowCardinality handle is null".to_owned()))?
            .low_cardinality_append_i64(value)
            .map_err(WireError::from)
    }

    /// Append an unsigned integer dictionary value.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying operation fails.
    pub fn append_u64(&mut self, value: u64) -> WireResult<()> {
        self.handle
            .as_ref()
            .ok_or_else(|| WireError::Unsupported("LowCardinality handle is null".to_owned()))?
            .low_cardinality_append_u64(value)
            .map_err(WireError::from)
    }

    /// Number of rows.
    pub fn len(&self) -> usize {
        self.handle.as_ref().map_or(0, |handle| handle.size())
    }

    /// Whether the column has no rows.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Reset the column while retaining capacity.
    pub fn clear(&mut self) {
        if let Some(handle) = self.handle.as_ref() {
            handle.clear();
        }
    }

    /// ClickHouse type name.
    pub fn type_name(&self) -> String {
        self.handle
            .as_ref()
            .map_or_else(String::new, |handle| handle.type_name())
    }

    pub(crate) fn wire_handle(&self) -> &sys::ColumnHandle {
        self.handle.as_ref().expect("LowCardinality column handle")
    }
}

/// A Tuple column.
pub struct TupleColumn {
    handle: UniquePtr<sys::ColumnHandle>,
}

impl TupleColumn {
    /// Create an empty Tuple column from element type names.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying operation fails.
    pub fn new(element_types: &[&str]) -> WireResult<Self> {
        if element_types.is_empty() {
            return Err(WireError::Unsupported(
                "empty Tuple is not supported".to_owned(),
            ));
        }
        let name = format!("Tuple({})", element_types.join(", "));
        Ok(Self {
            handle: sys::new_column(&name)?,
        })
    }

    /// Number of element columns.
    pub fn tuple_len(&self) -> usize {
        self.handle.as_ref().map_or(0, |handle| handle.tuple_len())
    }

    /// One element column handle.
    pub fn child(&self, index: usize) -> Option<SharedPtr<sys::ColumnHandle>> {
        self.handle.as_ref().map(|handle| handle.tuple_at(index))
    }

    /// Append fixed-width bytes to one element column.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying operation fails.
    pub fn append_fixed(&mut self, index: usize, bytes: &[u8]) -> WireResult<()> {
        self.handle
            .as_ref()
            .ok_or_else(|| WireError::Unsupported("Tuple handle is null".to_owned()))?
            .tuple_append_fixed(index, bytes)
            .map_err(WireError::from)
    }

    /// Append a string to one element column.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying operation fails.
    pub fn append_str(&mut self, index: usize, value: &str) -> WireResult<()> {
        self.handle
            .as_ref()
            .ok_or_else(|| WireError::Unsupported("Tuple handle is null".to_owned()))?
            .tuple_append_string(index, value.as_bytes())
            .map_err(WireError::from)
    }

    /// Number of rows.
    pub fn len(&self) -> usize {
        self.handle.as_ref().map_or(0, |handle| handle.size())
    }

    /// Whether the column has no rows.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Reset the column while retaining capacity.
    pub fn clear(&mut self) {
        if let Some(handle) = self.handle.as_ref() {
            handle.clear();
        }
    }

    /// ClickHouse type name.
    pub fn type_name(&self) -> String {
        self.handle
            .as_ref()
            .map_or_else(String::new, |handle| handle.type_name())
    }

    pub(crate) fn wire_handle(&self) -> &sys::ColumnHandle {
        self.handle.as_ref().expect("Tuple column handle")
    }
}

/// A Map column.
pub struct MapColumn {
    handle: UniquePtr<sys::ColumnHandle>,
}

impl MapColumn {
    /// Create an empty Map column.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying operation fails.
    pub fn new(key_type: &str, value_type: &str) -> WireResult<Self> {
        let name = format!("Map({key_type}, {value_type})");
        Ok(Self {
            handle: sys::new_column(&name)?,
        })
    }

    /// Append one map row from key and value columns.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying operation fails.
    pub fn append_row(&mut self, keys: &WireColumn, values: &WireColumn) -> WireResult<()> {
        let keys = keys.wire_handle().ok_or_else(|| {
            WireError::Unsupported("Decimal256 map keys are not supported".to_owned())
        })?;
        let values = values.wire_handle().ok_or_else(|| {
            WireError::Unsupported("Decimal256 map values are not supported".to_owned())
        })?;
        self.handle
            .as_ref()
            .ok_or_else(|| WireError::Unsupported("Map handle is null".to_owned()))?
            .map_append_row(keys, values)
            .map_err(WireError::from)
    }

    /// Number of rows.
    pub fn len(&self) -> usize {
        self.handle.as_ref().map_or(0, |handle| handle.size())
    }

    /// Whether the column has no rows.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Reset the column while retaining capacity.
    pub fn clear(&mut self) {
        if let Some(handle) = self.handle.as_ref() {
            handle.clear();
        }
    }

    /// ClickHouse type name.
    pub fn type_name(&self) -> String {
        self.handle
            .as_ref()
            .map_or_else(String::new, |handle| handle.type_name())
    }

    pub(crate) fn wire_handle(&self) -> &sys::ColumnHandle {
        self.handle.as_ref().expect("Map column handle")
    }
}

/// Supported Geo column kinds.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GeoKind {
    /// Point.
    Point,
    /// Ring.
    Ring,
    /// Polygon.
    Polygon,
    /// MultiPolygon.
    MultiPolygon,
}

impl GeoKind {
    fn type_name(self) -> &'static str {
        match self {
            Self::Point => "Point",
            Self::Ring => "Ring",
            Self::Polygon => "Polygon",
            Self::MultiPolygon => "MultiPolygon",
        }
    }
}

/// A Geo column.
pub struct GeoColumn {
    handle: UniquePtr<sys::ColumnHandle>,
    kind: GeoKind,
}

impl GeoColumn {
    /// Create a Geo column.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying operation fails.
    pub fn new(kind: GeoKind) -> WireResult<Self> {
        Ok(Self {
            handle: sys::new_column(kind.type_name())?,
            kind,
        })
    }

    /// Append one Point row.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying operation fails.
    pub fn append_point(&mut self, x: f64, y: f64) -> WireResult<()> {
        if self.kind != GeoKind::Point {
            return Err(WireError::Unsupported(
                "append_point is only valid for Point columns".to_owned(),
            ));
        }
        self.handle
            .as_ref()
            .ok_or_else(|| WireError::Unsupported("Geo handle is null".to_owned()))?
            .geo_append_point(x, y)
            .map_err(WireError::from)
    }

    /// Number of rows.
    pub fn len(&self) -> usize {
        self.handle.as_ref().map_or(0, |handle| handle.size())
    }

    /// Whether the column has no rows.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Reset the column while retaining capacity.
    pub fn clear(&mut self) {
        if let Some(handle) = self.handle.as_ref() {
            handle.clear();
        }
    }

    /// ClickHouse type name.
    pub fn type_name(&self) -> String {
        self.handle
            .as_ref()
            .map_or_else(String::new, |handle| handle.type_name())
    }

    /// Geo kind.
    pub fn kind(&self) -> GeoKind {
        self.kind
    }

    pub(crate) fn wire_handle(&self) -> &sys::ColumnHandle {
        self.handle.as_ref().expect("Geo column handle")
    }
}

/// A Nothing column.
pub struct NothingColumn {
    handle: UniquePtr<sys::ColumnHandle>,
}

impl NothingColumn {
    /// Create an empty Nothing column.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying operation fails.
    pub fn new() -> WireResult<Self> {
        Ok(Self {
            handle: sys::new_column("Void")?,
        })
    }

    /// Append one default row.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying operation fails.
    pub fn append_default(&mut self) -> WireResult<()> {
        self.handle
            .as_ref()
            .ok_or_else(|| WireError::Unsupported("Nothing handle is null".to_owned()))?
            .append_default()
            .map_err(WireError::from)
    }

    /// Number of rows.
    pub fn len(&self) -> usize {
        self.handle.as_ref().map_or(0, |handle| handle.size())
    }

    /// Whether the column has no rows.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Reset the column while retaining capacity.
    pub fn clear(&mut self) {
        if let Some(handle) = self.handle.as_ref() {
            handle.clear();
        }
    }

    /// ClickHouse type name.
    pub fn type_name(&self) -> String {
        self.handle
            .as_ref()
            .map_or_else(String::new, |handle| handle.type_name())
    }

    pub(crate) fn wire_handle(&self) -> &sys::ColumnHandle {
        self.handle.as_ref().expect("Nothing column handle")
    }
}

/// Rust-side storage for Decimal256, which the native column layer does not expose.
pub struct Decimal256Column {
    scale: u32,
    values: Vec<[u8; 32]>,
}

impl Decimal256Column {
    /// Create an empty Decimal256 column.
    pub fn new(scale: u32) -> Self {
        Self {
            scale,
            values: Vec::new(),
        }
    }

    /// Append 32-byte two's-complement values.
    pub fn append(&mut self, values: &[[u8; 32]]) {
        self.values.extend_from_slice(values);
    }

    /// Borrow the stored values.
    pub fn as_slice(&self) -> &[[u8; 32]] {
        &self.values
    }

    /// Number of rows.
    pub fn len(&self) -> usize {
        self.values.len()
    }

    /// Whether the column has no rows.
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    /// Reset the column while retaining capacity.
    pub fn clear(&mut self) {
        self.values.clear();
    }

    /// ClickHouse type name.
    pub fn type_name(&self) -> String {
        format!("Decimal256({})", self.scale)
    }

    /// Decimal scale.
    pub fn scale(&self) -> u32 {
        self.scale
    }
}
