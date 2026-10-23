//! Complete native column family model.

use clickhouse_sys as sys;

use super::{
    ArrayColumn, Decimal256Column, FixedKind, FixedStringColumn, FixedType, FixedWireColumn,
    GeoColumn, GeoKind, LowCardinalityColumn, MapColumn, NothingColumn, NullableColumn,
    StringColumn, TupleColumn,
};
use crate::wire_error::{WireError, WireResult};

/// ClickHouse type codes exposed by the native wire layer.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(i32)]
pub enum TypeCode {
    /// Nothing column.
    Void = 0,
    /// Signed 8-bit integer.
    Int8 = 1,
    /// Signed 16-bit integer.
    Int16 = 2,
    /// Signed 32-bit integer.
    Int32 = 3,
    /// Signed 64-bit integer.
    Int64 = 4,
    /// Unsigned 8-bit integer.
    UInt8 = 5,
    /// Unsigned 16-bit integer.
    UInt16 = 6,
    /// Unsigned 32-bit integer.
    UInt32 = 7,
    /// Unsigned 64-bit integer.
    UInt64 = 8,
    /// 32-bit floating point.
    Float32 = 9,
    /// 64-bit floating point.
    Float64 = 10,
    /// Variable-length string.
    String = 11,
    /// Fixed-length string.
    FixedString = 12,
    /// Date and time.
    DateTime = 13,
    /// Calendar date.
    Date = 14,
    /// Array.
    Array = 15,
    /// Nullable wrapper.
    Nullable = 16,
    /// Tuple.
    Tuple = 17,
    /// Enum with 8-bit storage.
    Enum8 = 18,
    /// Enum with 16-bit storage.
    Enum16 = 19,
    /// UUID.
    Uuid = 20,
    /// IPv4 address.
    Ipv4 = 21,
    /// IPv6 address.
    Ipv6 = 22,
    /// Signed 128-bit integer.
    Int128 = 23,
    /// Unsigned 128-bit integer.
    UInt128 = 24,
    /// Generic decimal.
    Decimal = 25,
    /// Decimal32.
    Decimal32 = 26,
    /// Decimal64.
    Decimal64 = 27,
    /// Decimal128.
    Decimal128 = 28,
    /// LowCardinality wrapper.
    LowCardinality = 29,
    /// DateTime64.
    DateTime64 = 30,
    /// Date32.
    Date32 = 31,
    /// Map.
    Map = 32,
    /// Point.
    Point = 33,
    /// Ring.
    Ring = 34,
    /// Polygon.
    Polygon = 35,
    /// MultiPolygon.
    MultiPolygon = 36,
    /// Time.
    Time = 37,
    /// Time64.
    Time64 = 38,
    /// JSON.
    Json = 39,
    /// Boolean.
    Bool = 40,
}

impl TypeCode {
    /// Convert a raw wire type code.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying operation fails.
    pub fn from_i32(value: i32) -> WireResult<Self> {
        let code = match value {
            0 => Self::Void,
            1 => Self::Int8,
            2 => Self::Int16,
            3 => Self::Int32,
            4 => Self::Int64,
            5 => Self::UInt8,
            6 => Self::UInt16,
            7 => Self::UInt32,
            8 => Self::UInt64,
            9 => Self::Float32,
            10 => Self::Float64,
            11 => Self::String,
            12 => Self::FixedString,
            13 => Self::DateTime,
            14 => Self::Date,
            15 => Self::Array,
            16 => Self::Nullable,
            17 => Self::Tuple,
            18 => Self::Enum8,
            19 => Self::Enum16,
            20 => Self::Uuid,
            21 => Self::Ipv4,
            22 => Self::Ipv6,
            23 => Self::Int128,
            24 => Self::UInt128,
            25 => Self::Decimal,
            26 => Self::Decimal32,
            27 => Self::Decimal64,
            28 => Self::Decimal128,
            29 => Self::LowCardinality,
            30 => Self::DateTime64,
            31 => Self::Date32,
            32 => Self::Map,
            33 => Self::Point,
            34 => Self::Ring,
            35 => Self::Polygon,
            36 => Self::MultiPolygon,
            37 => Self::Time,
            38 => Self::Time64,
            39 => Self::Json,
            40 => Self::Bool,
            other => {
                return Err(WireError::Unsupported(format!(
                    "unknown wire type code {other}"
                )));
            }
        };
        Ok(code)
    }
}

/// An owned column across all supported ClickHouse families.
pub enum WireColumn {
    /// Fixed-width column.
    Fixed(FixedWireColumn),
    /// Variable-length String column.
    String(StringColumn),
    /// FixedString column.
    FixedString(FixedStringColumn),
    /// Nullable wrapper.
    Nullable(Box<NullableColumn>),
    /// Array wrapper.
    Array(Box<ArrayColumn>),
    /// LowCardinality wrapper.
    LowCardinality(Box<LowCardinalityColumn>),
    /// Tuple.
    Tuple(TupleColumn),
    /// Map.
    Map(MapColumn),
    /// Geo point or nested geo type.
    Geo(GeoColumn),
    /// JSON column.
    Json(StringColumn),
    /// Nothing column.
    Nothing(NothingColumn),
    /// Rust-side Decimal256 storage.
    Decimal256(Decimal256Column),
}

impl WireColumn {
    /// Construct a fixed-width column from a Rust slice.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying operation fails.
    pub fn fixed<T: FixedType>(values: &[T]) -> WireResult<Self> {
        Ok(Self::Fixed(FixedWireColumn::from_slice(values)?))
    }

    /// Construct a fixed-width column from raw element bytes.
    ///
    /// # Errors
    ///
    /// Returns an error if the type name is unsupported or the byte length is
    /// not a multiple of the element width.
    pub fn fixed_bytes(kind: FixedKind, type_name: &str, bytes: &[u8]) -> WireResult<Self> {
        Ok(Self::Fixed(FixedWireColumn::from_bytes(
            kind, type_name, bytes,
        )?))
    }

    /// Append one default value.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying operation fails.
    pub fn append_default(&mut self) -> WireResult<()> {
        let handle = self
            .wire_handle_mut()
            .ok_or_else(|| WireError::Unsupported("column has no C++ handle".to_owned()))?;
        handle.append_default().map_err(WireError::from)
    }

    /// Construct a fixed-width column with an explicit ClickHouse type name.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying operation fails.
    pub fn fixed_named<T: FixedType>(
        kind: FixedKind,
        type_name: &str,
        values: &[T],
    ) -> WireResult<Self> {
        Ok(Self::Fixed(FixedWireColumn::from_slice_named(
            kind, type_name, values,
        )?))
    }

    /// Construct an empty String column.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying operation fails.
    pub fn string() -> WireResult<Self> {
        Ok(Self::String(StringColumn::new()?))
    }

    /// Construct a String column from borrowed strings.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying operation fails.
    pub fn strings<'a, I>(values: I) -> WireResult<Self>
    where
        I: IntoIterator<Item = &'a str>,
    {
        let mut column = StringColumn::new()?;
        for value in values {
            column.append_str(value)?;
        }
        Ok(Self::String(column))
    }

    /// Construct an empty JSON column.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying operation fails.
    pub fn json() -> WireResult<Self> {
        Ok(Self::Json(StringColumn::new_json()?))
    }

    /// Construct a FixedString column with the given byte width.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying operation fails.
    pub fn fixed_string(width: usize) -> WireResult<Self> {
        Ok(Self::FixedString(FixedStringColumn::new(width)?))
    }

    /// Construct an empty Nullable column for the given inner type name.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying operation fails.
    pub fn nullable(inner: &WireColumn) -> WireResult<Self> {
        Ok(Self::Nullable(Box::new(NullableColumn::new(inner)?)))
    }

    /// Construct an empty Array column for the given item type name.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying operation fails.
    pub fn array(item_type: &str) -> WireResult<Self> {
        Ok(Self::Array(Box::new(ArrayColumn::new(item_type)?)))
    }

    /// Construct an empty LowCardinality column.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying operation fails.
    pub fn low_cardinality(inner_type: &str) -> WireResult<Self> {
        Ok(Self::LowCardinality(Box::new(LowCardinalityColumn::new(
            inner_type,
        )?)))
    }

    /// Construct an empty Tuple column.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying operation fails.
    pub fn tuple(element_types: &[&str]) -> WireResult<Self> {
        Ok(Self::Tuple(TupleColumn::new(element_types)?))
    }

    /// Construct an empty Map column.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying operation fails.
    pub fn map(key_type: &str, value_type: &str) -> WireResult<Self> {
        Ok(Self::Map(MapColumn::new(key_type, value_type)?))
    }

    /// Construct a Geo column.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying operation fails.
    pub fn geo(kind: GeoKind) -> WireResult<Self> {
        Ok(Self::Geo(GeoColumn::new(kind)?))
    }

    /// Construct a Nothing column.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying operation fails.
    pub fn nothing() -> WireResult<Self> {
        Ok(Self::Nothing(NothingColumn::new()?))
    }

    /// Construct a Rust-side Decimal256 column.
    pub fn decimal256(scale: u32) -> Self {
        Self::Decimal256(Decimal256Column::new(scale))
    }

    /// ClickHouse type name.
    pub fn type_name(&self) -> String {
        match self {
            Self::Fixed(column) => column.type_name(),
            Self::String(column) => column.type_name(),
            Self::FixedString(column) => column.type_name(),
            Self::Nullable(column) => column.type_name(),
            Self::Array(column) => column.type_name(),
            Self::LowCardinality(column) => column.type_name(),
            Self::Tuple(column) => column.type_name(),
            Self::Map(column) => column.type_name(),
            Self::Geo(column) => column.type_name(),
            Self::Json(column) => column.type_name(),
            Self::Nothing(column) => column.type_name(),
            Self::Decimal256(column) => column.type_name(),
        }
    }

    /// Number of rows.
    pub fn len(&self) -> usize {
        match self {
            Self::Fixed(column) => column.len(),
            Self::String(column) => column.len(),
            Self::FixedString(column) => column.len(),
            Self::Nullable(column) => column.len(),
            Self::Array(column) => column.len(),
            Self::LowCardinality(column) => column.len(),
            Self::Tuple(column) => column.len(),
            Self::Map(column) => column.len(),
            Self::Geo(column) => column.len(),
            Self::Json(column) => column.len(),
            Self::Nothing(column) => column.len(),
            Self::Decimal256(column) => column.len(),
        }
    }

    /// Whether the column has no rows.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Reset the column while retaining capacity where the native column supports it.
    pub fn clear(&mut self) {
        match self {
            Self::Fixed(column) => column.clear(),
            Self::String(column) => column.clear(),
            Self::FixedString(column) => column.clear(),
            Self::Nullable(column) => column.clear(),
            Self::Array(column) => column.clear(),
            Self::LowCardinality(column) => column.clear(),
            Self::Tuple(column) => column.clear(),
            Self::Map(column) => column.clear(),
            Self::Geo(column) => column.clear(),
            Self::Json(column) => column.clear(),
            Self::Nothing(column) => column.clear(),
            Self::Decimal256(column) => column.clear(),
        }
    }

    pub(crate) fn wire_handle_mut(&mut self) -> Option<std::pin::Pin<&mut sys::ColumnHandle>> {
        match self {
            Self::Fixed(column) => column.wire_handle_mut().ok(),
            Self::String(column) => Some(column.wire_handle_mut()),
            Self::FixedString(column) => Some(column.wire_handle_mut()),
            Self::Nullable(column) => Some(column.wire_handle_mut()),
            Self::Array(column) => Some(column.wire_handle_mut()),
            Self::LowCardinality(column) => Some(column.wire_handle_mut()),
            Self::Tuple(column) => Some(column.wire_handle_mut()),
            Self::Map(column) => Some(column.wire_handle_mut()),
            Self::Geo(column) => Some(column.wire_handle_mut()),
            Self::Json(column) => Some(column.wire_handle_mut()),
            Self::Nothing(column) => Some(column.wire_handle_mut()),
            Self::Decimal256(_) => None,
        }
    }

    pub(crate) fn wire_handle(&self) -> Option<&sys::ColumnHandle> {
        match self {
            Self::Fixed(column) => column.wire_handle().ok(),
            Self::String(column) => Some(column.wire_handle()),
            Self::FixedString(column) => Some(column.wire_handle()),
            Self::Nullable(column) => Some(column.wire_handle()),
            Self::Array(column) => Some(column.wire_handle()),
            Self::LowCardinality(column) => Some(column.wire_handle()),
            Self::Tuple(column) => Some(column.wire_handle()),
            Self::Map(column) => Some(column.wire_handle()),
            Self::Geo(column) => Some(column.wire_handle()),
            Self::Json(column) => Some(column.wire_handle()),
            Self::Nothing(column) => Some(column.wire_handle()),
            Self::Decimal256(_) => None,
        }
    }
}
