//! Fixed-width column families and their zero-copy projections.

use clickhouse_sys as sys;
use cxx::UniquePtr;
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

use crate::cpp_error::{CppError, CppResult};

/// Fixed-width ClickHouse column kinds.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FixedKind {
    /// Unsigned 8-bit integer.
    UInt8,
    /// Unsigned 16-bit integer.
    UInt16,
    /// Unsigned 32-bit integer.
    UInt32,
    /// Unsigned 64-bit integer.
    UInt64,
    /// Unsigned 128-bit integer.
    UInt128,
    /// Signed 8-bit integer.
    Int8,
    /// Signed 16-bit integer.
    Int16,
    /// Signed 32-bit integer.
    Int32,
    /// Signed 64-bit integer.
    Int64,
    /// Signed 128-bit integer.
    Int128,
    /// 32-bit floating point.
    Float32,
    /// 64-bit floating point.
    Float64,
    /// Boolean stored as one byte.
    Bool,
    /// Calendar date stored as days since the epoch.
    Date,
    /// Extended calendar date.
    Date32,
    /// Date and time stored as seconds since the epoch.
    DateTime,
    /// Date and time with sub-second precision.
    DateTime64,
    /// Time of day.
    Time,
    /// Time of day with sub-second precision.
    Time64,
    /// Decimal with 32-bit storage.
    Decimal32,
    /// Decimal with 64-bit storage.
    Decimal64,
    /// Decimal with 128-bit storage.
    Decimal128,
    /// UUID stored as two 64-bit limbs.
    Uuid,
    /// IPv4 address stored as a 32-bit word.
    Ipv4,
    /// IPv6 address stored as 16 bytes.
    Ipv6,
    /// Enum with 8-bit storage.
    Enum8,
    /// Enum with 16-bit storage.
    Enum16,
}

/// Parse the precision from a ClickHouse decimal type name.
fn decimal_precision(type_name: &str) -> u32 {
    if type_name.starts_with("Decimal32") {
        return 9;
    }
    if type_name.starts_with("Decimal64") {
        return 18;
    }
    if type_name.starts_with("Decimal128") {
        return 38;
    }
    let Some(inner) = type_name
        .strip_prefix("Decimal(")
        .and_then(|rest| rest.split_once(','))
    else {
        return 0;
    };
    inner.0.parse().unwrap_or(0)
}

/// Map a concrete decimal type name to its fixed-width kind.
pub fn decimal_kind(type_name: &str) -> Option<FixedKind> {
    if !type_name.starts_with("Decimal") {
        return None;
    }
    let precision = decimal_precision(type_name);
    if precision <= 9 {
        Some(FixedKind::Decimal32)
    } else if precision <= 18 {
        Some(FixedKind::Decimal64)
    } else {
        Some(FixedKind::Decimal128)
    }
}

impl FixedKind {
    /// Whether this kind matches a concrete column type code and name.
    pub fn matches(self, actual: crate::cpp_column::TypeCode, type_name: &str) -> bool {
        match self {
            Self::Decimal32 => {
                actual == crate::cpp_column::TypeCode::Decimal && decimal_precision(type_name) <= 9
            }
            Self::Decimal64 => {
                actual == crate::cpp_column::TypeCode::Decimal
                    && (10..=18).contains(&decimal_precision(type_name))
            }
            Self::Decimal128 => {
                actual == crate::cpp_column::TypeCode::Decimal && decimal_precision(type_name) > 18
            }
            _ => crate::cpp_column::TypeCode::from_i32(self.type_code())
                .is_ok_and(|expected| expected == actual),
        }
    }

    /// The C++ type code used by clickhouse-cpp.
    pub fn type_code(self) -> i32 {
        match self {
            Self::UInt8 => 5,
            Self::UInt16 => 6,
            Self::UInt32 => 7,
            Self::UInt64 => 8,
            Self::UInt128 => 24,
            Self::Int8 => 1,
            Self::Int16 => 2,
            Self::Int32 => 3,
            Self::Int64 => 4,
            Self::Int128 => 23,
            Self::Float32 => 9,
            Self::Float64 => 10,
            Self::Bool => 40,
            Self::Date => 14,
            Self::Date32 => 31,
            Self::DateTime => 13,
            Self::DateTime64 => 30,
            Self::Time => 37,
            Self::Time64 => 38,
            Self::Decimal32 => 26,
            Self::Decimal64 => 27,
            Self::Decimal128 => 28,
            Self::Uuid => 20,
            Self::Ipv4 => 21,
            Self::Ipv6 => 22,
            Self::Enum8 => 18,
            Self::Enum16 => 19,
        }
    }

    /// Default ClickHouse type name used by the convenience constructors.
    pub fn default_type_name(self) -> &'static str {
        match self {
            Self::UInt8 => "UInt8",
            Self::UInt16 => "UInt16",
            Self::UInt32 => "UInt32",
            Self::UInt64 => "UInt64",
            Self::UInt128 => "UInt128",
            Self::Int8 => "Int8",
            Self::Int16 => "Int16",
            Self::Int32 => "Int32",
            Self::Int64 => "Int64",
            Self::Int128 => "Int128",
            Self::Float32 => "Float32",
            Self::Float64 => "Float64",
            Self::Bool => "Bool",
            Self::Date => "Date",
            Self::Date32 => "Date32",
            Self::DateTime => "DateTime",
            Self::DateTime64 => "DateTime64(3)",
            Self::Time => "Time",
            Self::Time64 => "Time64(3)",
            Self::Decimal32 => "Decimal32(2)",
            Self::Decimal64 => "Decimal64(4)",
            Self::Decimal128 => "Decimal128(8)",
            Self::Uuid => "UUID",
            Self::Ipv4 => "IPv4",
            Self::Ipv6 => "IPv6",
            Self::Enum8 => "Enum8('a' = 1)",
            Self::Enum16 => "Enum16('a' = 1)",
        }
    }
}

/// Rust element types with a canonical fixed-width ClickHouse representation.
pub trait FixedType: Copy + FromBytes + IntoBytes + Immutable + KnownLayout + 'static {
    /// CppColumn kind.
    const KIND: FixedKind;
    /// Default ClickHouse type name.
    const TYPE_NAME: &'static str;
}

macro_rules! impl_fixed_type {
    ($type:ty, $kind:ident, $name:literal) => {
        impl FixedType for $type {
            const KIND: FixedKind = FixedKind::$kind;
            const TYPE_NAME: &'static str = $name;
        }
    };
}

impl_fixed_type!(u8, UInt8, "UInt8");
impl_fixed_type!(u16, UInt16, "UInt16");
impl_fixed_type!(u32, UInt32, "UInt32");
impl_fixed_type!(u64, UInt64, "UInt64");
impl_fixed_type!(u128, UInt128, "UInt128");
impl_fixed_type!(i8, Int8, "Int8");
impl_fixed_type!(i16, Int16, "Int16");
impl_fixed_type!(i32, Int32, "Int32");
impl_fixed_type!(i64, Int64, "Int64");
impl_fixed_type!(i128, Int128, "Int128");
impl_fixed_type!(f32, Float32, "Float32");
impl_fixed_type!(f64, Float64, "Float64");

macro_rules! fixed_newtype {
    ($(#[$meta:meta])* $name:ident, $inner:ty, $kind:ident, $type_name:literal) => {
        $(#[$meta])*
        #[derive(
            Clone,
            Copy,
            Debug,
            PartialEq,
            Eq,
            FromBytes,
            IntoBytes,
            Immutable,
            KnownLayout,
        )]
        #[repr(transparent)]
        pub struct $name(pub $inner);

        impl FixedType for $name {
            const KIND: FixedKind = FixedKind::$kind;
            const TYPE_NAME: &'static str = $type_name;
        }
    };
}

fixed_newtype!(
    /// Boolean stored as a byte.
    ChBool,
    u8,
    Bool,
    "Bool"
);
fixed_newtype!(
    /// Calendar date stored as days since the epoch.
    Date,
    u16,
    Date,
    "Date"
);
fixed_newtype!(
    /// Extended calendar date.
    Date32,
    i32,
    Date32,
    "Date32"
);
fixed_newtype!(
    /// Date and time stored as seconds since the epoch.
    DateTime,
    u32,
    DateTime,
    "DateTime"
);
fixed_newtype!(
    /// Date and time with sub-second precision.
    DateTime64,
    i64,
    DateTime64,
    "DateTime64(3)"
);
fixed_newtype!(
    /// Time of day.
    Time,
    i32,
    Time,
    "Time"
);
fixed_newtype!(
    /// Time of day with sub-second precision.
    Time64,
    i64,
    Time64,
    "Time64(3)"
);
fixed_newtype!(
    /// Decimal with 32-bit storage.
    Decimal32,
    i32,
    Decimal32,
    "Decimal32(2)"
);
fixed_newtype!(
    /// Decimal with 64-bit storage.
    Decimal64,
    i64,
    Decimal64,
    "Decimal64(4)"
);
fixed_newtype!(
    /// Decimal with 128-bit storage.
    Decimal128,
    i128,
    Decimal128,
    "Decimal128(8)"
);
fixed_newtype!(
    /// UUID stored as two 64-bit limbs in little-endian order.
    Uuid,
    [u8; 16],
    Uuid,
    "UUID"
);
fixed_newtype!(
    /// IPv4 address stored as a 32-bit word.
    Ipv4,
    u32,
    Ipv4,
    "IPv4"
);
fixed_newtype!(
    /// IPv6 address stored as 16 bytes.
    Ipv6,
    [u8; 16],
    Ipv6,
    "IPv6"
);
fixed_newtype!(
    /// Enum with 8-bit storage.
    Enum8,
    i8,
    Enum8,
    "Enum8('a' = 1)"
);
fixed_newtype!(
    /// Enum with 16-bit storage.
    Enum16,
    i16,
    Enum16,
    "Enum16('a' = 1)"
);

/// An owned fixed-width column.
pub struct CppFixedColumn {
    handle: UniquePtr<sys::ColumnHandle>,
    kind: FixedKind,
}

impl CppFixedColumn {
    /// Create an empty column with an explicit ClickHouse type name.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying operation fails.
    pub fn new_named(kind: FixedKind, type_name: &str) -> CppResult<Self> {
        let handle = sys::new_column(type_name).map_err(CppError::from)?;
        let actual = crate::cpp_column::TypeCode::from_i32(handle.type_code())?;
        if !kind.matches(actual, &handle.type_name()) {
            return Err(CppError::Unsupported(format!(
                "type name '{type_name}' did not produce {kind:?} (got {actual:?})"
            )));
        }
        Ok(Self { handle, kind })
    }

    /// Create an empty column from a Rust fixed-width type.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying operation fails.
    pub fn new<T: FixedType>() -> CppResult<Self> {
        Self::new_named(T::KIND, T::TYPE_NAME)
    }

    /// Create a column from a contiguous slice.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying operation fails.
    pub fn from_slice<T: FixedType>(values: &[T]) -> CppResult<Self> {
        let mut column = Self::new::<T>()?;
        column.append_slice(values)?;
        Ok(column)
    }

    /// Create a column from a slice with an explicit ClickHouse type name.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying operation fails.
    pub fn from_slice_named<T: FixedType>(
        kind: FixedKind,
        type_name: &str,
        values: &[T],
    ) -> CppResult<Self> {
        let mut column = Self::new_named(kind, type_name)?;
        column.append_slice(values)?;
        Ok(column)
    }

    /// Create a column from raw little-endian element bytes.
    ///
    /// # Errors
    ///
    /// Returns an error if the type name is unsupported or the byte length is
    /// not a multiple of the element width.
    pub fn from_bytes(kind: FixedKind, type_name: &str, bytes: &[u8]) -> CppResult<Self> {
        let column = Self::new_named(kind, type_name)?;
        let handle = column
            .handle
            .as_ref()
            .ok_or_else(|| CppError::Unsupported("fixed column handle is null".to_owned()))?;
        handle.append_fixed(bytes).map_err(CppError::from)?;
        Ok(column)
    }

    /// Append contiguous values.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying operation fails.
    pub fn append_slice<T: FixedType>(&mut self, values: &[T]) -> CppResult<()> {
        if self.kind != T::KIND {
            return Err(CppError::Unsupported(format!(
                "cannot append {:?} values to a {:?} column",
                T::KIND,
                self.kind
            )));
        }
        let handle = self
            .handle
            .as_ref()
            .ok_or_else(|| CppError::Unsupported("fixed column handle is null".to_owned()))?;
        handle
            .append_fixed(values.as_bytes())
            .map_err(CppError::from)
    }

    /// Project the column as a zero-copy slice.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying operation fails.
    pub fn as_slice<T: FixedType>(&self) -> CppResult<&[T]> {
        if self.kind != T::KIND {
            return Err(CppError::Unsupported(format!(
                "column is {:?}, not {:?}",
                self.kind,
                T::KIND
            )));
        }
        let handle = self
            .handle
            .as_ref()
            .ok_or_else(|| CppError::Unsupported("fixed column handle is null".to_owned()))?;
        let bytes = handle.fixed_bytes();
        <[T]>::ref_from_bytes(bytes).map_err(|_| CppError::Alignment)
    }

    /// Copy the column into an aligned Rust vector.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying operation fails.
    pub fn to_vec<T: FixedType>(&self) -> CppResult<Vec<T>> {
        if self.kind != T::KIND {
            return Err(CppError::Unsupported(format!(
                "column is {:?}, not {:?}",
                self.kind,
                T::KIND
            )));
        }
        let handle = self
            .handle
            .as_ref()
            .ok_or_else(|| CppError::Unsupported("fixed column handle is null".to_owned()))?;
        let bytes = handle.fixed_bytes();
        let size = core::mem::size_of::<T>();
        if size == 0 || bytes.len() % size != 0 {
            return Err(CppError::InvalidLength {
                len: bytes.len(),
                element_size: size,
            });
        }
        let mut values = Vec::with_capacity(bytes.len() / size);
        for chunk in bytes.chunks_exact(size) {
            let value = T::read_from_bytes(chunk).map_err(|_| CppError::Alignment)?;
            values.push(value);
        }
        Ok(values)
    }

    /// Number of rows.
    pub fn len(&self) -> usize {
        self.handle.as_ref().map_or(0, |handle| handle.size())
    }

    /// Whether the column has no rows.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Retained capacity reported by clickhouse-cpp.
    pub fn capacity(&self) -> usize {
        self.handle.as_ref().map_or(0, |handle| handle.capacity())
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

    /// CppColumn kind.
    pub fn kind(&self) -> FixedKind {
        self.kind
    }

    pub(crate) fn cpp_handle(&self) -> CppResult<&sys::ColumnHandle> {
        self.handle
            .as_ref()
            .ok_or_else(|| CppError::Unsupported("fixed column handle is null".to_owned()))
    }
}
