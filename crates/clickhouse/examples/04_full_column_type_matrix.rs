//! A hands-on tour of the ClickHouse column families exposed through the
//! clickhouse-cpp matrix: numeric, Bool, strings, date/time, decimals, UUID,
//! IP, enums, wrappers, composite values, Geo, JSON, and Nothing.
//!
//! Run with:
//!
//! ```text
//! cargo run -p clickhouse --example 04_full_column_type_matrix
//! ```
//!
//! The example builds one in-memory block with one row per column, then reads
//! representative values back. It needs no server.

use clickhouse::{
    ArrayColumn, ChBool, CppBlock, CppColumn, Date, Date32, DateTime, DateTime64, Decimal32,
    Decimal64, Decimal128, Decimal256Column, Enum8, Enum16, FixedStringColumn, GeoColumn, GeoKind,
    Ipv4, Ipv6, LowCardinalityColumn, MapColumn, NothingColumn, NullableColumn, StringColumn, Time,
    Time64, TupleColumn, Uuid,
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut block = CppBlock::new()?;

    // ---------------------------------------------------------------------
    // Numeric and Bool.
    // ---------------------------------------------------------------------
    block.append("u8", CppColumn::fixed(&[1_u8])?)?;
    block.append("u16", CppColumn::fixed(&[2_u16])?)?;
    block.append("u32", CppColumn::fixed(&[3_u32])?)?;
    block.append("u64", CppColumn::fixed(&[4_u64])?)?;
    block.append("u128", CppColumn::fixed(&[5_u128])?)?;
    block.append("i8", CppColumn::fixed(&[-1_i8])?)?;
    block.append("i16", CppColumn::fixed(&[-2_i16])?)?;
    block.append("i32", CppColumn::fixed(&[-3_i32])?)?;
    block.append("i64", CppColumn::fixed(&[-4_i64])?)?;
    block.append("i128", CppColumn::fixed(&[-5_i128])?)?;
    block.append("f32", CppColumn::fixed(&[1.5_f32])?)?;
    block.append("f64", CppColumn::fixed(&[2.5_f64])?)?;
    block.append("b", CppColumn::fixed(&[ChBool(1)])?)?;

    // ---------------------------------------------------------------------
    // String, FixedString, and JSON.
    // ---------------------------------------------------------------------
    block.append("s", CppColumn::strings(["alpha"])?)?;
    let mut fixed_string = FixedStringColumn::new(8)?;
    fixed_string.append_str("abc")?;
    block.append("fs", CppColumn::FixedString(fixed_string))?;
    let mut json = StringColumn::new_json()?;
    json.append_str("{\"a\":1}")?;
    block.append("js", CppColumn::Json(json))?;

    // ---------------------------------------------------------------------
    // Date and time families.
    // ---------------------------------------------------------------------
    block.append("d", CppColumn::fixed(&[Date(20_000)])?)?;
    block.append("d32", CppColumn::fixed(&[Date32(-20_000)])?)?;
    block.append("dt", CppColumn::fixed(&[DateTime(1_700_000_000)])?)?;
    block.append("dt64", CppColumn::fixed(&[DateTime64(1_700_000_000_123)])?)?;
    block.append("tm", CppColumn::fixed(&[Time(3_600)])?)?;
    block.append("tm64", CppColumn::fixed(&[Time64(3_600_000_123)])?)?;

    // ---------------------------------------------------------------------
    // Decimals, UUID, IP, and enums.
    // ---------------------------------------------------------------------
    block.append("dec32", CppColumn::fixed(&[Decimal32(12_345)])?)?;
    block.append("dec64", CppColumn::fixed(&[Decimal64(1_234_567)])?)?;
    block.append("dec128", CppColumn::fixed(&[Decimal128(123_456_789)])?)?;
    block.append("uuid", CppColumn::fixed(&[Uuid([0x11; 16])])?)?;
    block.append("ip4", CppColumn::fixed(&[Ipv4(0x7f00_0001)])?)?;
    block.append("ip6", CppColumn::fixed(&[Ipv6([0x20; 16])])?)?;
    block.append("e8", CppColumn::fixed(&[Enum8(1)])?)?;
    block.append("e16", CppColumn::fixed(&[Enum16(100)])?)?;

    // ---------------------------------------------------------------------
    // Nullable, Array, LowCardinality, Tuple, and Map wrappers.
    // ---------------------------------------------------------------------
    let inner = CppColumn::fixed(&[0_u64])?;
    let mut nullable = NullableColumn::new(&inner)?;
    nullable.append_fixed(&[42_u64])?;
    block.append("n", CppColumn::Nullable(Box::new(nullable)))?;

    let mut array = ArrayColumn::new("UInt32")?;
    array.append_row(&CppColumn::fixed(&[1_u32, 2, 3])?)?;
    block.append("arr", CppColumn::Array(Box::new(array)))?;

    let mut low_cardinality = LowCardinalityColumn::new("String")?;
    low_cardinality.append_str("repeated")?;
    block.append("lc", CppColumn::LowCardinality(Box::new(low_cardinality)))?;

    let mut tuple = TupleColumn::new(&["UInt32", "String"])?;
    tuple.append_fixed(0, &7_u32.to_le_bytes())?;
    tuple.append_str(1, "tuple")?;
    block.append("tup", CppColumn::Tuple(tuple))?;

    let mut map = MapColumn::new("String", "UInt64")?;
    map.append_row(
        &CppColumn::strings(["a", "b"])?,
        &CppColumn::fixed(&[1_u64, 2])?,
    )?;
    block.append("map", CppColumn::Map(map))?;

    // ---------------------------------------------------------------------
    // Geo Point, JSON, and Nothing.
    // ---------------------------------------------------------------------
    let mut point = GeoColumn::new(GeoKind::Point)?;
    point.append_point(37.6173, 55.7558)?;
    block.append("pt", CppColumn::Geo(point))?;

    let mut nothing = NothingColumn::new()?;
    nothing.append_default()?;
    block.append("nothing", CppColumn::Nothing(nothing))?;

    // ---------------------------------------------------------------------
    // Inspect the completed block.
    // ---------------------------------------------------------------------
    println!(
        "C++ matrix block: columns={} rows={}",
        block.column_count(),
        block.row_count()
    );
    for name in block.column_names() {
        println!("  {name:<8} {}", block.column_type_name(name)?);
    }

    // ---------------------------------------------------------------------
    // Zero-copy reads for fixed-width columns.
    // ---------------------------------------------------------------------
    let u64_values: &[u64] = block.column_slice("u64")?;
    let f64_values: &[f64] = block.column_slice("f64")?;
    let bool_values: &[ChBool] = block.column_slice("b")?;
    println!("\nzero-copy reads");
    println!("  u64={u64_values:?}");
    println!("  f64={f64_values:?}");
    println!("  bool={bool_values:?}");

    // Variable-length and composite columns expose checked accessors rather
    // than Rust slices.
    println!("  string={:?}", block.column_str("s", 0)?);
    println!("  fixed_string={:?}", block.column_bytes("fs", 0)?);
    println!("  json={:?}", block.column_str("js", 0)?);
    println!("  nullable_nulls={:?}", block.column_nulls("n")?);
    println!("  array_offsets={:?}", block.column_offsets("arr")?);

    let tuple_handle = block.column_handle("tup")?;
    let tuple_handle = tuple_handle.as_ref().expect("tuple handle");
    let left = tuple_handle.tuple_at(0);
    let right = tuple_handle.tuple_at(1);
    println!(
        "  tuple_child_0_bytes={:?} tuple_child_1={:?}",
        left.as_ref().expect("tuple child 0").fixed_bytes(),
        right.as_ref().expect("tuple child 1").string_at(0)
    );

    // ---------------------------------------------------------------------
    // Geo nested shapes.
    //
    // Ring/Polygon/MultiPolygon are arrays of lower-dimensional Geo values.
    // They are valid columns but need no Point-style append helper here; a
    // zero-row block is enough to show their ClickHouse type names.
    // ---------------------------------------------------------------------
    let mut geo_shapes = CppBlock::new()?;
    geo_shapes.append("ring", CppColumn::geo(GeoKind::Ring)?)?;
    geo_shapes.append("polygon", CppColumn::geo(GeoKind::Polygon)?)?;
    geo_shapes.append("multipolygon", CppColumn::geo(GeoKind::MultiPolygon)?)?;
    println!("\nGeo nested shapes (zero rows)");
    for name in geo_shapes.column_names() {
        println!("  {name}: {}", geo_shapes.column_type_name(name)?);
    }

    // ---------------------------------------------------------------------
    // Decimal256 is stored on the Rust side: clickhouse-cpp 3.x has no
    // Decimal256 column handle, so this column cannot be appended to the C++
    // block. The same limitation is documented by the transport tests.
    // ---------------------------------------------------------------------
    let mut decimal256 = Decimal256Column::new(18);
    decimal256.append(&[[0x01; 32]]);
    println!("\nDecimal256 (Rust-side storage)");
    println!(
        "  type={} rows={} scale={}",
        decimal256.type_name(),
        decimal256.len(),
        decimal256.scale()
    );

    println!("\nNotes:");
    println!("  Nothing is represented by clickhouse-cpp as Void.");
    println!("  Decimal256 stays in Rust storage until the vendor exposes a column handle.");
    Ok(())
}
