//! Variable-length, composite, and wrapper column family coverage.

use clickhouse::{
    ArrayColumn, Decimal256Column, FixedStringColumn, GeoColumn, GeoKind, LowCardinalityColumn,
    MapColumn, NothingColumn, NullableColumn, StringColumn, TupleColumn, WireBlock, WireColumn,
};
use zerocopy::FromBytes;

#[test]
fn strings_fixed_strings_and_json_round_trip() {
    let mut block = WireBlock::new().expect("block");
    block
        .append(
            "s",
            WireColumn::strings(["alpha", "beta"]).expect("strings"),
        )
        .expect("append strings");
    let mut fixed = FixedStringColumn::new(4).expect("fixed string");
    fixed.append(b"ab").expect("append ab");
    fixed.append(b"wxyz").expect("append wxyz");
    block
        .append("fs", WireColumn::FixedString(fixed))
        .expect("append fixed string");
    let mut json = StringColumn::new_json().expect("json");
    json.append_str("{}").expect("append json");
    json.append_str("{\"a\":1}").expect("append json");
    block
        .append("j", WireColumn::Json(json))
        .expect("append json");

    assert_eq!(block.column_str("s", 0).expect("s0"), "alpha");
    assert_eq!(block.column_str("s", 1).expect("s1"), "beta");
    assert_eq!(block.column_bytes("fs", 0).expect("fs0"), b"ab\0\0");
    assert_eq!(block.column_bytes("fs", 1).expect("fs1"), b"wxyz");
    assert_eq!(block.column_str("j", 1).expect("j1"), "{\"a\":1}");
}

#[test]
fn nullable_primitive_and_string_round_trip() {
    let inner = WireColumn::fixed(&[0_u32]).expect("inner");
    let mut nullable = NullableColumn::new(&inner).expect("nullable");
    nullable.append_fixed(&[7_u32, 8]).expect("append values");
    nullable.append_null().expect("append null");

    let nested = nullable.nested();
    let nested = nested.as_ref().expect("nested");
    let bytes = nested.fixed_bytes();
    let values = <[u32]>::ref_from_bytes(bytes).expect("values");
    assert_eq!(values, &[7, 8, 0]);

    let mut block = WireBlock::new().expect("block");
    block
        .append("n", WireColumn::Nullable(Box::new(nullable)))
        .expect("append nullable");
    assert_eq!(block.column_nulls("n").expect("nulls"), &[0, 0, 1]);

    let inner = WireColumn::string().expect("string inner");
    let mut nullable = NullableColumn::new(&inner).expect("nullable string");
    nullable.append_str("x").expect("append string");
    nullable.append_null().expect("append null");
    let mut block = WireBlock::new().expect("block");
    block
        .append("ns", WireColumn::Nullable(Box::new(nullable)))
        .expect("append nullable string");
    assert_eq!(block.column_str("ns", 0).expect("ns0"), "x");
    assert_eq!(block.column_nulls("ns").expect("ns nulls"), &[0, 1]);
}

#[test]
fn array_offsets_and_values_round_trip() {
    let mut array = ArrayColumn::new("UInt32").expect("array");
    let first = WireColumn::fixed(&[1_u32, 2, 3]).expect("first values");
    array.append_row(&first).expect("append first row");
    let second = WireColumn::fixed(&[4_u32]).expect("second values");
    array.append_row(&second).expect("append second row");

    let values = array.values();
    let values = values.as_ref().expect("values handle");
    let bytes = values.fixed_bytes();
    let values = <[u32]>::ref_from_bytes(bytes).expect("values");
    assert_eq!(values, &[1, 2, 3, 4]);
    let mut block = WireBlock::new().expect("block");
    block
        .append("a", WireColumn::Array(Box::new(array)))
        .expect("append array");
    assert_eq!(block.column_offsets("a").expect("offsets"), &[3, 4]);
}

#[test]
fn tuple_children_round_trip() {
    let mut tuple = TupleColumn::new(&["UInt32", "String"]).expect("tuple");
    tuple
        .append_fixed(0, &7_u32.to_le_bytes())
        .expect("tuple u32");
    tuple.append_str(1, "x").expect("tuple string");
    tuple
        .append_fixed(0, &8_u32.to_le_bytes())
        .expect("tuple u32");
    tuple.append_str(1, "y").expect("tuple string");

    let left = tuple.child(0).expect("left");
    let right = tuple.child(1).expect("right");
    assert_eq!(
        <[u32]>::ref_from_bytes(left.as_ref().expect("left").fixed_bytes()).expect("left"),
        &[7, 8]
    );
    assert_eq!(right.as_ref().expect("right").string_at(0), b"x");
    assert_eq!(right.as_ref().expect("right").string_at(1), b"y");
    let mut block = WireBlock::new().expect("block");
    block
        .append("t", WireColumn::Tuple(tuple))
        .expect("append tuple");
    assert!(
        block
            .column_type_name("t")
            .expect("tuple type")
            .starts_with("Tuple(")
    );
}

#[test]
fn map_low_cardinality_geo_and_nothing_round_trip() {
    let mut map = MapColumn::new("String", "UInt64").expect("map");
    let keys = WireColumn::strings(["a"]).expect("keys");
    let values = WireColumn::fixed(&[1_u64]).expect("values");
    map.append_row(&keys, &values)
        .expect("append first map row");
    let keys = WireColumn::strings(["b"]).expect("keys");
    let values = WireColumn::fixed(&[2_u64]).expect("values");
    map.append_row(&keys, &values)
        .expect("append second map row");
    let mut block = WireBlock::new().expect("block");
    block.append("m", WireColumn::Map(map)).expect("append map");
    assert!(
        block
            .column_type_name("m")
            .expect("map type")
            .starts_with("Map(")
    );

    let mut low_cardinality = LowCardinalityColumn::new("String").expect("low cardinality");
    low_cardinality.append_str("a").expect("append a");
    low_cardinality.append_str("b").expect("append b");
    block
        .append("lc", WireColumn::LowCardinality(Box::new(low_cardinality)))
        .expect("append low cardinality");
    assert_eq!(block.column_str("lc", 0).expect("lc0"), "a");
    assert_eq!(block.column_str("lc", 1).expect("lc1"), "b");

    let mut point = GeoColumn::new(GeoKind::Point).expect("point");
    point.append_point(1.5, 2.5).expect("append point");
    point.append_point(3.5, 4.5).expect("append point");
    block
        .append("p", WireColumn::Geo(point))
        .expect("append point");
    assert_eq!(block.column_type_name("p").expect("point type"), "Point");

    let mut nothing = NothingColumn::new().expect("nothing");
    nothing.append_default().expect("append nothing");
    nothing.append_default().expect("append nothing");
    block
        .append("n", WireColumn::Nothing(nothing))
        .expect("append nothing");
    assert_eq!(block.column_type_name("n").expect("nothing type"), "Void");
}

#[test]
fn decimal256_rust_storage_and_block_clear() {
    let mut decimal = Decimal256Column::new(4);
    decimal.append(&[[1; 32], [2; 32]]);
    assert_eq!(decimal.len(), 2);
    assert!(decimal.type_name().starts_with("Decimal256("));
    assert_eq!(decimal.as_slice()[1], [2; 32]);
    decimal.clear();
    assert!(decimal.is_empty());

    let mut block = WireBlock::new().expect("block");
    block
        .append("u", WireColumn::fixed(&[1_u32, 2]).expect("u32"))
        .expect("append u32");
    assert_eq!(block.row_count(), 2);
    block.clear();
    assert_eq!(block.row_count(), 0);
    assert_eq!(block.column_count(), 1);
    assert!(
        block
            .column_slice::<u32>("u")
            .expect("cleared slice")
            .is_empty()
    );
}
