//! Rejection and retention contracts for publicly constructed column storage.

use std::collections::BTreeMap;

use clickhouse::{
    Block, BufferPolicy, Column, ColumnData,
    native_format::{encode_block, encode_rowbinary},
    row::{Field, FlatArray, MapBuffer, NullableBuffer},
};

#[derive(Clone, Copy, Debug)]
enum Shape {
    Array,
    Nullable,
    MapKey,
    MapValue,
}

fn column_for(shape: Shape, empty: bool) -> Column {
    let values = if empty { vec![] } else { vec![7_u64, 9] };
    let offsets = if empty { vec![] } else { vec![2] };
    match shape {
        Shape::Array => <Vec<u64> as Field>::column(&FlatArray { offsets, values }, "a"),
        Shape::Nullable => <Option<u64> as Field>::column(
            &NullableBuffer {
                null_map: if empty { vec![] } else { vec![0, 1] },
                values,
            },
            "n",
        ),
        Shape::MapKey | Shape::MapValue => <BTreeMap<u64, u64> as Field>::column(
            &MapBuffer {
                offsets,
                keys: values,
                values: if empty { vec![] } else { vec![11, 13] },
            },
            "m",
        ),
    }
    .expect("valid initial column")
}

fn declaration(shape: Shape) -> &'static str {
    match shape {
        Shape::Array => "Array(UInt8)",
        Shape::Nullable => "Nullable(UInt8)",
        Shape::MapKey => "Map(UInt8,UInt64)",
        Shape::MapValue => "Map(UInt64,UInt8)",
    }
}

fn block_from(column: Column) -> Block {
    let mut block = Block::new();
    block
        .append_column(column)
        .expect("matching row cardinality");
    block
}

fn assert_schema_source(block: &Block, shape: Shape, empty: bool) {
    let column = block.column(0).unwrap();
    let rows = if empty {
        0
    } else if matches!(shape, Shape::Nullable) {
        2
    } else {
        1
    };
    assert_eq!(block.column_count(), 1);
    assert_eq!(block.row_count(), rows);
    assert_eq!(column.len(), rows);
    assert_eq!(column.type_name, declaration(shape));
    let expected: &[u64] = if empty { &[] } else { &[7, 9] };
    let expected_offsets: &[u64] = if empty { &[] } else { &[2] };
    match &column.data {
        ColumnData::Array { offsets, values } => {
            assert!(matches!(shape, Shape::Array));
            assert_eq!(offsets, expected_offsets);
            assert_eq!(values.type_name, "UInt64");
            assert_eq!(values.slice::<u64>().unwrap(), expected);
        }
        ColumnData::Nullable { null_map, values } => {
            assert!(matches!(shape, Shape::Nullable));
            assert_eq!(null_map, if empty { &[][..] } else { &[0, 1][..] });
            assert_eq!(values.type_name, "UInt64");
            assert_eq!(values.slice::<u64>().unwrap(), expected);
        }
        ColumnData::Map {
            offsets,
            keys,
            values,
        } => {
            assert!(matches!(shape, Shape::MapKey | Shape::MapValue));
            assert_eq!(offsets, expected_offsets);
            assert_eq!(keys.type_name, "UInt64");
            assert_eq!(values.type_name, "UInt64");
            assert_eq!(keys.slice::<u64>().unwrap(), expected);
            assert_eq!(
                values.slice::<u64>().unwrap(),
                if empty { &[][..] } else { &[11, 13][..] }
            );
        }
        _ => panic!("recursive storage shape changed"),
    }
}

fn native_bytes(block: &Block) -> clickhouse::Result<Vec<u8>> {
    let mut bytes = Vec::new();
    encode_block(block, 0, &mut bytes)?;
    Ok(bytes)
}

macro_rules! schema_rejection {
    ($name:ident, $encode:path, $shape:expr, $empty:expr) => {
        #[test]
        fn $name() {
            let shape = $shape;
            let mut column = column_for(shape, $empty);
            column.type_name = declaration(shape).into();
            let block = block_from(column);
            let result = $encode(&block);
            assert_schema_source(&block, shape, $empty);
            assert!(
                result.is_err(),
                "accepted mismatched {shape:?}, empty={}",
                $empty
            );
        }
    };
}

schema_rejection!(native_array_schema, native_bytes, Shape::Array, false);
schema_rejection!(native_empty_array_schema, native_bytes, Shape::Array, true);
schema_rejection!(native_nullable_schema, native_bytes, Shape::Nullable, false);
schema_rejection!(
    native_empty_nullable_schema,
    native_bytes,
    Shape::Nullable,
    true
);
schema_rejection!(native_map_key_schema, native_bytes, Shape::MapKey, false);
schema_rejection!(
    native_empty_map_key_schema,
    native_bytes,
    Shape::MapKey,
    true
);
schema_rejection!(
    native_map_value_schema,
    native_bytes,
    Shape::MapValue,
    false
);
schema_rejection!(
    native_empty_map_value_schema,
    native_bytes,
    Shape::MapValue,
    true
);
schema_rejection!(
    rowbinary_array_schema,
    encode_rowbinary,
    Shape::Array,
    false
);
schema_rejection!(
    rowbinary_empty_array_schema,
    encode_rowbinary,
    Shape::Array,
    true
);
schema_rejection!(
    rowbinary_nullable_schema,
    encode_rowbinary,
    Shape::Nullable,
    false
);
schema_rejection!(
    rowbinary_empty_nullable_schema,
    encode_rowbinary,
    Shape::Nullable,
    true
);
schema_rejection!(
    rowbinary_map_key_schema,
    encode_rowbinary,
    Shape::MapKey,
    false
);
schema_rejection!(
    rowbinary_empty_map_key_schema,
    encode_rowbinary,
    Shape::MapKey,
    true
);
schema_rejection!(
    rowbinary_map_value_schema,
    encode_rowbinary,
    Shape::MapValue,
    false
);
schema_rejection!(
    rowbinary_empty_map_value_schema,
    encode_rowbinary,
    Shape::MapValue,
    true
);

#[test]
fn materialization_rejects_bad_flags() {
    let buffer = NullableBuffer::<u64> {
        null_map: vec![0, 2],
        values: vec![7, 9],
    };
    let result = <Option<u64> as Field>::column(&buffer, "n");
    assert_eq!(buffer.null_map, [0, 2]);
    assert_eq!(buffer.values, [7, 9]);
    assert!(result.is_err(), "materialized an invalid null flag");
}

#[cfg(feature = "tcp")]
#[test]
fn native_rejects_bad_null_flags() {
    let mut column = column_for(Shape::Nullable, false);
    let ColumnData::Nullable { null_map, .. } = &mut column.data else {
        panic!("nullable fixture");
    };
    null_map[1] = 2;
    let block = block_from(column);
    let result = block.to_native();
    assert_eq!(block.row_count(), 2);
    let ColumnData::Nullable { null_map, values } = &block.column(0).unwrap().data else {
        panic!("nullable storage changed");
    };
    assert_eq!(null_map, &[0, 2]);
    assert_eq!(values.slice::<u64>().unwrap(), &[7, 9]);
    assert!(
        result.is_err(),
        "Native conversion normalized an invalid flag"
    );
}

#[cfg(feature = "tcp")]
#[test]
fn native_rejects_orphan_array_values() {
    let mut column = column_for(Shape::Array, false);
    let ColumnData::Array { offsets, .. } = &mut column.data else {
        panic!("array fixture");
    };
    offsets[0] = 1;
    let block = block_from(column);
    let result = block.to_native();
    assert_eq!(block.row_count(), 1);
    let ColumnData::Array { offsets, values } = &block.column(0).unwrap().data else {
        panic!("array storage changed");
    };
    assert_eq!(offsets, &[1]);
    assert_eq!(values.slice::<u64>().unwrap(), &[7, 9]);
    assert!(
        result.is_err(),
        "Native conversion discarded an orphan value"
    );
}

#[cfg(feature = "tcp")]
#[test]
fn native_rejects_orphan_map_values() {
    let mut column = column_for(Shape::MapValue, false);
    let ColumnData::Map { offsets, .. } = &mut column.data else {
        panic!("map fixture");
    };
    offsets[0] = 1;
    let block = block_from(column);
    let result = block.to_native();
    assert_eq!(block.row_count(), 1);
    let ColumnData::Map {
        offsets,
        keys,
        values,
    } = &block.column(0).unwrap().data
    else {
        panic!("map storage changed");
    };
    assert_eq!(offsets, &[1]);
    assert_eq!(keys.slice::<u64>().unwrap(), &[7, 9]);
    assert_eq!(values.slice::<u64>().unwrap(), &[11, 13]);
    assert!(
        result.is_err(),
        "Native conversion discarded an orphan entry"
    );
}

fn fixed_block(type_name: &'static str, width: usize) -> Block {
    let mut block = Block::new();
    match width {
        1 => block.append_fixed_slice("value", type_name, &[1u8; 256]),
        2 => block.append_fixed_slice("value", type_name, &[1u16; 128]),
        8 => block.append_fixed_slice("value", type_name, &[1u64; 32]),
        _ => panic!("unsupported fixture width"),
    }
    .expect("valid declared fixed width");
    block
}

fn fixed_payload(block: &Block) -> &[u8] {
    let ColumnData::Fixed(bytes) = &block.column(0).unwrap().data else {
        panic!("fixed fixture");
    };
    bytes.as_bytes()
}

fn check_retention(type_name: &'static str, width: usize) {
    let source = fixed_block(type_name, width);
    let mut destination = fixed_block(type_name, width);
    assert_eq!(source.row_count(), 256 / width);
    assert_eq!(fixed_payload(&source).len(), 256);
    assert_eq!(fixed_payload(&destination), fixed_payload(&source));
    let policy = BufferPolicy {
        max_retained_rows: 16,
        max_retained_bytes: usize::MAX,
    };
    destination.clear_with_policy(policy);
    let retained = destination.column(0).unwrap().allocated_bytes();
    assert_eq!(destination.row_count(), 0);
    assert_eq!(destination.column(0).unwrap().len(), 0);
    assert!(fixed_payload(&destination).is_empty());
    assert_eq!(retained % 64, 0);
    destination.refill_from(&source).unwrap();
    assert_eq!(destination.row_count(), source.row_count());
    assert_eq!(fixed_payload(&destination), fixed_payload(&source));
    assert_eq!(fixed_payload(&source).len(), 256);
    assert!(
        retained / width <= policy.max_retained_rows,
        "{type_name} retained {} actual rows ({retained} bytes, width {width})",
        retained / width
    );
}

macro_rules! retention_case {
    ($name:ident, $type_name:literal, $width:literal) => {
        #[test]
        fn $name() {
            check_retention($type_name, $width);
        }
    };
}

retention_case!(retention_datetime64_rows, "DateTime64(3)", 8);
retention_case!(retention_date_rows, "Date", 2);
retention_case!(retention_enum8_rows, "Enum8('a' = 1)", 1);
retention_case!(retention_fixedstring_rows, "FixedString(1)", 1);
retention_case!(retention_spaced_uint64_rows, " UInt64 ", 8);
