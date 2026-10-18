//! Edge coverage for `SerdeBridge`: nullable, array, byte, and rejected shapes.
//!
//! Each test names one shape so a failure identifies the unsupported case.
//! These tests exercise only the public `SerdeBridge` and `Block` API. Map and
//! logical-type coverage lives in `serde_map_logical_tests.rs`.

use clickhouse::{Block, Error, SerdeBridge};

fn is_serde_error<T>(result: clickhouse::Result<T>) -> bool {
    matches!(result, Err(Error::Serde { .. }))
}

fn type_name(block: &Block, name: &str) -> String {
    block
        .column_by_name(name)
        .expect("column")
        .type_name
        .to_string()
}

// ---------------------------------------------------------------------------
// Nullable / Option
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq, serde::Serialize, serde::Deserialize)]
struct OptionStringRow {
    value: Option<String>,
}

#[test]
fn option_string_first_none_then_some_roundtrips() {
    let rows = vec![
        OptionStringRow { value: None },
        OptionStringRow {
            value: Some(String::from("later")),
        },
        OptionStringRow {
            value: Some(String::new()),
        },
    ];

    let (_, bytes) = SerdeBridge::to_rowbinary(&rows).expect("encode rows");
    let decoded: Vec<OptionStringRow> = SerdeBridge::from_rowbinary(&bytes).expect("decode rows");
    assert_eq!(decoded, rows);

    let block = SerdeBridge::rows_to_block(&rows).expect("build block");
    assert_eq!(type_name(&block, "value"), "Nullable(String)");
    let decoded: Vec<OptionStringRow> = SerdeBridge::block_to_rows(&block).expect("read block");
    assert_eq!(decoded, rows);
}

#[derive(Debug, PartialEq, serde::Serialize, serde::Deserialize)]
struct OptionIntRow {
    value: Option<i64>,
}

#[test]
fn all_none_option_falls_back_to_nullable_string_and_still_decodes() {
    let rows = vec![OptionIntRow { value: None }, OptionIntRow { value: None }];

    let block = SerdeBridge::rows_to_block(&rows).expect("build block");
    assert_eq!(type_name(&block, "value"), "Nullable(String)");
    let decoded: Vec<OptionIntRow> = SerdeBridge::block_to_rows(&block).expect("read block");
    assert_eq!(decoded, rows);

    let (_, bytes) = SerdeBridge::to_rowbinary(&rows).expect("encode rows");
    let decoded: Vec<OptionIntRow> = SerdeBridge::from_rowbinary(&bytes).expect("decode rows");
    assert_eq!(decoded, rows);
}

#[test]
fn option_option_roundtrips_in_rowbinary_but_is_rejected_by_block_columns() {
    #[derive(Debug, PartialEq, serde::Serialize, serde::Deserialize)]
    struct DoubleOption {
        value: Option<Option<i64>>,
    }

    let rows = vec![
        DoubleOption {
            value: Some(Some(7)),
        },
        DoubleOption { value: Some(None) },
        DoubleOption { value: None },
    ];

    // RowBinary is positional, so nested nullable tags round-trip.
    let (_, bytes) = SerdeBridge::to_rowbinary(&rows).expect("encode rows");
    let decoded: Vec<DoubleOption> = SerdeBridge::from_rowbinary(&bytes).expect("decode rows");
    assert_eq!(decoded, rows);

    // ClickHouse has no Nullable(Nullable(T)); the block writer refuses it.
    assert!(is_serde_error(SerdeBridge::rows_to_block(&rows)));
}

// ---------------------------------------------------------------------------
// Array / Vec
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq, serde::Serialize, serde::Deserialize)]
struct VecIntRow {
    values: Vec<i64>,
}

#[test]
fn all_empty_vec_falls_back_to_array_string_and_still_decodes() {
    let rows = vec![
        VecIntRow { values: Vec::new() },
        VecIntRow { values: Vec::new() },
    ];

    let block = SerdeBridge::rows_to_block(&rows).expect("build block");
    assert_eq!(type_name(&block, "values"), "Array(String)");
    let decoded: Vec<VecIntRow> = SerdeBridge::block_to_rows(&block).expect("read block");
    assert_eq!(decoded, rows);
}

#[derive(Debug, PartialEq, serde::Serialize, serde::Deserialize)]
struct NestedOptionRow {
    outer: Option<Vec<u32>>,
    inner: Vec<Option<i64>>,
}

#[test]
fn option_vec_and_vec_option_roundtrip() {
    let rows = vec![
        NestedOptionRow {
            outer: Some(vec![1, 2]),
            inner: vec![Some(1), None],
        },
        NestedOptionRow {
            outer: None,
            inner: Vec::new(),
        },
        NestedOptionRow {
            outer: Some(Vec::new()),
            inner: vec![None],
        },
    ];

    let (_, bytes) = SerdeBridge::to_rowbinary(&rows).expect("encode rows");
    let decoded: Vec<NestedOptionRow> = SerdeBridge::from_rowbinary(&bytes).expect("decode rows");
    assert_eq!(decoded, rows);

    let block = SerdeBridge::rows_to_block(&rows).expect("build block");
    assert_eq!(type_name(&block, "outer"), "Nullable(Array(UInt32))");
    assert_eq!(type_name(&block, "inner"), "Array(Nullable(Int64))");
    let decoded: Vec<NestedOptionRow> = SerdeBridge::block_to_rows(&block).expect("read block");
    assert_eq!(decoded, rows);

    // Negative control: the element width is checked, not just the wrapper.
    #[derive(Debug, serde::Deserialize)]
    struct WrongElement {
        #[allow(dead_code)]
        inner: Vec<Option<u32>>,
    }
    assert!(is_serde_error(SerdeBridge::block_to_rows::<WrongElement>(
        &block
    )));
}

// ---------------------------------------------------------------------------
// Bytes and binary strings
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq, serde::Serialize, serde::Deserialize)]
struct PlainByteRow {
    data: Vec<u8>,
}

#[derive(Debug, PartialEq, serde::Serialize, serde::Deserialize)]
struct BinaryByteRow {
    #[serde(with = "serde_bytes")]
    data: Vec<u8>,
}

#[test]
fn vec_u8_is_array_uint8_while_serde_bytes_is_string() {
    // Non-UTF-8 bytes must survive both representations.
    let raw = vec![0x00_u8, 0xff, 0x80];
    let plain = vec![PlainByteRow { data: raw.clone() }];
    let binary = vec![BinaryByteRow { data: raw }];

    let plain_block = SerdeBridge::rows_to_block(&plain).expect("plain block");
    let binary_block = SerdeBridge::rows_to_block(&binary).expect("binary block");
    assert_eq!(type_name(&plain_block, "data"), "Array(UInt8)");
    assert_eq!(type_name(&binary_block, "data"), "String");

    let decoded: Vec<PlainByteRow> =
        SerdeBridge::block_to_rows(&plain_block).expect("read plain block");
    assert_eq!(decoded, plain);
    let decoded: Vec<BinaryByteRow> =
        SerdeBridge::block_to_rows(&binary_block).expect("read binary block");
    assert_eq!(decoded, binary);

    // RowBinary stores a sequence of u8 exactly like a byte string, so the two
    // shapes are wire-compatible even though their column types differ.
    let (plain_names, plain_bytes) = SerdeBridge::to_rowbinary(&plain).expect("plain rows");
    let (binary_names, binary_bytes) = SerdeBridge::to_rowbinary(&binary).expect("binary rows");
    assert_eq!(plain_names, binary_names);
    assert_eq!(plain_bytes, binary_bytes);
    let decoded: Vec<BinaryByteRow> =
        SerdeBridge::from_rowbinary(&plain_bytes).expect("cross decode bytes");
    assert_eq!(decoded, binary);
    let decoded: Vec<PlainByteRow> =
        SerdeBridge::from_rowbinary(&binary_bytes).expect("cross decode seq");
    assert_eq!(decoded, plain);
}

#[test]
fn fixed_byte_arrays_require_serde_bytes() {
    #[derive(Debug, PartialEq, serde::Serialize, serde::Deserialize)]
    struct WithSerdeBytes {
        #[serde(with = "serde_bytes")]
        mac: [u8; 6],
    }

    #[derive(serde::Serialize)]
    struct WithoutSerdeBytes {
        #[allow(dead_code)]
        mac: [u8; 6],
    }

    let rows = vec![WithSerdeBytes {
        mac: [0x00, 0xff, 0x80, 0x01, 0x02, 0x03],
    }];
    let block = SerdeBridge::rows_to_block(&rows).expect("build block");
    assert_eq!(type_name(&block, "mac"), "String");
    let decoded: Vec<WithSerdeBytes> = SerdeBridge::block_to_rows(&block).expect("read block");
    assert_eq!(decoded, rows);

    // Without serde_bytes, serde serializes an array as a tuple.
    let plain = vec![WithoutSerdeBytes { mac: [0; 6] }];
    assert!(is_serde_error(SerdeBridge::to_rowbinary(&plain)));
    assert!(is_serde_error(SerdeBridge::rows_to_block(&plain)));
}

#[test]
fn invalid_utf8_is_rejected_for_strings_and_accepted_for_bytes() {
    #[derive(Debug, PartialEq, serde::Deserialize)]
    struct TextRow {
        text: String,
    }

    #[derive(Debug, PartialEq, serde::Deserialize)]
    struct RawRow {
        // The block column is named `text`; RowBinary ignores names.
        #[serde(rename = "text", with = "serde_bytes")]
        raw: Vec<u8>,
    }

    // Positive control: the same prefix with valid UTF-8 decodes.
    let text: Vec<TextRow> = SerdeBridge::from_rowbinary(&[1, b'a']).expect("valid UTF-8");
    assert_eq!(
        text,
        [TextRow {
            text: String::from("a")
        }]
    );
    assert!(is_serde_error(SerdeBridge::from_rowbinary::<TextRow>(&[
        1, 0xff
    ])));

    let mut invalid_block = Block::new();
    invalid_block
        .append_strings_flat("text", &[1], &[0xff])
        .expect("string column");
    assert!(is_serde_error(SerdeBridge::block_to_rows::<TextRow>(
        &invalid_block
    )));

    let mut valid_block = Block::new();
    valid_block
        .append_strings_flat("text", &[1], b"a")
        .expect("string column");
    let text: Vec<TextRow> = SerdeBridge::block_to_rows(&valid_block).expect("valid UTF-8");
    assert_eq!(
        text,
        [TextRow {
            text: String::from("a")
        }]
    );

    // The byte-string path keeps non-UTF-8 payloads.
    let raw: Vec<RawRow> = SerdeBridge::from_rowbinary(&[1, 0xff]).expect("raw bytes");
    assert_eq!(raw, [RawRow { raw: vec![0xff] }]);
    let raw: Vec<RawRow> = SerdeBridge::block_to_rows(&invalid_block).expect("raw block bytes");
    assert_eq!(raw, [RawRow { raw: vec![0xff] }]);
}

// ---------------------------------------------------------------------------
// Rejected shapes
// ---------------------------------------------------------------------------

#[test]
fn nested_struct_tuple_and_unit_are_rejected_on_both_paths() {
    #[derive(serde::Serialize)]
    struct Nested {
        #[allow(dead_code)]
        inner: u32,
    }

    #[derive(serde::Serialize)]
    struct WithNested {
        #[allow(dead_code)]
        nested: Nested,
    }

    #[derive(serde::Serialize)]
    struct WithTuple {
        #[allow(dead_code)]
        pair: (u32, u32),
    }

    #[derive(serde::Serialize)]
    struct WithUnit {
        #[allow(dead_code)]
        marker: (),
    }

    let nested = vec![WithNested {
        nested: Nested { inner: 1 },
    }];
    assert!(is_serde_error(SerdeBridge::to_rowbinary(&nested)));
    assert!(is_serde_error(SerdeBridge::rows_to_block(&nested)));

    let tuple = vec![WithTuple { pair: (1, 2) }];
    assert!(is_serde_error(SerdeBridge::to_rowbinary(&tuple)));
    assert!(is_serde_error(SerdeBridge::rows_to_block(&tuple)));

    let unit = vec![WithUnit { marker: () }];
    assert!(is_serde_error(SerdeBridge::to_rowbinary(&unit)));
    assert!(is_serde_error(SerdeBridge::rows_to_block(&unit)));
}

#[test]
fn rows_with_changing_column_types_are_rejected() {
    #[derive(serde::Serialize)]
    #[serde(untagged)]
    enum NumericShape {
        Wide { value: u64 },
        Narrow { value: i64 },
    }

    let rows = vec![
        NumericShape::Wide { value: 1 },
        NumericShape::Narrow { value: 2 },
    ];
    // The block writer infers a column type from the first row and refuses a
    // later change; RowBinary is positional and has no such check.
    assert!(is_serde_error(SerdeBridge::rows_to_block(&rows)));
    assert!(SerdeBridge::to_rowbinary(&rows).is_ok());
}

#[test]
fn vec_option_blocks_are_rejected_by_the_native_edge() {
    #[derive(serde::Serialize)]
    struct NestedNullable {
        values: Vec<Option<i64>>,
    }

    let block = SerdeBridge::rows_to_block(&[NestedNullable {
        values: vec![Some(1), None],
    }])
    .expect("build block");
    assert_eq!(type_name(&block, "values"), "Array(Nullable(Int64))");

    // The bridge read path supports this shape, but the native transport
    // cannot copy a Nullable nested column yet. This pins the limitation so a
    // future transport fix updates this test instead of changing behavior
    // silently.
    assert!(block.to_native().is_err());
}

// ---------------------------------------------------------------------------
// Names, newtypes, ordering, and empty input
// ---------------------------------------------------------------------------

#[test]
fn serde_attributes_drive_names_and_binary_storage() {
    #[derive(Debug, PartialEq, serde::Serialize, serde::Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct RenamedRow {
        event_id: u64,
        #[serde(rename = "explicit")]
        label: String,
        #[serde(with = "serde_bytes")]
        payload: Vec<u8>,
    }

    let rows = vec![RenamedRow {
        event_id: 7,
        label: String::from("alpha"),
        payload: vec![0x00, 0xff],
    }];

    let (names, bytes) = SerdeBridge::to_rowbinary(&rows).expect("encode rows");
    assert_eq!(
        names.iter().map(AsRef::as_ref).collect::<Vec<_>>(),
        ["eventId", "explicit", "payload"]
    );

    let block = SerdeBridge::rows_to_block(&rows).expect("build block");
    assert_eq!(block.column(0).expect("column").name, "eventId");
    assert_eq!(block.column(1).expect("column").name, "explicit");
    assert_eq!(block.column(2).expect("column").name, "payload");
    assert_eq!(type_name(&block, "payload"), "String");

    let decoded: Vec<RenamedRow> = SerdeBridge::from_rowbinary(&bytes).expect("decode rows");
    assert_eq!(decoded, rows);
    let decoded: Vec<RenamedRow> = SerdeBridge::block_to_rows(&block).expect("read block");
    assert_eq!(decoded, rows);
}

#[test]
fn newtype_fields_are_transparent() {
    #[derive(Debug, PartialEq, serde::Serialize, serde::Deserialize)]
    struct Meters(u64);

    #[derive(Debug, PartialEq, serde::Serialize, serde::Deserialize)]
    struct NewtypeRow {
        distance: Meters,
    }

    let rows = vec![NewtypeRow {
        distance: Meters(1_500),
    }];
    let (_, bytes) = SerdeBridge::to_rowbinary(&rows).expect("encode rows");
    let decoded: Vec<NewtypeRow> = SerdeBridge::from_rowbinary(&bytes).expect("decode rows");
    assert_eq!(decoded, rows);

    let block = SerdeBridge::rows_to_block(&rows).expect("build block");
    assert_eq!(type_name(&block, "distance"), "UInt64");
    let decoded: Vec<NewtypeRow> = SerdeBridge::block_to_rows(&block).expect("read block");
    assert_eq!(decoded, rows);
}

#[test]
fn block_read_matches_by_name_in_any_order_and_ignores_extra_columns() {
    #[derive(Debug, PartialEq, serde::Deserialize)]
    struct Ordered {
        id: u64,
        symbol: String,
    }

    let mut block = Block::new();
    block
        .append_fixed_slice("extra", "UInt8", &[9_u8, 10])
        .expect("extra column");
    block
        .append_strings_flat("symbol", &[1, 2], b"ab")
        .expect("symbol column");
    block
        .append_fixed_slice("id", "UInt64", &[7_u64, 8])
        .expect("id column");

    let rows: Vec<Ordered> = SerdeBridge::block_to_rows(&block).expect("read block");
    assert_eq!(
        rows,
        [
            Ordered {
                id: 7,
                symbol: String::from("a"),
            },
            Ordered {
                id: 8,
                symbol: String::from("b"),
            },
        ]
    );
}

#[test]
fn empty_inputs_produce_empty_rows_and_blocks() {
    let block = SerdeBridge::rows_to_block::<OptionStringRow>(&[]).expect("empty block");
    assert_eq!(block.column_count(), 0);
    assert_eq!(block.row_count(), 0);

    let rows: Vec<OptionStringRow> =
        SerdeBridge::block_to_rows(&Block::new()).expect("read empty block");
    assert!(rows.is_empty());

    let (names, bytes) = SerdeBridge::to_rowbinary::<OptionStringRow>(&[]).expect("empty encode");
    assert!(names.is_empty());
    assert!(bytes.is_empty());
    let rows: Vec<OptionStringRow> =
        SerdeBridge::from_rowbinary(&bytes).expect("read empty buffer");
    assert!(rows.is_empty());

    // A schema-only block (columns present, zero rows) has no rows to decode.
    #[derive(Debug, serde::Deserialize)]
    struct SchemaOnly {
        #[allow(dead_code)]
        value: u64,
    }

    let mut block = Block::new();
    block
        .append_fixed_slice("value", "UInt64", &[] as &[u64])
        .expect("empty column");
    let rows: Vec<SchemaOnly> = SerdeBridge::block_to_rows(&block).expect("read empty column");
    assert!(rows.is_empty());
}

// ---------------------------------------------------------------------------
// Numeric boundaries and length prefixes
// ---------------------------------------------------------------------------

#[test]
fn numeric_boundaries_and_float_bits_roundtrip() {
    #[derive(Debug, serde::Serialize, serde::Deserialize)]
    struct Numbers {
        signed: i64,
        unsigned: u64,
        signed128: i128,
        unsigned128: u128,
        float: f64,
        float32: f32,
        flag: bool,
    }

    let rows = vec![Numbers {
        signed: i64::MIN,
        unsigned: u64::MAX,
        signed128: i128::MIN,
        unsigned128: u128::MAX,
        float: f64::from_bits(0x7ff8_0000_0000_0001),
        float32: f32::from_bits(0x7fc0_0001),
        flag: true,
    }];

    let (_, bytes) = SerdeBridge::to_rowbinary(&rows).expect("encode rows");
    let decoded: Vec<Numbers> = SerdeBridge::from_rowbinary(&bytes).expect("decode rows");
    assert_eq!(decoded[0].signed, i64::MIN);
    assert_eq!(decoded[0].unsigned, u64::MAX);
    assert_eq!(decoded[0].signed128, i128::MIN);
    assert_eq!(decoded[0].unsigned128, u128::MAX);
    assert_eq!(decoded[0].float.to_bits(), 0x7ff8_0000_0000_0001);
    assert_eq!(decoded[0].float32.to_bits(), 0x7fc0_0001);
    assert!(decoded[0].flag);

    let block = SerdeBridge::rows_to_block(&rows).expect("build block");
    let decoded: Vec<Numbers> = SerdeBridge::block_to_rows(&block).expect("read block");
    assert_eq!(decoded[0].signed, i64::MIN);
    assert_eq!(decoded[0].unsigned, u64::MAX);
    assert_eq!(decoded[0].signed128, i128::MIN);
    assert_eq!(decoded[0].unsigned128, u128::MAX);
    assert_eq!(decoded[0].float.to_bits(), 0x7ff8_0000_0000_0001);
    assert_eq!(decoded[0].float32.to_bits(), 0x7fc0_0001);
    assert!(decoded[0].flag);
}

#[test]
fn lengths_above_127_use_multibyte_varints() {
    #[derive(Debug, PartialEq, serde::Serialize, serde::Deserialize)]
    struct LongRow {
        text: String,
        items: Vec<u16>,
    }

    let rows = vec![LongRow {
        text: "x".repeat(200),
        items: (0..200).collect(),
    }];

    let (_, bytes) = SerdeBridge::to_rowbinary(&rows).expect("encode rows");
    // 200 = 0x48, encoded as LEB128 0xc8 0x01.
    assert_eq!(&bytes[..2], &[0xc8, 0x01]);
    let decoded: Vec<LongRow> = SerdeBridge::from_rowbinary(&bytes).expect("decode rows");
    assert_eq!(decoded, rows);

    let block = SerdeBridge::rows_to_block(&rows).expect("build block");
    assert_eq!(type_name(&block, "text"), "String");
    assert_eq!(type_name(&block, "items"), "Array(UInt16)");
    assert_eq!(block.column_bytes(0, 0).expect("text bytes").len(), 200);
    let decoded: Vec<LongRow> = SerdeBridge::block_to_rows(&block).expect("read block");
    assert_eq!(decoded, rows);
}

#[test]
fn truncated_and_overflowing_length_prefixes_are_rejected() {
    #[derive(Debug, serde::Deserialize)]
    struct TextRow {
        #[allow(dead_code)]
        text: String,
    }

    // Continuation bit set on the last available byte.
    assert!(is_serde_error(SerdeBridge::from_rowbinary::<TextRow>(&[
        0x80
    ])));
    // Ten continuation bytes push the shift past u64.
    assert!(is_serde_error(SerdeBridge::from_rowbinary::<TextRow>(&[
        0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80
    ])));
    // A declared length longer than the remaining input.
    assert!(is_serde_error(SerdeBridge::from_rowbinary::<TextRow>(&[
        0x05, b'a'
    ])));
}
