//! Map and logical-type coverage for `SerdeBridge`.
//!
//! Serde maps round-trip through RowBinary and owned blocks. Logical fixed
//! columns keep the little-endian integer layout of their underlying Rust
//! width, so a block read accepts `Date`, `DateTime`, `Decimal`, and `IPv4`
//! storage into that width and `UUID`/`IPv6` storage into a byte field.

use std::collections::{BTreeMap, HashMap};

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
// Maps
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
struct BTreeRow {
    attributes: BTreeMap<String, u64>,
}

#[test]
fn btree_map_roundtrips_through_rowbinary_and_blocks() {
    let rows = vec![
        BTreeRow {
            attributes: BTreeMap::from([
                (String::from("venue"), 7),
                (String::from("strategy"), 11),
            ]),
        },
        BTreeRow {
            attributes: BTreeMap::from([(String::from("venue"), 9)]),
        },
        BTreeRow {
            attributes: BTreeMap::new(),
        },
    ];

    let (_, bytes) = SerdeBridge::to_rowbinary(&rows).expect("encode rows");
    // RowBinary starts each map with its entry count as LEB128.
    assert_eq!(bytes[0], 2);
    let decoded: Vec<BTreeRow> = SerdeBridge::from_rowbinary(&bytes).expect("decode rows");
    assert_eq!(decoded, rows);

    let block = SerdeBridge::rows_to_block(&rows).expect("build block");
    assert_eq!(type_name(&block, "attributes"), "Map(String,UInt64)");
    let decoded: Vec<BTreeRow> = SerdeBridge::block_to_rows(&block).expect("read block");
    assert_eq!(decoded, rows);
}

#[derive(Debug, PartialEq, serde::Serialize, serde::Deserialize)]
struct HashRow {
    attributes: HashMap<String, i32>,
}

#[test]
fn hash_map_roundtrips_through_rowbinary_and_blocks() {
    let first = HashRow {
        attributes: HashMap::from([(String::from("alpha"), 1), (String::from("beta"), -2)]),
    };
    // The same entries in the opposite insertion order: map equality ignores
    // order, so both rows must decode back to equal maps.
    let second = HashRow {
        attributes: HashMap::from([(String::from("beta"), -2), (String::from("alpha"), 1)]),
    };
    let rows = vec![first, second];

    let (_, bytes) = SerdeBridge::to_rowbinary(&rows).expect("encode rows");
    let decoded: Vec<HashRow> = SerdeBridge::from_rowbinary(&bytes).expect("decode rows");
    assert_eq!(decoded, rows);

    let block = SerdeBridge::rows_to_block(&rows).expect("build block");
    assert_eq!(type_name(&block, "attributes"), "Map(String,Int32)");
    let decoded: Vec<HashRow> = SerdeBridge::block_to_rows(&block).expect("read block");
    assert_eq!(decoded, rows);
}

#[test]
fn empty_maps_require_a_type_for_blocks_and_still_decode_rowbinary() {
    let rows = vec![
        BTreeRow {
            attributes: BTreeMap::new(),
        },
        BTreeRow {
            attributes: BTreeMap::new(),
        },
    ];

    assert!(is_serde_error(SerdeBridge::rows_to_block(&rows)));

    let (_, bytes) = SerdeBridge::to_rowbinary(&rows).expect("encode rows");
    assert_eq!(bytes, [0, 0]);
    let decoded: Vec<BTreeRow> = SerdeBridge::from_rowbinary(&bytes).expect("decode rows");
    assert_eq!(decoded, rows);
}

#[test]
fn a_leading_empty_map_does_not_block_later_type_inference() {
    let rows = vec![
        BTreeRow {
            attributes: BTreeMap::new(),
        },
        BTreeRow {
            attributes: BTreeMap::from([(String::from("venue"), 7)]),
        },
    ];

    let block = SerdeBridge::rows_to_block(&rows).expect("build block");
    assert_eq!(type_name(&block, "attributes"), "Map(String,UInt64)");
    let decoded: Vec<BTreeRow> = SerdeBridge::block_to_rows(&block).expect("read block");
    assert_eq!(decoded, rows);
}

/// Serde map reads accept equivalent whitespace in schema names.
#[test]
fn map_columns_accept_whitespace_in_declared_type_names() {
    let rows = vec![BTreeRow {
        attributes: BTreeMap::from([(String::from("venue"), 7)]),
    }];
    let (_, rowbinary) = SerdeBridge::to_rowbinary(&rows).expect("encode map rows");
    for spelling in ["Map(String, UInt64)", " Map ( String , UInt64 ) "] {
        let mut encoded = vec![1, 10];
        encoded.extend_from_slice(b"attributes");
        encoded.push(u8::try_from(spelling.len()).unwrap());
        encoded.extend_from_slice(spelling.as_bytes());
        encoded.extend_from_slice(&rowbinary);
        let block =
            clickhouse::native_format::decode_rowbinary(&encoded).expect("decode spaced map");
        let decoded: Vec<BTreeRow> = SerdeBridge::block_to_rows(&block).expect("read spaced map");
        assert_eq!(decoded, rows);
        let mut visited = Vec::new();
        assert_eq!(
            SerdeBridge::block_to_rows_with(&block, |row: BTreeRow| {
                visited.push(row);
                Ok(())
            })
            .expect("visit spaced map"),
            rows.len()
        );
        assert_eq!(visited, rows);
        let mut appended: Vec<BTreeRow> = Vec::new();
        assert_eq!(
            SerdeBridge::block_to_rows_into(&block, &mut appended).expect("append spaced map"),
            rows.len()
        );
        assert_eq!(appended, rows);
        let mut filled = vec![
            BTreeRow {
                attributes: BTreeMap::new()
            };
            rows.len()
        ];
        assert_eq!(
            SerdeBridge::block_to_rows_into_slice(&block, &mut filled).expect("fill spaced map"),
            rows.len()
        );
        assert_eq!(filled, rows);
    }
}

#[test]
fn map_columns_reject_wrong_key_value_and_container_shapes() {
    #[derive(serde::Serialize)]
    struct Source {
        attributes: BTreeMap<String, i32>,
    }

    let block = SerdeBridge::rows_to_block(&[Source {
        attributes: BTreeMap::from([(String::from("venue"), 7)]),
    }])
    .expect("build block");
    assert_eq!(type_name(&block, "attributes"), "Map(String,Int32)");

    #[derive(serde::Deserialize)]
    struct WrongValue {
        #[allow(dead_code)]
        attributes: BTreeMap<String, u64>,
    }
    assert!(is_serde_error(SerdeBridge::block_to_rows::<WrongValue>(
        &block
    )));

    #[derive(serde::Deserialize)]
    struct WrongKey {
        #[allow(dead_code)]
        attributes: BTreeMap<u64, i32>,
    }
    assert!(is_serde_error(SerdeBridge::block_to_rows::<WrongKey>(
        &block
    )));

    #[derive(serde::Deserialize)]
    struct WrongContainer {
        #[allow(dead_code)]
        attributes: Vec<(String, i32)>,
    }
    assert!(is_serde_error(
        SerdeBridge::block_to_rows::<WrongContainer>(&block)
    ));
}

#[test]
fn truncated_rowbinary_maps_are_rejected() {
    let rows = vec![BTreeRow {
        attributes: BTreeMap::from([(String::from("venue"), 7)]),
    }];
    let (_, bytes) = SerdeBridge::to_rowbinary(&rows).expect("encode rows");
    assert!(is_serde_error(SerdeBridge::from_rowbinary::<BTreeRow>(
        &bytes[..bytes.len() - 1]
    )));

    // The entry count claims two pairs and the buffer holds none.
    assert!(is_serde_error(SerdeBridge::from_rowbinary::<BTreeRow>(&[
        2
    ])));
}

// ---------------------------------------------------------------------------
// Logical fixed types
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq, serde::Deserialize)]
struct LogicalRow {
    dated: u16,
    dated32: i32,
    stamp: u32,
    stamp64: i64,
    address: u32,
    amount32: i32,
    amount64: i64,
    amount128: i128,
    #[serde(with = "serde_bytes")]
    id: Vec<u8>,
    #[serde(with = "serde_bytes")]
    address6: Vec<u8>,
}

#[test]
fn logical_fixed_columns_read_into_their_underlying_rust_types() {
    let mut block = Block::new();
    block
        .append_fixed_slice("dated", "Date", &[20_000_u16])
        .expect("Date column");
    block
        .append_fixed_slice("dated32", "Date32", &[-20_000_i32])
        .expect("Date32 column");
    block
        .append_fixed_slice("stamp", "DateTime", &[1_700_000_000_u32])
        .expect("DateTime column");
    block
        .append_fixed_slice("stamp64", "DateTime64(3)", &[1_700_000_000_123_i64])
        .expect("DateTime64 column");
    block
        .append_fixed_slice("address", "IPv4", &[0x7f00_0001_u32])
        .expect("IPv4 column");
    block
        .append_fixed_slice("amount32", "Decimal32(2)", &[12_345_i32])
        .expect("Decimal32 column");
    block
        .append_fixed_slice("amount64", "Decimal64(4)", &[1_234_567_i64])
        .expect("Decimal64 column");
    block
        .append_fixed_slice("amount128", "Decimal128(8)", &[123_456_789_i128])
        .expect("Decimal128 column");
    block
        .append_fixed_slice("id", "UUID", &[[0x11_u8; 16]])
        .expect("UUID column");
    block
        .append_fixed_slice("address6", "IPv6", &[[0x20_u8; 16]])
        .expect("IPv6 column");

    // The columns keep their logical names instead of the integer width.
    assert_eq!(type_name(&block, "dated"), "Date");
    assert_eq!(type_name(&block, "stamp"), "DateTime");
    assert_eq!(type_name(&block, "stamp64"), "DateTime64(3)");
    assert_eq!(type_name(&block, "amount128"), "Decimal128(8)");

    let rows: Vec<LogicalRow> = SerdeBridge::block_to_rows(&block).expect("read block");
    assert_eq!(
        rows,
        [LogicalRow {
            dated: 20_000,
            dated32: -20_000,
            stamp: 1_700_000_000,
            stamp64: 1_700_000_000_123,
            address: 0x7f00_0001,
            amount32: 12_345,
            amount64: 1_234_567,
            amount128: 123_456_789,
            id: vec![0x11; 16],
            address6: vec![0x20; 16],
        }]
    );
}

#[derive(Debug, PartialEq, serde::Deserialize)]
struct NullableDateRow {
    dated: Option<u16>,
}

#[test]
fn nullable_date_reads_through_the_nullable_wrapper() {
    let mut block = Block::new();
    block
        .append_nullable_fixed("dated", "Date", &[20_000_u16, 0], &[0, 1])
        .expect("Nullable(Date) column");
    assert_eq!(type_name(&block, "dated"), "Nullable(Date)");

    let rows: Vec<NullableDateRow> = SerdeBridge::block_to_rows(&block).expect("read block");
    assert_eq!(
        rows,
        [
            NullableDateRow {
                dated: Some(20_000)
            },
            NullableDateRow { dated: None },
        ]
    );
}

#[test]
fn logical_columns_reject_wrong_widths_and_shapes() {
    #[derive(serde::Deserialize)]
    struct DateAsWide {
        #[allow(dead_code)]
        dated: u32,
    }
    let mut date = Block::new();
    date.append_fixed_slice("dated", "Date", &[20_000_u16])
        .expect("Date column");
    assert!(is_serde_error(SerdeBridge::block_to_rows::<DateAsWide>(
        &date
    )));

    #[derive(serde::Deserialize)]
    struct DateTimeAsNarrow {
        #[allow(dead_code)]
        stamp: u16,
    }
    let mut stamp = Block::new();
    stamp
        .append_fixed_slice("stamp", "DateTime", &[1_u32])
        .expect("DateTime column");
    assert!(is_serde_error(
        SerdeBridge::block_to_rows::<DateTimeAsNarrow>(&stamp)
    ));

    #[derive(serde::Deserialize)]
    struct Decimal64AsNarrow {
        #[allow(dead_code)]
        amount: i32,
    }
    let mut amount = Block::new();
    amount
        .append_fixed_slice("amount", "Decimal64(4)", &[7_i64])
        .expect("Decimal64 column");
    assert!(is_serde_error(SerdeBridge::block_to_rows::<
        Decimal64AsNarrow,
    >(&amount)));

    // A UUID is binary storage, not a UTF-8 string.
    #[derive(serde::Deserialize)]
    struct UuidAsText {
        #[allow(dead_code)]
        id: String,
    }
    let mut uuid = Block::new();
    uuid.append_fixed_slice("id", "UUID", &[[0x11_u8; 16]])
        .expect("UUID column");
    assert!(is_serde_error(SerdeBridge::block_to_rows::<UuidAsText>(
        &uuid
    )));

    // A UUID column rejects undersized Rust elements before storage is committed.
    let mut short_uuid = Block::new();
    assert!(matches!(
        short_uuid.append_fixed_slice("id", "UUID", &[0_u64]),
        Err(Error::SchemaMismatch { .. })
    ));
    assert_eq!(short_uuid.column_count(), 0);
}

#[test]
fn writing_scalars_does_not_infer_logical_types() {
    #[derive(serde::Serialize)]
    struct WriteStamp {
        #[allow(dead_code)]
        stamp: u32,
    }

    let block = SerdeBridge::rows_to_block(&[WriteStamp { stamp: 1 }]).expect("build block");
    assert_eq!(type_name(&block, "stamp"), "UInt32");
}

// ---------------------------------------------------------------------------
// Callback and caller-buffer reads
// ---------------------------------------------------------------------------

#[test]
fn map_rows_roundtrip_through_callback_and_caller_buffer_reads() {
    let rows = vec![
        BTreeRow {
            attributes: BTreeMap::from([(String::from("venue"), 7)]),
        },
        BTreeRow {
            attributes: BTreeMap::new(),
        },
    ];
    let (_, bytes) = SerdeBridge::to_rowbinary(&rows).expect("encode rows");
    let block = SerdeBridge::rows_to_block(&rows).expect("build block");

    let mut from_bytes = Vec::new();
    let count = SerdeBridge::from_rowbinary_with(&bytes, |row: BTreeRow| {
        from_bytes.push(row);
        Ok(())
    })
    .expect("callback rows");
    assert_eq!(count, rows.len());
    assert_eq!(from_bytes, rows);

    let mut appended: Vec<BTreeRow> = Vec::new();
    assert_eq!(
        SerdeBridge::block_to_rows_into(&block, &mut appended).expect("append block rows"),
        rows.len()
    );
    assert_eq!(appended, rows);

    let mut filled = vec![
        BTreeRow {
            attributes: BTreeMap::new(),
        };
        rows.len()
    ];
    assert_eq!(
        SerdeBridge::block_to_rows_into_slice(&block, &mut filled).expect("fill block slice"),
        rows.len()
    );
    assert_eq!(filled, rows);

    // The caller-buffer write path accepts an input that exactly fills it.
    let mut exact = vec![
        BTreeRow {
            attributes: BTreeMap::new(),
        };
        rows.len()
    ];
    assert_eq!(
        SerdeBridge::from_rowbinary_into_slice(&bytes, &mut exact).expect("fill RowBinary slice"),
        rows.len()
    );
    assert_eq!(exact, rows);
}
