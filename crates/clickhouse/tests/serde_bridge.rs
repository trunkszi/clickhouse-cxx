//! Serde bridge roundtrips over RowBinary and owned block columns.

use clickhouse::{Block, Error, SerdeBridge, row::RowClient};

#[derive(Debug, PartialEq, serde::Serialize, serde::Deserialize)]
struct Record {
    id: u64,
    delta: i32,
    price: f64,
    active: bool,
    name: String,
    #[serde(with = "serde_bytes")]
    payload: Vec<u8>,
    note: Option<String>,
    tags: Vec<String>,
    scores: Vec<i64>,
    maybe: Option<i64>,
}

fn records() -> Vec<Record> {
    vec![
        Record {
            id: 1,
            delta: -5,
            price: 10.5,
            active: true,
            name: String::from("alpha"),
            payload: vec![0, 1, 255],
            note: Some(String::from("first")),
            tags: vec![String::from("a"), String::from("bb")],
            scores: vec![1, -2],
            maybe: Some(42),
        },
        Record {
            id: 2,
            delta: 7,
            price: -0.0,
            active: false,
            name: String::new(),
            payload: Vec::new(),
            note: None,
            tags: Vec::new(),
            scores: Vec::new(),
            maybe: None,
        },
    ]
}

#[test]
fn rowbinary_roundtrip_mixed_shape() {
    let rows = records();
    let (names, bytes) = SerdeBridge::to_rowbinary(&rows).expect("encode rows");
    let names: Vec<&str> = names.iter().map(|name| name.as_ref()).collect();
    assert_eq!(
        names,
        [
            "id", "delta", "price", "active", "name", "payload", "note", "tags", "scores", "maybe",
        ]
    );
    assert!(!bytes.is_empty());

    let decoded: Vec<Record> = SerdeBridge::from_rowbinary(&bytes).expect("decode rows");
    assert_eq!(decoded, rows);
}

#[test]
fn block_roundtrip_matches_rowbinary_shape() {
    let rows = records();
    let block = SerdeBridge::rows_to_block(&rows).expect("build block");
    assert_eq!(block.row_count(), rows.len());
    assert_eq!(block.column_count(), 10);

    let type_name = |name: &str| {
        let index = block.column_index(name).expect("column index");
        (*block.column(index).expect("column").type_name).to_owned()
    };
    assert_eq!(type_name("id"), "UInt64");
    assert_eq!(type_name("delta"), "Int32");
    assert_eq!(type_name("price"), "Float64");
    assert_eq!(type_name("active"), "Bool");
    assert_eq!(type_name("name"), "String");
    assert_eq!(type_name("payload"), "String");
    assert_eq!(type_name("note"), "Nullable(String)");
    assert_eq!(type_name("tags"), "Array(String)");
    assert_eq!(type_name("scores"), "Array(Int64)");
    assert_eq!(type_name("maybe"), "Nullable(Int64)");

    let decoded: Vec<Record> = SerdeBridge::block_to_rows(&block).expect("read block");
    assert_eq!(decoded, rows);
}

#[test]
fn block_infers_nullable_type_from_later_rows() {
    #[derive(Debug, PartialEq, serde::Serialize, serde::Deserialize)]
    struct Sparse {
        value: Option<i64>,
    }

    let rows = vec![Sparse { value: None }, Sparse { value: Some(9) }];
    let block = SerdeBridge::rows_to_block(&rows).expect("build block");
    let column = block
        .column(block.column_index("value").expect("column index"))
        .expect("column");
    assert_eq!(column.type_name, "Nullable(Int64)");
    let decoded: Vec<Sparse> = SerdeBridge::block_to_rows(&block).expect("read block");
    assert_eq!(decoded, rows);
}

#[test]
fn rowbinary_rejects_truncated_and_trailing_bytes() {
    let rows = records();
    let (_, bytes) = SerdeBridge::to_rowbinary(&rows).expect("encode rows");

    let truncated = SerdeBridge::from_rowbinary::<Record>(&bytes[..bytes.len() - 1]);
    assert!(matches!(truncated, Err(Error::Serde { .. })));

    let mut trailing = bytes;
    trailing.push(0x01);
    let extra = SerdeBridge::from_rowbinary::<Record>(&trailing);
    assert!(matches!(extra, Err(Error::Serde { .. })));
}

#[test]
fn unsupported_serde_shapes_are_rejected() {
    #[derive(serde::Serialize)]
    struct WithChar {
        value: char,
    }

    #[derive(serde::Serialize)]
    enum Choice {
        One,
    }

    #[derive(serde::Serialize)]
    struct WithEnum {
        choice: Choice,
    }

    let char_rows = vec![WithChar { value: 'x' }];
    assert!(matches!(
        SerdeBridge::rows_to_block(&char_rows),
        Err(Error::Serde { .. })
    ));
    assert!(matches!(
        SerdeBridge::to_rowbinary(&char_rows),
        Err(Error::Serde { .. })
    ));

    let enum_rows = vec![WithEnum {
        choice: Choice::One,
    }];
    assert!(matches!(
        SerdeBridge::rows_to_block(&enum_rows),
        Err(Error::Serde { .. })
    ));
    assert!(matches!(
        SerdeBridge::to_rowbinary(&enum_rows),
        Err(Error::Serde { .. })
    ));
}

#[test]
fn rows_must_agree_on_shape() {
    #[derive(serde::Serialize)]
    #[serde(untagged)]
    enum Shape {
        Full { a: u64, b: u64 },
        Partial { a: u64 },
    }

    let rows = vec![Shape::Full { a: 1, b: 2 }, Shape::Partial { a: 3 }];
    assert!(matches!(
        SerdeBridge::to_rowbinary(&rows),
        Err(Error::Serde { .. })
    ));
    assert!(matches!(
        SerdeBridge::rows_to_block(&rows),
        Err(Error::Serde { .. })
    ));

    // RowBinary encodes scalar rows, but block columns need named fields.
    let scalars = vec![7_u64, 8];
    let (names, encoded) = SerdeBridge::to_rowbinary(&scalars).expect("scalar rows");
    assert!(names.is_empty());
    let decoded: Vec<u64> = SerdeBridge::from_rowbinary(&encoded).expect("scalar decode");
    assert_eq!(decoded, scalars);
    assert!(matches!(
        SerdeBridge::rows_to_block(&scalars),
        Err(Error::Serde { .. })
    ));
}

#[test]
fn block_read_rejects_missing_and_incompatible_columns() {
    let mut missing = Block::new();
    missing
        .append_fixed_slice("id", "UInt64", &[1_u64])
        .expect("id column");
    missing
        .append_strings_flat("symbol", &[3], b"abc")
        .expect("symbol column");
    let error = SerdeBridge::block_to_rows::<Tiny>(&missing).expect_err("missing score");
    assert!(matches!(error, Error::Serde { .. }));

    let mut incompatible = Block::new();
    incompatible
        .append_strings_flat("id", &[3], b"abc")
        .expect("id column");
    incompatible
        .append_strings_flat("symbol", &[3], b"abc")
        .expect("symbol column");
    incompatible
        .append_fixed_slice("score", "Float64", &[1.0_f64])
        .expect("score column");
    let error = SerdeBridge::block_to_rows::<Tiny>(&incompatible).expect_err("wrong id type");
    assert!(matches!(error, Error::Serde { .. }));
}

#[derive(Debug, PartialEq, serde::Serialize, serde::Deserialize)]
struct Tiny {
    id: u64,
    symbol: String,
    score: f64,
}

#[derive(Default)]
struct MemoryClient {
    inserted: Vec<Tiny>,
}

impl RowClient for MemoryClient {
    fn insert_block(&mut self, _table: &str, block: &Block) -> Result<(), Error> {
        self.inserted
            .extend(SerdeBridge::block_to_rows::<Tiny>(block)?);
        Ok(())
    }

    fn query_blocks(&mut self, _sql: &str) -> Result<Vec<Block>, Error> {
        let mut first = Block::new();
        first
            .append_fixed_slice("id", "UInt64", &[7_u64])
            .expect("id column");
        first
            .append_strings_flat("symbol", &[3], b"abc")
            .expect("symbol column");
        first
            .append_fixed_slice("score", "Float64", &[1.25_f64])
            .expect("score column");

        let mut second = Block::new();
        second
            .append_fixed_slice("id", "UInt64", &[8_u64])
            .expect("id column");
        second
            .append_strings_flat("symbol", &[3], b"xyz")
            .expect("symbol column");
        second
            .append_fixed_slice("score", "Float64", &[2.5_f64])
            .expect("score column");
        Ok(vec![first, second])
    }
}

#[test]
fn row_client_serde_methods_use_the_block_primitives() {
    let rows = vec![
        Tiny {
            id: 1,
            symbol: String::from("one"),
            score: 0.5,
        },
        Tiny {
            id: 2,
            symbol: String::from("two"),
            score: 1.5,
        },
    ];

    let mut client = MemoryClient::default();
    client
        .insert_serde("events", &rows)
        .expect("insert serde rows");
    assert_eq!(client.inserted, rows);

    let queried: Vec<Tiny> = client.query_serde("SELECT * FROM events").expect("query");
    assert_eq!(
        queried,
        [
            Tiny {
                id: 7,
                symbol: String::from("abc"),
                score: 1.25,
            },
            Tiny {
                id: 8,
                symbol: String::from("xyz"),
                score: 2.5,
            },
        ]
    );

    client
        .insert_serde::<Tiny>("events", &[])
        .expect("empty insert");
    assert_eq!(client.inserted, rows);
}
