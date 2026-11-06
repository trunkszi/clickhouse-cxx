//! Integration coverage for derived fields that share runtime binding names.

use clickhouse::{Row, row::OwnedRow};

mod schema {
    use super::Row;

    #[derive(Debug, PartialEq, Row)]
    pub struct HygieneRow {
        pub block: u64,
        pub row: u32,
        pub len: String,
    }
}

use schema::{HygieneRow, HygieneRowBatch, HygieneRowBlockView, HygieneRowRef};

/// Runtime binding and method names remain valid row field names.
#[test]
fn integration_binding_names_roundtrip_through_batches_and_checked_views() {
    let expected = HygieneRow {
        block: 7,
        row: 42,
        len: String::from("first"),
    };
    let mut batch = HygieneRowBatch::with_capacity(2);
    batch.push(&expected);
    batch.push_ref(HygieneRowRef {
        block: 9,
        row: 13,
        len: "second",
    });
    let block = batch.to_block().expect("build block");
    let view = HygieneRowBlockView::try_from_block(&block).expect("project block");
    assert_eq!(view.len(), 2);
    assert_eq!(view.column_block(), &[7, 9]);
    assert_eq!(view.column_row(), &[42, 13]);
    assert_eq!(view.column_len().get(1), Some("second"));
    assert_eq!(
        HygieneRow::from_ref(view.get(0).expect("first row")),
        expected
    );
    let second = view.iter().nth(1).expect("second row");
    assert_eq!((second.block, second.row, second.len), (9, 13, "second"));
    assert!(view.get(2).is_none());
    let restored = HygieneRowBatch::from_block(&block).expect("restore batch");
    let restored_block = restored.to_block().expect("restore block");
    let restored_view =
        HygieneRowBlockView::try_from_block(&restored_block).expect("restored view");
    assert_eq!(
        restored_view
            .iter()
            .map(HygieneRow::from_ref)
            .collect::<Vec<_>>(),
        view.iter().map(HygieneRow::from_ref).collect::<Vec<_>>()
    );
}

#[derive(Row, serde::Serialize)]
struct MapRow {
    attributes: std::collections::BTreeMap<String, u64>,
}

/// Derived map projections accept equivalent whitespace in schema names.
#[test]
fn integration_map_projection_accepts_whitespace_in_declared_types() {
    let expected = std::collections::BTreeMap::from([(String::from("venue"), 7)]);
    let (_, rowbinary) = clickhouse::SerdeBridge::to_rowbinary(&[MapRow {
        attributes: expected.clone(),
    }])
    .expect("encode map row");
    let spelling = " Map ( String , UInt64 ) ";
    let mut encoded = vec![1, 10];
    encoded.extend_from_slice(b"attributes");
    encoded.push(u8::try_from(spelling.len()).unwrap());
    encoded.extend_from_slice(spelling.as_bytes());
    encoded.extend_from_slice(&rowbinary);
    let block = clickhouse::native_format::decode_rowbinary(&encoded).expect("decode spaced map");
    let view = MapRowBlockView::try_from_block(&block).expect("project spaced map");
    assert_eq!(
        MapRow::from_ref(view.get(0).expect("map row")).attributes,
        expected
    );
}
