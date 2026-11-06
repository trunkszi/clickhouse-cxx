//! Standard serde attributes drive column names and binary string storage.
use clickhouse::Row;
use serde::{Deserialize, Serialize};

#[derive(Row, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SerdeEvent {
    event_id: u64,
    #[serde(rename = "explicit")]
    user_label: String,
    #[serde(with = "serde_bytes")]
    payload: Vec<u8>,
}

/// Container and field renames produce the same columns as `#[row(...)]`.
#[test]
fn serde_attributes_drive_schema_and_binary_strings() {
    assert_eq!(
        SerdeEvent::COLUMN_NAMES,
        &["eventId", "explicit", "payload"][..]
    );
    let mut batch = SerdeEventBatch::with_capacity(2);
    batch.push(&SerdeEvent {
        event_id: 7,
        user_label: String::from("alpha"),
        payload: vec![0, 0xff, 0x80],
    });
    batch.push_ref(SerdeEventRef {
        event_id: 8,
        user_label: "beta",
        payload: &[1, 2],
    });
    let block = batch.to_block().unwrap();
    let payload = block.column_by_name("payload").unwrap();
    assert_eq!(payload.type_name.as_ref(), "String");
    assert!(matches!(
        &payload.data,
        clickhouse::column::ColumnData::Strings { .. }
    ));
    let payload_idx = block.column_index("payload").unwrap();
    assert_eq!(
        block.column_bytes(payload_idx, 0).unwrap(),
        &[0, 0xff, 0x80]
    );
    assert_eq!(block.column_bytes(payload_idx, 1).unwrap(), &[1, 2]);
    let view = SerdeEventBlockView::try_from_block(&block).unwrap();
    let first = view.get(0).unwrap();
    assert_eq!(first.event_id, 7);
    assert_eq!(first.user_label, "alpha");
    assert_eq!(first.payload, &[0, 0xff, 0x80]);
    assert_eq!(view.get(1).unwrap().payload, &[1, 2]);
    assert_eq!(SerdeEventBatch::from_block(&block).unwrap().len(), 2);
}

#[derive(Row, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[clickhouse(rename_all = "snake_case")]
struct OverrideEvent {
    event_id: u64,
    #[serde(rename = "serdeName")]
    #[clickhouse(rename = "clickhouse_name")]
    field_value: u32,
    #[serde(with = "serde_bytes")]
    #[clickhouse(bytes = false)]
    raw: Vec<u8>,
}

/// Explicit `#[clickhouse(...)]` values take precedence over serde values.
#[test]
fn clickhouse_attributes_override_serde() {
    assert_eq!(
        OverrideEvent::COLUMN_NAMES,
        &["event_id", "clickhouse_name", "raw"][..]
    );
    let mut batch = OverrideEventBatch::default();
    batch.push(&OverrideEvent {
        event_id: 1,
        field_value: 2,
        raw: vec![9],
    });
    let block = batch.to_block().unwrap();
    assert_eq!(
        block.column_by_name("raw").unwrap().type_name.as_ref(),
        "Array(UInt8)"
    );
}
