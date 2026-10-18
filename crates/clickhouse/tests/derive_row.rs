//! Integration and lifecycle coverage for derived rows over real owned blocks.
use std::{borrow::Cow, collections::BTreeMap};

use clickhouse::{Block, Row};

#[derive(Row)]
#[row(table = "events", rename_all = "camelCase")]
struct Event<'input> {
    event_id: u64,
    symbol: Cow<'input, str>,
    payload: &'input [u8],
    prices: Vec<f64>,
    note: Option<Cow<'input, str>>,
    active: bool,
    attributes: BTreeMap<String, i64>,
}

/// Generated batches retain all column allocations across clear and refill.
#[test]
fn flat_batch_roundtrip_and_reuse() {
    let labels = BTreeMap::from([(String::from("venue"), 7)]);
    let row = Event {
        event_id: 42,
        symbol: Cow::Borrowed("BTC/USDT"),
        payload: &[0, 255, 7],
        prices: vec![1.0, -0.0, f64::from_bits(0x7ff8000000000001)],
        note: Some(Cow::Borrowed("a note")),
        active: true,
        attributes: labels,
    };
    let mut batch = EventBatch::with_capacity(8);
    batch.push(&row);
    batch.push_ref(EventRef {
        event_id: 43,
        symbol: "ETH",
        payload: &[],
        prices: &[3.0],
        note: None,
        active: false,
        attributes: clickhouse::row::MapRef::Empty,
    });
    assert_eq!(Event::TABLE_NAME, Some("events"));
    assert_eq!(Event::COLUMN_NAMES[0], "eventId");
    let block = batch.to_block().unwrap();
    let view = EventBlockView::try_from_block(&block).unwrap();
    assert_eq!(view.event_id, &[42, 43]);
    let first = view.get(0).unwrap();
    assert_eq!(first.symbol, "BTC/USDT");
    assert_eq!(first.payload, &[0, 255, 7]);
    assert_eq!(first.prices[1].to_bits(), (-0.0_f64).to_bits());
    assert_eq!(first.prices[2].to_bits(), 0x7ff8000000000001);
    assert_eq!(first.note, Some("a note"));
    assert!(first.active);
    assert_eq!(
        first.attributes.iter().collect::<Vec<_>>(),
        vec![("venue", 7)]
    );
    let second = view.get(1).unwrap();
    assert_eq!(second.note, None);
    assert!(!second.active);
    assert!(second.attributes.is_empty());
    assert_eq!(view.iter().count(), 2);
    assert!(view.get(usize::MAX).is_none());
    let restored = EventBatch::from_block(&block).unwrap();
    assert_eq!(restored.len(), 2);
    let capacity = (
        batch.event_id.capacity(),
        batch.symbol.bytes.capacity(),
        batch.symbol.offsets.capacity(),
        batch.prices.values.capacity(),
        batch.note.values.bytes.capacity(),
        batch.attributes.keys.bytes.capacity(),
    );
    let pointers = (
        batch.event_id.as_ptr(),
        batch.symbol.bytes.as_ptr(),
        batch.prices.values.as_ptr(),
    );
    batch.clear();
    assert!(batch.is_empty());
    batch.push(&row);
    assert_eq!(
        capacity,
        (
            batch.event_id.capacity(),
            batch.symbol.bytes.capacity(),
            batch.symbol.offsets.capacity(),
            batch.prices.values.capacity(),
            batch.note.values.bytes.capacity(),
            batch.attributes.keys.bytes.capacity()
        )
    );
    assert_eq!(
        pointers,
        (
            batch.event_id.as_ptr(),
            batch.symbol.bytes.as_ptr(),
            batch.prices.values.as_ptr()
        )
    );
}

/// Every fixed or variable row projection borrows the original column memory.
#[test]
fn projected_slices_share_block_storage() {
    let mut batch = EventBatch::default();
    batch.push_ref(EventRef {
        event_id: 9,
        symbol: "hello",
        payload: &[0xff],
        prices: &[2.5],
        note: Some("x"),
        active: true,
        attributes: clickhouse::row::MapRef::Empty,
    });
    let block = batch.to_block().unwrap();
    let view = EventBlockView::try_from_block(&block).unwrap();
    assert_eq!(
        view.event_id.as_ptr(),
        block
            .column_slice_by_name::<u64>("eventId")
            .unwrap()
            .as_ptr()
    );
    let row = view.get(0).unwrap();
    let name_idx = block.column_index("symbol").unwrap();
    assert_eq!(
        row.symbol.as_ptr(),
        block.column_str(name_idx, 0).unwrap().as_ptr()
    );
    assert_eq!(
        row.payload.as_ptr(),
        block
            .column_bytes(block.column_index("payload").unwrap(), 0)
            .unwrap()
            .as_ptr()
    );
    assert_eq!(row.prices.as_ptr(), view.prices.values.as_ptr());
    let mut iter = view.iter();
    assert_eq!(iter.len(), 1);
    assert!(iter.next().is_some());
    assert_eq!(iter.len(), 0);
    assert!(iter.next().is_none());
    assert!(iter.next().is_none());
}

/// Views reject incompatible types and invalid row shapes before exposing a row.
#[test]
fn schema_errors_are_explicit() {
    let mut block = Block::new();
    block
        .append_fixed_slice("eventId", "Int64", &[1_i64])
        .unwrap();
    assert!(EventBlockView::try_from_block(&block).is_err());
    let mut batch = EventBatch::default();
    batch.event_id.push(1);
    assert!(batch.to_block().is_err());
}

#[derive(Row)]
struct StaticBorrow {
    text: &'static str,
}
#[derive(Row)]
struct OwnedCow {
    text: Cow<'static, str>,
}
/// Static borrows remain borrowed schemas while owned Cow rows materialize safely.
#[test]
fn static_borrows_and_owned_cow() {
    let mut borrowed = StaticBorrowBatch::default();
    borrowed.push(&StaticBorrow { text: "static" });
    let block = borrowed.to_block().unwrap();
    assert_eq!(
        StaticBorrowBlockView::try_from_block(&block)
            .unwrap()
            .get(0)
            .unwrap()
            .text,
        "static"
    );
    let mut batch = OwnedCowBatch::default();
    batch.push(&OwnedCow {
        text: Cow::Borrowed("owned result"),
    });
    let block = batch.to_block().unwrap();
    let view = OwnedCowBlockView::try_from_block(&block).unwrap();
    let owned = <OwnedCow as clickhouse::row::OwnedRow>::from_ref(view.get(0).unwrap());
    assert!(matches!(owned.text, Cow::Owned(_)));
    assert_eq!(owned.text, "owned result");
}
