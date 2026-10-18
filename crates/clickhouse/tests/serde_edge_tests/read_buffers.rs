//! Integration scenarios for ordered callbacks and caller-owned read buffers.

use super::{Error, SerdeBridge, is_serde_error};

#[derive(Debug, Default, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
struct FixedRow {
    id: u64,
    sequence: u32,
    price: f64,
}

fn fixed_rows() -> Vec<FixedRow> {
    vec![
        FixedRow {
            id: 10,
            sequence: 1,
            price: 1.5,
        },
        FixedRow {
            id: 20,
            sequence: 2,
            price: 2.5,
        },
        FixedRow {
            id: 30,
            sequence: 3,
            price: 3.5,
        },
    ]
}

#[test]
fn callback_reads_deliver_every_row_in_order() {
    let rows = fixed_rows();
    let (_, bytes) = SerdeBridge::to_rowbinary(&rows).expect("encode rows");
    let block = SerdeBridge::rows_to_block(&rows).expect("build block");

    let mut from_bytes = Vec::new();
    let count = SerdeBridge::from_rowbinary_with(&bytes, |row: FixedRow| {
        from_bytes.push(row);
        Ok(())
    })
    .expect("decode rows with callback");
    assert_eq!(count, rows.len());
    assert_eq!(from_bytes, rows);

    let mut from_block = Vec::new();
    let count = SerdeBridge::block_to_rows_with(&block, |row: FixedRow| {
        from_block.push(row);
        Ok(())
    })
    .expect("read block with callback");
    assert_eq!(count, rows.len());
    assert_eq!(from_block, rows);
}

#[test]
fn callback_errors_stop_decoding_and_propagate() {
    let rows = fixed_rows();
    let (_, bytes) = SerdeBridge::to_rowbinary(&rows).expect("encode rows");
    let block = SerdeBridge::rows_to_block(&rows).expect("build block");

    let stop = || Error::Serde {
        message: std::borrow::Cow::Borrowed("callback stop"),
    };

    let mut delivered = 0;
    let result = SerdeBridge::from_rowbinary_with(&bytes, |_row: FixedRow| {
        delivered += 1;
        Err(stop())
    });
    assert!(is_serde_error(result));
    assert_eq!(delivered, 1);

    let mut delivered = 0;
    let result = SerdeBridge::block_to_rows_with(&block, |_row: FixedRow| {
        delivered += 1;
        Err(stop())
    });
    assert!(is_serde_error(result));
    assert_eq!(delivered, 1);
}

#[test]
fn from_rowbinary_into_appends_and_counts_without_replacing_existing_rows() {
    let rows = fixed_rows();
    let (_, bytes) = SerdeBridge::to_rowbinary(&rows).expect("encode rows");

    let existing = FixedRow {
        id: 1,
        sequence: 0,
        price: 0.0,
    };
    let mut dst = vec![existing.clone()];
    let count = SerdeBridge::from_rowbinary_into(&bytes, &mut dst).expect("append rows");
    assert_eq!(count, rows.len());
    assert_eq!(dst.len(), rows.len() + 1);
    assert_eq!(dst[0], existing);
    assert_eq!(&dst[1..], rows.as_slice());

    // Empty input appends nothing and is not an error.
    let mut empty: Vec<FixedRow> = Vec::new();
    assert_eq!(
        SerdeBridge::from_rowbinary_into(&[], &mut empty).expect("empty input"),
        0
    );
    assert!(empty.is_empty());
}

#[test]
fn from_rowbinary_into_slice_writes_from_zero_and_leaves_the_tail() {
    let rows = fixed_rows();
    let (_, bytes) = SerdeBridge::to_rowbinary(&rows).expect("encode rows");

    let untouched = FixedRow {
        id: 99,
        sequence: 99,
        price: 99.0,
    };
    let mut dst = vec![untouched.clone(); rows.len() + 2];
    let count = SerdeBridge::from_rowbinary_into_slice(&bytes, &mut dst).expect("fill slice");
    assert_eq!(count, rows.len());
    assert_eq!(&dst[..rows.len()], rows.as_slice());
    assert_eq!(&dst[rows.len()..], &[untouched.clone(), untouched]);

    // Zero rows fill an empty destination.
    assert_eq!(
        SerdeBridge::from_rowbinary_into_slice::<FixedRow>(&[], &mut []).expect("empty input"),
        0
    );
}

#[test]
fn from_rowbinary_into_slice_rejects_input_over_capacity() {
    let rows = fixed_rows();
    let (_, bytes) = SerdeBridge::to_rowbinary(&rows).expect("encode rows");

    // Two rows cannot fit one slot. The first slot is already overwritten when
    // the capacity error is detected, which matches the streaming contract.
    let mut one = vec![FixedRow::default(); 1];
    assert!(is_serde_error(SerdeBridge::from_rowbinary_into_slice(
        &bytes, &mut one
    )));
    assert_eq!(one[0], rows[0]);

    // A non-empty input cannot fit an empty destination.
    assert!(is_serde_error(SerdeBridge::from_rowbinary_into_slice::<
        FixedRow,
    >(&bytes, &mut [])));
}

#[test]
fn block_reads_append_and_fill_caller_buffers() {
    let rows = fixed_rows();
    let block = SerdeBridge::rows_to_block(&rows).expect("build block");

    let existing = FixedRow {
        id: 1,
        sequence: 0,
        price: 0.0,
    };
    let mut appended = vec![existing.clone()];
    let count = SerdeBridge::block_to_rows_into(&block, &mut appended).expect("append block rows");
    assert_eq!(count, rows.len());
    assert_eq!(appended[0], existing);
    assert_eq!(&appended[1..], rows.as_slice());

    let untouched = FixedRow {
        id: 99,
        sequence: 99,
        price: 99.0,
    };
    let mut filled = vec![untouched.clone(); rows.len() + 1];
    let count =
        SerdeBridge::block_to_rows_into_slice(&block, &mut filled).expect("fill block slice");
    assert_eq!(count, rows.len());
    assert_eq!(&filled[..rows.len()], rows.as_slice());
    assert_eq!(filled[rows.len()], untouched);

    // The row count is known before decoding, so a too-small slice is rejected
    // without overwriting any slot.
    let mut too_small = vec![untouched.clone(); rows.len() - 1];
    assert!(is_serde_error(SerdeBridge::block_to_rows_into_slice(
        &block,
        &mut too_small
    )));
    assert_eq!(too_small, vec![untouched; rows.len() - 1]);
}

#[test]
fn zero_row_blocks_and_rowbinary_feed_callbacks_and_buffers() {
    let empty_block = SerdeBridge::rows_to_block::<FixedRow>(&[]).expect("empty block");
    let mut dst: Vec<FixedRow> = Vec::new();
    assert_eq!(
        SerdeBridge::block_to_rows_into(&empty_block, &mut dst).expect("empty block append"),
        0
    );
    assert!(dst.is_empty());
    assert_eq!(
        SerdeBridge::block_to_rows_into_slice::<FixedRow>(&empty_block, &mut []).expect("empty"),
        0
    );

    let mut calls = 0;
    assert_eq!(
        SerdeBridge::block_to_rows_with(&empty_block, |_row: FixedRow| {
            calls += 1;
            Ok(())
        })
        .expect("empty callback"),
        0
    );
    assert_eq!(calls, 0);
}
