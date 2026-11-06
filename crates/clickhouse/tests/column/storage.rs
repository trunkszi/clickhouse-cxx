use super::{
    AlignedBytes, Column, ColumnData, monotonic_offsets, valid_null_map, validate_offsets,
};

#[test]
fn storage_charges_cache_line_chunks() {
    use crate::MemoryBuffer;
    for len in [0usize, 1, 15, 16, 63, 64, 65, 127, 128, 129] {
        let source: Vec<_> = (0..len).map(|index| index as u8).collect();
        let storage = AlignedBytes::from_bytes(&source);
        assert_eq!(storage.as_bytes(), source);
        assert_eq!(storage.as_bytes().as_ptr() as usize % 64, 0);
        assert_eq!(storage.words.allocated_bytes(), len.div_ceil(64) * 64);
        assert_eq!(
            AlignedBytes::allocation_size(len),
            Some(len.div_ceil(64) * 64)
        );
    }
    assert_eq!(AlignedBytes::allocation_size(usize::MAX), None);
}

#[test]
fn storage_reuses_numeric_capacity() {
    let source = Column {
        name: "value".into(),
        type_name: "UInt128".into(),
        rows: 5,
        data: ColumnData::Fixed(AlignedBytes::from_bytes(&[0x5a; 80])),
    };
    let mut target = Column {
        name: "value".into(),
        type_name: "UInt128".into(),
        rows: 5,
        data: ColumnData::Fixed(AlignedBytes::from_bytes(&[0; 80])),
    };
    let address = target.slice::<u128>().unwrap().as_ptr();
    let allocated = target.allocated_bytes();
    target.clear_unbounded();
    assert_eq!(target.allocated_bytes(), allocated);
    target.copy_values(&source);
    assert_eq!(target.slice::<u128>().unwrap().as_ptr(), address);
    assert_eq!(
        target.slice::<u128>().unwrap(),
        &[u128::from_le_bytes([0x5a; 16]); 5]
    );
    target.clear_with_policy(crate::BufferPolicy {
        max_retained_bytes: 63,
        ..Default::default()
    });
    assert_eq!(target.allocated_bytes(), 0);
}

#[test]
fn null_batches_reject_each_invalid_lane() {
    for len in [0, 1, 63, 64, 65, 127, 128, 129] {
        let mut flags: Vec<_> = (0..len).map(|row| (row % 2) as u8).collect();
        assert!(valid_null_map(&flags));
        for row in 0..len {
            let saved = flags[row];
            for invalid in [2, 128, 255] {
                flags[row] = invalid;
                assert!(!valid_null_map(&flags), "length {len}, row {row}");
            }
            flags[row] = saved;
        }
    }
}

#[test]
fn offsets_check_chunk_and_terminal_edges() {
    for len in 0..=33 {
        let offsets: Vec<_> = (0..len as u64).map(|row| 2 * row + 2).collect();
        assert!(monotonic_offsets(&offsets));
        assert!(validate_offsets(&offsets, len * 2).is_ok());
        assert!(validate_offsets(&offsets, len * 2 + 1).is_err());
        for index in 1..len {
            let mut descending = offsets.clone();
            descending[index] = descending[index - 1] - 1;
            assert!(
                !monotonic_offsets(&descending),
                "length {len}, index {index}"
            );
        }
    }
}

#[test]
fn slice_rejects_storage_shorter_than_row_count() {
    let column = Column {
        name: "value".into(),
        type_name: "UInt64".into(),
        data: ColumnData::Fixed(AlignedBytes::from_bytes(&7_u64.to_le_bytes())),
        rows: 3,
    };
    assert!(column.slice::<u64>().is_err());
}

#[test]
fn slice_accepts_storage_matching_row_count() {
    let column = Column {
        name: "value".into(),
        type_name: "UInt64".into(),
        data: ColumnData::Fixed(AlignedBytes::from_bytes(&7_u64.to_le_bytes())),
        rows: 1,
    };
    assert_eq!(column.slice::<u64>().expect("one row"), [7]);
}

#[test]
fn read_bytes_into_checks_only_the_requested_row() {
    let column = Column {
        name: "value".into(),
        type_name: "String".into(),
        data: ColumnData::Strings {
            offsets: vec![5, 9, 12],
            bytes: b"helloworld!!".to_vec(),
        },
        rows: 3,
    };
    let mut dst = Vec::new();

    assert_eq!(column.read_bytes_into(0, &mut dst).expect("row 0"), 5);
    assert_eq!(dst, b"hello".to_vec());

    dst.extend_from_slice(b"stale");
    assert_eq!(column.read_bytes_into(1, &mut dst).expect("row 1"), 4);
    assert_eq!(dst, b"worl".to_vec());

    assert!(column.read_bytes_into(3, &mut dst).is_err());

    let nonmonotonic = Column {
        name: "value".into(),
        type_name: "String".into(),
        data: ColumnData::Strings {
            offsets: vec![5, 3],
            bytes: b"helloworld".to_vec(),
        },
        rows: 2,
    };
    assert_eq!(nonmonotonic.read_bytes_into(0, &mut dst).expect("row 0"), 5);
    assert!(nonmonotonic.read_bytes_into(1, &mut dst).is_err());
}
