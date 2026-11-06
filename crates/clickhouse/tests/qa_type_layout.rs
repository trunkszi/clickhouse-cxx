use clickhouse::{
    ColumnData,
    native_format::{decode_blocks, encode_block},
};

type TestResult = Result<(), Box<dyn std::error::Error>>;

fn write_string(out: &mut Vec<u8>, value: &str) {
    assert!(value.len() < 128);
    out.push(value.len() as u8);
    out.extend_from_slice(value.as_bytes());
}

#[test]
fn quoted_enum_delimiters_decode_in_maps() -> TestResult {
    for label in [
        "(".to_owned(),
        ")".to_owned(),
        "left(right".to_owned(),
        "quote\\'(label".to_owned(),
        "(".repeat(65),
    ] {
        let ty = format!("Map(Enum8('{label}' = 1), UInt8)");
        let mut encoded = vec![1, 1];
        write_string(&mut encoded, "m");
        write_string(&mut encoded, &ty);
        encoded.extend_from_slice(&1_u64.to_le_bytes());
        encoded.extend_from_slice(&[1, 7]);
        let blocks = decode_blocks(&encoded, 0)?;
        assert_eq!(blocks.len(), 1);
        assert_eq!(blocks[0].row_count(), 1);
        let ColumnData::Map {
            offsets,
            keys,
            values,
        } = &blocks[0].column(0)?.data
        else {
            return Err("expected a map".into());
        };
        assert_eq!(offsets, &[1]);
        assert_eq!(keys.slice::<i8>()?, &[1]);
        assert_eq!(values.slice::<u8>()?, &[7]);
        let mut round_trip = Vec::new();
        encode_block(&blocks[0], 0, &mut round_trip)?;
        assert_eq!(round_trip, encoded);
    }
    Ok(())
}
