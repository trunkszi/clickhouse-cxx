//! Integration and scenario coverage of Native/RowBinary codecs and serde mappings.
//! Inputs are controlled; no external service is started.

use clickhouse::{
    Endpoint, HTTP_NATIVE_REVISION, Scheme, SerdeBridge, decode_blocks, encode_block,
};
use serde::{Deserialize, Serialize};

#[test]
fn endpoint_parses_user_password_database_and_ipv6() {
    let endpoint = Endpoint::parse("http://user:secret@localhost:8123/analytics").unwrap();
    assert_eq!(endpoint.scheme(), Scheme::Http);
    assert_eq!(endpoint.host(), "localhost");
    assert_eq!(endpoint.port(), 8123);
    assert_eq!(endpoint.user(), Some("user"));
    assert_eq!(endpoint.password(), Some("secret"));
    assert_eq!(endpoint.database(), Some("analytics"));
    assert!(!format!("{endpoint:?}").contains("secret"));

    let v6 = Endpoint::parse("tcp://[::1]").unwrap();
    assert_eq!(v6.host(), "::1");
    assert_eq!(v6.port(), 9000);
    assert!(Endpoint::parse("nntp://localhost").is_err());
}

#[test]
fn fnv1a64_matches_known_vectors() {
    assert_eq!(SerdeBridge::fnv1a64(b""), 0xcbf2_9ce4_8422_2325);
    assert_eq!(SerdeBridge::fnv1a64(b"a"), 0xaf63_dc4c_8601_ec8c);
    assert_eq!(SerdeBridge::fnv1a64(b"foobar"), 0x8594_4171_f739_67e8);
    assert_eq!(SerdeBridge::fnv1a64(b"hello"), 0xa430_d846_80aa_bd0b);
}

#[test]
fn native_codec_preserves_float_bits() {
    let mut block = clickhouse::Block::new();
    let neg = f64::from_bits(0x8000_0000_0000_0000);
    let nan = f64::from_bits(0x7ff8_0000_0000_0001);
    block
        .append_fixed_slice("id", "UInt64", &[7_u64, 8])
        .unwrap();
    block.append_strings_flat("s", &[2, 4], b"hiyo").unwrap();
    block
        .append_fixed_slice("z", "Float64", &[neg, nan])
        .unwrap();
    block
        .append_nullable_fixed("flag", "UInt8", &[0_u8, 9], &[1, 0])
        .unwrap();
    block
        .append_array_fixed_flat("tags", "UInt64", &[2, 3], &[1_u64, 2, 3])
        .unwrap();

    {
        let revision = HTTP_NATIVE_REVISION;
        let mut bytes = Vec::new();
        encode_block(&block, revision, &mut bytes).unwrap();
        let decoded = decode_blocks(&bytes, revision).unwrap();
        assert_eq!(decoded.len(), 1);
        let back = &decoded[0];
        assert_eq!(back.column_slice_by_name::<u64>("id").unwrap(), &[7, 8]);
        let floats = back.column_slice_by_name::<f64>("z").unwrap();
        assert_eq!(floats[0].to_bits(), neg.to_bits());
        assert_eq!(floats[1].to_bits(), nan.to_bits());
        let token = SerdeBridge::deduplication_token(&block).unwrap();
        assert_eq!(token.len(), 16);
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
struct Sample {
    id: u64,
    name: String,
    score: Option<i64>,
    tags: Vec<u64>,
    z: f64,
}

#[test]
fn rowbinary_and_dynamic_block_roundtrip_preserve_sign_and_nulls() {
    let rows = vec![
        Sample {
            id: 1,
            name: "hi".to_owned(),
            score: None,
            tags: vec![1, 2],
            z: f64::from_bits(0x8000_0000_0000_0000),
        },
        Sample {
            id: 2,
            name: "yo".to_owned(),
            score: Some(-4),
            tags: vec![],
            z: f64::from_bits(0x7ff8_0000_0000_0001),
        },
    ];
    let (names, bytes) = SerdeBridge::to_rowbinary(&rows).unwrap();
    assert_eq!(
        names.iter().map(|name| name.as_ref()).collect::<Vec<_>>(),
        ["id", "name", "score", "tags", "z"]
    );
    let decoded: Vec<Sample> = SerdeBridge::from_rowbinary(&bytes).unwrap();
    assert_eq!(decoded[0].id, 1);
    assert_eq!(decoded[0].score, None);
    assert_eq!(decoded[1].tags, Vec::<u64>::new());
    assert_eq!(decoded[0].z.to_bits(), rows[0].z.to_bits());
    assert_eq!(decoded[1].z.to_bits(), rows[1].z.to_bits());

    let block = SerdeBridge::rows_to_block(&rows).unwrap();
    let back: Vec<Sample> = SerdeBridge::block_to_rows(&block).unwrap();
    assert_eq!(back[0].name, "hi");
    assert_eq!(back[1].score, Some(-4));
    assert_eq!(back[0].z.to_bits(), rows[0].z.to_bits());
    assert_eq!(back[1].z.to_bits(), rows[1].z.to_bits());
    assert_eq!(block.column_slice_by_name::<u64>("id").unwrap(), &[1, 2]);
}

/// Truncated payloads fail instead of yielding partially materialized rows.
#[test]
fn truncated_wire_payloads_are_rejected() {
    assert!(decode_blocks(&[1], HTTP_NATIVE_REVISION).is_err());
    assert!(SerdeBridge::from_rowbinary::<Sample>(&[0x80]).is_err());
    let rows = [Sample {
        id: 7,
        name: String::from("bounded"),
        score: Some(-4),
        tags: vec![1, 2],
        z: -0.0,
    }];
    let (_, payload) = SerdeBridge::to_rowbinary(&rows).unwrap();
    assert!(SerdeBridge::from_rowbinary::<Sample>(&payload[..payload.len() - 1]).is_err());
    let block = SerdeBridge::rows_to_block(&rows).unwrap();
    let first = SerdeBridge::deduplication_token(&block).unwrap();
    assert_eq!(first, SerdeBridge::deduplication_token(&block).unwrap());
    let different = [Sample {
        z: 0.0,
        ..rows.into_iter().next().unwrap()
    }];
    assert_ne!(
        first,
        SerdeBridge::deduplication_token(&SerdeBridge::rows_to_block(&different).unwrap()).unwrap()
    );
}

/// Decoders reject malformed booleans and row shapes that consume no wire bytes.
#[test]
fn invalid_or_zero_progress_rows_fail_closed() {
    #[derive(Deserialize)]
    struct Empty {}
    assert!(SerdeBridge::from_rowbinary::<Empty>(&[1]).is_err());
    assert!(SerdeBridge::from_rowbinary::<bool>(&[2]).is_err());
    assert!(SerdeBridge::from_rowbinary::<Vec<Empty>>(&[1]).is_err());
    #[derive(Serialize)]
    struct UnknownType {
        value: Option<i64>,
    }
    assert!(SerdeBridge::rows_to_block(&[UnknownType { value: None }]).is_err());
}
