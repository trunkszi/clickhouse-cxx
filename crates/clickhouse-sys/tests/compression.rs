//! Native compression frames carry CityHash128 over the header and encoded body.
use clickhouse_sys::ffi::decode_compressed_frames;

const LZ4_VALID: &[u8] = &[
    201, 36, 123, 84, 68, 18, 45, 44, 74, 219, 210, 118, 89, 246, 53, 222, 130, 14, 0, 0, 0, 4, 0,
    0, 0, 64, 11, 22, 33, 44,
];

const LZ4_MISMATCH: &[u8] = &[
    73, 68, 227, 242, 119, 109, 101, 2, 204, 166, 90, 238, 148, 37, 189, 103, 130, 14, 0, 0, 0, 8,
    0, 0, 0, 64, 11, 22, 33, 44,
];

const ZSTD_VALID: &[u8] = &[
    24, 68, 176, 136, 85, 22, 148, 191, 3, 41, 202, 239, 54, 176, 78, 229, 144, 22, 0, 0, 0, 4, 0,
    0, 0, 40, 181, 47, 253, 32, 4, 33, 0, 0, 11, 22, 33, 44,
];

const ZSTD_MISMATCH: &[u8] = &[
    86, 142, 30, 1, 227, 51, 107, 65, 0, 139, 26, 32, 127, 208, 146, 40, 144, 22, 0, 0, 0, 8, 0, 0,
    0, 40, 181, 47, 253, 32, 4, 33, 0, 0, 11, 22, 33, 44,
];

#[test]
fn lz4_size_mismatch_fails_closed() {
    let result = decode_compressed_frames(LZ4_MISMATCH);
    assert!(
        result.is_err(),
        "checksum-valid mismatched LZ4 frame accepted: {result:?}"
    );
}

#[test]
fn zstd_size_mismatch_fails_closed() {
    let result = decode_compressed_frames(ZSTD_MISMATCH);
    assert!(
        result.is_err(),
        "checksum-valid mismatched ZSTD frame accepted: {result:?}"
    );
}

#[test]
fn valid_frames_and_checksum_validation() -> Result<(), Box<dyn std::error::Error>> {
    for frame in [LZ4_VALID, ZSTD_VALID] {
        assert_eq!(decode_compressed_frames(frame)?, [11, 22, 33, 44]);
        let mut corrupt = frame.to_vec();
        corrupt[0] ^= 1;
        assert!(decode_compressed_frames(&corrupt).is_err());
        assert!(decode_compressed_frames(&frame[..frame.len() - 1]).is_err());
    }
    let mut combined = LZ4_VALID.to_vec();
    combined.extend_from_slice(ZSTD_VALID);
    assert_eq!(
        decode_compressed_frames(&combined)?,
        [11, 22, 33, 44, 11, 22, 33, 44]
    );
    Ok(())
}
