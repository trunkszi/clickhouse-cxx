#![cfg(feature = "tcp")]
use clickhouse::{NullableColumn, WireColumn};

type TestResult = Result<(), Box<dyn std::error::Error>>;

#[test]
fn nullable_rejects_wrong_fixed_type() -> TestResult {
    let inner = WireColumn::fixed(&[0_u64])?;
    let mut nullable = NullableColumn::new(&inner)?;
    nullable.append_fixed(&[7_u64])?;
    let before = nullable.nested_fixed_bytes().to_vec();
    let result = nullable.append_fixed(&[1_u32, 2_u32]);
    assert!(
        result.is_err(),
        "two u32 values must not be accepted by Nullable(UInt64)"
    );
    assert_eq!(nullable.len(), 1);
    assert_eq!(nullable.nulls(), &[0]);
    assert_eq!(nullable.nested_fixed_bytes(), before);
    nullable.append_fixed(&[9_u64])?;
    assert_eq!(nullable.len(), 2);
    assert_eq!(nullable.nested_fixed_bytes().len(), 16);
    Ok(())
}

#[test]
fn nullable_rejects_same_width_wrong_type() -> TestResult {
    let inner = WireColumn::fixed(&[0_u64])?;
    let mut nullable = NullableColumn::new(&inner)?;
    assert!(nullable.append_fixed(&[1.0_f64]).is_err());
    assert!(nullable.is_empty());
    assert!(nullable.nested_fixed_bytes().is_empty());
    Ok(())
}
