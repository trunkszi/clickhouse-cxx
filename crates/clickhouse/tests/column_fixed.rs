//! Fixed-width column family coverage.

use clickhouse::{
    ChBool, Date, Date32, DateTime, DateTime64, Decimal32, Decimal64, Decimal128, Enum8, Enum16,
    FixedKind, FixedWireColumn, Ipv4, Ipv6, Time, Time64, Uuid, WireBlock, WireColumn,
};

#[test]
fn numeric_columns_round_trip_with_zero_copy_slices() {
    let mut block = WireBlock::new().expect("block");
    block
        .append("u8", WireColumn::fixed(&[1_u8, 2]).expect("u8"))
        .expect("append u8");
    block
        .append("u16", WireColumn::fixed(&[10_u16, 20]).expect("u16"))
        .expect("append u16");
    block
        .append("u32", WireColumn::fixed(&[100_u32, 200]).expect("u32"))
        .expect("append u32");
    block
        .append("u64", WireColumn::fixed(&[1_000_u64, 2_000]).expect("u64"))
        .expect("append u64");
    block
        .append("u128", WireColumn::fixed(&[1_u128, 2]).expect("u128"))
        .expect("append u128");
    block
        .append("i8", WireColumn::fixed(&[-1_i8, 2]).expect("i8"))
        .expect("append i8");
    block
        .append("i16", WireColumn::fixed(&[-10_i16, 20]).expect("i16"))
        .expect("append i16");
    block
        .append("i32", WireColumn::fixed(&[-100_i32, 200]).expect("i32"))
        .expect("append i32");
    block
        .append("i64", WireColumn::fixed(&[-1_000_i64, 2_000]).expect("i64"))
        .expect("append i64");
    block
        .append("i128", WireColumn::fixed(&[-1_i128, 2]).expect("i128"))
        .expect("append i128");
    block
        .append("f32", WireColumn::fixed(&[1.5_f32, -2.5]).expect("f32"))
        .expect("append f32");
    block
        .append("f64", WireColumn::fixed(&[1.5_f64, -2.5]).expect("f64"))
        .expect("append f64");

    assert_eq!(block.column_slice::<u8>("u8").expect("u8 slice"), &[1, 2]);
    assert_eq!(
        block.column_slice::<u16>("u16").expect("u16 slice"),
        &[10, 20]
    );
    assert_eq!(
        block.column_slice::<u32>("u32").expect("u32 slice"),
        &[100, 200]
    );
    assert_eq!(
        block.column_slice::<u64>("u64").expect("u64 slice"),
        &[1_000, 2_000]
    );
    assert_eq!(
        block.column_slice::<u128>("u128").expect("u128 slice"),
        &[1, 2]
    );
    assert_eq!(block.column_slice::<i8>("i8").expect("i8 slice"), &[-1, 2]);
    assert_eq!(
        block.column_slice::<i16>("i16").expect("i16 slice"),
        &[-10, 20]
    );
    assert_eq!(
        block.column_slice::<i32>("i32").expect("i32 slice"),
        &[-100, 200]
    );
    assert_eq!(
        block.column_slice::<i64>("i64").expect("i64 slice"),
        &[-1_000, 2_000]
    );
    assert_eq!(
        block.column_slice::<i128>("i128").expect("i128 slice"),
        &[-1, 2]
    );
    assert_eq!(
        block.column_slice::<f32>("f32").expect("f32 slice"),
        &[1.5, -2.5]
    );
    assert_eq!(
        block.column_slice::<f64>("f64").expect("f64 slice"),
        &[1.5, -2.5]
    );
    assert!(block.column_slice::<u64>("u8").is_err());
}

#[test]
fn logical_fixed_columns_round_trip() {
    let mut block = WireBlock::new().expect("block");
    block
        .append(
            "b",
            WireColumn::fixed(&[ChBool(1), ChBool(0)]).expect("bool"),
        )
        .expect("append bool");
    block
        .append(
            "date",
            WireColumn::fixed(&[Date(1), Date(2)]).expect("date"),
        )
        .expect("append date");
    block
        .append(
            "date32",
            WireColumn::fixed(&[Date32(-1), Date32(2)]).expect("date32"),
        )
        .expect("append date32");
    block
        .append(
            "dt",
            WireColumn::fixed(&[DateTime(1), DateTime(2)]).expect("datetime"),
        )
        .expect("append datetime");
    block
        .append(
            "dt64",
            WireColumn::fixed(&[DateTime64(1), DateTime64(2)]).expect("datetime64"),
        )
        .expect("append datetime64");
    block
        .append(
            "time",
            WireColumn::fixed(&[Time(1), Time(2)]).expect("time"),
        )
        .expect("append time");
    block
        .append(
            "time64",
            WireColumn::fixed(&[Time64(1), Time64(2)]).expect("time64"),
        )
        .expect("append time64");
    block
        .append(
            "dec32",
            WireColumn::fixed(&[Decimal32(100), Decimal32(-100)]).expect("decimal32"),
        )
        .expect("append decimal32");
    block
        .append(
            "dec64",
            WireColumn::fixed(&[Decimal64(100), Decimal64(-100)]).expect("decimal64"),
        )
        .expect("append decimal64");
    block
        .append(
            "dec128",
            WireColumn::fixed(&[Decimal128(100), Decimal128(-100)]).expect("decimal128"),
        )
        .expect("append decimal128");
    block
        .append(
            "uuid",
            WireColumn::fixed(&[Uuid([1; 16]), Uuid([2; 16])]).expect("uuid"),
        )
        .expect("append uuid");
    block
        .append("ip4", WireColumn::fixed(&[Ipv4(1), Ipv4(2)]).expect("ipv4"))
        .expect("append ipv4");
    block
        .append(
            "ip6",
            WireColumn::fixed(&[Ipv6([1; 16]), Ipv6([2; 16])]).expect("ipv6"),
        )
        .expect("append ipv6");
    block
        .append(
            "enum8",
            WireColumn::fixed_named(
                FixedKind::Enum8,
                "Enum8('a' = 1, 'b' = 2)",
                &[Enum8(1), Enum8(2)],
            )
            .expect("enum8"),
        )
        .expect("append enum8");
    block
        .append(
            "enum16",
            WireColumn::fixed_named(
                FixedKind::Enum16,
                "Enum16('a' = 100, 'b' = 200)",
                &[Enum16(100), Enum16(200)],
            )
            .expect("enum16"),
        )
        .expect("append enum16");

    assert_eq!(
        block.column_slice::<ChBool>("b").expect("bool"),
        &[ChBool(1), ChBool(0)]
    );
    assert_eq!(
        block.column_to_vec::<Date>("date").expect("date"),
        vec![Date(1), Date(2)]
    );
    assert_eq!(
        block.column_to_vec::<Date32>("date32").expect("date32"),
        vec![Date32(-1), Date32(2)]
    );
    assert_eq!(
        block.column_to_vec::<DateTime>("dt").expect("datetime"),
        vec![DateTime(1), DateTime(2)]
    );
    assert_eq!(
        block
            .column_to_vec::<DateTime64>("dt64")
            .expect("datetime64"),
        vec![DateTime64(1), DateTime64(2)]
    );
    assert_eq!(
        block.column_to_vec::<Time>("time").expect("time"),
        vec![Time(1), Time(2)]
    );
    assert_eq!(
        block.column_to_vec::<Time64>("time64").expect("time64"),
        vec![Time64(1), Time64(2)]
    );
    assert_eq!(
        block
            .column_to_vec::<Decimal32>("dec32")
            .expect("decimal32"),
        vec![Decimal32(100), Decimal32(-100)]
    );
    assert_eq!(
        block
            .column_to_vec::<Decimal64>("dec64")
            .expect("decimal64"),
        vec![Decimal64(100), Decimal64(-100)]
    );
    assert_eq!(
        block
            .column_to_vec::<Decimal128>("dec128")
            .expect("decimal128"),
        vec![Decimal128(100), Decimal128(-100)]
    );
    assert_eq!(
        block.column_to_vec::<Uuid>("uuid").expect("uuid"),
        vec![Uuid([1; 16]), Uuid([2; 16])]
    );
    assert_eq!(
        block.column_to_vec::<Ipv4>("ip4").expect("ipv4"),
        vec![Ipv4(1), Ipv4(2)]
    );
    assert_eq!(
        block.column_to_vec::<Ipv6>("ip6").expect("ipv6"),
        vec![Ipv6([1; 16]), Ipv6([2; 16])]
    );
    assert_eq!(
        block.column_to_vec::<Enum8>("enum8").expect("enum8"),
        vec![Enum8(1), Enum8(2)]
    );
    assert_eq!(
        block.column_to_vec::<Enum16>("enum16").expect("enum16"),
        vec![Enum16(100), Enum16(200)]
    );
}

#[test]
fn clear_retains_fixed_capacity() {
    let mut column = FixedWireColumn::from_slice(&[1_u32; 32]).expect("column");
    let before = column.capacity();
    assert!(before >= 32);
    column.clear();
    assert!(column.is_empty());
    assert!(column.capacity() >= before);
}
