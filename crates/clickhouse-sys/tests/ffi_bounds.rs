use std::process::Command;

#[test]
fn string_bounds_stay_inside_bridge() -> Result<(), Box<dyn std::error::Error>> {
    let child = Command::new(std::env::current_exe()?)
        .args(["--exact", "bounds_child", "--nocapture"])
        .env("CLICKHOUSE_AUDIT_BOUNDS", "string")
        .output()?;
    assert!(
        child.status.success(),
        "string bridge boundary failed: {:?}\n{}",
        child.status,
        String::from_utf8_lossy(&child.stderr)
    );
    Ok(())
}

#[test]
fn map_bounds_stay_inside_bridge() -> Result<(), Box<dyn std::error::Error>> {
    let child = Command::new(std::env::current_exe()?)
        .args(["--exact", "bounds_child", "--nocapture"])
        .env("CLICKHOUSE_AUDIT_BOUNDS", "map")
        .output()?;
    assert!(
        child.status.success(),
        "map bridge boundary failed: {:?}\n{}",
        child.status,
        String::from_utf8_lossy(&child.stderr)
    );
    Ok(())
}

#[test]
fn empty_block_indices_stay_inside_bridge() -> Result<(), Box<dyn std::error::Error>> {
    let child = Command::new(std::env::current_exe()?)
        .args(["--exact", "bounds_child", "--nocapture"])
        .env("CLICKHOUSE_AUDIT_BOUNDS", "block")
        .output()?;
    assert!(
        child.status.success(),
        "empty block bridge boundary failed: {:?}\n{}",
        child.status,
        String::from_utf8_lossy(&child.stderr)
    );
    Ok(())
}

#[test]
fn zero_width_fixed_strings_are_rejected() -> Result<(), Box<dyn std::error::Error>> {
    let child = Command::new(std::env::current_exe()?)
        .args(["--exact", "bounds_child", "--nocapture"])
        .env("CLICKHOUSE_AUDIT_BOUNDS", "zero-fixed")
        .output()?;
    assert!(
        child.status.success(),
        "zero-width fixed string boundary failed: {:?}\n{}",
        child.status,
        String::from_utf8_lossy(&child.stderr)
    );
    Ok(())
}

#[test]
fn bounds_child() -> Result<(), Box<dyn std::error::Error>> {
    match std::env::var("CLICKHOUSE_AUDIT_BOUNDS").as_deref() {
        Ok("string") => {
            for name in ["String", "FixedString(2)", "JSON"] {
                let mut column = clickhouse_sys::new_column(name)?;
                assert!(column.string_at(0).is_empty());
                if name == "FixedString(2)" {
                    column.pin_mut().append_fixed(b"ab")?;
                    assert_eq!(column.string_at(0), b"ab");
                } else {
                    column.pin_mut().append_string(b"{}")?;
                    assert_eq!(column.string_at(0), b"{}");
                }
                assert!(column.string_at(1).is_empty());
                assert!(column.string_at(usize::MAX).is_empty());
            }
        }
        Ok("map") => {
            let column = clickhouse_sys::new_column("Map(String, UInt64)")?;
            assert!(column.map_row(0).is_null());
            let column = clickhouse_sys::new_column("UInt64")?;
            assert!(column.map_row(0).is_null());
        }
        Ok("block") => {
            let block = clickhouse_sys::ffi::block_new();
            for index in [0, usize::MAX] {
                assert!(clickhouse_sys::ffi::block_column_handle(&block, index).is_null());
            }
        }
        Ok("zero-fixed") => {
            for name in [
                "FixedString(0)",
                "Nullable(FixedString(0))",
                "Array(FixedString(0))",
                "Tuple(UInt8, FixedString(0))",
                "Map(String, FixedString(0))",
                "LowCardinality(FixedString(0))",
                "LowCardinality(Nullable(FixedString(0)))",
            ] {
                assert!(clickhouse_sys::new_column(name).is_err(), "accepted {name}");
            }
        }
        _ => {}
    }
    Ok(())
}

#[test]
fn unsigned_dictionary_default_appends_zero() -> Result<(), Box<dyn std::error::Error>> {
    for name in ["UInt8", "UInt16", "UInt32", "UInt64"] {
        let mut column = clickhouse_sys::new_column(&format!("LowCardinality({name})"))?;
        column.pin_mut().low_cardinality_append_u64(7)?;
        column.pin_mut().append_default()?;
        assert_eq!(column.size(), 2);
        assert!(column.item_bytes(1).iter().all(|byte| *byte == 0));
    }
    Ok(())
}
