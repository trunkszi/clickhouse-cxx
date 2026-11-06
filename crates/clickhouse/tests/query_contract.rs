//! Unit scenarios for borrowed query metadata and handler ownership.

use std::{borrow::Cow, cell::Cell};

use clickhouse::{CUSTOM, IMPORTANT, OBSOLETE, Query, TracingContext};

#[test]
fn unit_scenario_metadata_preserves_null_and_borrows() {
    let sql = String::from("SELECT {value:Nullable(String)}");
    let count = Cell::new(0);
    let query = Query::new(sql.as_str())
        .with_id("stable-id")
        .with_setting("max_block_size", "2", IMPORTANT)
        .with_setting("max_block_size", "3", IMPORTANT | CUSTOM | OBSOLETE)
        .with_param("empty", "")
        .with_param("value", "overwritten")
        .with_null_param("value")
        .with_tracing(TracingContext {
            trace_id: u128::MAX,
            span_id: u64::MAX,
            tracestate: "a=b".into(),
            trace_flags: 1,
        })
        .on_progress(|p| count.set(count.get() + p.rows));
    assert!(matches!(query.sql, Cow::Borrowed(_)));
    assert!(std::ptr::eq(query.sql.as_ptr(), sql.as_ptr()));
    assert!(matches!(query.query_id, Cow::Borrowed(_)));
    assert_eq!(query.settings.len(), 1);
    assert_eq!(query.settings["max_block_size"].value, "3");
    assert_eq!(
        query.settings["max_block_size"].flags,
        IMPORTANT | CUSTOM | OBSOLETE
    );
    assert_eq!(query.params["value"], None);
    assert_eq!(query.params["empty"].as_deref(), Some(""));
    assert_eq!(query.tracing_context.as_ref().unwrap().trace_id, u128::MAX);
    assert!(query.has_event_callbacks());
    drop(query);
    assert_eq!(
        count.get(),
        0,
        "constructing or dropping an idle query invokes no handlers"
    );
}
