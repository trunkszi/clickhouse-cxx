//! Stack-scoped callbacks; no pointer is retained beyond a synchronous FFI call.

use std::{
    any::Any,
    panic::{AssertUnwindSafe, catch_unwind},
};

use clickhouse_sys::ffi;

use crate::{Error, NativeBlock, Profile, Progress, Query, Result, ServerException};

pub(crate) struct Frame<'scope, 'query> {
    query: &'scope mut Query<'query>,
    on_block: Option<&'scope mut dyn FnMut(NativeBlock) -> bool>,
    exception: Option<ServerException>,
    panic: Option<Box<dyn Any + Send>>,
}

impl<'scope, 'query> Frame<'scope, 'query> {
    pub(crate) fn new(
        query: &'scope mut Query<'query>,
        on_block: Option<&'scope mut dyn FnMut(NativeBlock) -> bool>,
    ) -> Self {
        Self {
            query,
            on_block,
            exception: None,
            panic: None,
        }
    }

    pub(crate) fn pointer(&mut self) -> *mut u8 {
        std::ptr::from_mut(self).cast()
    }

    pub(crate) fn finish<T>(self, value: std::result::Result<T, cxx::Exception>) -> Result<T> {
        if self.panic.is_some() {
            return Err(Error::CallbackPanicked);
        }
        if let Some(error) = self.exception {
            return Err(Error::Server(error));
        }
        value.map_err(Error::from)
    }

    fn dispatch(&mut self, event: &ffi::NativeEvent) -> bool {
        let fields = ffi::event_fields(event);
        match ffi::event_kind(event) {
            0 => {
                if let Some(callback) = &mut self.on_block {
                    return callback(NativeBlock::from_inner(ffi::event_block(event)));
                }
            }
            1 => {
                if let Some(callback) = &mut self.query.on_progress {
                    callback(Progress {
                        rows: fields.rows,
                        bytes: fields.bytes,
                        total_rows: fields.total_rows,
                        written_rows: fields.written_rows,
                        written_bytes: fields.written_bytes,
                    });
                }
            }
            2 => {
                if let Some(callback) = &mut self.query.on_profile {
                    callback(Profile {
                        rows: fields.rows,
                        bytes: fields.bytes,
                        blocks: fields.blocks,
                        rows_before_limit: fields.rows_before_limit,
                        applied_limit: fields.applied_limit,
                        calculated_rows_before_limit: fields.calculated_rows_before_limit,
                    });
                }
            }
            3 => {
                if let Some(callback) = &mut self.query.on_profile_events {
                    callback(&NativeBlock::from_inner(ffi::event_block(event)));
                }
            }
            4 => {
                if let Some(callback) = &mut self.query.on_server_log {
                    callback(&NativeBlock::from_inner(ffi::event_block(event)));
                }
            }
            5 => {
                let source = ffi::event_exception(event);
                let error = ServerException {
                    code: source.code,
                    name: source.name,
                    display_text: source.display_text,
                    stack_trace: source.stack_trace,
                };
                // Record the error before calling user code so a panic cannot erase it.
                self.exception = Some(error);
                if let (Some(callback), Some(error)) =
                    (&mut self.query.on_exception, &self.exception)
                {
                    callback(error);
                }
            }
            _ => return false,
        }
        true
    }
}

/// Called only by C++ EventScope while the originating Rust Frame is borrowed.
///
/// Kept private to the crate because the context is meaningful only to its caller.
pub(crate) fn dispatch(context: *mut u8, event: &ffi::NativeEvent) -> bool {
    // SAFETY: every caller passes Frame::pointer for a live, uniquely borrowed
    // stack frame. EventScope clears its pointer before returning, even on errors;
    // no client reference is stored in Frame, preventing reentrant client borrows.
    let frame = unsafe { &mut *context.cast::<Frame<'_, '_>>() };
    if frame.panic.is_some() {
        return false;
    }
    match catch_unwind(AssertUnwindSafe(|| frame.dispatch(event))) {
        Ok(keep_reading) => keep_reading,
        Err(payload) => {
            frame.panic = Some(payload);
            false
        }
    }
}

pub(crate) fn event_mask(query: &Query<'_>) -> u8 {
    u8::from(query.on_progress.is_some())
        | (u8::from(query.on_profile.is_some()) << 1)
        | (u8::from(query.on_profile_events.is_some()) << 2)
        | (u8::from(query.on_server_log.is_some()) << 3)
}

pub(crate) fn native_query(query: &Query<'_>) -> cxx::UniquePtr<ffi::NativeQuery> {
    let mut native = ffi::new_query(&query.sql, &query.query_id);
    for (key, field) in &query.settings {
        ffi::set_setting(native.pin_mut(), key, &field.value, field.flags);
    }
    for (key, value) in &query.params {
        ffi::set_param(
            native.pin_mut(),
            key,
            value.as_deref().unwrap_or(""),
            value.is_none(),
        );
    }
    if let Some(trace) = &query.tracing_context {
        ffi::set_tracing(
            native.pin_mut(),
            (trace.trace_id >> 64) as u64,
            trace.trace_id as u64,
            trace.span_id,
            &trace.tracestate,
            trace.trace_flags,
        );
    }
    native
}
