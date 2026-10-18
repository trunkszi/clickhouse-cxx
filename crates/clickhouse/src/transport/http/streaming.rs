//! Scoped borrowed receivers driven directly by the owning curl event loop.
use std::{
    cell::{Cell, RefCell},
    rc::Rc,
};

use curl::{EventLoop, Token};

use super::{HttpTransport, lossy_text};
use crate::{Compression, Error, HttpVersionChoice, Query, Result};

/// Revokes the borrowed receiver before its stack frame can unwind.
struct ReceiverScope<'a> {
    event_loop: &'a mut EventLoop,
    address: Rc<Cell<*mut ()>>,
    token: Option<Token>,
}
impl Drop for ReceiverScope<'_> {
    fn drop(&mut self) {
        self.address.set(std::ptr::null_mut());
        if let Some(token) = self.token.take() {
            // Cancellation is best-effort during unwinding. Revocation is unconditional:
            // retained callbacks can only observe null and cannot access the receiver.
            if let Ok(handle) = self.event_loop.cancel(token) {
                drop(handle);
            }
        }
    }
}

/// Invoke the exclusively borrowed receiver while its scope remains live.
///
/// # Safety
/// The pointer must be non-null, aligned, and point to a live, exclusively
/// borrowed F. Its owning scope must revoke it before F is moved or dropped;
/// serialized callbacks must not access F reentrantly.
unsafe fn invoke<F: FnMut(&[u8]) -> Result<()>>(pointer: *mut (), data: &[u8]) -> Result<()> {
    // SAFETY: execute_with keeps F at a stable stack address until ReceiverScope
    // revokes the pointer. The single-thread event loop serializes callbacks;
    // no reference to F is used outside this invocation while run is active.
    unsafe { (&mut *pointer.cast::<F>())(data) }
}

impl HttpTransport {
    /// Deliver successful response chunks synchronously without buffering their body.
    ///
    /// Previously delivered chunks remain observable if a later callback or transfer
    /// fails. Compressed response streaming is rejected before delivering any bytes.
    /// # Errors
    /// Compression, callback rejection, HTTP rejection, and transfer errors propagate.
    pub(crate) fn execute_with<F>(
        &mut self,
        database: &str,
        query: &Query<'_>,
        body: &[u8],
        compression: Compression,
        dedup: Option<&str>,
        mut receive: F,
    ) -> Result<usize>
    where
        F: FnMut(&[u8]) -> Result<()>,
    {
        if query.compression_override().unwrap_or(compression) != Compression::None {
            return Err(Error::unsupported(
                "incremental HTTP requires uncompressed responses",
            ));
        }
        self.negotiated = None;
        let url = self.url(database, query, query.sql(), dedup, Compression::None)?;
        let mut easy = self.configured_easy(&url, &self.headers(None, Compression::None), body)?;
        let address = Rc::new(Cell::new((&mut receive as *mut F).cast::<()>()));
        let callback_error = Rc::new(RefCell::new(None));
        let callback_panic = Rc::new(RefCell::new(None));
        let status = Rc::new(Cell::new(0u32));
        let encoded = Rc::new(Cell::new(false));
        let received = Rc::new(Cell::new(0usize));
        let diagnostic = Rc::new(RefCell::new(Vec::new()));
        let negotiated = Rc::new(Cell::new(None));
        // The guard precedes all callback registration. On every return or panic it
        // revokes the pointer before receive drops, even if curl retains a callback.
        let mut scope = ReceiverScope {
            event_loop: &mut self.loop_,
            address: Rc::clone(&address),
            token: None,
        };
        {
            let status = Rc::clone(&status);
            let encoded = Rc::clone(&encoded);
            let negotiated = Rc::clone(&negotiated);
            let requested = self.version;
            easy.header_function(move |line| {
                if line.starts_with(b"HTTP/") {
                    let text = String::from_utf8_lossy(line);
                    status.set(
                        text.split_ascii_whitespace()
                            .nth(1)
                            .and_then(|s| s.parse().ok())
                            .unwrap_or(0),
                    );
                    encoded.set(false);
                    negotiated.set(if line.starts_with(b"HTTP/2 ") {
                        Some(if requested == HttpVersionChoice::Http2PriorKnowledge {
                            requested
                        } else {
                            HttpVersionChoice::Http2Tls
                        })
                    } else if line.starts_with(b"HTTP/1.") {
                        Some(HttpVersionChoice::Http1)
                    } else {
                        None
                    });
                } else if let Some((name, value)) =
                    String::from_utf8_lossy(line).trim().split_once(':')
                    && name.eq_ignore_ascii_case("content-encoding")
                    && !value.trim().eq_ignore_ascii_case("identity")
                {
                    encoded.set(true);
                }
                true
            })
            .map_err(Error::curl)?;
        }
        {
            let address = Rc::clone(&address);
            let callback_error = Rc::clone(&callback_error);
            let callback_panic = Rc::clone(&callback_panic);
            let status = Rc::clone(&status);
            let encoded = Rc::clone(&encoded);
            let received = Rc::clone(&received);
            let diagnostic = Rc::clone(&diagnostic);
            // A plain function pointer erases the receiver type without extending
            // its lifetime; ReceiverScope controls the only usable address.
            let call: unsafe fn(*mut (), &[u8]) -> Result<()> = invoke::<F>;
            easy.write_function(move |data| {
                if callback_error.borrow().is_some() || callback_panic.borrow().is_some() {
                    return Ok(0);
                }
                if encoded.get() {
                    *callback_error.borrow_mut() =
                        Some(Error::unsupported("encoded incremental HTTP response"));
                    return Ok(0);
                }
                if !(200..300).contains(&status.get()) {
                    let mut diagnostic = diagnostic.borrow_mut();
                    let remaining = 4_096usize.saturating_sub(diagnostic.len());
                    diagnostic.extend_from_slice(&data[..data.len().min(remaining)]);
                    return Ok(data.len());
                }
                let pointer = address.get();
                if pointer.is_null() {
                    return Ok(0);
                }
                let Some(total) = received.get().checked_add(data.len()) else {
                    *callback_error.borrow_mut() =
                        Some(Error::protocol("HTTP response length overflow"));
                    return Ok(0);
                };
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    // SAFETY: the owning scope is live and its address is non-null.
                    // The event loop serializes callbacks; the scope revokes this
                    // address before F can be dropped or reused.
                    unsafe { call(pointer, data) }
                }));
                match result {
                    Ok(Ok(())) => {
                        received.set(total);
                        Ok(data.len())
                    }
                    Ok(Err(error)) => {
                        *callback_error.borrow_mut() = Some(error);
                        Ok(0)
                    }
                    Err(payload) => {
                        *callback_panic.borrow_mut() = Some(payload);
                        Ok(0)
                    }
                }
            })
            .map_err(Error::curl)?;
        }
        let transfer_error = Rc::new(RefCell::new(None));
        {
            let transfer_error = Rc::clone(&transfer_error);
            scope.token = Some(
                scope
                    .event_loop
                    .add(easy, move |_, result| {
                        if let Err(error) = result {
                            *transfer_error.borrow_mut() = Some(Error::curl(error));
                        }
                    })
                    .map_err(|error| Error::curl(error.error))?,
            );
        }
        let run_result = scope.event_loop.run().map_err(Error::curl);
        // No callbacks remain on normal completion; the drop guard also protects
        // the exceptional path where cancellation itself is rejected.
        drop(scope);
        self.negotiated = negotiated.get();
        let panic_payload = callback_panic.borrow_mut().take();
        if let Some(payload) = panic_payload {
            std::panic::resume_unwind(payload);
        }
        if let Some(error) = callback_error.borrow_mut().take() {
            return Err(error);
        }
        run_result?;
        if let Some(error) = transfer_error.borrow_mut().take() {
            return Err(error);
        }
        if encoded.get() {
            return Err(Error::unsupported("encoded incremental HTTP response"));
        }
        if !(200..300).contains(&status.get()) {
            return Err(Error::Http {
                status: status.get(),
                message: lossy_text(&diagnostic.borrow()),
            });
        }
        Ok(received.get())
    }
}
