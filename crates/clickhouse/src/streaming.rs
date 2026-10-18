//! Exclusive RAII sessions; cleanup runs synchronously under the configured timeout.

use std::marker::PhantomData;

use clickhouse_sys::ffi;

use crate::{
    Batch, Client, Error, NativeBlock, Query, Result,
    callbacks::{self, Frame},
};

/// An interactive query holding the client's unique borrow until exhaustion or Drop.
pub struct BlockStream<'a> {
    pub(crate) client: &'a mut Client,
    pub(crate) query: Query<'a>,
    pub(crate) active: bool,
    pub(crate) _not_send: PhantomData<*const ()>,
}

impl BlockStream<'_> {
    /// Whether this session still needs server output or cancellation cleanup.
    pub fn is_selecting(&self) -> bool {
        self.active && ffi::is_selecting(&self.client.inner)
    }

    /// Cancel the active query and drain all in-flight packets before reuse.
    ///
    /// # Errors
    /// If draining fails, the connection is reset; a failed reset poisons it.
    pub fn cancel(&mut self) -> Result<()> {
        if !self.active {
            return Ok(());
        }
        let mut frame = Frame::new(&mut self.query, None);
        // SAFETY: frame remains uniquely borrowed for this synchronous FFI call.
        // EventScope clears the context before returning or throwing.
        let result = unsafe {
            ffi::cancel(
                self.client.inner.pin_mut(),
                frame.pointer(),
                callbacks::dispatch,
            )
        };
        self.active = false;
        let result = frame.finish(result);
        if result.is_err() {
            self.client.recover();
        }
        result
    }
}

impl Iterator for BlockStream<'_> {
    type Item = Result<NativeBlock>;

    fn next(&mut self) -> Option<Self::Item> {
        if !self.active {
            return None;
        }
        let mut frame = Frame::new(&mut self.query, None);
        // SAFETY: the stack frame is live until C++ returns; callbacks cannot
        // borrow the client or escape their per-call EventScope.
        let result = unsafe {
            ffi::next_block(
                self.client.inner.pin_mut(),
                frame.pointer(),
                callbacks::dispatch,
            )
        };
        match frame.finish(result) {
            Ok(inner) if inner.is_null() => {
                self.active = false;
                None
            }
            Ok(inner) => Some(Ok(NativeBlock::from_inner(inner))),
            Err(error) => {
                self.active = false;
                self.client.recover();
                Some(Err(error))
            }
        }
    }
}

impl Drop for BlockStream<'_> {
    fn drop(&mut self) {
        if self.active
            && let Err(error) = self.cancel()
        {
            self.client.record_cleanup(error);
        }
    }
}

/// A single INSERT session accepting any number of schema-compatible blocks.
pub struct Inserter<'a> {
    pub(crate) client: &'a mut Client,
    pub(crate) schema: NativeBlock,
    pub(crate) active: bool,
    pub(crate) _not_send: PhantomData<*const ()>,
}

impl Inserter<'_> {
    /// Borrow the server-provided insert schema.
    pub fn schema_block(&self) -> &NativeBlock {
        &self.schema
    }

    /// Whether the server is waiting for another block or the end marker.
    pub fn is_inserting(&self) -> bool {
        self.active && ffi::is_inserting(&self.client.inner)
    }

    /// Send one block without finishing the statement.
    ///
    /// # Errors
    /// Rejects schema mismatches without sending data. IO failures reset the session.
    pub fn send_block(&mut self, block: &NativeBlock) -> Result<()> {
        if !self.active {
            return Err(Error::Invalid("insert session is closed".into()));
        }
        if block.columns() != self.schema.columns() {
            return Err(Error::Invalid(
                "insert column count differs from server schema".into(),
            ));
        }
        for i in 0..block.columns() {
            if block.column_name(i) != self.schema.column_name(i)
                || block.column_type(i)? != self.schema.column_type(i)?
            {
                return Err(Error::Invalid(
                    "insert column name or type differs from server schema".into(),
                ));
            }
        }
        if let Err(error) = ffi::send_insert_block(self.client.inner.pin_mut(), block.inner()) {
            self.active = false;
            self.client.recover();
            return Err(error.into());
        }
        Ok(())
    }

    /// Send a reusable generated or application-defined batch.
    ///
    /// # Errors
    /// Returns batch conversion or block schema/transport errors.
    pub fn send_batch<B: Batch>(&mut self, batch: &B) -> Result<()> {
        let block = crate::row::to_native_block(&batch.to_block()?)?;
        self.send_block(&block)
    }

    /// Send the end marker and wait for the server to finish the INSERT.
    ///
    /// # Errors
    /// Server or transport errors reset the connection before returning.
    pub fn end(mut self) -> Result<()> {
        if !self.active {
            return Err(Error::Invalid("insert session is closed".into()));
        }
        let result = ffi::end_insert(self.client.inner.pin_mut());
        self.active = false;
        if result.is_err() {
            self.client.recover();
        }
        result.map_err(Error::from)
    }
}

impl Drop for Inserter<'_> {
    fn drop(&mut self) {
        if self.active {
            self.active = false;
            if let Err(error) = self.client.reset_connection() {
                self.client.record_cleanup(error);
            }
        }
    }
}
