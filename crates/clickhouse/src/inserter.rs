//! Explicitly finalized inserts with owned encoded staging buffers.
use crate::{Block, Client, Error, Result, native_format::encode_block};

/// Whether a staged HTTP insert is empty or holding bytes.
///
/// `end` is the only send. This phase does not flush.
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum InserterPhase {
    /// No encoded bytes are staged.
    Idle = 0,
    /// One or more blocks are staged and waiting for `end`.
    Buffering = 1,
}

/// Stages Native-format blocks; only `end` sends data to the server.
/// Dropping an unfinished inserter discards its staged bytes without insertion.
pub struct HttpInserter<'a> {
    client: &'a mut Client,
    table: String,
    bytes: Vec<u8>,
    schema: Option<Vec<(String, String)>>,
}
impl<'a> HttpInserter<'a> {
    pub(crate) fn new(client: &'a mut Client, table: &str) -> Self {
        Self {
            client,
            table: table.to_owned(),
            bytes: Vec::new(),
            schema: None,
        }
    }

    /// `Idle` when nothing is staged, otherwise `Buffering`.
    #[must_use]
    pub fn phase(&self) -> InserterPhase {
        if self.bytes.is_empty() {
            InserterPhase::Idle
        } else {
            InserterPhase::Buffering
        }
    }
    /// Stage one nonempty block without sending a request.
    /// # Errors
    /// Schema mismatch or unsupported wire encoding rejects this block atomically.
    pub fn write(&mut self, block: &Block) -> Result<()> {
        if block.row_count() == 0 {
            return Ok(());
        }
        let schema = (0..block.column_count())
            .map(|i| {
                let c = block.column(i)?;
                Ok((c.name.as_ref().to_owned(), c.type_name.as_ref().to_owned()))
            })
            .collect::<Result<Vec<_>>>()?;
        if self.schema.as_ref().is_some_and(|old| old != &schema) {
            return Err(Error::protocol("insert batch schema changed"));
        }
        let before = self.bytes.len();
        if let Err(error) = encode_block(block, 0, &mut self.bytes) {
            self.bytes.truncate(before);
            return Err(error);
        }
        if self.schema.is_none() {
            self.schema = Some(schema);
        }
        Ok(())
    }
    /// Send one HTTP request or combine staged columns for one Native insert.
    /// # Errors
    /// Transport and server rejection errors propagate. A failed send may have reached the server.
    pub fn end(self) -> Result<()> {
        if self.bytes.is_empty() {
            return Ok(());
        }
        self.client.insert_native_payload(
            &self.table,
            &self.bytes,
            self.schema.as_deref().unwrap_or(&[]),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::{HttpInserter, InserterPhase};
    use crate::{Block, Client, Endpoint};

    #[test]
    fn phase_follows_staged_bytes() {
        let endpoint = Endpoint::parse("http://127.0.0.1:1").expect("http endpoint");
        let mut client = Client::connect(endpoint, "default", "", "default").expect("http client");
        let mut staged = HttpInserter::new(&mut client, "events");
        assert_eq!(staged.phase(), InserterPhase::Idle);
        staged.write(&Block::new()).expect("empty block");
        assert_eq!(staged.phase(), InserterPhase::Idle);

        let mut block = Block::new();
        block
            .append_fixed_slice("id", "UInt64", &[7u64])
            .expect("one row");
        staged.write(&block).expect("stage one row");
        assert_eq!(staged.phase(), InserterPhase::Buffering);
    }
}
