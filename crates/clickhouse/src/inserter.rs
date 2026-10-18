//! Explicitly finalized inserts with owned encoded staging buffers.
use crate::{Block, Client, Error, Result, native_format::encode_block};
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
        self.client.insert_native_payload(&self.table, &self.bytes)
    }
}
