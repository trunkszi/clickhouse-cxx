//! Synchronous typed operations built on the shared block transport boundary.
use super::{BlockView, OwnedRow};
use crate::{Batch, Block, Result, Row, error::schema};
/// Block transports implement two primitives to obtain all typed row operations.
pub trait RowClient {
    /// Send one complete columnar block to the named table.
    /// # Errors
    /// Configuration, schema, transport, or server rejection returns an error.
    fn insert_block(&mut self, table: &str, block: &Block) -> Result<()>;
    /// Retrieve owned columnar blocks for a synchronous query.
    /// # Errors
    /// Transport, server, or decoding failures return an error.
    fn query_blocks(&mut self, sql: &str) -> Result<Vec<Block>>;
    /// Insert a reusable batch using its declared table name.
    /// # Errors
    /// Missing table metadata or block/transport errors reject the operation.
    fn insert_batch<B: Batch>(&mut self, batch: &B) -> Result<()> {
        let table = <B::Row as Row>::TABLE_NAME
            .ok_or_else(|| schema("Row requires table metadata for insertion"))?;
        let block = batch.to_block()?;
        if block.row_count() != 0 {
            self.insert_block(table, &block)?;
        }
        Ok(())
    }
    /// Insert rows through the same reusable column buffer implementation.
    /// # Errors
    /// Schema, table metadata, or transport errors reject insertion.
    fn insert_rows<R: Row>(&mut self, rows: &[R]) -> Result<()> {
        let mut batch = R::Batch::with_capacity(rows.len());
        batch.extend_from_slice(rows);
        self.insert_batch(&batch)
    }
    /// Visit checked projections while each backing block remains alive.
    /// # Errors
    /// Transport, schema, projection, or callback errors propagate unchanged.
    fn query_views<R, F>(&mut self, sql: &str, mut visit: F) -> Result<()>
    where
        R: Row,
        F: for<'a> FnMut(R::View<'a>) -> Result<()>,
    {
        for block in self.query_blocks(sql)? {
            visit(R::View::try_from_block(&block)?)?;
        }
        Ok(())
    }
    /// Materialize owned rows from the same checked borrowed projections.
    /// # Errors
    /// Transport, schema, projection, or invalid row shape returns an error.
    fn query_rows<R: OwnedRow>(&mut self, sql: &str) -> Result<Vec<R>> {
        let mut rows = Vec::new();
        self.query_views::<R, _>(sql, |view| {
            rows.reserve(view.len());
            for i in 0..view.len() {
                rows.push(R::from_ref(
                    view.get(i)
                        .ok_or_else(|| schema("validated view has a missing row"))?,
                ));
            }
            Ok(())
        })?;
        Ok(rows)
    }
    /// Insert serde-serializable rows by inferring one block column per field.
    /// # Errors
    /// Column inference, serialization, or transport failures return an error.
    fn insert_serde<T: serde::Serialize>(&mut self, table: &str, rows: &[T]) -> Result<()> {
        let block = crate::serde_bridge::SerdeBridge::rows_to_block(rows)?;
        if block.row_count() != 0 {
            self.insert_block(table, &block)?;
        }
        Ok(())
    }
    /// Query blocks and deserialize every row by matching column names.
    /// # Errors
    /// Transport failures or incompatible column shapes return an error.
    fn query_serde<T: serde::de::DeserializeOwned>(&mut self, sql: &str) -> Result<Vec<T>> {
        let blocks = self.query_blocks(sql)?;
        let mut rows = Vec::with_capacity(blocks.iter().map(Block::row_count).sum());
        for block in &blocks {
            rows.extend(crate::serde_bridge::SerdeBridge::block_to_rows(block)?);
        }
        Ok(rows)
    }
}
