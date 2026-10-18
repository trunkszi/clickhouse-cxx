//! Synchronous typed operations built on the shared block transport boundary.
use super::{BlockView, OwnedRow};
use crate::{Batch, Block, Result, Row, error::schema, serde_bridge::SerdeBridge};
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
            rows.extend(SerdeBridge::block_to_rows(block)?);
        }
        Ok(rows)
    }
    /// Visit each owned row without collecting a result vector.
    /// # Errors
    /// Transport, projection, materialization, or callback errors propagate.
    fn query_for_each<R, F>(&mut self, sql: &str, mut on_row: F) -> Result<usize>
    where
        R: OwnedRow,
        F: FnMut(R) -> Result<()>,
    {
        let mut count = 0;
        self.query_views::<R, _>(sql, |view| {
            for index in 0..view.len() {
                on_row(R::from_ref(
                    view.get(index)
                        .ok_or_else(|| schema("validated view has a missing row"))?,
                ))?;
                count += 1;
            }
            Ok(())
        })?;
        Ok(count)
    }
    /// Visit each borrowed row reference without materializing owned rows.
    /// # Errors
    /// Transport, projection, or callback errors propagate.
    fn query_row_refs<R, F>(&mut self, sql: &str, mut on_row: F) -> Result<usize>
    where
        R: Row,
        F: for<'a> FnMut(<R::View<'a> as BlockView<'a>>::Ref) -> Result<()>,
    {
        let mut count = 0;
        self.query_views::<R, _>(sql, |view| {
            for index in 0..view.len() {
                on_row(
                    view.get(index)
                        .ok_or_else(|| schema("validated view has a missing row"))?,
                )?;
                count += 1;
            }
            Ok(())
        })?;
        Ok(count)
    }
    /// Visit each serde row without collecting a result vector.
    /// # Errors
    /// Transport, decoding, or callback errors propagate.
    fn query_serde_for_each<T, F>(&mut self, sql: &str, mut on_row: F) -> Result<usize>
    where
        T: serde::de::DeserializeOwned,
        F: FnMut(T) -> Result<()>,
    {
        let blocks = self.query_blocks(sql)?;
        let mut count = 0;
        for block in &blocks {
            count += SerdeBridge::block_to_rows_with(block, &mut on_row)?;
        }
        Ok(count)
    }
    /// Append queried owned rows to `dst`.
    /// # Errors
    /// Transport, projection, materialization, or allocation errors propagate.
    fn query_rows_into<R: OwnedRow>(&mut self, sql: &str, dst: &mut Vec<R>) -> Result<usize> {
        let start = dst.len();
        self.query_for_each::<R, _>(sql, |row| {
            dst.push(row);
            Ok(())
        })?;
        Ok(dst.len() - start)
    }
    /// Write queried owned rows into a caller-provided slice.
    /// # Errors
    /// The destination must hold every returned row; a short slice is rejected.
    fn query_rows_into_slice<R: OwnedRow>(&mut self, sql: &str, dst: &mut [R]) -> Result<usize> {
        let blocks = self.query_blocks(sql)?;
        let total: usize = blocks.iter().map(Block::row_count).sum();
        if total > dst.len() {
            return Err(schema(
                "destination slice is too small for the query result",
            ));
        }
        let mut written = 0;
        for block in &blocks {
            let view = R::View::try_from_block(block)?;
            for index in 0..view.len() {
                dst[written] = R::from_ref(
                    view.get(index)
                        .ok_or_else(|| schema("validated view has a missing row"))?,
                );
                written += 1;
            }
        }
        Ok(written)
    }
    /// Clear and refill a caller-provided reusable batch.
    /// # Errors
    /// Transport, projection, or batch capacity errors propagate.
    fn query_batch_into<R: Row>(&mut self, sql: &str, dst: &mut R::Batch) -> Result<usize> {
        dst.clear();
        let mut count = 0;
        self.query_views::<R, _>(sql, |view| {
            for index in 0..view.len() {
                dst.push_ref(
                    view.get(index)
                        .ok_or_else(|| schema("validated view has a missing row"))?,
                );
                count += 1;
            }
            Ok(())
        })?;
        Ok(count)
    }
    /// Refill a caller-provided reusable block in place.
    /// # Errors
    /// The query must return at most one non-empty block with the expected schema.
    fn query_block_into<R: Row>(&mut self, sql: &str, dst: &mut Block) -> Result<usize> {
        let mut blocks = self.query_blocks(sql)?;
        let mut nonempty = blocks.drain(..).filter(|block| block.row_count() != 0);
        let Some(block) = nonempty.next() else {
            dst.clear();
            return Ok(0);
        };
        if nonempty.next().is_some() {
            return Err(schema(
                "query returned multiple non-empty blocks for one destination",
            ));
        }
        R::View::try_from_block(&block)?;
        let rows = block.row_count();
        if dst.column_count() == 0 && dst.row_count() == 0 {
            *dst = block;
        } else {
            dst.refill_from(&block)?;
        }
        Ok(rows)
    }
    /// Append serde rows to `dst` without an intermediate vector.
    /// # Errors
    /// Transport, decoding, or callback errors propagate.
    fn query_serde_into<T: serde::de::DeserializeOwned>(
        &mut self,
        sql: &str,
        dst: &mut Vec<T>,
    ) -> Result<usize> {
        let start = dst.len();
        self.query_serde_for_each::<T, _>(sql, |row| {
            dst.push(row);
            Ok(())
        })?;
        Ok(dst.len() - start)
    }
    /// Write serde rows into a caller-provided slice.
    /// # Errors
    /// The destination must hold every returned row; a short slice is rejected.
    fn query_serde_into_slice<T: serde::de::DeserializeOwned>(
        &mut self,
        sql: &str,
        dst: &mut [T],
    ) -> Result<usize> {
        let blocks = self.query_blocks(sql)?;
        let total: usize = blocks.iter().map(Block::row_count).sum();
        if total > dst.len() {
            return Err(schema(
                "destination slice is too small for the query result",
            ));
        }
        let mut written = 0;
        for block in &blocks {
            SerdeBridge::block_to_rows_with(block, |row| {
                dst[written] = row;
                written += 1;
                Ok(())
            })?;
        }
        Ok(written)
    }
}

impl RowClient for crate::Client {
    fn insert_block(&mut self, table: &str, block: &Block) -> Result<()> {
        self.insert_owned_block(table, block)
    }
    fn query_blocks(&mut self, sql: &str) -> Result<Vec<Block>> {
        self.query_owned_blocks(sql)
    }
}
