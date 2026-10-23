//! Shared block, typed row, and serde operations over the selected transport.
#[cfg(feature = "http")]
use super::http::{ensure_format, quote_ident, quote_table};
use super::{Backend, Client};
#[cfg(feature = "http")]
use crate::Error;
use crate::{
    Batch, Block, Query, Result, Row,
    row::{BlockView, OwnedRow, RowClient},
    serde_bridge::SerdeBridge,
};

impl Client {
    /// Read the first available UInt64 cell through the selected transport.
    /// # Errors
    /// Query failures, wrong types, or missing cells return an error.
    pub fn query_u64(&mut self, sql: &str) -> Result<u64> {
        match &mut self.backend {
            #[cfg(feature = "tcp")]
            Backend::Native(session) => session.query_u64(sql),
            #[cfg(feature = "http")]
            Backend::Http(_) => {
                let blocks = self.query_owned_blocks(sql)?;
                for block in &blocks {
                    if block.row_count() > 0 {
                        let column = block.column(0)?;
                        if column.type_name.as_ref() != "UInt64"
                            || column.len() != block.row_count()
                        {
                            return Err(Error::block(
                                "query_u64 requires a complete UInt64 first column",
                            ));
                        }
                        return block
                            .column_slice::<u64>(0)?
                            .first()
                            .copied()
                            .ok_or_else(|| Error::native("query returned no UInt64 cell"));
                    }
                }
                Err(Error::native("query returned no UInt64 cell"))
            }
        }
    }
    /// Insert serde rows without changing the derived RowClient API.
    /// # Errors
    /// Unsupported serde shapes, schema mismatch, or server errors reject insertion.
    pub fn serde_insert_rows<T: serde::Serialize>(
        &mut self,
        table: &str,
        rows: &[T],
    ) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }
        match &self.backend {
            #[cfg(feature = "tcp")]
            Backend::Native(_) => {
                self.insert_owned_block(table, &SerdeBridge::rows_to_block(rows)?)
            }
            #[cfg(feature = "http")]
            Backend::Http(_) => {
                let (names, body) = SerdeBridge::to_rowbinary(rows)?;
                let names = names
                    .iter()
                    .map(|n| quote_ident(n))
                    .collect::<Result<Vec<_>>>()?
                    .join(", ");
                let sql = format!(
                    "INSERT INTO {} ({names}) FORMAT RowBinary",
                    quote_table(table)?
                );
                self.http_call(&Query::new(""), &sql, &body)?;
                Ok(())
            }
        }
    }
    /// Materialize serde rows from HTTP RowBinary or shared Native columns.
    /// # Errors
    /// Invalid response shapes, unsupported settings, or transport failures propagate.
    pub fn serde_query_rows<T: serde::de::DeserializeOwned>(
        &mut self,
        query: &Query<'_>,
    ) -> Result<Vec<T>> {
        match &self.backend {
            #[cfg(feature = "http")]
            Backend::Http(_) => {
                let sql = ensure_format(query.sql(), "RowBinaryWithNamesAndTypes")?;
                let bytes = self.http_call(query, &sql, &[])?;
                SerdeBridge::block_to_rows(&crate::native_format::decode_rowbinary(&bytes)?)
            }
            #[cfg(feature = "tcp")]
            Backend::Native(_) => {
                let mut rows = Vec::new();
                for block in self.query_blocks(query.reborrow())? {
                    rows.extend(SerdeBridge::block_to_rows(&block)?);
                }
                Ok(rows)
            }
        }
    }
    pub(crate) fn insert_owned_block(&mut self, table: &str, block: &Block) -> Result<()> {
        match &mut self.backend {
            #[cfg(feature = "tcp")]
            Backend::Native(session) => {
                let mut native = block.to_native()?;
                session.insert_block(table, &mut native)
            }
            #[cfg(feature = "http")]
            Backend::Http(_) => {
                if block.row_count() == 0 {
                    return Ok(());
                }
                let mut names = Vec::new();
                for i in 0..block.column_count() {
                    names.push(quote_ident(&block.column(i)?.name)?);
                }
                let sql = format!(
                    "INSERT INTO {} ({}) FORMAT Native",
                    quote_table(table)?,
                    names.join(", ")
                );
                let mut body = Vec::new();
                crate::native_format::encode_block(block, 0, &mut body)?;
                self.http_call(&Query::new(""), &sql, &body)?;
                Ok(())
            }
        }
    }
    pub(crate) fn query_owned_blocks(&mut self, sql: &str) -> Result<Vec<Block>> {
        self.query_blocks(sql)
    }
}

impl Client {
    /// Insert a shared columnar block through the selected transport.
    /// # Errors
    /// Invalid layouts, unsupported conversions, or server rejection propagate.
    pub fn insert_block(&mut self, table: &str, block: &Block) -> Result<()> {
        self.insert_owned_block(table, block)
    }

    /// Collect shared blocks while preserving complete Native query metadata.
    /// # Errors
    /// Transport, query metadata, conversion, or server failures propagate.
    pub fn query_blocks<'a>(&mut self, query: impl Into<Query<'a>>) -> Result<Vec<Block>> {
        let query = query.into();
        match &mut self.backend {
            #[cfg(feature = "tcp")]
            Backend::Native(session) => session
                .query_blocks(query)?
                .iter()
                .filter(|block| block.rows() != 0)
                .map(Block::from_native)
                .collect(),
            #[cfg(feature = "http")]
            Backend::Http(_) => {
                let sql = ensure_format(query.sql(), "Native")?;
                let bytes = self.http_call(&query, &sql, &[])?;
                Ok(crate::native_format::decode_blocks(&bytes, 0)?
                    .into_iter()
                    .filter(|block| block.row_count() != 0)
                    .collect())
            }
        }
    }

    /// Deliver shared blocks to a caller callback.
    ///
    /// Native streams blocks from the socket. HTTP currently buffers and validates
    /// the response before delivering decoded blocks. Raw callbacks deliver decoded
    /// byte chunks after the encoded response has been collected.
    /// # Errors
    /// Query, conversion, and callback errors propagate.
    pub fn query_blocks_for_each<'a, F>(
        &mut self,
        query: impl Into<Query<'a>>,
        mut on_block: F,
    ) -> Result<usize>
    where
        F: FnMut(Block) -> Result<()>,
    {
        match &mut self.backend {
            #[cfg(feature = "tcp")]
            Backend::Native(session) => {
                let mut count = 0;
                session.query_blocks_for_each(query, |block| {
                    if block.rows() != 0 {
                        on_block(Block::from_native(&block)?)?;
                        count += 1;
                    }
                    Ok(())
                })?;
                Ok(count)
            }
            #[cfg(feature = "http")]
            Backend::Http(_) => {
                let mut count = 0;
                for block in self.query_blocks(query)? {
                    on_block(block)?;
                    count += 1;
                }
                Ok(count)
            }
        }
    }

    /// Append shared result blocks to reusable caller-owned storage.
    /// # Errors
    /// Query, conversion, and transport failures propagate; a prefix may remain.
    pub fn query_blocks_into<'a>(
        &mut self,
        query: impl Into<Query<'a>>,
        dst: &mut Vec<Block>,
    ) -> Result<usize> {
        self.query_blocks_for_each(query, |block| {
            dst.push(block);
            Ok(())
        })
    }
}

impl Client {
    /// Apply the shared typed row contract through the selected transport.
    /// # Errors
    /// Transport, schema, projection, and destination-bound failures propagate.
    pub fn insert_batch<B: Batch>(&mut self, batch: &B) -> Result<()> {
        RowClient::insert_batch(self, batch)
    }

    /// Apply the shared typed row contract through the selected transport.
    /// # Errors
    /// Transport, schema, projection, and destination-bound failures propagate.
    pub fn insert_rows<R: Row>(&mut self, rows: &[R]) -> Result<()> {
        RowClient::insert_rows(self, rows)
    }

    /// Apply the shared typed row contract through the selected transport.
    /// # Errors
    /// Transport, schema, projection, and destination-bound failures propagate.
    pub fn query_rows<R: OwnedRow>(&mut self, sql: &str) -> Result<Vec<R>> {
        RowClient::query_rows(self, sql)
    }

    /// Apply the shared typed row contract through the selected transport.
    /// # Errors
    /// Transport, schema, projection, and destination-bound failures propagate.
    pub fn query_rows_into<R: OwnedRow>(&mut self, sql: &str, dst: &mut Vec<R>) -> Result<usize> {
        RowClient::query_rows_into(self, sql, dst)
    }

    /// Apply the shared typed row contract through the selected transport.
    /// # Errors
    /// Transport, schema, projection, and destination-bound failures propagate.
    pub fn query_rows_into_slice<R: OwnedRow>(
        &mut self,
        sql: &str,
        dst: &mut [R],
    ) -> Result<usize> {
        RowClient::query_rows_into_slice(self, sql, dst)
    }

    /// Apply the shared typed row contract through the selected transport.
    /// # Errors
    /// Transport, schema, projection, and destination-bound failures propagate.
    pub fn query_batch_into<R: Row>(&mut self, sql: &str, dst: &mut R::Batch) -> Result<usize> {
        RowClient::query_batch_into::<R>(self, sql, dst)
    }

    /// Apply the shared typed row contract through the selected transport.
    /// # Errors
    /// Transport, schema, projection, and destination-bound failures propagate.
    pub fn query_block_into<R: Row>(&mut self, sql: &str, dst: &mut Block) -> Result<usize> {
        RowClient::query_block_into::<R>(self, sql, dst)
    }

    /// Apply the shared typed row contract through the selected transport.
    /// # Errors
    /// Transport, schema, projection, and destination-bound failures propagate.
    pub fn insert_serde<T: serde::Serialize>(&mut self, table: &str, rows: &[T]) -> Result<()> {
        RowClient::insert_serde(self, table, rows)
    }

    /// Apply the shared typed row contract through the selected transport.
    /// # Errors
    /// Transport, schema, projection, and destination-bound failures propagate.
    pub fn query_serde<T: serde::de::DeserializeOwned>(&mut self, sql: &str) -> Result<Vec<T>> {
        RowClient::query_serde(self, sql)
    }

    /// Apply the shared typed row contract through the selected transport.
    /// # Errors
    /// Transport, schema, projection, and destination-bound failures propagate.
    pub fn query_serde_into<T: serde::de::DeserializeOwned>(
        &mut self,
        sql: &str,
        dst: &mut Vec<T>,
    ) -> Result<usize> {
        RowClient::query_serde_into(self, sql, dst)
    }

    /// Apply the shared typed row contract through the selected transport.
    /// # Errors
    /// Transport, schema, projection, and destination-bound failures propagate.
    pub fn query_serde_into_slice<T: serde::de::DeserializeOwned>(
        &mut self,
        sql: &str,
        dst: &mut [T],
    ) -> Result<usize> {
        RowClient::query_serde_into_slice(self, sql, dst)
    }

    /// Visit typed query results while preserving callback errors.
    /// # Errors
    /// Transport, projection, and callback failures propagate.
    pub fn query_views<R, F>(&mut self, sql: &str, callback: F) -> Result<()>
    where
        R: Row,
        F: for<'a> FnMut(R::View<'a>) -> Result<()>,
    {
        RowClient::query_views::<R, F>(self, sql, callback)
    }

    /// Visit typed query results while preserving callback errors.
    /// # Errors
    /// Transport, projection, and callback failures propagate.
    pub fn query_for_each<R, F>(&mut self, sql: &str, callback: F) -> Result<usize>
    where
        R: OwnedRow,
        F: FnMut(R) -> Result<()>,
    {
        RowClient::query_for_each::<R, F>(self, sql, callback)
    }

    /// Visit typed query results while preserving callback errors.
    /// # Errors
    /// Transport, projection, and callback failures propagate.
    pub fn query_row_refs<R, F>(&mut self, sql: &str, callback: F) -> Result<usize>
    where
        R: Row,
        F: for<'a> FnMut(<R::View<'a> as BlockView<'a>>::Ref) -> Result<()>,
    {
        RowClient::query_row_refs::<R, F>(self, sql, callback)
    }

    /// Visit deserialized rows without a collected output vector.
    /// # Errors
    /// Transport, decoding, and callback failures propagate.
    pub fn query_serde_for_each<T, F>(&mut self, sql: &str, callback: F) -> Result<usize>
    where
        T: serde::de::DeserializeOwned,
        F: FnMut(T) -> Result<()>,
    {
        RowClient::query_serde_for_each::<T, F>(self, sql, callback)
    }
}
