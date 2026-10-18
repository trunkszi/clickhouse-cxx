//! Inherent typed conveniences over the shared RowClient contract.
use crate::{
    Batch, Block, Client, Result, Row,
    row::{BlockView, OwnedRow, RowClient},
};

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
