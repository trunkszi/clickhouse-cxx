//! Explicit HTTP format operations over the shared synchronous client.
use super::{Backend, ensure_format, quote_table};
#[cfg(feature = "tcp")]
use crate::Error;
use crate::{ArrowBlockView, Client, Query, Result};

impl Client {
    fn require_http_format(&self) -> Result<()> {
        match &self.backend {
            Backend::Http(_) => Ok(()),
            #[cfg(feature = "tcp")]
            Backend::Native(_) => Err(Error::unsupported(
                "HTTP format operations require an HTTP endpoint",
            )),
        }
    }

    /// Insert serde rows using ClickHouse RowBinary over HTTP.
    ///
    /// Serialization and transport buffering may allocate; no derived Row metadata is required.
    /// # Errors
    /// Native endpoints, unsupported serde values, and server rejections return errors.
    pub fn insert_rowbinary<T: serde::Serialize>(&mut self, table: &str, rows: &[T]) -> Result<()> {
        self.require_http_format()?;
        self.serde_insert_rows(table, rows)
    }

    /// Materialize serde rows from HTTP RowBinaryWithNamesAndTypes metadata.
    /// # Errors
    /// Native endpoints, unsupported query metadata, incompatible rows, and transfer errors
    /// propagate.
    pub fn query_rowbinary<T: serde::de::DeserializeOwned>(
        &mut self,
        query: &Query<'_>,
    ) -> Result<Vec<T>> {
        self.require_http_format()?;
        self.serde_query_rows(query)
    }

    /// Insert an encoded Arrow IPC stream into a validated table name over HTTP.
    /// # Errors
    /// Native endpoints, invalid identifiers, malformed IPC, and server rejection return errors.
    pub fn insert_arrow_stream(&mut self, table: &str, bytes: &[u8]) -> Result<()> {
        self.require_http_format()?;
        let sql = format!("INSERT INTO {} FORMAT ArrowStream", quote_table(table)?);
        self.insert_formatted(&Query::new(sql), bytes)
    }

    /// Fetch ArrowStream over HTTP and decode owned batches with borrowed projection access.
    ///
    /// HTTP buffering and IPC decoding may allocate; projections borrow the decoded arrays.
    /// # Errors
    /// Native endpoints, unsupported metadata, malformed IPC, and transport failures return errors.
    pub fn query_arrow_stream(&mut self, query: &Query<'_>) -> Result<Vec<ArrowBlockView>> {
        self.require_http_format()?;
        let sql = ensure_format(query.sql(), "ArrowStream");
        let bytes = self.http_call(query, &sql, &[])?;
        ArrowBlockView::decode_stream(&bytes)
    }
}
