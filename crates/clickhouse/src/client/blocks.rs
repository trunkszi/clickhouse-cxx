//! Common owned block operations across Native and HTTP transports.
use super::{Backend, Client};
use crate::{Block, Query, Result};

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
                let sql = super::ensure_format(query.sql(), "Native")?;
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
    /// the response before delivering decoded blocks; use raw callbacks for byte streaming.
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
