//! Buffered block iteration with an exclusive client lifetime.
use crate::{Block, Client, Result};
/// Fully buffered results; constructing the iterator drains the server response.
/// Dropping early releases unread local blocks and never leaves a wire query pending.
pub struct HttpBlockStream<'a> {
    pub(crate) client: &'a mut Client,
    pub(crate) blocks: std::vec::IntoIter<Block>,
}
impl Iterator for HttpBlockStream<'_> {
    type Item = Result<Block>;
    fn next(&mut self) -> Option<Self::Item> {
        self.blocks.next().map(Ok)
    }
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.blocks.size_hint()
    }
}
impl ExactSizeIterator for HttpBlockStream<'_> {}
impl HttpBlockStream<'_> {
    /// Endpoint whose completed response backs this local iterator.
    pub fn endpoint(&self) -> &crate::Endpoint {
        self.client.endpoint()
    }
}
