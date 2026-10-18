//! Reusable column-oriented write buffers.
use crate::{Block, Result, Row};
/// A reusable SoA buffer for one derived row schema.
pub trait Batch: Default {
    type Row: Row;
    fn with_capacity(rows: usize) -> Self;
    fn len(&self) -> usize;
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
    fn clear(&mut self);
    /// Copy a borrowed row once into reusable column buffers.
    fn push(&mut self, row: &Self::Row);
    fn extend_from_slice(&mut self, rows: &[Self::Row]) {
        for row in rows {
            self.push(row);
        }
    }
    /// Materialize an owned block while retaining the batch buffers.
    /// # Errors
    /// Inconsistent column sizes or invalid data return a schema error.
    fn to_block(&self) -> Result<Block>;
}
