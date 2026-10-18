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
    /// Clear lengths using the default aggregate retention budget.
    fn clear(&mut self) {
        self.clear_with_policy(crate::BufferPolicy::default());
    }
    /// Exact aggregate capacity of the underlying Vec buffers in bytes.
    fn allocated_bytes(&self) -> usize;
    /// Clear lengths and reclaim burst capacity above the aggregate budget.
    fn clear_with_policy(&mut self, policy: crate::BufferPolicy);
    /// Clear lengths without reclaiming any capacity.
    fn clear_unbounded(&mut self) {
        self.clear_with_policy(crate::BufferPolicy::unbounded());
    }
    /// Release unused capacity while preserving current values.
    fn shrink_to_fit(&mut self);
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
