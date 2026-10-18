//! Capacity budgets for reusable buffers; byte counts describe Vec allocations,
//! excluding schema strings, Box headers, and allocator bookkeeping.

/// Warm capacity retained after clearing a batch or block.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BufferPolicy {
    /// Maximum retained row capacity.
    pub max_retained_rows: usize,
    /// Maximum aggregate retained Vec capacity in bytes.
    pub max_retained_bytes: usize,
}
impl Default for BufferPolicy {
    fn default() -> Self {
        Self {
            max_retained_rows: 65_536,
            max_retained_bytes: 8 * 1024 * 1024,
        }
    }
}
impl BufferPolicy {
    /// Retain all warmed allocations regardless of burst size.
    pub const fn unbounded() -> Self {
        Self {
            max_retained_rows: usize::MAX,
            max_retained_bytes: usize::MAX,
        }
    }
    /// Release all value-buffer capacity when cleared.
    pub const fn minimal() -> Self {
        Self {
            max_retained_rows: 0,
            max_retained_bytes: 0,
        }
    }
    /// Calculate a shared proportional budget without floating-point rounding.
    #[doc(hidden)]
    pub fn retention(self, rows: usize, bytes: usize) -> Retention {
        let mut result = Retention {
            numerator: 1,
            denominator: 1,
        };
        for (cap, used) in [
            (self.max_retained_rows, rows),
            (self.max_retained_bytes, bytes),
        ] {
            if used > cap
                && (cap as u128) * (result.denominator as u128)
                    < (result.numerator as u128) * (used as u128)
            {
                result = Retention {
                    numerator: cap,
                    denominator: used,
                };
            }
        }
        result
    }
}
/// A common scaling factor shared by every nested buffer in one owner.
#[doc(hidden)]
#[derive(Clone, Copy)]
pub struct Retention {
    numerator: usize,
    denominator: usize,
}
impl Retention {
    /// Floor the target so the sum of child budgets never exceeds the parent.
    pub fn target(self, capacity: usize) -> usize {
        ((capacity as u128 * self.numerator as u128) / self.denominator as u128) as usize
    }
}
/// Capacity control shared by fixed-width and recursive field buffers.
pub trait MemoryBuffer {
    /// Exact sum of underlying Vec capacities multiplied by element sizes.
    fn allocated_bytes(&self) -> usize;
    /// Row capacity, excluding the element count inside variable-length rows.
    fn row_capacity(&self) -> usize;
    /// Clear lengths and proportionally trim each underlying Vec.
    #[doc(hidden)]
    fn clear_retained(&mut self, retention: Retention);
    /// Release only unused capacity without discarding current values.
    fn shrink_to_fit(&mut self);
    /// Clear lengths using the balanced default warm-capacity budget.
    fn clear(&mut self) {
        self.clear_with_policy(BufferPolicy::default());
    }
    /// Clear lengths using one aggregate row and byte budget.
    fn clear_with_policy(&mut self, policy: BufferPolicy) {
        let retention = policy.retention(self.row_capacity(), self.allocated_bytes());
        self.clear_retained(retention);
    }
    /// Clear lengths without reclaiming warmed capacity.
    fn clear_unbounded(&mut self) {
        self.clear_with_policy(BufferPolicy::unbounded());
    }
}
impl<T> MemoryBuffer for Vec<T> {
    fn allocated_bytes(&self) -> usize {
        self.capacity().saturating_mul(size_of::<T>())
    }
    fn row_capacity(&self) -> usize {
        if size_of::<T>() == 0 {
            0
        } else {
            self.capacity()
        }
    }
    fn clear_retained(&mut self, retention: Retention) {
        let target = retention.target(self.row_capacity());
        self.clear();
        if target < self.capacity() {
            self.shrink_to(target);
        }
    }
    fn shrink_to_fit(&mut self) {
        Vec::shrink_to_fit(self);
    }
}
