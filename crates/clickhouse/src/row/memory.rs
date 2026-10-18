//! Aggregate retention for flat and recursive SoA field buffers.
use super::{Field, FlatArray, FlatBytes, MapBuffer, NullableBuffer};
use crate::{BufferPolicy, MemoryBuffer, buffer_policy::Retention};

macro_rules! buffer_memory {
    ([$($generics:tt)*] $ty:ty; $rows:ident; $($field:ident),+) => {
        impl<$($generics)*> MemoryBuffer for $ty {
            fn allocated_bytes(&self) -> usize { 0 $(+ self.$field.allocated_bytes())+ }
            fn row_capacity(&self) -> usize { self.$rows.capacity() }
            fn clear_retained(&mut self, retention: Retention) {
                $(self.$field.clear_retained(retention);)+
            }
            fn shrink_to_fit(&mut self) { $(self.$field.shrink_to_fit();)+ }
        }
        impl<$($generics)*> $ty {
            /// Exact aggregate Vec capacity in bytes, including nested values.
            pub fn allocated_bytes(&self) -> usize { MemoryBuffer::allocated_bytes(self) }
            /// Clear lengths while retaining capacity within the default budget.
            pub fn clear(&mut self) { MemoryBuffer::clear(self); }
            /// Clear lengths and trim excess capacity using an aggregate budget.
            pub fn clear_with_policy(&mut self, policy: BufferPolicy) { MemoryBuffer::clear_with_policy(self, policy); }
            /// Clear lengths while retaining all warmed capacity.
            pub fn clear_unbounded(&mut self) { MemoryBuffer::clear_unbounded(self); }
            /// Release unused capacity while preserving current values.
            pub fn shrink_to_fit(&mut self) { MemoryBuffer::shrink_to_fit(self); }
        }
    };
}
buffer_memory!([] FlatBytes; offsets; offsets, bytes);
buffer_memory!([T] FlatArray<T>; offsets; offsets, values);
buffer_memory!([T: Field] NullableBuffer<T>; null_map; null_map, values);
buffer_memory!([K: Field, V: Field] MapBuffer<K,V>; offsets; offsets, keys, values);
