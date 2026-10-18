//! Explicit ownership conversion for callers requesting owned query results.
use std::collections::{BTreeMap, HashMap};

use super::{BlockView, Field, Fixed, Row};
/// Materialize a field from its borrowed projection, allocating only for owned values.
pub trait OwnedField: Field {
    fn owned(value: Self::Input<'_>) -> Self;
}
macro_rules! owned_fixed {($($ty:ty),*)=>{$(impl OwnedField for $ty{fn owned(v:Self)->Self{v}})*};}
owned_fixed!(
    u8, u16, u32, u64, u128, i8, i16, i32, i64, i128, f32, f64, bool
);
impl OwnedField for String {
    fn owned(v: &str) -> Self {
        String::from(v)
    }
}
impl<T: Fixed> OwnedField for Vec<T> {
    fn owned(v: &[T]) -> Self {
        v.to_vec()
    }
}
impl<T: OwnedField> OwnedField for Option<T> {
    fn owned(v: Self::Input<'_>) -> Self {
        v.map(T::owned)
    }
}
impl<K: OwnedField + Ord, V: OwnedField> OwnedField for BTreeMap<K, V> {
    fn owned(v: Self::Input<'_>) -> Self {
        v.iter().map(|(k, v)| (K::owned(k), V::owned(v))).collect()
    }
}
impl<K: OwnedField + Eq + std::hash::Hash, V: OwnedField> OwnedField for HashMap<K, V> {
    fn owned(v: Self::Input<'_>) -> Self {
        v.iter().map(|(k, v)| (K::owned(k), V::owned(v))).collect()
    }
}
/// A derived row that does not require borrowed input lifetimes.
pub trait OwnedRow: Row {
    /// Materialize a row from checked borrowed fields.
    fn from_ref<'a>(value: <Self::View<'a> as BlockView<'a>>::Ref) -> Self;
}
