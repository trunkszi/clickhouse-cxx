//! The transport and streaming handles are strictly single-threaded.

use clickhouse::{
    BlockStream, Client, Inserter, NativeBlock, NativeClient, Query, WireBlock, WireColumn,
};

/// Fail to compile when the concrete type implements the named auto trait.
macro_rules! assert_not_impl {
    ($type:ty: $trait:path) => {
        const _: fn() = || {
            trait AmbiguousIfImpl<A> {
                fn some_item() {}
            }
            impl<T: ?Sized> AmbiguousIfImpl<()> for T {}
            impl<T: ?Sized + $trait> AmbiguousIfImpl<::core::marker::PhantomData<fn() -> T>> for T {}
            let _ = <$type as AmbiguousIfImpl<_>>::some_item;
        };
    };
}

assert_not_impl!(Client: Send);
assert_not_impl!(Client: Sync);
assert_not_impl!(NativeClient: Send);
assert_not_impl!(NativeClient: Sync);
assert_not_impl!(NativeBlock: Send);
assert_not_impl!(NativeBlock: Sync);
assert_not_impl!(WireBlock: Send);
assert_not_impl!(WireBlock: Sync);
assert_not_impl!(WireColumn: Send);
assert_not_impl!(WireColumn: Sync);
assert_not_impl!(BlockStream<'static>: Send);
assert_not_impl!(BlockStream<'static>: Sync);
assert_not_impl!(Inserter<'static>: Send);
assert_not_impl!(Inserter<'static>: Sync);
assert_not_impl!(Query<'static>: Send);
assert_not_impl!(Query<'static>: Sync);

#[test]
fn callbacks_accept_non_send_state() {
    let local = std::rc::Rc::new(0_u64);
    let _query = Query::new("SELECT 1").on_progress(move |_| {
        let _ = &local;
    });
}
