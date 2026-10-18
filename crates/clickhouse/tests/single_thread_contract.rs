//! Compile-time integration contracts for thread-confined client state.

use clickhouse::{Client, ClientConfig, Endpoint, FailoverPolicy, NativeClient, sys::TlsSession};

trait AmbiguousSend<A> {
    fn verify() {}
}
impl<T: ?Sized> AmbiguousSend<()> for T {}
impl<T: ?Sized + Send> AmbiguousSend<u8> for T {}

trait AmbiguousSync<A> {
    fn verify() {}
}
impl<T: ?Sized> AmbiguousSync<()> for T {}
impl<T: ?Sized + Sync> AmbiguousSync<u8> for T {}

/// Thread transfer or sharing makes these trait selections ambiguous and fails compilation.
#[test]
fn integration_client_state_is_thread_confined() {
    macro_rules! assert_thread_confined {
        ($state:ty) => {
            let _ = <$state as AmbiguousSend<_>>::verify;
            let _ = <$state as AmbiguousSync<_>>::verify;
        };
    }
    assert_thread_confined!(Client);
    assert_thread_confined!(NativeClient);
    assert_thread_confined!(Endpoint);
    assert_thread_confined!(FailoverPolicy);
    assert_thread_confined!(ClientConfig<'static>);
    assert_thread_confined!(TlsSession);
}
