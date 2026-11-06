//! I/O diagnostics are constructed outside the session data path.

use std::io;

use uarch::prelude::*;

type BoxError = Box<dyn std::error::Error + Send + Sync>;

#[cold_sink]
pub(super) fn other(error: impl Into<BoxError>) -> io::Error {
    io::Error::other(error)
}

#[cold_sink]
pub(super) fn invalid_data(error: impl Into<BoxError>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, error)
}

#[cold_sink]
pub(super) fn message(kind: io::ErrorKind, message: &'static str) -> io::Error {
    io::Error::new(kind, message)
}

#[cold_sink]
pub(super) fn errno(code: i32) -> io::Error {
    io::Error::from_raw_os_error(code)
}

#[cold_sink]
pub(super) fn dns_failure(host: &str, status: &str) -> io::Error {
    io::Error::new(
        io::ErrorKind::NotFound,
        format!("DNS resolution failed for {host}: {status}"),
    )
}

#[cold_sink]
pub(super) fn dns_status(status: impl std::fmt::Debug) -> String {
    format!("{status:?}")
}

#[cold_sink]
pub(super) fn invalid_server_name(name: &str) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidInput,
        format!("invalid server name `{name}`"),
    )
}

#[cold_sink]
pub(super) fn certificate_store(errors: impl std::fmt::Debug) -> io::Error {
    invalid_data(format!("native certificate store load failed: {errors:?}"))
}
