//! Native socket bytes move only through completion-driven ring operations.
use std::{
    io::{self, Read, Write},
    os::fd::{AsRawFd, FromRawFd, OwnedFd},
    time::{Duration, Instant},
};

use uarch::prelude::*;

use super::{
    driver::{self, DMA_BYTES, Operation},
    error::{message, other},
};

bitword! {
    struct StreamStateWord(u64) {
        [0] CLOSED,
        [1] QUARANTINED,
        [2] READ_DEADLINE,
        [3] WRITE_DEADLINE,
    }
}
assert_layout!(
    StreamStateWord,
    size = 8,
    align = std::mem::align_of::<u64>()
);

pub(super) struct ProactorStream {
    fd: Option<OwnedFd>,
    read_budget: Duration,
    write_budget: Duration,
    state: StreamStateWord,
}

impl ProactorStream {
    #[hot_inline]
    pub fn set_budgets(&mut self, read: Duration, write: Duration) {
        self.read_budget = read;
        self.write_budget = write;
        self.state = self.state.patch(
            BitPatch::EMPTY
                .with_flag(StreamStateWord::READ_DEADLINE, !read.is_zero())
                .with_flag(StreamStateWord::WRITE_DEADLINE, !write.is_zero()),
        );
    }

    #[hot_inline]
    fn available(&self) -> bool {
        self.state
            .matches(pattern!(StreamStateWord { !CLOSED & !QUARANTINED }))
    }

    #[hot_inline]
    pub fn socket_ref(&self) -> io::Result<socket2::SockRef<'_>> {
        if !self.available() {
            return Err(message(
                io::ErrorKind::NotConnected,
                "native socket quarantined",
            ));
        }
        self.fd
            .as_ref()
            .map(socket2::SockRef::from)
            .ok_or_else(|| message(io::ErrorKind::NotConnected, "native socket quarantined"))
    }

    pub fn dial(host: &str, port: u16, budget: Duration) -> io::Result<Self> {
        let budget = if budget.is_zero() {
            Duration::from_secs(5)
        } else {
            budget
        };
        let deadline = Instant::now().checked_add(budget).ok_or_else(|| {
            message(
                io::ErrorKind::InvalidInput,
                "native connect budget exceeds clock range",
            )
        })?;
        driver::with(|driver| {
            let addresses = driver.resolve(host, port, deadline)?;
            let mut failure = message(io::ErrorKind::TimedOut, "native connect deadline exceeded");
            for address in addresses {
                let remaining = deadline.saturating_duration_since(Instant::now());
                if remaining.is_zero() {
                    break;
                }
                let family = if address.is_ipv4() {
                    libc::AF_INET
                } else {
                    libc::AF_INET6
                };
                // SAFETY: Socket creation returns a new descriptor or a negative errno result.
                let raw = unsafe {
                    libc::socket(
                        family,
                        libc::SOCK_STREAM | libc::SOCK_NONBLOCK | libc::SOCK_CLOEXEC,
                        libc::IPPROTO_TCP,
                    )
                };
                if raw < 0 {
                    failure = io::Error::last_os_error();
                    continue;
                }
                // SAFETY: The successful socket call transferred exclusive descriptor ownership.
                let fd = unsafe { OwnedFd::from_raw_fd(raw) };
                driver.peer = Some(Box::new(socket2::SockAddr::from(address)));
                match driver.perform(Operation::Connect(raw), remaining) {
                    Ok(_) => {
                        driver.peer = None;
                        return Ok(Self {
                            fd: Some(fd),
                            read_budget: Duration::ZERO,
                            write_budget: Duration::ZERO,
                            state: StreamStateWord::new(0),
                        });
                    }
                    Err(error) => {
                        if driver.quarantined() {
                            std::mem::forget(fd);
                            return Err(error);
                        }
                        driver.peer = None;
                        if driver.closed() {
                            return Err(error);
                        }
                        failure = error;
                    }
                }
            }
            Err(failure)
        })
    }

    fn transfer(&mut self, input: Option<&[u8]>, output: &mut [u8]) -> io::Result<usize> {
        if !self.available() {
            return Err(message(
                io::ErrorKind::NotConnected,
                "native socket quarantined",
            ));
        }
        let fd = self
            .fd
            .as_ref()
            .ok_or_else(|| message(io::ErrorKind::NotConnected, "native socket quarantined"))?
            .as_raw_fd();
        let size = input.map_or(output.len(), <[u8]>::len).min(DMA_BYTES);
        let budget = if input.is_some() {
            if self
                .state
                .matches(pattern!(StreamStateWord { WRITE_DEADLINE }))
            {
                self.write_budget
            } else {
                Duration::ZERO
            }
        } else if self
            .state
            .matches(pattern!(StreamStateWord { READ_DEADLINE }))
        {
            self.read_budget
        } else {
            Duration::ZERO
        };
        let (result, quarantine, closed) = driver::with(|driver| {
            if let Some(input) = input {
                driver
                    .dma
                    .as_mut()
                    .ok_or_else(|| other("native DMA unavailable"))?[..size]
                    .copy_from_slice(&input[..size]);
            }
            let operation = if input.is_some() {
                Operation::Send(fd, size)
            } else {
                Operation::Receive(fd, size)
            };
            let result = driver.perform(operation, budget);
            if input.is_none()
                && let Ok(count) = result.as_ref()
            {
                if *count > size {
                    return Err(other("native receive exceeded DMA budget"));
                }
                output[..*count].copy_from_slice(
                    &driver
                        .dma
                        .as_ref()
                        .ok_or_else(|| other("native DMA unavailable"))?[..*count],
                );
            }
            Ok((result, driver.quarantined(), driver.closed()))
        })?;
        if closed {
            self.state = self.state.patch(patch!(StreamStateWord { CLOSED: true }));
            let descriptor = self.fd.take();
            if quarantine {
                self.state = self
                    .state
                    .patch(patch!(StreamStateWord { QUARANTINED: true }));
                std::mem::forget(descriptor);
            }
        }
        result
    }
}

impl Read for ProactorStream {
    #[hot_inline]
    fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        if bytes.is_empty() {
            return Ok(0);
        }
        self.transfer(None, bytes)
    }
}
impl Write for ProactorStream {
    #[hot_inline]
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.is_empty() {
            return Ok(0);
        }
        self.transfer(Some(bytes), &mut [])
    }
    #[hot_inline]
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
#[path = "../../tests/session/stream.rs"]
mod tests;
