//! Native socket bytes move only through completion-driven ring operations.
use std::{
    io::{self, Read, Write},
    os::fd::{AsFd, AsRawFd, BorrowedFd, FromRawFd, OwnedFd},
    time::{Duration, Instant},
};

use super::driver::{self, DMA_BYTES, Operation};

pub(super) struct ProactorStream {
    fd: Option<OwnedFd>,
    pub read_budget: Duration,
    pub write_budget: Duration,
}

impl ProactorStream {
    pub fn dial(host: &str, port: u16, budget: Duration) -> io::Result<Self> {
        let budget = if budget.is_zero() {
            Duration::from_secs(5)
        } else {
            budget
        };
        let deadline = Instant::now().checked_add(budget).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "native connect budget exceeds clock range",
            )
        })?;
        driver::with(|driver| {
            let addresses = driver.resolve(host, port, deadline)?;
            let mut failure =
                io::Error::new(io::ErrorKind::TimedOut, "native connect deadline exceeded");
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
                        });
                    }
                    Err(error) => {
                        if driver.quarantined {
                            std::mem::forget(fd);
                            return Err(error);
                        }
                        driver.peer = None;
                        if driver.closed {
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
        let fd = self
            .fd
            .as_ref()
            .ok_or_else(|| {
                io::Error::new(io::ErrorKind::NotConnected, "native socket quarantined")
            })?
            .as_raw_fd();
        let size = input.map_or(output.len(), <[u8]>::len).min(DMA_BYTES);
        let budget = if input.is_some() {
            self.write_budget
        } else {
            self.read_budget
        };
        let (result, quarantine, closed) = driver::with(|driver| {
            if let Some(input) = input {
                driver
                    .dma
                    .as_mut()
                    .ok_or_else(|| io::Error::other("native DMA unavailable"))?[..size]
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
                    return Err(io::Error::other("native receive exceeded DMA budget"));
                }
                output[..*count].copy_from_slice(
                    &driver
                        .dma
                        .as_ref()
                        .ok_or_else(|| io::Error::other("native DMA unavailable"))?[..*count],
                );
            }
            Ok((result, driver.quarantined, driver.closed))
        })?;
        if closed {
            let descriptor = self.fd.take();
            if quarantine {
                std::mem::forget(descriptor);
            }
        }
        result
    }
}

impl AsFd for ProactorStream {
    fn as_fd(&self) -> BorrowedFd<'_> {
        self.fd.as_ref().expect("live native descriptor").as_fd()
    }
}
impl Read for ProactorStream {
    fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        if bytes.is_empty() {
            return Ok(0);
        }
        self.transfer(None, bytes)
    }
}
impl Write for ProactorStream {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.is_empty() {
            return Ok(0);
        }
        self.transfer(Some(bytes), &mut [])
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    //! Native transfers reuse stable DMA storage; ring failure closes affected streams.
    use std::{net::TcpListener, thread};

    use super::*;

    #[test]
    fn integration_lifecycle_dma_reused_across_large_transfers_and_connections() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let payload: Vec<u8> = (0..=250).cycle().take(1024 * 1024 + 257).collect();
        let peer_payload = payload.clone();
        let peer = thread::spawn(move || {
            for _ in 0..2 {
                let (mut socket, _) = listener.accept().unwrap();
                socket
                    .set_read_timeout(Some(Duration::from_secs(5)))
                    .unwrap();
                socket
                    .set_write_timeout(Some(Duration::from_secs(5)))
                    .unwrap();
                let mut received = vec![0; peer_payload.len()];
                socket.read_exact(&mut received).unwrap();
                assert_eq!(received, peer_payload);
                socket.write_all(&received).unwrap();
            }
        });
        let allocation = driver::with(|driver| {
            let dma = driver.dma.as_ref().unwrap();
            assert_eq!(dma.len(), 1024 * 1024);
            Ok(dma.as_ptr())
        })
        .unwrap();
        for _ in 0..2 {
            let mut stream =
                ProactorStream::dial("127.0.0.1", port, Duration::from_secs(5)).unwrap();
            stream.read_budget = Duration::from_secs(5);
            stream.write_budget = Duration::from_secs(5);
            let first = stream.write(&payload).unwrap();
            assert!(first > 0 && first <= 1024 * 1024);
            stream.write_all(&payload[first..]).unwrap();
            let mut received = vec![0; payload.len()];
            stream.read_exact(&mut received).unwrap();
            assert_eq!(received, payload);
            drop(stream);
            driver::with(|driver| {
                let dma = driver.dma.as_ref().unwrap();
                assert_eq!(dma.len(), 1024 * 1024);
                assert_eq!(dma.as_ptr(), allocation);
                Ok(())
            })
            .unwrap();
        }
        peer.join().unwrap();
    }

    #[test]
    fn integration_retired_ring_fails_closed_without_reusing_a_stream() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let peer = thread::spawn(move || {
            let (mut socket, _) = listener.accept().unwrap();
            socket
                .set_read_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            assert_eq!(socket.read(&mut [0; 1]).unwrap(), 0);
        });
        let mut stream = ProactorStream::dial("127.0.0.1", port, Duration::from_secs(2)).unwrap();
        stream.read_budget = Duration::from_millis(100);
        driver::with(|driver| driver.ring.shutdown().map_err(io::Error::other)).unwrap();
        assert!(stream.read(&mut [0; 1]).is_err());
        assert!(stream.fd.is_none());
        assert_eq!(
            stream.read(&mut [0; 1]).unwrap_err().kind(),
            io::ErrorKind::NotConnected
        );
        peer.join().unwrap();
    }
}
