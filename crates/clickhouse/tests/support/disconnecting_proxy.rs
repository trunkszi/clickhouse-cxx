//! Controlled nonblocking forwarding peer for connection retirement scenarios.
use std::{
    io::{self, Read, Write},
    net::{Shutdown, TcpListener, TcpStream},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

pub struct DisconnectingProxy {
    pub port: u16,
    stop: Arc<AtomicBool>,
    worker: Option<JoinHandle<Result<(), String>>>,
}

impl DisconnectingProxy {
    /// Opens an owned forwarding fixture.
    /// # Errors
    /// Returns listener setup failures.
    pub fn start(upstream_port: u16) -> Result<Self, String> {
        let listener = TcpListener::bind(("127.0.0.1", 0)).map_err(|e| e.to_string())?;
        let port = listener.local_addr().map_err(|e| e.to_string())?.port();
        listener.set_nonblocking(true).map_err(|e| e.to_string())?;
        let stop = Arc::new(AtomicBool::new(false));
        let signal = Arc::clone(&stop);
        let worker = thread::spawn(move || {
            run(listener, upstream_port, &signal).map_err(|error| error.to_string())
        });
        Ok(Self {
            port,
            stop,
            worker: Some(worker),
        })
    }

    /// Retires the forwarding peer and joins its worker.
    /// # Errors
    /// Returns worker or socket failures.
    pub fn disconnect(&mut self) -> Result<(), String> {
        self.stop.store(true, Ordering::Release);
        self.worker.take().map_or(Ok(()), |worker| {
            worker
                .join()
                .map_err(|_| "proxy worker panicked".to_string())?
        })
    }
}
impl Drop for DisconnectingProxy {
    fn drop(&mut self) {
        if let Err(error) = self.disconnect() {
            eprintln!("proxy cleanup failed: {error}");
        }
    }
}

fn run(listener: TcpListener, port: u16, stop: &AtomicBool) -> io::Result<()> {
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut front = loop {
        if stop.load(Ordering::Acquire) {
            return Ok(());
        }
        match listener.accept() {
            Ok((socket, _)) => break socket,
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                if Instant::now() >= deadline {
                    return Err(io::Error::new(
                        io::ErrorKind::TimedOut,
                        "proxy accept deadline",
                    ));
                }
                thread::yield_now();
            }
            Err(error) => return Err(error),
        }
    };
    let mut back = TcpStream::connect(("127.0.0.1", port))?;
    front.set_nonblocking(true)?;
    back.set_nonblocking(true)?;
    let mut outgoing = Buffer::new();
    let mut incoming = Buffer::new();
    while !stop.load(Ordering::Acquire) {
        if !outgoing.step(&mut front, &mut back)? || !incoming.step(&mut back, &mut front)? {
            break;
        }
        thread::yield_now();
    }
    for socket in [&front, &back] {
        match socket.shutdown(Shutdown::Both) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::NotConnected => {}
            Err(error) => return Err(error),
        }
    }
    Ok(())
}
struct Buffer {
    bytes: Box<[u8]>,
    start: usize,
    end: usize,
}
impl Buffer {
    fn new() -> Self {
        Self {
            bytes: vec![0; 64 * 1024].into_boxed_slice(),
            start: 0,
            end: 0,
        }
    }
    fn step(&mut self, source: &mut TcpStream, target: &mut TcpStream) -> io::Result<bool> {
        if self.start == self.end {
            match source.read(&mut self.bytes) {
                Ok(0) => return Ok(false),
                Ok(count) => {
                    self.start = 0;
                    self.end = count;
                }
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => return Ok(true),
                Err(error) => return tolerate(error),
            }
        }
        match target.write(&self.bytes[self.start..self.end]) {
            Ok(0) => Ok(false),
            Ok(count) => {
                self.start += count;
                Ok(true)
            }
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => Ok(true),
            Err(error) => tolerate(error),
        }
    }
}
fn tolerate(error: io::Error) -> io::Result<bool> {
    if matches!(
        error.kind(),
        io::ErrorKind::ConnectionReset
            | io::ErrorKind::ConnectionAborted
            | io::ErrorKind::BrokenPipe
            | io::ErrorKind::NotConnected
    ) {
        Ok(false)
    } else {
        Err(error)
    }
}
