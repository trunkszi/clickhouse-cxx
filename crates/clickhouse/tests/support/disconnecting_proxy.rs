//! Controlled blocking TCP peer for reconnect-before-write lifecycle scenarios.

use std::{
    io::{self, ErrorKind},
    net::{Shutdown, TcpListener, TcpStream},
    sync::mpsc::{self, Receiver},
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

/// Forward one connection until its owner deliberately disconnects both peers.
pub struct DisconnectingProxy {
    /// Ephemeral local port used by the client under test.
    pub port: u16,
    control: Receiver<(TcpStream, TcpStream)>,
    worker: Option<JoinHandle<Result<(), String>>>,
}

impl DisconnectingProxy {
    /// Start a bounded test-only forwarding peer for a live Native TCP server.
    ///
    /// # Errors
    /// Returns bind or listener configuration errors.
    pub fn start(upstream_port: u16) -> Result<Self, String> {
        let listener = TcpListener::bind(("127.0.0.1", 0)).map_err(|e| e.to_string())?;
        let port = listener.local_addr().map_err(|e| e.to_string())?.port();
        listener.set_nonblocking(true).map_err(|e| e.to_string())?;
        let (sender, control) = mpsc::channel();
        let worker = thread::spawn(move || {
            let deadline = Instant::now() + Duration::from_secs(5);
            let front = loop {
                match listener.accept() {
                    Ok((stream, _)) => break stream,
                    Err(error) if error.kind() == ErrorKind::WouldBlock => {
                        if Instant::now() >= deadline {
                            return Err("proxy did not receive a client connection".into());
                        }
                        // This controlled peer is outside the SDK I/O path.
                        thread::sleep(Duration::from_millis(10));
                    }
                    Err(error) => return Err(error.to_string()),
                }
            };
            let back =
                TcpStream::connect(("127.0.0.1", upstream_port)).map_err(|e| e.to_string())?;
            for socket in [&front, &back] {
                socket
                    .set_read_timeout(Some(Duration::from_secs(5)))
                    .map_err(|e| e.to_string())?;
                socket
                    .set_write_timeout(Some(Duration::from_secs(5)))
                    .map_err(|e| e.to_string())?;
            }
            sender
                .send((
                    front.try_clone().map_err(|e| e.to_string())?,
                    back.try_clone().map_err(|e| e.to_string())?,
                ))
                .map_err(|e| e.to_string())?;
            let outgoing = front.try_clone().map_err(|e| e.to_string())?;
            let upstream = back.try_clone().map_err(|e| e.to_string())?;
            let writer = thread::spawn(move || forward(outgoing, upstream));
            let incoming = forward(back, front);
            let outgoing = writer.join().map_err(|_| "proxy writer panicked")?;
            incoming.and(outgoing)
        });
        Ok(Self {
            port,
            control,
            worker: Some(worker),
        })
    }

    /// Close both forwarded sockets and join the controlled peer threads.
    ///
    /// # Errors
    /// Returns missing-connection, shutdown, or forwarding errors.
    pub fn disconnect(&mut self) -> Result<(), String> {
        let Some(worker) = self.worker.take() else {
            return Ok(());
        };
        let controls = self.control.recv_timeout(Duration::from_secs(6));
        let closed = match controls {
            Ok((front, back)) => close(&front).and(close(&back)),
            Err(error) => Err(error.to_string()),
        };
        let forwarded = worker.join().map_err(|_| "proxy worker panicked")?;
        closed.and(forwarded)
    }
}

impl Drop for DisconnectingProxy {
    fn drop(&mut self) {
        if let Err(error) = self.disconnect() {
            eprintln!("disconnecting proxy cleanup failed: {error}");
        }
    }
}

fn close(socket: &TcpStream) -> Result<(), String> {
    match socket.shutdown(Shutdown::Both) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == ErrorKind::NotConnected => Ok(()),
        Err(error) => Err(error.to_string()),
    }
}

fn forward(mut source: TcpStream, mut destination: TcpStream) -> Result<(), String> {
    match io::copy(&mut source, &mut destination) {
        Ok(_) => Ok(()),
        Err(error)
            if matches!(
                error.kind(),
                ErrorKind::ConnectionReset
                    | ErrorKind::ConnectionAborted
                    | ErrorKind::BrokenPipe
                    | ErrorKind::NotConnected
            ) =>
        {
            Ok(())
        }
        Err(error) => Err(error.to_string()),
    }
}
