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
        let mut stream = ProactorStream::dial("127.0.0.1", port, Duration::from_secs(5)).unwrap();
        stream.set_budgets(Duration::from_secs(5), Duration::from_secs(5));
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
    stream.set_budgets(Duration::from_millis(100), Duration::ZERO);
    driver::with(|driver| driver.ring.shutdown().map_err(io::Error::other)).unwrap();
    assert!(stream.read(&mut [0; 1]).is_err());
    assert!(stream.fd.is_none());
    assert_eq!(
        stream.socket_ref().err().map(|error| error.kind()),
        Some(io::ErrorKind::NotConnected)
    );
    assert_eq!(
        stream.read(&mut [0; 1]).unwrap_err().kind(),
        io::ErrorKind::NotConnected
    );
    peer.join().unwrap();
}
