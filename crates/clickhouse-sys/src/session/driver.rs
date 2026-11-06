//! Thread-local ring, stable DMA storage, and final-completion retirement.
use std::{cell::RefCell, io, time::Duration};

use cares::{LibraryGuard, Resolver};
use liburing::{
    Completion, CompletionDispatcher, OperationId, OperationPayload, Proactor, ProactorConfig,
    SubmissionContext, SubsystemId,
    ops::{Timeout, net},
};
use uarch::prelude::*;

use super::{
    completion::OperationState,
    error::{errno, message, other},
};

bitword! {
    pub(super) struct DriverStateWord(u64) {
        [0] pub CLOSED,
        [1] pub QUARANTINED,
        [2] pub ACTIVE,
        // Preserve the full payload range instead of exhausting identifiers after 32 bits.
        [8..=63] pub SEQ: u64,
    }
}
assert_layout!(
    DriverStateWord,
    size = 8,
    align = std::mem::align_of::<u64>()
);
const _: () = assert!(DriverStateWord::SEQ.mask() >> 8 == OperationPayload::MAX_RAW);

impl DriverStateWord {
    fn initial() -> Self {
        Self::new(0).patch(patch!(Self { SEQ: 1 }))
    }
    #[hot_inline]
    fn advance(self) -> Self {
        self.wrapping_add_top_field(Self::SEQ, 1)
    }
    #[hot_inline]
    fn take_payload(&mut self) -> io::Result<u64> {
        let value = self.get(Self::SEQ);
        if value >= OperationPayload::MAX_RAW {
            return Err(identifiers_exhausted());
        }
        *self = self.advance();
        Ok(value)
    }
}
#[cold_sink]
fn identifiers_exhausted() -> io::Error {
    io::Error::other("native operation identifiers exhausted")
}
#[cold_sink]
fn driver_error(message: &'static str) -> io::Error {
    io::Error::other(message)
}

pub(super) const DMA_BYTES: usize = 1024 * 1024;
const IO: SubsystemId = SubsystemId::new_unchecked(1);
pub(super) const DNS: SubsystemId = SubsystemId::new_unchecked(2);

thread_local! { static DRIVER: RefCell<Option<Driver>> = const { RefCell::new(None) }; }

pub(super) fn with<T>(action: impl FnOnce(&mut Driver) -> io::Result<T>) -> io::Result<T> {
    DRIVER
        .try_with(|slot| {
            let mut slot = slot
                .try_borrow_mut()
                .map_err(|_| driver_error("reentrant native ring access"))?;
            if slot.as_ref().is_none_or(|driver| driver.closed()) {
                *slot = Some(Driver::new()?);
            }
            action(
                slot.as_mut()
                    .ok_or_else(|| driver_error("native ring unavailable"))?,
            )
        })
        .map_err(|_| driver_error("native ring owner has exited"))?
}

pub(super) enum Operation {
    Connect(i32),
    Receive(i32, usize),
    Send(i32, usize),
}

pub(super) struct Driver {
    pub ring: Proactor,
    pub dns: Option<Resolver>,
    pub dma: Option<Box<[u8]>>,
    pub peer: Option<Box<socket2::SockAddr>>,
    timer: Option<Timeout>,
    pub guard: Option<LibraryGuard>,
    state: DriverStateWord,
    #[cfg(test)]
    completion_fault: Option<(OperationId, i32)>,
}

impl Driver {
    #[hot_inline]
    pub(super) fn closed(&self) -> bool {
        self.state.matches(pattern!(DriverStateWord { CLOSED }))
    }
    #[hot_inline]
    pub(super) fn quarantined(&self) -> bool {
        self.state
            .matches(pattern!(DriverStateWord { QUARANTINED }))
    }

    fn new() -> io::Result<Self> {
        let ring = Proactor::new(ProactorConfig {
            entries: 64,
            staged_capacity: 256,
            max_cqe_batch: 64,
            ..ProactorConfig::default()
        })
        .map_err(other)?;
        Ok(Self {
            ring,
            dns: None,
            dma: Some(vec![0; DMA_BYTES].into_boxed_slice()),
            peer: None,
            timer: None,
            guard: None,
            state: DriverStateWord::initial(),
            #[cfg(test)]
            completion_fault: None,
        })
    }

    #[hot_inline]
    fn id(&mut self) -> io::Result<OperationId> {
        let value = self.state.take_payload()?;
        Ok(OperationId::pack(
            IO,
            OperationPayload::new_unchecked(value),
        ))
    }

    #[hot_inline]
    pub fn poll(&mut self, observe: impl FnMut(Completion)) -> io::Result<()> {
        if let Some(resolver) = self.dns.as_mut() {
            resolver.flush(&mut self.ring).map_err(other)?;
        }
        let resolver = &mut self.dns;
        let mut observe = observe;
        #[cfg(test)]
        let fault = self.completion_fault;
        let mut dispatch = |completion: Completion, submissions: &mut SubmissionContext<'_>| {
            if completion.operation_id.subsystem_id() == DNS {
                if let Some(resolver) = resolver.as_mut() {
                    resolver.dispatch(completion, submissions)?;
                }
            } else {
                #[cfg(test)]
                let completion = if let Some((id, result)) = fault
                    && completion.operation_id == id
                {
                    Completion {
                        result,
                        ..completion
                    }
                } else {
                    completion
                };
                observe(completion);
            }
            Ok(())
        };
        self.ring.tick(&mut dispatch).map_err(other)?;
        Ok(())
    }

    pub fn delay(&mut self, duration: Duration) -> io::Result<()> {
        if duration.is_zero() {
            return Ok(());
        }
        let id = self.id()?;
        self.timer = Some(Timeout::new(duration).map_err(other)?);
        self.state = self.state.patch(patch!(DriverStateWord { ACTIVE: true }));
        let result = (|| {
            let timer = self
                .timer
                .as_ref()
                .ok_or_else(|| driver_error("missing retry timer"))?;
            self.ring
                .prepare_sqe_with(id, |sqe| {
                    // SAFETY: The driver retains the timespec until this timer's final CQE.
                    unsafe { timer.prepare(sqe) };
                })
                .map_err(other)?;
            let mut result = None;
            while result.is_none() {
                self.poll(|completion| {
                    if completion.operation_id == id {
                        result = Some(completion.result);
                    }
                })?;
            }
            self.state = self.state.patch(patch!(DriverStateWord { ACTIVE: false }));
            self.timer = None;
            match result {
                Some(value) if value == 0 || value == -libc::ETIME => Ok(()),
                Some(value) if value < 0 => Err(errno(-value)),
                _ => Err(driver_error("invalid retry timer completion")),
            }
        })();
        if result.is_err() && self.state.matches(pattern!(DriverStateWord { ACTIVE })) {
            self.retire();
        }
        result
    }

    fn cancel(&mut self, target: OperationId) -> io::Result<OperationId> {
        let id = self.id()?;
        self.ring
            .prepare_sqe_with(id, |sqe| {
                // SAFETY: Exclusive SQE storage; target operands remain owned until final CQEs.
                unsafe { net::prep_cancel(sqe, target, 0) };
            })
            .map_err(other)?;
        Ok(id)
    }

    pub fn perform(&mut self, operation: Operation, budget: Duration) -> io::Result<usize> {
        let result = self.perform_inner(operation, budget);
        if result.is_err() && self.state.matches(pattern!(DriverStateWord { ACTIVE })) {
            self.retire();
        }
        result
    }

    fn perform_inner(&mut self, operation: Operation, budget: Duration) -> io::Result<usize> {
        // An empty connect SQE still occupies the ring. Reject a missing address
        // before the operation is marked active so the guard does not retire it.
        if self.peer.is_none() && matches!(&operation, Operation::Connect(_)) {
            return Err(message(
                io::ErrorKind::InvalidInput,
                "native connect address is missing",
            ));
        }
        let target = self.id()?;
        let deadline = if budget.is_zero() {
            None
        } else {
            Some(self.id()?)
        };
        self.timer = deadline
            .map(|_| Timeout::new(budget))
            .transpose()
            .map_err(other)?;
        let dma = self
            .dma
            .as_mut()
            .ok_or_else(|| driver_error("native DMA storage unavailable"))?;
        let peer = self.peer.as_ref();
        self.state = self.state.patch(patch!(DriverStateWord { ACTIVE: true }));
        self.ring
            .prepare_sqe_with(target, |sqe| {
                // SAFETY: The driver retains DMA and address allocations through the target final
                // CQE.
                unsafe {
                    match operation {
                        Operation::Connect(fd) => {
                            if let Some(peer) = peer {
                                net::prep_connect(sqe, fd, peer.as_ptr(), peer.len());
                            }
                        }
                        Operation::Receive(fd, size) => {
                            net::prep_recv(sqe, fd, dma.as_mut_ptr(), size, 0)
                        }
                        Operation::Send(fd, size) => {
                            net::prep_send(sqe, fd, dma.as_ptr(), size, libc::MSG_NOSIGNAL)
                        }
                    }
                }
            })
            .map_err(other)?;
        if let (Some(id), Some(timer)) = (deadline, self.timer.as_ref()) {
            self.ring
                .prepare_sqe_with(id, |sqe| {
                    // SAFETY: The boxed timespec remains owned until its timeout final CQE.
                    unsafe { timer.prepare(sqe) };
                })
                .map_err(other)?;
        }
        let mut state = OperationState::new(target, deadline);
        loop {
            let mut failure = None;
            self.poll(|completion| {
                if let Err(error) = state.record(completion)
                    && failure.is_none()
                {
                    failure = Some(error);
                }
            })?;
            if let Some(error) = failure {
                return Err(error);
            }
            if state.needs_target_cancel() {
                state.request_target_cancel(self.cancel(target)?);
            }
            if state.needs_timer_cancel()
                && let Some(id) = deadline
            {
                state.request_timer_cancel(self.cancel(id)?);
            }
            if state.complete() {
                break;
            }
        }
        self.state = self.state.patch(patch!(DriverStateWord { ACTIVE: false }));
        self.timer = None;
        let result = state
            .result()
            .ok_or_else(|| driver_error("missing native I/O completion"))?;
        if result == -libc::ECANCELED && state.expired() {
            return Err(message(
                io::ErrorKind::TimedOut,
                "native I/O deadline exceeded",
            ));
        }
        if result < 0 {
            return Err(errno(-result));
        }
        Ok(result as usize)
    }

    fn retire(&mut self) {
        self.state = self.state.patch(patch!(DriverStateWord { CLOSED: true }));
        let resolver = &mut self.dns;
        let mut dispatch = |completion: Completion, submissions: &mut SubmissionContext<'_>| {
            if completion.operation_id.subsystem_id() == DNS
                && let Some(resolver) = resolver.as_mut()
            {
                resolver.dispatch(completion, submissions)?;
            }
            Ok(())
        };
        if self.ring.shutdown_with(&mut dispatch).is_err() {
            self.state = self
                .state
                .patch(patch!(DriverStateWord { QUARANTINED: true }));
        } else {
            self.timer = None;
            self.peer = None;
        }
    }
}

impl Drop for Driver {
    fn drop(&mut self) {
        self.retire();
        if self.quarantined() {
            // A failed drain is not proof of DMA retirement. Retain every referenced allocation.
            if let Some(value) = self.dma.take() {
                std::mem::forget(value);
            }
            if let Some(value) = self.peer.take() {
                std::mem::forget(value);
            }
            if let Some(value) = self.timer.take() {
                std::mem::forget(value);
            }
            if let Some(value) = self.dns.take() {
                std::mem::forget(value);
            }
            if let Some(value) = self.guard.take() {
                std::mem::forget(value);
            }
        }
    }
}

#[cfg(test)]
mod integration_lifecycle_tests {
    use std::os::{fd::AsRawFd, unix::net::UnixStream};

    use super::*;

    fn failed_cancel(operation: impl FnOnce(i32) -> Operation, budget: Duration) {
        let (socket, _peer) = UnixStream::pair().unwrap();
        socket.set_nonblocking(true).unwrap();
        let mut driver = Driver::new().unwrap();
        let cancel_id = OperationId::pack(IO, OperationPayload::new_unchecked(3));
        driver.completion_fault = Some((cancel_id, -libc::EINVAL));
        let error = driver
            .perform(operation(socket.as_raw_fd()), budget)
            .unwrap_err();
        assert_eq!(error.raw_os_error(), Some(libc::EINVAL));
        assert!(driver.closed());
        assert!(!driver.quarantined());
        assert!(driver.timer.is_none());
    }

    #[test]
    fn resolver_operands_retire_through_the_shutdown_dispatcher() {
        let socket = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        let guard = LibraryGuard::acquire().unwrap();
        let mut driver = Driver::new().unwrap();
        let mut resolver = Resolver::new(
            DNS,
            cares::ResolverOptions::builder().build().unwrap(),
            &socket.local_addr().unwrap().to_string(),
            64,
        )
        .unwrap();
        let _query = resolver
            .gethostbyname("retirement.test", libc::AF_INET, |_, _| {})
            .unwrap();
        resolver.flush(&mut driver.ring).unwrap();
        driver.ring.submit().unwrap();
        assert!(resolver.inflight_count() > 0);
        driver.dns = Some(resolver);
        driver.guard = Some(guard);
        driver.retire();
        assert!(driver.closed());
        assert!(!driver.quarantined());
        assert_eq!(driver.dns.as_ref().unwrap().inflight_count(), 0);
    }

    #[test]
    fn failed_target_cancel_retires_the_ring_with_a_pending_receive() {
        failed_cancel(|fd| Operation::Receive(fd, 1), Duration::from_millis(1));
    }

    #[test]
    fn failed_timer_cancel_retires_the_ring_after_a_successful_send() {
        failed_cancel(|fd| Operation::Send(fd, 1), Duration::from_secs(60));
    }

    #[test]
    fn connect_without_peer_stays_open() {
        let (socket, _peer) = UnixStream::pair().unwrap();
        let mut driver = Driver::new().unwrap();
        let error = driver
            .perform(Operation::Connect(socket.as_raw_fd()), Duration::ZERO)
            .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
        assert!(!driver.closed());
        assert!(!driver.quarantined());
        assert!(driver.peer.is_none());
    }
}

#[cfg(test)]
mod state_word_tests {
    use super::*;
    #[test]
    fn flags_preserve_full_payload_sequence() {
        let mut word = DriverStateWord::initial();
        word = word.patch(patch!(DriverStateWord {
            CLOSED: true,
            QUARANTINED: true,
            ACTIVE: true
        }));
        assert_eq!(word.take_payload().unwrap(), 1);
        assert!(word.matches(pattern!(DriverStateWord { CLOSED & QUARANTINED & ACTIVE })));
        word = word.patch(patch!(DriverStateWord {
            SEQ: (u32::MAX as u64)
        }));
        assert_eq!(word.take_payload().unwrap(), u32::MAX as u64);
        assert_eq!(word.take_payload().unwrap(), u32::MAX as u64 + 1);
        word = word.patch(patch!(DriverStateWord {
            SEQ: (OperationPayload::MAX_RAW - 1)
        }));
        assert_eq!(word.take_payload().unwrap(), OperationPayload::MAX_RAW - 1);
        assert!(word.take_payload().is_err());
        assert!(word.take_payload().is_err());
    }
}
