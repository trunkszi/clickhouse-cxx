//! Thread-local ring, stable DMA storage, and final-completion retirement.
use std::{cell::RefCell, io, time::Duration};

use cares::{LibraryGuard, ProactorResolver};
use liburing::{
    Completion, CompletionDispatcher, OperationId, OperationPayload, Proactor, ProactorConfig,
    SubmissionContext, SubsystemId,
    ops::{Timeout, net},
};

use super::completion::OperationState;

pub(super) const DMA_BYTES: usize = 1024 * 1024;
const IO: SubsystemId = SubsystemId::new_unchecked(1);
pub(super) const DNS: SubsystemId = SubsystemId::new_unchecked(2);

thread_local! { static DRIVER: RefCell<Option<Driver>> = const { RefCell::new(None) }; }

pub(super) fn with<T>(action: impl FnOnce(&mut Driver) -> io::Result<T>) -> io::Result<T> {
    DRIVER
        .try_with(|slot| {
            let mut slot = slot
                .try_borrow_mut()
                .map_err(|_| io::Error::other("reentrant native ring access"))?;
            if slot.as_ref().is_none_or(|driver| driver.closed) {
                *slot = Some(Driver::new()?);
            }
            action(
                slot.as_mut()
                    .ok_or_else(|| io::Error::other("native ring unavailable"))?,
            )
        })
        .map_err(|_| io::Error::other("native ring owner has exited"))?
}

pub(super) enum Operation {
    Connect(i32),
    Receive(i32, usize),
    Send(i32, usize),
}

pub(super) struct Driver {
    pub ring: Proactor,
    pub resolver: Option<ProactorResolver>,
    pub dma: Option<Box<[u8]>>,
    pub peer: Option<Box<socket2::SockAddr>>,
    timer: Option<Timeout>,
    pub guard: Option<LibraryGuard>,
    next: u64,
    pub closed: bool,
    pub quarantined: bool,
    active: bool,
    #[cfg(test)]
    completion_fault: Option<(OperationId, i32)>,
}

impl Driver {
    fn new() -> io::Result<Self> {
        let ring = Proactor::new(ProactorConfig {
            entries: 64,
            staged_capacity: 256,
            max_cqe_batch: 64,
            ..ProactorConfig::default()
        })
        .map_err(io::Error::other)?;
        Ok(Self {
            ring,
            resolver: None,
            dma: Some(vec![0; DMA_BYTES].into_boxed_slice()),
            peer: None,
            timer: None,
            guard: None,
            next: 1,
            closed: false,
            quarantined: false,
            active: false,
            #[cfg(test)]
            completion_fault: None,
        })
    }

    fn id(&mut self) -> io::Result<OperationId> {
        let value = self.next;
        self.next = value
            .checked_add(1)
            .filter(|value| *value <= OperationPayload::MAX_RAW)
            .ok_or_else(|| io::Error::other("native operation identifiers exhausted"))?;
        Ok(OperationId::pack(
            IO,
            OperationPayload::new_unchecked(value),
        ))
    }

    pub fn poll(&mut self, observe: impl FnMut(Completion)) -> io::Result<()> {
        if let Some(resolver) = self.resolver.as_mut() {
            resolver.flush(&mut self.ring).map_err(io::Error::other)?;
        }
        let resolver = &mut self.resolver;
        self.ring.submit().map_err(io::Error::other)?;
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
        self.ring
            .poll_completions(64, &mut dispatch)
            .map_err(io::Error::other)?;
        Ok(())
    }

    pub fn delay(&mut self, duration: Duration) -> io::Result<()> {
        if duration.is_zero() {
            return Ok(());
        }
        let id = self.id()?;
        self.timer = Some(Timeout::new(duration).map_err(io::Error::other)?);
        self.active = true;
        let result = (|| {
            let timer = self
                .timer
                .as_ref()
                .ok_or_else(|| io::Error::other("missing retry timer"))?;
            self.ring
                .prepare_sqe_with(id, |sqe| {
                    // SAFETY: The driver retains the timespec until this timer's final CQE.
                    unsafe { timer.prepare(sqe) };
                })
                .map_err(io::Error::other)?;
            let mut result = None;
            while result.is_none() {
                self.poll(|completion| {
                    if completion.operation_id == id {
                        result = Some(completion.result);
                    }
                })?;
            }
            self.active = false;
            self.timer = None;
            match result {
                Some(value) if value == 0 || value == -libc::ETIME => Ok(()),
                Some(value) if value < 0 => Err(io::Error::from_raw_os_error(-value)),
                _ => Err(io::Error::other("invalid retry timer completion")),
            }
        })();
        if result.is_err() && self.active {
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
            .map_err(io::Error::other)?;
        Ok(id)
    }

    pub fn perform(&mut self, operation: Operation, budget: Duration) -> io::Result<usize> {
        let result = self.perform_inner(operation, budget);
        if result.is_err() && self.active {
            self.retire();
        }
        result
    }

    fn perform_inner(&mut self, operation: Operation, budget: Duration) -> io::Result<usize> {
        let target = self.id()?;
        let deadline = if budget.is_zero() {
            None
        } else {
            Some(self.id()?)
        };
        self.timer = deadline
            .map(|_| Timeout::new(budget))
            .transpose()
            .map_err(io::Error::other)?;
        let dma = self
            .dma
            .as_mut()
            .ok_or_else(|| io::Error::other("native DMA storage unavailable"))?;
        let peer = self.peer.as_ref();
        self.active = true;
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
            .map_err(io::Error::other)?;
        if let (Some(id), Some(timer)) = (deadline, self.timer.as_ref()) {
            self.ring
                .prepare_sqe_with(id, |sqe| {
                    // SAFETY: The boxed timespec remains owned until its timeout final CQE.
                    unsafe { timer.prepare(sqe) };
                })
                .map_err(io::Error::other)?;
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
            if state.result.is_none() && state.expired && state.cancel_target.is_none() {
                state.cancel_target = Some(self.cancel(target)?);
            }
            if state.result.is_some()
                && !state.timer_done
                && state.cancel_timer.is_none()
                && let Some(id) = deadline
            {
                state.cancel_timer = Some(self.cancel(id)?);
            }
            if state.complete() {
                break;
            }
        }
        self.active = false;
        self.timer = None;
        let result = state
            .result
            .ok_or_else(|| io::Error::other("missing native I/O completion"))?;
        if result == -libc::ECANCELED && state.expired {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "native I/O deadline exceeded",
            ));
        }
        if result < 0 {
            return Err(io::Error::from_raw_os_error(-result));
        }
        Ok(result as usize)
    }

    fn retire(&mut self) {
        self.closed = true;
        let resolver = &mut self.resolver;
        let mut dispatch = |completion: Completion, submissions: &mut SubmissionContext<'_>| {
            if completion.operation_id.subsystem_id() == DNS
                && let Some(resolver) = resolver.as_mut()
            {
                resolver.dispatch(completion, submissions)?;
            }
            Ok(())
        };
        if self.ring.shutdown_with(&mut dispatch).is_err() {
            self.quarantined = true;
        } else {
            self.timer = None;
            self.peer = None;
        }
    }
}

impl Drop for Driver {
    fn drop(&mut self) {
        self.retire();
        if self.quarantined {
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
            if let Some(value) = self.resolver.take() {
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
        assert!(driver.closed);
        assert!(!driver.quarantined);
        assert!(driver.timer.is_none());
    }

    #[test]
    fn resolver_operands_retire_through_the_shutdown_dispatcher() {
        let socket = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        let guard = LibraryGuard::acquire().unwrap();
        let mut driver = Driver::new().unwrap();
        let mut resolver = ProactorResolver::new(
            DNS,
            cares::ResolverOptions::builder().build().unwrap(),
            &socket.local_addr().unwrap().to_string(),
            64,
        )
        .unwrap();
        let _query = resolver
            .resolver
            .gethostbyname("retirement.test", libc::AF_INET, |_, _| {})
            .unwrap();
        resolver.flush(&mut driver.ring).unwrap();
        driver.ring.submit().unwrap();
        assert!(resolver.inflight_count() > 0);
        driver.resolver = Some(resolver);
        driver.guard = Some(guard);
        driver.retire();
        assert!(driver.closed);
        assert!(!driver.quarantined);
        assert_eq!(driver.resolver.as_ref().unwrap().inflight_count(), 0);
    }

    #[test]
    fn failed_target_cancel_retires_the_ring_with_a_pending_receive() {
        failed_cancel(|fd| Operation::Receive(fd, 1), Duration::from_millis(1));
    }

    #[test]
    fn failed_timer_cancel_retires_the_ring_after_a_successful_send() {
        failed_cancel(|fd| Operation::Send(fd, 1), Duration::from_secs(60));
    }
}
