//! c-ares host lookups share the dialing deadline and the native ring.
use std::{
    cell::RefCell,
    io,
    net::{IpAddr, SocketAddr},
    rc::Rc,
    time::Instant,
};

use super::{
    driver::Driver,
    error::{dns_failure, dns_status, message, other},
};

impl Driver {
    pub(super) fn resolve(
        &mut self,
        host: &str,
        port: u16,
        deadline: Instant,
    ) -> io::Result<Vec<SocketAddr>> {
        if let Ok(ip) = host.parse::<IpAddr>() {
            return Ok(vec![SocketAddr::new(ip, port)]);
        }
        if self.dns.is_none() {
            let guard = cares::LibraryGuard::acquire().map_err(other)?;
            self.dns = Some(
                cares::Resolver::from_system(
                    super::driver::DNS,
                    cares::ResolverOptions::builder().build().map_err(other)?,
                    64,
                )
                .map_err(other)?,
            );
            self.guard = Some(guard);
        }
        let answers = Rc::new(RefCell::new((0, Vec::new(), String::new())));
        let resolver = self
            .dns
            .as_mut()
            .ok_or_else(|| other("native resolver unavailable"))?;
        for family in [libc::AF_INET, libc::AF_INET6] {
            let sink = Rc::clone(&answers);
            match resolver.gethostbyname(host, family, move |_, outcome| {
                let mut state = sink.borrow_mut();
                state.0 += 1;
                if let Some(host) = outcome.host {
                    for ip in host.addresses {
                        let address = SocketAddr::new(ip, port);
                        if !state.1.contains(&address) {
                            state.1.push(address);
                        }
                    }
                } else {
                    state.2 = dns_status(outcome.status);
                }
            }) {
                Ok(_handle) => {}
                Err(error) => {
                    resolver.cancel().map_err(other)?;
                    return Err(other(error));
                }
            }
        }
        while answers.borrow().0 < 2 && Instant::now() < deadline {
            self.poll(|_| {})?;
        }
        let resolver = self
            .dns
            .as_mut()
            .ok_or_else(|| other("native resolver unavailable"))?;
        if answers.borrow().0 < 2 {
            resolver.cancel().map_err(other)?;
        }
        resolver.flush(&mut self.ring).map_err(other)?;
        let mut state = answers.borrow_mut();
        if Instant::now() >= deadline {
            return Err(message(
                io::ErrorKind::TimedOut,
                "native DNS deadline exceeded",
            ));
        }
        if state.1.is_empty() {
            return Err(dns_failure(host, &state.2));
        }
        Ok(std::mem::take(&mut state.1))
    }
}
