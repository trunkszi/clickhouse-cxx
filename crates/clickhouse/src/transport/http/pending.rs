//! Owned nonblocking HTTP requests with exactly-once result extraction.
use std::{
    cell::{Cell, RefCell},
    fmt,
    rc::Rc,
};

use nexora::{RequestId, Response};

use super::*;

/// A thread-affine request submitted to [`crate::Client::poll_http`].
///
/// Polling the owning client advances I/O. Result extraction may decode a bounded
/// body, but never waits for network work. Dropping this handle does not cancel
/// the request; use [`crate::Client::cancel_http`] to retire it early.
///
/// ```compile_fail
/// fn require_send<T: Send>() {}
/// require_send::<clickhouse::HttpRequest>();
/// ```
pub struct HttpRequest {
    id: RequestId,
    owner: Rc<()>,
    result: Rc<RefCell<Option<nexora::Result<Response>>>>,
    finished: Rc<Cell<bool>>,
    version: HttpVersionChoice,
}
impl fmt::Debug for HttpRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("HttpRequest")
            .field("id", &self.id)
            .field("complete", &self.is_complete())
            .finish()
    }
}
impl HttpRequest {
    /// Reports terminal completion, including after the result was consumed.
    pub fn is_complete(&self) -> bool {
        self.finished.get()
    }

    /// Takes the decoded result once, or returns `None` while pending or consumed.
    ///
    /// Transport, HTTP status, server exception, and decoding errors are retained
    /// inside the returned result. No network operation runs in this method.
    pub fn try_take(&mut self) -> Option<crate::Result<Vec<u8>>> {
        self.take_response().map(|result| {
            let response = result?;
            if self.version == HttpVersionChoice::Http2Tls && response.version != Version::Http2 {
                return Err(Error::protocol(
                    "HTTP/2 over TLS was required but not negotiated",
                ));
            }
            ResponseHeaders::from_response(&response, self.version)
                .decode_body(response.body.as_ref())
        })
    }

    pub(super) fn take_response(&mut self) -> Option<crate::Result<Response>> {
        self.result
            .borrow_mut()
            .take()
            .map(|result| result.map_err(Error::from))
    }
}

impl HttpTransport {
    pub(crate) fn submit_raw(
        &mut self,
        database: &str,
        query: &Query<'_>,
        body: &[u8],
        compression: Compression,
        dedup: Option<&str>,
    ) -> crate::Result<HttpRequest> {
        let compression = query.compression_override().unwrap_or(compression);
        let (payload, encoding) = compress_body(compression, body)?;
        let url = self.url(database, query, query.sql(), dedup, compression)?;
        self.start_request(&url, self.headers(encoding, compression), payload.as_ref())
    }

    pub(super) fn start_request(
        &mut self,
        url: &str,
        headers: nexora::Headers,
        body: &[u8],
    ) -> crate::Result<HttpRequest> {
        if self.reconfigure {
            return Err(Error::Config {
                reason: "HTTP configuration requires reset_http before nonblocking admission"
                    .into(),
            });
        }
        self.negotiated = None;
        self.observed.set(None);
        if let Ok(ip) = self.host.parse::<IpAddr>() {
            self.client.resolve_host(&self.host, self.port, vec![ip]);
        }
        let mut request = Request::new(
            Method::Post,
            Uri::parse(url).map_err(Error::http_transport)?,
        );
        if self.version == HttpVersionChoice::Http2Tls {
            request.version = Version::Http2;
        }
        request.headers = headers;
        request
            .headers
            .insert("content-length", body.len().to_string());
        request.body = nexora::Bytes::from(body);
        let result = Rc::new(RefCell::new(None));
        let completion = Rc::clone(&result);
        let finished = Rc::new(Cell::new(false));
        let terminal = Rc::clone(&finished);
        let observed = Rc::clone(&self.observed);
        let version = self.version;
        let id = self
            .client
            .submit(&mut self.proactor, request, move |_, _, response| {
                if let Ok(response) = &response {
                    observed.set(Some(match response.version {
                        Version::Http2 if version == HttpVersionChoice::Http2PriorKnowledge => {
                            version
                        }
                        Version::Http2 => HttpVersionChoice::Http2Tls,
                        _ => HttpVersionChoice::Http1,
                    }));
                }
                *completion.borrow_mut() = Some(response);
                terminal.set(true);
            })?;
        Ok(HttpRequest {
            id,
            owner: Rc::clone(&self.request_owner),
            result,
            finished,
            version,
        })
    }

    pub(crate) fn reset(&mut self) -> crate::Result<()> {
        let (client, proactor) = driver(self.version, self.timeout)?;
        self.client.shutdown(&mut self.proactor);
        self.client = client;
        self.proactor = proactor;
        self.reconfigure = false;
        self.negotiated = None;
        self.observed.set(None);
        Ok(())
    }

    pub(crate) fn poll(&mut self) -> crate::Result<usize> {
        let count = self.client.drive(&mut self.proactor)?;
        self.negotiated = self.observed.get();
        Ok(count)
    }

    pub(crate) fn cancel(&mut self, request: &HttpRequest) -> crate::Result<bool> {
        if !Rc::ptr_eq(&request.owner, &self.request_owner) {
            return Err(Error::Config {
                reason: "HTTP request belongs to another client".into(),
            });
        }
        if request.is_complete() {
            return Ok(false);
        }
        Ok(self.client.cancel(&mut self.proactor, request.id))
    }
}
