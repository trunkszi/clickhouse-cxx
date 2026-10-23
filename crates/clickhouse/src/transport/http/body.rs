//! Bounded HTTP body codecs, response metadata, and borrowed delivery.
use std::{
    borrow::Cow,
    io::{Read, Write},
};

use super::HttpTransport;
use crate::{Compression, Error, HttpVersionChoice, Query, Result};

pub(super) const MAX_RESPONSE_BYTES: usize = 256 * 1024 * 1024;

pub(super) struct ResponseHeaders {
    pub(super) status: u32,
    pub(super) encoding: String,
    pub(super) exception: Option<String>,
    pub(super) negotiated: Option<HttpVersionChoice>,
}

impl ResponseHeaders {
    pub(super) fn from_response(response: &nexora::Response, requested: HttpVersionChoice) -> Self {
        Self {
            status: u32::from(response.status.as_u16()),
            encoding: response
                .headers
                .get("content-encoding")
                .unwrap_or("")
                .to_owned(),
            exception: response
                .headers
                .get("x-clickhouse-exception-code")
                .map(str::to_owned),
            negotiated: Some(match response.version {
                nexora::Version::Http2 if requested == HttpVersionChoice::Http2PriorKnowledge => {
                    requested
                }
                nexora::Version::Http2 => HttpVersionChoice::Http2Tls,
                _ => HttpVersionChoice::Http1,
            }),
        }
    }

    /// Preserve server status and exception metadata when its diagnostic body is corrupt.
    pub(super) fn decode_body(&self, body: &[u8]) -> Result<Vec<u8>> {
        match decompress_body(self.encoding.clone(), body) {
            Ok(decoded) => {
                self.check(&decoded)?;
                Ok(decoded)
            }
            Err(error) => {
                self.check(format!("response body decoding failed: {error}").as_bytes())?;
                Err(error)
            }
        }
    }

    pub(super) fn check(&self, diagnostic: &[u8]) -> Result<()> {
        if !(200..300).contains(&self.status) || self.exception.is_some() {
            let message = lossy_text(diagnostic);
            return Err(Error::Http {
                status: self.status,
                message: match &self.exception {
                    Some(code) => format!("ClickHouse exception {code}: {message}"),
                    None => message,
                },
            });
        }
        Ok(())
    }
}

pub(super) fn compress_body(
    compression: Compression,
    body: &[u8],
) -> Result<(Cow<'_, [u8]>, Option<&'static str>)> {
    if body.is_empty() {
        return Ok((Cow::Borrowed(body), None));
    }
    match compression {
        Compression::None => Ok((Cow::Borrowed(body), None)),
        Compression::Lz4 => {
            let mut encoder = lz4_flex::frame::FrameEncoder::new(Vec::new());
            encoder.write_all(body)?;
            let compressed = encoder
                .finish()
                .map_err(|error| Error::protocol(format!("lz4 frame: {error}")))?;
            Ok((Cow::Owned(compressed), Some("lz4")))
        }
        Compression::Zstd => {
            let compressed = zstd::stream::encode_all(body, 0)?;
            Ok((Cow::Owned(compressed), Some("zstd")))
        }
    }
}

fn decompress_body(encoding: String, body: &[u8]) -> Result<Vec<u8>> {
    if body.len() > MAX_RESPONSE_BYTES {
        return Err(Error::protocol("HTTP response exceeds 256 MiB limit"));
    }
    if body.is_empty() || encoding.is_empty() || encoding.eq_ignore_ascii_case("identity") {
        return Ok(body.to_vec());
    }
    if encoding.eq_ignore_ascii_case("lz4") {
        return read_bounded(lz4_flex::frame::FrameDecoder::new(body), MAX_RESPONSE_BYTES);
    }
    if encoding.eq_ignore_ascii_case("zstd") {
        return read_bounded(zstd::stream::read::Decoder::new(body)?, MAX_RESPONSE_BYTES);
    }
    Err(Error::protocol(format!(
        "unsupported HTTP content encoding: {encoding}"
    )))
}

/// Stop decompression before a small compressed frame can expand without bounds.
fn read_bounded(reader: impl Read, limit: usize) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    reader.take((limit + 1) as u64).read_to_end(&mut out)?;
    if out.len() > limit {
        return Err(Error::protocol(
            "decoded HTTP response exceeds 256 MiB limit",
        ));
    }
    Ok(out)
}

fn lossy_text(body: &[u8]) -> String {
    let text = String::from_utf8_lossy(body);
    let mut message = text.trim().to_owned();
    if message.len() > 4_096 {
        let mut end = 4_096;
        while !message.is_char_boundary(end) {
            end -= 1;
        }
        message.truncate(end);
    }
    message
}

impl HttpTransport {
    /// Delivers decoded chunks after the network response completes.
    ///
    /// Nexora collects the encoded body before invoking its terminal callback.
    /// The receiver borrows only this stack frame; no receiver crosses an I/O callback.
    /// Receiver errors stop delivery, and receiver panics unwind normally after I/O completes.
    ///
    /// # Errors
    /// Returns transport, status, decoding, size-limit, or receiver failures.
    pub(crate) fn execute_with<F>(
        &mut self,
        database: &str,
        query: &Query<'_>,
        body: &[u8],
        compression: Compression,
        dedup: Option<&str>,
        mut receive: F,
    ) -> Result<usize>
    where
        F: FnMut(&[u8]) -> Result<()>,
    {
        let compression = query.compression_override().unwrap_or(compression);
        let (payload, encoding) = compress_body(compression, body)?;
        let url = self.url(database, query, query.sql(), dedup, compression)?;
        let response =
            self.transfer(&url, self.headers(encoding, compression), payload.as_ref())?;
        let metadata = ResponseHeaders::from_response(&response, self.version);
        if !(200..300).contains(&metadata.status) || metadata.exception.is_some() {
            metadata.decode_body(response.body.as_ref())?;
        }
        let encoding = metadata.encoding.as_str();
        let body = response.body.as_ref();
        let mut reader: Box<dyn Read + '_> = if encoding.eq_ignore_ascii_case("lz4") {
            Box::new(lz4_flex::frame::FrameDecoder::new(body))
        } else if encoding.eq_ignore_ascii_case("zstd") {
            Box::new(zstd::stream::read::Decoder::new(body)?)
        } else if encoding.is_empty() || encoding.eq_ignore_ascii_case("identity") {
            Box::new(body)
        } else {
            return Err(Error::protocol(format!(
                "unsupported HTTP content encoding: {encoding}"
            )));
        };
        let mut buffer = [0_u8; 16 * 1024];
        let mut total = 0;
        loop {
            let size = reader.read(&mut buffer)?;
            if size == 0 {
                return Ok(total);
            }
            if size > MAX_RESPONSE_BYTES - total {
                return Err(Error::protocol(
                    "decoded HTTP response exceeds 256 MiB limit",
                ));
            }
            receive(&buffer[..size])?;
            total += size;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compressed_bodies_round_trip() {
        let body = b"clickhouse column values";
        for compression in [Compression::Lz4, Compression::Zstd] {
            let (encoded, name) = compress_body(compression, body).unwrap();
            let decoded = decompress_body(name.unwrap().to_owned(), &encoded).unwrap();
            assert_eq!(decoded, body);
        }
        assert!(decompress_body("gzip".to_owned(), body).is_err());
        assert_eq!(decompress_body("identity".to_owned(), body).unwrap(), body);
    }

    #[test]
    fn error_truncation_preserves_utf8() {
        let body = "\u{20ac}".repeat(2_000);
        let diagnostic = lossy_text(body.as_bytes());
        assert!(diagnostic.len() <= 4_096);
        assert!(!diagnostic.is_empty());
        assert!(body.starts_with(&diagnostic));
    }

    /// Bounded decoding rejects expansion beyond its limit, including exact EOF edges.
    #[test]
    fn decoded_body_limit_rejects_expansion() {
        let body = vec![b'x'; 4096];
        let encoded = zstd::stream::encode_all(body.as_slice(), 0).unwrap();
        let decoder = zstd::stream::read::Decoder::new(encoded.as_slice()).unwrap();
        assert!(super::read_bounded(decoder, 4095).is_err());
        let decoder = zstd::stream::read::Decoder::new(encoded.as_slice()).unwrap();
        assert_eq!(super::read_bounded(decoder, 4096).unwrap(), body);
    }

    #[test]
    fn structured_exception_headers_reject_success() {
        let mut response = nexora::Response::ok("denied");
        response
            .headers
            .insert("X-ClickHouse-Exception-Code", "241");
        let metadata =
            super::ResponseHeaders::from_response(&response, crate::HttpVersionChoice::Http1);
        assert!(metadata.check(b"denied").is_err());
    }
}
