//! HTTP request admission, explicit progress, body formats, and SQL framing.
use super::{Backend, Client};
use crate::{
    ArrowBlockView, Compression, Error, HttpPreferences, HttpRequest, HttpVersionChoice, Query,
    Result,
};

impl Client {
    /// HTTP connection options, absent for Native TCP.
    #[cfg(feature = "http")]
    pub fn http_preferences(&self) -> Option<HttpPreferences> {
        match &self.backend {
            #[cfg(feature = "http")]
            Backend::Http(h) => Some(h.preferences()),
            #[cfg(feature = "tcp")]
            Backend::Native(_) => None,
        }
    }
    /// Last observed HTTP protocol, absent before the first request or on TCP.
    #[cfg(feature = "http")]
    pub fn negotiated_http_version(&self) -> Option<HttpVersionChoice> {
        match &self.backend {
            #[cfg(feature = "http")]
            Backend::Http(h) => h.negotiated_version(),
            #[cfg(feature = "tcp")]
            Backend::Native(_) => None,
        }
    }
    /// Set the requested HTTP protocol for subsequent requests.
    ///
    /// Nonblocking submission requires [`Self::reset_http`] to apply a changed
    /// protocol. Convenience HTTP calls apply it synchronously.
    /// # Errors
    /// Native endpoints and unsupported HTTP options reject the change.
    #[cfg(feature = "http")]
    pub fn set_http_version(&mut self, version: HttpVersionChoice) -> Result<()> {
        match &mut self.backend {
            #[cfg(feature = "http")]
            Backend::Http(h) => h.set_version(version),
            #[cfg(feature = "tcp")]
            Backend::Native(_) => Err(Error::unsupported("HTTP protocol selection requires HTTP")),
        }
    }
    /// Configure compression for subsequent HTTP requests.
    /// # Errors
    /// Native endpoints reject HTTP compression settings explicitly.
    #[cfg(feature = "http")]
    pub fn set_compression(&mut self, compression: Compression) -> Result<()> {
        #[cfg(feature = "tcp")]
        if matches!(self.backend, Backend::Native(_)) {
            return Err(Error::unsupported(
                "HTTP compression requires an HTTP endpoint",
            ));
        }
        self.compression = compression;
        Ok(())
    }
    /// Configure the default HTTP server insert deduplication token.
    /// # Errors
    /// Native endpoints reject settings unavailable through the native bridge.
    #[cfg(feature = "http")]
    pub fn set_insert_deduplication_token(&mut self, token: impl Into<String>) -> Result<()> {
        #[cfg(feature = "tcp")]
        if matches!(self.backend, Backend::Native(_)) {
            return Err(Error::unsupported(
                "Native insert deduplication settings are not supported",
            ));
        }
        self.dedup = Some(token.into());
        Ok(())
    }
    /// Deliver decoded HTTP response chunks on the owning thread.
    /// Nexora buffers the encoded response before delivery. The callback borrows
    /// each decoded chunk during that call. Receiver errors stop delivery and
    /// propagate unchanged; previously delivered chunks remain observable.
    /// # Errors
    /// Native endpoints, callback errors, decoding failures, transport failures,
    /// and unsuccessful HTTP responses reject the operation.
    /// # Panics
    /// A callback panic unwinds normally after the network request has completed.
    #[cfg(feature = "http")]
    pub fn query_raw_with<F>(&mut self, query: &Query<'_>, receive: F) -> Result<usize>
    where
        F: FnMut(&[u8]) -> Result<()>,
    {
        match &mut self.backend {
            #[cfg(feature = "http")]
            Backend::Http(http) => http.execute_with(
                &self.database,
                query,
                &[],
                self.compression,
                self.dedup.as_deref(),
                receive,
            ),
            #[cfg(feature = "tcp")]
            Backend::Native(_) => Err(Error::unsupported(
                "raw HTTP response streaming requires an HTTP endpoint",
            )),
        }
    }
    /// Clear and refill a caller-owned byte buffer through incremental delivery.
    /// Existing capacity is retained; enough preallocated capacity avoids response
    /// buffer growth. On failure the buffer may retain a partial response.
    /// # Errors
    /// Native endpoints, compression, transport, and server failures propagate.
    #[cfg(feature = "http")]
    pub fn query_raw_into(&mut self, query: &Query<'_>, dst: &mut Vec<u8>) -> Result<usize> {
        dst.clear();
        self.query_raw_with(query, |chunk| {
            dst.extend_from_slice(chunk);
            Ok(())
        })
    }
    /// Write raw HTTP chunks into a bounded caller-owned slice without truncation.
    /// Unused trailing bytes are preserved. On failure a written prefix may remain.
    /// # Errors
    /// Insufficient capacity returns a configuration error. Native endpoints,
    /// compression, transport, and server failures propagate.
    #[cfg(feature = "http")]
    pub fn query_raw_into_slice(&mut self, query: &Query<'_>, dst: &mut [u8]) -> Result<usize> {
        let mut written = 0usize;
        self.query_raw_with(query, |chunk| {
            let end = written
                .checked_add(chunk.len())
                .filter(|end| *end <= dst.len())
                .ok_or_else(|| Error::Config {
                    reason: "raw response exceeds destination slice capacity".into(),
                })?;
            dst[written..end].copy_from_slice(chunk);
            written = end;
            Ok(())
        })
    }
    /// Query raw HTTP bytes with an explicitly selected server format.
    /// # Errors
    /// Native endpoints or HTTP/server errors reject the call.
    #[cfg(feature = "http")]
    pub fn fetch_bytes(&mut self, query: &Query<'_>) -> Result<Vec<u8>> {
        self.http_call(query, query.sql(), &[])
    }
    /// Insert an encoded HTTP body using the format stated in SQL.
    /// # Errors
    /// Native endpoints, malformed payloads, and server errors reject insertion.
    #[cfg(feature = "http")]
    pub fn insert_formatted(&mut self, query: &Query<'_>, bytes: &[u8]) -> Result<()> {
        self.http_call(query, query.sql(), bytes)?;
        Ok(())
    }
    /// Buffer a complete HTTP query response before exposing an iterator.
    /// # Errors
    /// Unsupported Native settings, conversion, or server failures reject construction.
    #[cfg(feature = "http")]
    pub fn select_http_stream<'a>(
        &'a mut self,
        query: &Query<'_>,
    ) -> Result<crate::stream::HttpBlockStream<'a>> {
        let blocks = match &self.backend {
            #[cfg(feature = "http")]
            Backend::Http(_) => {
                let sql = ensure_format(query.sql(), "Native")?;
                let bytes = self.http_call(query, &sql, &[])?;
                crate::native_format::decode_blocks(&bytes, 0)?
            }
            #[cfg(feature = "tcp")]
            Backend::Native(_) => {
                return Err(Error::unsupported(
                    "buffered HTTP iteration requires an HTTP endpoint",
                ));
            }
        };
        Ok(crate::stream::HttpBlockStream {
            client: self,
            blocks: blocks.into_iter(),
        })
    }
    /// Create a buffered inserter that sends nothing until explicitly finalized.
    /// # Errors
    /// Invalid identifiers reject this staging interface.
    #[cfg(feature = "http")]
    pub fn http_inserter<'a>(
        &'a mut self,
        table: &str,
    ) -> Result<crate::inserter::HttpInserter<'a>> {
        #[cfg(feature = "tcp")]
        if matches!(self.backend, Backend::Native(_)) {
            return Err(Error::unsupported(
                "HTTP insert staging requires an HTTP endpoint",
            ));
        }
        quote_table(table)?;
        Ok(crate::inserter::HttpInserter::new(self, table))
    }
    #[cfg(feature = "http")]
    pub(crate) fn insert_native_payload(
        &mut self,
        table: &str,
        bytes: &[u8],
        schema: &[(String, String)],
    ) -> Result<()> {
        match &self.backend {
            #[cfg(feature = "tcp")]
            Backend::Native(_) => Err(Error::unsupported(
                "HTTP insert staging requires an HTTP endpoint",
            )),
            #[cfg(feature = "http")]
            Backend::Http(_) => {
                if schema.is_empty() {
                    return Err(Error::protocol(
                        "nonempty Native insert requires a column schema",
                    ));
                }
                let names = schema
                    .iter()
                    .map(|(name, _)| quote_ident(name))
                    .collect::<Result<Vec<_>>>()?;
                let sql = format!(
                    "INSERT INTO {} ({}) FORMAT Native",
                    quote_table(table)?,
                    names.join(", ")
                );
                self.http_call(&Query::new(""), &sql, bytes)?;
                Ok(())
            }
        }
    }
    #[cfg(feature = "http")]
    pub(super) fn http_call(
        &mut self,
        query: &Query<'_>,
        sql: &str,
        body: &[u8],
    ) -> Result<Vec<u8>> {
        match &mut self.backend {
            #[cfg(feature = "http")]
            Backend::Http(h) => h.execute(
                &self.database,
                query,
                sql,
                body,
                self.compression,
                self.dedup.as_deref(),
            ),
            #[cfg(feature = "tcp")]
            Backend::Native(_) => Err(Error::unsupported(
                "formatted HTTP bytes require an HTTP endpoint",
            )),
        }
    }
}

impl Client {
    fn require_http_format(&self) -> Result<()> {
        match &self.backend {
            Backend::Http(_) => Ok(()),
            #[cfg(feature = "tcp")]
            Backend::Native(_) => Err(Error::unsupported(
                "HTTP format operations require an HTTP endpoint",
            )),
        }
    }

    /// Insert serde rows using ClickHouse RowBinary over HTTP.
    ///
    /// Serialization and transport buffering may allocate; no derived Row metadata is required.
    /// # Errors
    /// Native endpoints, unsupported serde values, and server rejections return errors.
    pub fn insert_rowbinary<T: serde::Serialize>(&mut self, table: &str, rows: &[T]) -> Result<()> {
        self.require_http_format()?;
        self.serde_insert_rows(table, rows)
    }

    /// Materialize serde rows from HTTP RowBinaryWithNamesAndTypes metadata.
    /// # Errors
    /// Native endpoints, unsupported query metadata, incompatible rows, and transfer errors
    /// propagate.
    pub fn query_rowbinary<T: serde::de::DeserializeOwned>(
        &mut self,
        query: &Query<'_>,
    ) -> Result<Vec<T>> {
        self.require_http_format()?;
        self.serde_query_rows(query)
    }

    /// Insert an encoded Arrow IPC stream into a validated table name over HTTP.
    /// # Errors
    /// Native endpoints, invalid identifiers, malformed IPC, and server rejection return errors.
    pub fn insert_arrow_stream(&mut self, table: &str, bytes: &[u8]) -> Result<()> {
        self.require_http_format()?;
        let sql = format!("INSERT INTO {} FORMAT ArrowStream", quote_table(table)?);
        self.insert_formatted(&Query::new(sql), bytes)
    }

    /// Fetch ArrowStream over HTTP and decode owned batches with borrowed projection access.
    ///
    /// HTTP buffering and IPC decoding may allocate; projections borrow the decoded arrays.
    /// # Errors
    /// Native endpoints, unsupported metadata, malformed IPC, and transport failures return errors.
    pub fn query_arrow_stream(&mut self, query: &Query<'_>) -> Result<Vec<ArrowBlockView>> {
        self.require_http_format()?;
        let sql = ensure_format(query.sql(), "ArrowStream")?;
        let bytes = self.http_call(query, &sql, &[])?;
        ArrowBlockView::decode_stream(&bytes)
    }
}

impl Client {
    /// Submits a raw HTTP query or insert without waiting for its response.
    ///
    /// SQL supplies its output/input FORMAT, and `body` supplies optional insert
    /// bytes. Drive completions through [`Self::poll_http`] and extract the result
    /// through [`HttpRequest::try_take`]. The request owns its input bytes. After
    /// protocol changes, explicitly call [`Self::reset_http`] before admission.
    ///
    /// # Errors
    /// Returns an unsupported transport, query, configuration, or admission error.
    pub fn submit_http(&mut self, query: &Query<'_>, body: &[u8]) -> Result<HttpRequest> {
        match &mut self.backend {
            Backend::Http(http) => http.submit_raw(
                &self.database,
                query,
                body,
                self.compression,
                self.dedup.as_deref(),
            ),
            #[cfg(feature = "tcp")]
            Backend::Native(_) => Err(Error::unsupported(
                "nonblocking HTTP submission requires an HTTP endpoint",
            )),
        }
    }

    /// Synchronously retires the HTTP driver and applies pending configuration.
    ///
    /// Call this outside the nonblocking polling loop after changing protocol.
    /// This explicit boundary cancels outstanding handles and drains socket and
    /// timer completions. Submission itself never performs this drain.
    ///
    /// # Errors
    /// Returns an unsupported transport or new driver initialization error.
    /// # Panics
    /// Panics if the old proactor cannot retire its registered operations.
    pub fn reset_http(&mut self) -> Result<()> {
        match &mut self.backend {
            Backend::Http(http) => http.reset(),
            #[cfg(feature = "tcp")]
            Backend::Native(_) => Err(Error::unsupported("HTTP reset requires an HTTP endpoint")),
        }
    }

    /// Advances one bounded HTTP/DNS completion turn without waiting for a peer.
    ///
    /// A zero count is normal while a request is pending. Run other application
    /// work between turns; no background executor advances this client.
    ///
    /// # Errors
    /// Returns an unsupported transport, DNS, or proactor dispatch error.
    pub fn poll_http(&mut self) -> Result<usize> {
        match &mut self.backend {
            Backend::Http(http) => http.poll(),
            #[cfg(feature = "tcp")]
            Backend::Native(_) => Err(Error::unsupported("HTTP polling requires an HTTP endpoint")),
        }
    }

    /// Cancels a request owned by this client, delivering its terminal error once.
    ///
    /// Returns false after terminal completion. Polling still retires associated
    /// cancellation and socket completions before the buffers are reclaimed.
    ///
    /// # Errors
    /// Returns an unsupported transport or foreign request ownership error.
    pub fn cancel_http(&mut self, request: &HttpRequest) -> Result<bool> {
        match &mut self.backend {
            Backend::Http(http) => http.cancel(request),
            #[cfg(feature = "tcp")]
            Backend::Native(_) => Err(Error::unsupported(
                "HTTP cancellation requires an HTTP endpoint",
            )),
        }
    }
}

#[cfg(feature = "http")]
pub(super) fn quote_ident(name: &str) -> Result<String> {
    if name.is_empty() || !name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
        return Err(Error::protocol("invalid SQL identifier"));
    }
    Ok(format!("`{name}`"))
}
#[cfg(feature = "http")]
pub(super) fn quote_table(table: &str) -> Result<String> {
    Ok(table
        .split('.')
        .map(quote_ident)
        .collect::<Result<Vec<_>>>()?
        .join("."))
}

/// Preserve compatible terminal FORMAT clauses and append absent clauses safely.
pub(super) fn ensure_format(sql: &str, format: &str) -> Result<String> {
    let tokens = tokens(sql)?;
    let mut end = tokens.len();
    while end > 0 && tokens[end - 1].text == ";" {
        end -= 1;
    }
    if let Some(index) = tokens[..end]
        .iter()
        .rposition(|token| token.text.eq_ignore_ascii_case("FORMAT"))
        && index + 2 == end
        && tokens[index + 1].identifier
    {
        if !tokens[index + 1].text.eq_ignore_ascii_case(format) {
            return Err(Error::Invalid(
                format!(
                    "output FORMAT {} is incompatible with {format}",
                    tokens[index + 1].text
                )
                .into(),
            ));
        }
        return Ok(sql[..tokens[end - 1].end].to_owned());
    }
    // Drop only trailing comments, whitespace, and statement delimiters. Interior
    // comments stay intact; the newline also terminates any final line comment.
    let meaningful_end = tokens[..end].last().map_or(0, |token| token.end);
    Ok(format!("{}\nFORMAT {format}", &sql[..meaningful_end]))
}

struct Token<'a> {
    text: &'a str,
    end: usize,
    identifier: bool,
}

/// Ignore quoted text and comments while retaining their statement boundaries.
fn tokens(sql: &str) -> Result<Vec<Token<'_>>> {
    let bytes = sql.as_bytes();
    let mut out = Vec::new();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i].is_ascii_whitespace() {
            i += 1;
            continue;
        }
        if bytes[i..].starts_with(b"--") {
            while i < bytes.len() && bytes[i] != b'\n' {
                i += 1;
            }
            continue;
        }
        if bytes[i..].starts_with(b"/*") {
            i += 2;
            let mut depth = 1;
            while i < bytes.len() && depth > 0 {
                if bytes[i..].starts_with(b"/*") {
                    depth += 1;
                    i += 2;
                } else if bytes[i..].starts_with(b"*/") {
                    depth -= 1;
                    i += 2;
                } else {
                    i += 1;
                }
            }
            if depth != 0 {
                return Err(Error::Invalid("unterminated SQL block comment".into()));
            }
            continue;
        }
        let start = i;
        let quote = bytes[i];
        if matches!(quote, b'\'' | b'"' | b'`') {
            i += 1;
            let mut closed = false;
            while i < bytes.len() {
                if bytes[i] == b'\\' {
                    i = (i + 2).min(bytes.len());
                } else if bytes[i] == quote {
                    i += 1;
                    if i < bytes.len() && bytes[i] == quote {
                        i += 1;
                    } else {
                        closed = true;
                        break;
                    }
                } else {
                    i += 1;
                }
            }
            if !closed {
                return Err(Error::Invalid("unterminated SQL quoted token".into()));
            }
            out.push(Token {
                text: &sql[start..i],
                end: i,
                identifier: false,
            });
            continue;
        }
        let identifier = bytes[i].is_ascii_alphabetic() || bytes[i] == b'_';
        if identifier {
            i += 1;
            while i < bytes.len() && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_') {
                i += 1;
            }
        } else {
            // Consume complete UTF-8 tokens so all slices end on character boundaries.
            i += sql[i..].chars().next().map_or(1, char::len_utf8);
        }
        out.push(Token {
            text: &sql[start..i],
            end: i,
            identifier,
        });
    }
    Ok(out)
}

#[cfg(test)]
mod unit {
    use super::ensure_format;

    /// Literal/comment FORMAT text cannot suppress the required terminal clause.
    #[test]
    fn literals_comments_and_delimiters() {
        for sql in [
            "SELECT ' format ' AS x",
            "SELECT 1;",
            "SELECT 1; -- FORMAT JSON",
            "SELECT \"format\" /* FORMAT JSON */",
            "SELECT 'it\\'s format JSON'",
            "SELECT 1 /* outer /* nested */ done */",
        ] {
            let result = ensure_format(sql, "Native").unwrap();
            assert!(result.ends_with("\nFORMAT Native"));
            assert!(!result.contains(";\nFORMAT"));
        }
    }

    /// Existing incompatible formats fail before issuing any request.
    #[test]
    fn terminal_format_is_validated() {
        assert_eq!(
            ensure_format("SELECT 1 FORMAT Native; -- done", "native").unwrap(),
            "SELECT 1 FORMAT Native"
        );
        assert!(ensure_format("SELECT 1 FORMAT JSON", "Native").is_err());
        assert!(ensure_format("SELECT 'unterminated", "Native").is_err());
        assert!(ensure_format("SELECT 1 /* unclosed", "Native").is_err());
    }
}
