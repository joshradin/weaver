//! A secured client connection

use std::io::{Read, Write};
use openssl::ssl::{SslConnector, SslMethod, SslStream};
use crate::common::stream_support::Stream;
use crate::error::Error;
/// Wrapper type around a secured stream
#[derive(Debug)]
pub struct Secured<T : Stream> {
    inner: SslStream<T>
}

impl<T: Stream> Secured<T> {
    pub(super) fn wrap(stream: SslStream<T>) -> Self {
        Self { inner: stream }
    }

    /// Secures an existing stream over tls
    pub fn new(host: &str, stream: T) -> Result<Self, Error> {
        let mut connector = SslConnector::builder(SslMethod::tls_client())
            .map_err(|_| Error::SslHandshakeError)
            ?.build();
        let stream = connector.connect(host, stream).map_err(|_| Error::SslHandshakeError)?;
        Ok(Self::wrap(stream))
    }
}

impl<T : Stream> AsRef<T> for Secured<T> {
    fn as_ref(&self) -> &T {
        self.inner.get_ref()
    }
}

impl<T: Stream> Write for Secured<T> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.inner.write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

impl<T: Stream> Read for Secured<T> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.inner.read(buf)
    }
}

