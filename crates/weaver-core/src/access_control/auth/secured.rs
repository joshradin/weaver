//! A secured client connection

use crate::common::stream_support::Stream;
use crate::error::WeaverError;
use openssl::ssl::{SslConnector, SslMethod, SslStream, SslVerifyMode};
use openssl::x509::X509;
use std::io::{Read, Write};
use tracing::{debug, trace};

/// Wrapper type around a secured stream
#[derive(Debug)]
pub struct Secured<T: Stream> {
    inner: SslStream<T>,
}

impl<T: Stream> Secured<T> {
    pub(super) fn wrap(stream: SslStream<T>) -> Self {
        Self { inner: stream }
    }

    /// Secures an existing stream over tls
    pub fn new(host: &str, stream: T) -> Result<Self, WeaverError> {
        let mut connector = Self::connector()?;
        let stream = connector.connect(host, stream)?;
        Ok(Self::wrap(stream))
    }

    fn connector() -> Result<SslConnector, WeaverError> {
        (|| {
            let mut builder = SslConnector::builder(SslMethod::tls_client())?;
            builder.set_verify_callback(SslVerifyMode::all(), |res, store| {
                trace!("verifying x509 cert from server: initial resolution = {res}");

                let Some(cert_chain) = store.chain() else {
                    return false;
                };

                for x509_ref in cert_chain {
                    trace!("checking against x509");
                    if let Ok(text) = x509_ref.to_text() {
                        let text = String::from_utf8_lossy(&text[..]);
                        trace!("human readable: {}", text);
                    }
                }

                true
            });
            Ok(builder.build())
        })()
        .map_err(|e| WeaverError::SslConnectorBuilderError(e))
    }
}

impl<T: Stream> AsRef<T> for Secured<T> {
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
