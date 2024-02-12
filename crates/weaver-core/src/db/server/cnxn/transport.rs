//! Transport provider

use crate::access_control::auth::secured::Secured;
use crate::common::pretty_bytes::PrettyBytes;
use crate::common::stream_support::Stream;
use cfg_if::cfg_if;
use std::io;
use std::io::{Read, Write};
use tracing::{trace, trace_span};

#[derive(Debug)]
pub enum Transport<T: Stream> {
    Insecure(StreamSniffer<T>),
    Secure(Secured<StreamSniffer<T>>),
}

impl<T: Stream> AsRef<T> for Transport<T> {
    fn as_ref(&self) -> &T {
        match self {
            Transport::Insecure(i) => i.as_ref(),
            Transport::Secure(i) => i.as_ref().as_ref(),
        }
    }
}

impl<T: Stream> Write for Transport<T> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match self {
            Transport::Insecure(i) => i.write(buf),
            Transport::Secure(i) => trace_span!("ssl").in_scope(|| i.write(buf)),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match self {
            Transport::Insecure(i) => i.flush(),
            Transport::Secure(s) => trace_span!("ssl").in_scope(|| s.flush()),
        }
    }
}

impl<T: Stream> Read for Transport<T> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match self {
            Transport::Insecure(i) => i.read(buf),
            Transport::Secure(i) => trace_span!("ssl").in_scope(|| i.read(buf)),
        }
    }
}

#[derive(Debug)]
pub struct StreamSniffer<T>(T);

impl<T> AsRef<T> for StreamSniffer<T> {
    fn as_ref(&self) -> &T {
        &self.0
    }
}

impl<T> From<T> for StreamSniffer<T> {
    fn from(value: T) -> Self {
        Self(value)
    }
}

impl<T> StreamSniffer<T> {
    pub fn stream(stream: T) -> Self
    where
        T: Stream,
    {
        Self(stream)
    }

    pub fn read_only(stream: T) -> Self
    where
        T: Read,
    {
        Self(stream)
    }

    pub fn write_only(stream: T) -> Self
    where
        T: Write,
    {
        Self(stream)
    }
}

impl<T: Read> Read for StreamSniffer<T> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let output = self.0.read(buf);
        cfg_if! {
            if #[cfg(feature="transport-sniffing")] {
                match output {
                    Ok(bytes) => {
                        trace!("read ({bytes} bytes): {}", PrettyBytes(&buf[..bytes]));
                    }
                    Err(ref e) => {
                        tracing::warn!("read failed: {}", e);
                    }
                }
            }
        }
        output
    }
}

impl<T: Write> Write for StreamSniffer<T> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        cfg_if! {
            if #[cfg(feature="transport-sniffing")] {
                trace!("write: {}", PrettyBytes(buf));
            }
        }
        let output = self.0.write(buf);
        cfg_if! {
            if #[cfg(feature="transport-sniffing")] {
                trace!("output: {:?}", output);
            }
        }
        output
    }

    fn flush(&mut self) -> io::Result<()> {
        let emit = self.0.flush();
        cfg_if! {
            if #[cfg(feature="transport-sniffing")] {
                trace!("flushed: ({emit:?})");
            }
        }
        emit
    }
}

#[cfg(feature = "transport-sniffing")]
impl<S> Drop for StreamSniffer<S> {
    fn drop(&mut self) {
        trace!("dropping stream");
    }
}
