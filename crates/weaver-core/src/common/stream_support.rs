//! Support for streams

use std::io;
use std::io::{Read, Write};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde::de::DeserializeOwned;

/// Marker trait for something that you can both read and write to
pub trait Stream : Read + Write {}

impl<S : Read + Write> Stream for S {
}

pub struct Timeout<S> {
    stream: S,
    timeout: Duration
}

impl<S> Timeout<S> {

    /// Creates a new stream that can timeout
    pub fn new(stream: S, timeout: Duration) -> Self {
        Self {
            stream,
            timeout
        }
    }
}


/// to send a discrete packet of information to a stream
pub fn packet_write<T : Serialize, W: Write>(writer: &mut W, data: &T) -> Result<usize, io::Error> {
    let mut data_buffer = vec![];
    serde_json::to_writer(&mut data_buffer, data).map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
    let len_buffer = (data_buffer.len() as u64).to_be_bytes();
    writer.write_all(&len_buffer)?;
    writer.write_all(&data_buffer[..])?;
    Ok(data_buffer.len() + len_buffer.len())
}

/// to send a discrete packet of information to a stream
pub fn packet_read<T: DeserializeOwned, R: Read>(reader: &mut R) -> Result<T, io::Error> {
    let mut packet_len_buffer = [0_u8; std::mem::size_of::<u64>()];
    reader.read_exact(&mut packet_len_buffer)?;
    let len = u64::from_be_bytes(packet_len_buffer);
    let mut buffer = vec![0u8; len as usize];
    reader.read_exact(&mut buffer)?;
    serde_json::from_slice(&buffer[..]).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
}


#[cfg(test)]
mod tests {
    use std::collections::VecDeque;

    use serde::{Deserialize, Serialize};

    use crate::common::stream_support::{packet_read, packet_write};

    #[derive(Debug, Deserialize, Serialize, PartialEq)]
    struct Test {
        value: String,
        other_value: i32,
    }

    #[test]
    fn read_write_packets() {
        let mut buffer = VecDeque::new();
        let data = Test {
            value: "hello, world".to_string(),
            other_value: 123,
        };
        let bytes = packet_write(&mut buffer, &data).expect("couldn't write packet");
        assert!(bytes > 0, "wrote some number of bytes");
        let len_bytes = buffer.range(0..8).copied().collect::<Vec<u8>>();
        let mut len_bytes_buf= [0; 8];
        len_bytes_buf.copy_from_slice(&len_bytes[..]);
        assert_eq!(u64::from_be_bytes(len_bytes_buf) as usize, buffer.len() - 8);
        let new_data: Test = packet_read(&mut buffer).expect("couldn't read data");
        assert_eq!(new_data, data);
    }
}