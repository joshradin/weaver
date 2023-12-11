//! Connections that are used for connecting to [`WeaverDb`](WeaverDb) instances

use std::io::{Read, Write};
use std::time::Duration;
use serde::{Deserialize, Serialize};
use crate::db::concurrency::{DbReq, DbResp};
use crate::error::Error;

pub mod tcp;
mod handshake;
pub mod cnxn_loop;

/// Messages that can be sent between shards
#[derive(Debug, Deserialize, Serialize)]
pub enum Message {
    Handshake {
        ack: bool,
        nonce: Vec<u8>
    },
    Req(DbReq),
    Resp(DbResp)
}

pub fn write_msg<W : Write>(writer: W, msg: &Message) -> Result<(), Error> {
    Ok(serde_json::to_writer(writer, msg).map_err(|e| Error::SerializationError(Box::new(e)))?)
}

pub fn read_msg<R : Read>(reader: R) -> Result<Message, Error> {
    Ok(serde_json::from_reader(reader).map_err(|e| Error::DeserializationError(Box::new(e)))?)
}

/// A message stream
pub trait MessageStream {

    /// Read a message
    fn read(&mut self) -> Result<Message, Error>;

    /// Read a message with a timeout. Requires mutable access to the message stream
    fn read_timeout(&mut self, duration: Duration) -> Result<Message, Error>;

    /// Write a message
    fn write(&mut self, message: &Message) -> Result<(), Error>;
}