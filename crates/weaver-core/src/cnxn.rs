//! Connections that are used for connecting to [`WeaverDb`](WeaverDb) instances

use crate::cnxn::stream::WeaverStream;
use crate::common::stream_support::Stream;
use crate::data::row::OwnedRow;
use crate::db::server::processes::WeaverProcessInfo;
use crate::error::Error;
use crate::queries::ast::Query;
use crate::tables::table_schema::TableSchema;
use serde::{Deserialize, Serialize};
use std::io::{Read, Write};

pub mod cnxn_loop;
mod handshake;
pub mod interprocess;
pub mod stream;
pub mod tcp;
pub mod transport;

/// The default port to use
pub static DEFAULT_PORT: u16 = 5234;

/// A remote db request
#[derive(Debug, Clone, Deserialize, Serialize)]
pub enum RemoteDbReq {
    /// A remote query
    Query(Query),
    /// A remote query that's parsed server-side
    DelegatedQuery(String),
    GetRow,
    GetSchema,
    ConnectionInfo,
    /// Tell the remote connection to sleep for some number of seconds
    Sleep(u64),
    Ping,
    /// Start a transaction
    StartTransaction,
    /// Commit a transaction
    Commit,
    /// Rollback a transaction
    Rollback,
}

/// A remote db response
#[derive(Debug, Deserialize, Serialize)]
pub enum RemoteDbResp {
    Pong,
    Ok,
    Schema(TableSchema),
    Row(Option<OwnedRow>),
    ConnectionInfo(WeaverProcessInfo),
    Err(String),
}

/// Messages that can be sent between shards
#[derive(Debug, Deserialize, Serialize)]
pub enum Message {
    Handshake { ack: bool, nonce: Vec<u8> },
    Req(RemoteDbReq),
    Resp(RemoteDbResp),
}

pub fn write_msg<W: Write>(writer: W, msg: &Message) -> Result<(), Error> {
    Ok(serde_json::to_writer(writer, msg).map_err(|e| Error::SerializationError(Box::new(e)))?)
}

pub fn read_msg<R: Read>(reader: R) -> Result<Message, Error> {
    Ok(serde_json::from_reader(reader).map_err(|e| Error::DeserializationError(Box::new(e)))?)
}

/// A message stream
pub trait MessageStream {
    /// Read a message
    fn read(&mut self) -> Result<Message, Error>;

    /// Write a message
    fn write(&mut self, message: &Message) -> Result<(), Error>;

    /// Wrapper around sending a request and receiving response
    fn send(&mut self, message: &RemoteDbReq) -> Result<RemoteDbResp, Error> {
        self.write(&Message::Req(message.clone()))?;
        let Message::Resp(resp) = self.read()? else {
            unreachable!();
        };
        Ok(resp)
    }
}

impl<M: MessageStream> MessageStream for &mut M {
    fn read(&mut self) -> Result<Message, Error> {
        (*self).read()
    }

    fn write(&mut self, message: &Message) -> Result<(), Error> {
        (*self).write(message)
    }
}

pub trait WeaverStreamListener {
    type Stream: Stream;
    /// Accepts an incoming connection
    fn accept(&self) -> Result<WeaverStream<Self::Stream>, Error>;
}
