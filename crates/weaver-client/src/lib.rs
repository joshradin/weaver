use std::net::{TcpStream, ToSocketAddrs};
use std::path::Path;
use std::time::{Duration, Instant};

use eyre::eyre;
use interprocess::local_socket::LocalSocketStream;
use log::{debug, trace};

use weaver_ast::ast::Query;
use weaver_core::access_control::auth::LoginContext;
use weaver_core::cnxn::stream::WeaverStream;
use weaver_core::cnxn::{MessageStream, RemoteDbReq, RemoteDbResp};
use weaver_core::common::stream_support::Stream;
use weaver_core::data::row::Row;
use weaver_core::db::server::processes::WeaverPid;

use weaver_core::rows::Rows;
use weaver_core::storage::tables::table_schema::TableSchema;

pub mod write_rows;

/// A client to attach to a weaver instance
#[derive(Debug)]
pub struct WeaverClient<T: Stream> {
    stream: WeaverStream<T>,
    pid: WeaverPid,
}

impl<T: Stream> Drop for WeaverClient<T> {
    fn drop(&mut self) {
        trace!("dropping weaver client");
        trace!("sending disconnect to server as a courtesy");
        match self.stream.send(&RemoteDbReq::Disconnect) {
            Ok(RemoteDbResp::Disconnect) => {
                trace!("disconnect acknowledged");
            }
            _other => {
                trace!("Unexpected response after disconnect: {_other:?}");
            }
        }
    }
}

impl WeaverClient<TcpStream> {
    /// Connect to a weaver db instance
    pub fn connect<A: ToSocketAddrs>(addr: A, login_context: LoginContext) -> eyre::Result<Self> {
        let mut client = WeaverStream::connect(addr, login_context)?;
        let RemoteDbResp::ConnectionInfo(cnxn) = client.send(&RemoteDbReq::ConnectionInfo)? else {
            return Err(eyre!("couldn't get connection info"));
        };
        let pid = cnxn.pid;
        Ok(Self {
            stream: client,
            pid,
        })
    }
}

impl WeaverClient<LocalSocketStream> {
    pub fn connect_localhost<P: AsRef<Path>>(
        socket_path: P,
        login_context: LoginContext,
    ) -> eyre::Result<Self> {
        let mut client = WeaverStream::local_socket(socket_path, login_context)?;
        let RemoteDbResp::ConnectionInfo(cnxn) = client.send(&RemoteDbReq::ConnectionInfo)? else {
            return Err(eyre!("couldn't get connection info"));
        };
        let pid = cnxn.pid;
        Ok(Self {
            stream: client,
            pid: pid,
        })
    }
}
impl<T: Stream> WeaverClient<T> {
    pub fn query(&mut self, query: &Query) -> eyre::Result<(impl Rows, Duration)> {
        debug!("query: {query}");
        let start = Instant::now();
        match self.stream.send(&RemoteDbReq::Query(query.clone()))? {
            RemoteDbResp::Ok => {}
            RemoteDbResp::Err(e) => return Err(eyre!("query failed: {e}")),
            e => return Err(eyre!("unexpected response: {e:?}")),
        };

        let RemoteDbResp::Schema(schema) = self.stream.send(&RemoteDbReq::GetSchema)? else {
            return Err(eyre!("couldn't get table schema"));
        };

        Ok((
            RemoteRows {
                schema,
                stream: &mut self.stream,
            },
            start.elapsed(),
        ))
    }

    /// Gets the *reported* pid of this client.
    pub fn pid(&self) -> WeaverPid {
        self.pid
    }

    /// Check if this client is still connected
    pub fn connected(&mut self) -> bool {
        match self.stream.send(&RemoteDbReq::Ping) {
            Ok(RemoteDbResp::Pong) => true,
            _ => false,
        }
    }
}

#[derive(Debug)]
pub struct RemoteRows<'a, T: Stream> {
    schema: TableSchema,
    stream: &'a mut WeaverStream<T>,
}

impl<'a, T: Stream> Rows<'a> for RemoteRows<'a, T> {
    fn schema(&self) -> &TableSchema {
        &self.schema
    }

    fn next(&mut self) -> Option<Row<'a>> {
        match self.stream.send(&RemoteDbReq::GetRow) {
            Ok(RemoteDbResp::Row(row)) => row.map(Row::from),
            _ => None,
        }
    }
}
