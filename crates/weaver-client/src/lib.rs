use eyre::eyre;
use std::net::ToSocketAddrs;
use std::time::{Duration, Instant};
use weaver_core::cnxn::tcp::WeaverTcpStream;
use weaver_core::cnxn::{MessageStream, RemoteDbReq, RemoteDbResp};
use weaver_core::data::row::Row;
use weaver_core::db::server::processes::WeaverPid;
use weaver_core::queries::ast::Query;
use weaver_core::rows::{DefaultOwnedRows, OwnedRows, OwnedRowsExt, Rows};
use weaver_core::tables::table_schema::TableSchema;

pub mod write_rows;

/// A client to attach to a weaver instance
#[derive(Debug)]
pub struct WeaverClient {
    stream: WeaverTcpStream,
    pid: WeaverPid,
}

impl WeaverClient {
    /// Connect to a weaver db instance
    pub fn connect<A: ToSocketAddrs>(addr: A) -> eyre::Result<WeaverClient> {
        let mut client = WeaverTcpStream::connect(addr)?;
        let RemoteDbResp::ConnectionInfo(cnxn) = client.send(&RemoteDbReq::ConnectionInfo)? else {
            return Err(eyre!("couldn't get connection info"));
        };
        let pid = cnxn.pid;
        Ok(Self {
            stream: client,
            pid,
        })
    }

    pub fn query(&mut self, query: &Query) -> eyre::Result<(impl Rows, Duration)> {
        let start = Instant::now();
        let RemoteDbResp::Ok = self.stream.send(&RemoteDbReq::Query(query.clone()))? else {
            return Err(eyre!("couldn't send query"));
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
}

#[derive(Debug)]
pub struct RemoteRows<'a> {
    schema: TableSchema,
    stream: &'a mut WeaverTcpStream,
}

impl<'a> Rows<'a> for RemoteRows<'a> {
    fn schema(&self) -> &TableSchema {
        &self.schema
    }

    fn next(&mut self) -> Option<Row<'a>> {
        match self.stream.send(&RemoteDbReq::GetRow) {
            Ok(RemoteDbResp::Row(row)) => row.map(|row| Row::from(row)),
            _ => None,
        }
    }
}
