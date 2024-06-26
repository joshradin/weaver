use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::sync::atomic::{AtomicU64, Ordering};

use crossbeam::channel::Receiver;
use serde::{Deserialize, Serialize};
use tracing::Span;

use weaver_ast::ast::Query;

use crate::cancellable_task::{Cancel, CancelRecv, Cancelled};
use crate::db::core::WeaverDbCore;
use crate::db::server::processes::WeaverProcessInfo;
use crate::db::server::WeaverDb;
use crate::error::WeaverError;
use crate::rows::OwnedRows;
use crate::storage::tables::shared_table::SharedTable;
use crate::tx::Tx;

/// Headers are used to convey extra data in requests
#[derive(Default, Serialize, Deserialize, PartialEq, Eq, Clone)]
pub struct Headers {
    header_map: HashMap<String, Vec<String>>,
}

impl Debug for Headers {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_map().entries(self.header_map.iter()).finish()
    }
}

impl Headers {
    /// Gets header values, if present
    pub fn get(&self, header: impl AsRef<str>) -> Option<&[String]> {
        self.header_map.get(header.as_ref()).map(|s| s.as_slice())
    }

    /// Sets a value, appending it to an already existing header if it's already present
    pub fn set(&mut self, header: impl AsRef<str>, value: impl ToString) {
        self.header_map
            .entry(header.as_ref().to_string())
            .or_default()
            .push(value.to_string());
    }

    /// Clears a header if present, removing it from the map
    pub fn clear(&mut self, header: impl AsRef<str>) {
        let _ = self.header_map.remove(header.as_ref());
    }
}

/// A request that is send to a [`WeaverDb`](WeaverDb)
#[derive(Debug)]
pub struct DbReq {
    headers: Headers,
    ctx: Option<WeaverProcessInfo>,
    body: DbReqBody,
    span: Option<Span>,
}

impl DbReq {
    /// Create a new db response
    pub fn new(headers: Headers, body: DbReqBody, span: impl Into<Option<Span>>) -> Self {
        Self {
            headers,
            ctx: None,
            body,
            span: span.into(),
        }
    }

    pub fn set_ctx(&mut self, ctx: WeaverProcessInfo) {
        self.ctx = Some(ctx);
    }

    pub fn ctx(&self) -> Option<&WeaverProcessInfo> {
        self.ctx.as_ref()
    }

    pub fn on_core<F, T: IntoDbResponse>(cb: F) -> Self
    where
        F: FnOnce(&mut WeaverDbCore, &CancelRecv) -> T + Send + Sync + 'static,
    {
        DbReqBody::on_core_write(|core, cancel| Ok(cb(core, cancel).into_db_resp())).into()
    }

    /// Gets the headers
    pub fn headers(&self) -> &Headers {
        &self.headers
    }

    /// Get a mutable reference to the headers
    pub fn headers_mut(&mut self) -> &mut Headers {
        &mut self.headers
    }

    /// Replaces the body
    pub fn replace_body(&mut self, body: DbReqBody) {
        self.body = body;
    }

    /// Gets a reference to the body
    pub fn body(&self) -> &DbReqBody {
        &self.body
    }

    pub fn to_parts(self) -> (Headers, Option<WeaverProcessInfo>, DbReqBody) {
        let DbReq {
            headers, ctx, body, ..
        } = self;
        (headers, ctx, body)
    }

    /// Gets the associated span for this packet
    pub fn span(&self) -> Option<&Span> {
        self.span.as_ref()
    }

    /// Allows for altering the span
    pub fn span_mut(&mut self) -> &mut Option<Span> {
        &mut self.span
    }
}

impl From<DbReqBody> for DbReq {
    /// Creates db request with default headers
    fn from(value: DbReqBody) -> Self {
        Self {
            headers: Default::default(),
            ctx: None,
            body: value,
            span: None,
        }
    }
}

impl From<(Tx, Query)> for DbReq {
    fn from((tx, query): (Tx, Query)) -> Self {
        DbReq::new(Headers::default(), DbReqBody::TxQuery(tx, query), None)
    }
}

impl From<(Tx, Query, Span)> for DbReq {
    fn from((tx, query, span): (Tx, Query, Span)) -> Self {
        DbReq::new(Headers::default(), DbReqBody::TxQuery(tx, query), span)
    }
}

impl From<(DbReqBody, Span)> for DbReq {
    fn from((body, span): (DbReqBody, Span)) -> Self {
        DbReq::new(Headers::default(), body, span)
    }
}

pub type CoreWriteCallback =
    dyn FnOnce(&mut WeaverDbCore, &Receiver<Cancel>) -> Result<DbResp, Cancelled> + Send + Sync;
pub type CoreReadCallback =
    dyn FnOnce(&WeaverDbCore, &Receiver<Cancel>) -> Result<DbResp, Cancelled> + Send + Sync;
pub type ServerCallback =
    dyn FnOnce(&mut WeaverDb, &Receiver<Cancel>) -> Result<DbResp, Cancelled> + Send + Sync;

/// The base request that is sent to the database

pub enum DbReqBody {
    OnCoreWrite(Box<CoreWriteCallback>),
    OnCore(Box<CoreReadCallback>),
    OnServer(Box<ServerCallback>),
    /// Send a query to the request
    TxQuery(Tx, Query),
    Ping,
    StartTransaction,
    Commit(Tx),
    Rollback(Tx),
}

impl Debug for DbReqBody {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            DbReqBody::TxQuery(tx, query, ..) => {
                f.debug_tuple("TxQuery").field(tx).field(query).finish()
            }
            DbReqBody::Ping => f.debug_tuple("Ping").finish(),
            DbReqBody::StartTransaction => f.debug_tuple("StartTransaction").finish(),
            DbReqBody::Commit(c) => f.debug_tuple("Commit").field(c).finish(),
            DbReqBody::Rollback(r) => f.debug_tuple("Rollback").field(r).finish(),
            DbReqBody::OnCore(_) => f.debug_struct("OnCore").finish_non_exhaustive(),
            DbReqBody::OnCoreWrite(_) => f.debug_struct("OnCoreWrite").finish_non_exhaustive(),
            DbReqBody::OnServer(_) => f.debug_struct("OnServer").finish_non_exhaustive(),
            #[allow(unreachable_patterns)]
            _ => f.debug_struct("DbReqBody").finish_non_exhaustive(),
        }
    }
}

impl DbReqBody {
    /// Gets full access of db core
    pub fn on_core_write<
        F: FnOnce(&mut WeaverDbCore, &CancelRecv) -> Result<DbResp, Cancelled> + Send + Sync + 'static,
    >(
        func: F,
    ) -> Self {
        Self::OnCoreWrite(Box::new(func))
    }

    /// Gets full access of db core
    pub fn on_core<
        F: FnOnce(&WeaverDbCore, &CancelRecv) -> Result<DbResp, Cancelled> + Send + Sync + 'static,
    >(
        func: F,
    ) -> Self {
        Self::OnCore(Box::new(func))
    }

    /// Gets full access of db server
    pub fn on_server<
        F: FnOnce(&mut WeaverDb, &CancelRecv) -> Result<DbResp, Cancelled> + Send + Sync + 'static,
    >(
        func: F,
    ) -> Self {
        Self::OnServer(Box::new(func))
    }
}

/// Converts something to a db response
pub trait IntoDbResponse {
    /// Convert to a db response
    fn into_db_resp(self) -> DbResp;
}
impl<R: IntoDbResponse, E: IntoDbResponse> IntoDbResponse for Result<R, E> {
    fn into_db_resp(self) -> DbResp {
        match self {
            Ok(ok) => ok.into_db_resp(),
            Err(err) => err.into_db_resp(),
        }
    }
}

impl IntoDbResponse for () {
    fn into_db_resp(self) -> DbResp {
        DbResp::Ok
    }
}

#[derive(Debug)]
pub enum DbResp {
    Pong,
    Ok,
    Tx(Tx),
    TxTable(Tx, SharedTable),
    TxRows(Tx, OwnedRows),
    Rows(OwnedRows),
    Err(WeaverError),
}

impl IntoDbResponse for DbResp {
    fn into_db_resp(self) -> DbResp {
        self
    }
}

impl DbResp {
    pub fn rows(rows: OwnedRows) -> Self {
        Self::Rows(rows)
    }

    pub fn to_result(self) -> Result<DbResp, WeaverError> {
        match self {
            DbResp::Err(e) => Err(WeaverError::custom(e)),
            db => Ok(db),
        }
    }
}

impl<E: Into<WeaverError>> From<E> for DbResp {
    fn from(value: E) -> Self {
        Self::Err(value.into())
    }
}

/// An id of a packet. Useful for multiplexing
pub type PacketId = u64;

static PACKET_ID_SOURCE: AtomicU64 = AtomicU64::new(1);

/// Packets contain a body and a packet id
#[derive(Debug, Serialize, Deserialize)]
pub struct Packet<T> {
    id: PacketId,
    body: T,
}

impl<T> Packet<T> {
    /// Create a new packet with a generated id
    pub fn new(body: T) -> Self {
        Self::with_id(body, PACKET_ID_SOURCE.fetch_add(1, Ordering::SeqCst))
    }

    /// Create a new packet with a given id
    pub fn with_id(body: T, id: PacketId) -> Self {
        Self { id, body }
    }

    /// Gets the id of the packet
    pub fn id(&self) -> &PacketId {
        &self.id
    }

    /// Gets the body
    pub fn body(&self) -> &T {
        &self.body
    }

    /// Unwraps this packet into just it's body
    pub fn unwrap(self) -> T {
        self.body
    }
}

impl<T> From<T> for Packet<T> {
    fn from(value: T) -> Self {
        Self::new(value)
    }
}
