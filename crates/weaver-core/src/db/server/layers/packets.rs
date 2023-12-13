use std::borrow::Cow;
use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;
use serde::{Deserialize, Serialize};
use crate::db::core::WeaverDbCore;
use crate::db::server::WeaverDb;
use crate::dynamic_table::Table;
use crate::error::Error;
use crate::queries::ast::Query;
use crate::rows::OwnedRows;
use crate::tx::Tx;

/// Headers are used to convey extra data in requests
#[derive(Default, Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
pub struct Headers {
    header: HashMap<String, Vec<String>>
}

impl Headers {

    /// Gets header values, if present
    pub fn get(&self, header: impl AsRef<str>) -> Option<&[String]> {
        self.header.get(header.as_ref()).map(|s| s.as_slice())
    }

    /// Sets a value, appending it to an already existing header if it's already present
    pub fn set(&mut self, header: impl AsRef<str>, value: impl ToString) {
        self.header.entry(header.as_ref().to_string())
            .or_default()
            .push(value.to_string());
    }

    /// Clears a header if present, removing it from the map
    pub fn clear(&mut self, header: impl AsRef<str>) {
        let _ = self.header.remove(header.as_ref());
    }
}

/// A request that is send to a [`WeaverDb`](WeaverDb)
#[derive(Debug)]
pub struct DbReq {
    headers: Headers,
    body: DbReqBody
}

impl DbReq {
    /// Create a new db response
    pub fn new(headers: Headers, body: DbReqBody) -> Self {
        Self { headers, body }
    }

    pub fn on_core<F, T : IntoDbResponse>(cb: F) -> Self
        where F : FnOnce(&mut WeaverDbCore) -> T + Send + Sync + 'static
    {
        DbReqBody::on_core(|core| Ok(cb(core).into_db_resp())).into()
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

    pub fn to_parts(self) -> (Headers, DbReqBody) {
        let DbReq { headers, body } = self;
        (headers, body)
    }
}

impl From<DbReqBody> for DbReq {

    /// Creates db request with default headers
    fn from(value: DbReqBody) -> Self {
        Self {
            headers: Default::default(),
            body: value,
        }
    }
}


/// The base request that is sent to the database

pub enum DbReqBody {
    OnCore(Box<dyn FnOnce(&mut WeaverDbCore) -> Result<DbResp, Error> + Send + Sync>),
    OnServer(Box<dyn FnOnce(&mut WeaverDb) -> Result<DbResp, Error> + Send + Sync>),
    /// Send a query to the request
    TxQuery(Tx, Query),
    Ping,
    StartTransaction,
    Commit(Tx),
    Rollback(Tx),
}

impl Debug for DbReqBody {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DbReq").finish_non_exhaustive()
    }
}

impl DbReqBody {
    /// Gets full access of db core
    pub fn on_core<
        F: FnOnce(&mut WeaverDbCore) -> Result<DbResp, Error> + Send + Sync + 'static,
    >(
        func: F,
    ) -> Self {
        Self::OnCore(Box::new(func))
    }

    /// Gets full access of db server
    pub fn on_server<F: FnOnce(&mut WeaverDb) -> Result<DbResp, Error> + Send + Sync + 'static>(
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
impl<R : IntoDbResponse, E : IntoDbResponse> IntoDbResponse for Result<R, E> {
    fn into_db_resp(self) -> DbResp {
        match self {
            Ok(ok) => {
                ok.into_db_resp()
            }
            Err(err) => {
                err.into_db_resp()
            }
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
    TxTable(Tx, Arc<Table>),
    TxRows(Tx, Box<dyn OwnedRows + Send + Sync>),
    Rows(Box<dyn OwnedRows + Send + Sync>),
    Err(String),
}

impl IntoDbResponse for DbResp {
    fn into_db_resp(self) -> DbResp {
        self
    }
}

impl DbResp {
    pub fn rows<T: OwnedRows + Send + Sync + 'static>(rows: T) -> Self {
        Self::Rows(Box::new(rows))
    }
}

impl<E : std::error::Error> From<E> for DbResp {
    fn from(value: E) -> Self {
        Self::Err(value.to_string())
    }
}