use crate::data::types::Type;
use crate::data::values::Value;
use crate::db::server::processes::WeaverPid;
use crate::dynamic_table::{OpenTableError, OwnedCol, StorageError};
use crossbeam::channel::{RecvError, Sender, SendError};
use serde::ser::StdError;
use std::io;
use thiserror::Error;
use crate::db::server::layers::packets::{DbReq, DbReqBody, DbResp, IntoDbResponse};

#[derive(Debug, Error)]
pub enum Error {
    #[error("Illegal auto increment: {reason}")]
    IllegalAutoIncrement { reason: String },
    #[error("Unexpected value of type found. (expected {expected:?}, received: {actual:?})")]
    TypeError { expected: Type, actual: Value },
    #[error("Illegal definition for column {col:?}: {reason}")]
    IllegalColumnDefinition { col: OwnedCol, reason: Box<Error> },
    #[error("Expected {expected} columns, but found {actual}")]
    BadColumnCount { expected: usize, actual: usize },
    #[error("Primary key must be unique and non null")]
    PrimaryKeyMustBeUniqueAndNonNull,
    #[error("Every table must have a primary key")]
    NoPrimaryKey,
    #[error("Error creating table")]
    CreateTableError,
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    OpenTableError(#[from] OpenTableError),
    #[error(transparent)]
    IoError(#[from] io::Error),
    #[error("No key named {0:?}")]
    BadKeyName(String),
    #[error(transparent)]
    SerializationError(Box<dyn StdError + Send + Sync>),
    #[error(transparent)]
    DeserializationError(Box<dyn StdError + Send + Sync>),
    #[error("failed to connect because handshake failed")]
    HandshakeFailed,
    #[error("A timeout occurred")]
    Timeout,
    #[error("WeaverDb instance already bound to tcp socket")]
    TcpAlreadyBound,
    #[error(transparent)]
    SendError(#[from] SendError<(DbReq, Sender<DbResp>)>),
    #[error(transparent)]
    RecvError(#[from] RecvError),
    #[error("No core available")]
    NoCoreAvailable,
    #[error("No table named {0:?} found in schema {1:?}")]
    NoTableFound(String, String),
    #[error("no transaction")]
    NoTransaction,
    #[error("Process {0} failed")]
    ProcessFailed(WeaverPid),
    #[error("A server error occurred ({0})")]
    ServerError(String),
    #[error("thread panicked")]
    ThreadPanicked,
}

impl Error {

    /// A server error occurred
    pub fn server_error(error: impl ToString) -> Self {
        Self::ServerError(error.to_string())
    }
}

impl IntoDbResponse for Error {
    fn into_db_resp(self) -> DbResp {
        DbResp::Err(self.to_string())
    }
}

