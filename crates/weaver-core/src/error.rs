use crate::data::types::Type;
use crate::data::values::Value;
use crate::db::concurrency::{DbReq, DbResp};
use crate::dynamic_table::{OpenTableError, OwnedCol, StorageError};
use crossbeam::channel::{RecvError, SendError, Sender};
use serde::ser::StdError;
use std::io;
use thiserror::Error;

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
}
