use crate::access_control::auth::error::AuthInitError;
use crate::cancellable_task::Cancelled;
use crate::data::types::Type;
use crate::data::values::Value;
use crate::db::server::layers::packets::{DbReq, DbReqBody, DbResp, IntoDbResponse};
use crate::db::server::processes::WeaverPid;
use crate::db::server::socket::MainQueueItem;
use crate::dynamic_table::{OpenTableError, OwnedCol, StorageError, TableCol};
use crate::storage::slotted_page::PageType;
use crate::storage::{ReadDataError, WriteDataError};
use crossbeam::channel::{RecvError, SendError, Sender};
use openssl::error::ErrorStack;
use openssl::ssl::HandshakeError;
use serde::ser::StdError;
use std::backtrace::Backtrace;
use std::convert::Infallible;
use std::io;

#[derive(Debug, thiserror::Error)]
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
    SendError(#[from] SendError<MainQueueItem>),
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
    #[error("Authentication failed")]
    AuthenticationFailed,
    #[error("No host name")]
    NoHostName,
    #[error(transparent)]
    AuthInitError(#[from] AuthInitError),
    #[error(transparent)]
    SslConnectorBuilderError(ErrorStack),
    #[error("Ssl handshake setup error: ({0})")]
    SslHandshakeSetupError(ErrorStack),
    #[error("Ssl handshake failure error: ({0})")]
    SslHandshakeFailure(openssl::ssl::Error),
    #[error("Ssl handshake would block: ({0})")]
    SslHandshakeWouldBlock(openssl::ssl::Error),
    #[error("Could not parse {0:?}")]
    ParseError(String),
    #[error("Could not use unqualified table reference without in-use schema")]
    UnQualifedTableWithoutInUseSchema,
    #[error("Task was cancelled")]
    TaskCancelled,
    #[error("Channel empty")]
    ChannelEmpty,
    #[error("No column named {0:?} could be found")]
    ColumnNotFound(String),
    #[error("Mutiple options found for column {col:?}: {positives:#?}")]
    AmbiguousColumn {
        col: String,
        positives: Vec<TableCol>,
    },
    #[error("encountered an error trying to read a cell: {0}")]
    ReadDataError(#[from] ReadDataError),
    #[error("encountered an error trying to write a cell: {0}")]
    WriteDataError(#[from] WriteDataError),
    #[error(
        "Given cell can not be written on this page (expected: {expected:?}, actual: {actual:?})"
    )]
    CellTypeMismatch {
        expected: PageType,
        actual: PageType,
    },
    #[error("No child with id {0} found")]
    ChildNotFound(u32),
    #[error("Out of range")]
    OutOfRange,
    #[error("Failed to allocate {0} bytes")]
    AllocationFailed(usize),
    #[error("Error: {msg}")]
    WrappedException {
        msg: String,
        #[source]
        source: Box<Error>,
        #[backtrace]
        backtrace: Backtrace,
    },
    #[error("{0}")]
    Custom(String),
}

impl Error {
    /// A server error occurred
    pub fn server_error(error: impl ToString) -> Self {
        Self::ServerError(error.to_string())
    }

    pub fn custom<T: ToString + 'static>(error: T) -> Self {
        Self::Custom(error.to_string())
    }

    #[inline]
    #[track_caller]
    pub fn wrap<T: ToString + 'static, E: Into<Self>>(msg: T, error: E) -> Self {
        Self::WrappedException {
            msg: msg.to_string(),
            source: Box::new(error.into()),
            backtrace: Backtrace::capture(),
        }
    }
}

impl IntoDbResponse for Error {
    fn into_db_resp(self) -> DbResp {
        DbResp::Err(self)
    }
}

impl<S> From<HandshakeError<S>> for Error {
    fn from(value: HandshakeError<S>) -> Self {
        match value {
            HandshakeError::SetupFailure(error) => Error::SslHandshakeSetupError(error),
            HandshakeError::Failure(error) => Error::SslHandshakeFailure(error.into_error()),
            HandshakeError::WouldBlock(error) => Error::SslHandshakeWouldBlock(error.into_error()),
        }
    }
}

impl From<Cancelled> for Error {
    fn from(_: Cancelled) -> Self {
        Self::TaskCancelled
    }
}

impl From<Infallible> for Error {
    fn from(_: Infallible) -> Self {
        unreachable!("infallible values are not constructable")
    }
}
