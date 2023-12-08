use std::io;
use crate::dynamic_table::{OpenTableError, OwnedCol, StorageError};
use thiserror::Error;
use crate::data::{Type, Value};

#[derive(Debug, Error)]
pub enum Error {
    #[error("Illegal auto increment: {reason}")]
    IllegalAutoIncrement {
        reason: String
    },
    #[error("Unexpected value of type found. (expected {expected:?}, received: {actual:?})")]
    TypeError {
        expected: Type,
        actual: Value
    },
    #[error("Illegal definition for column {col:?}: {reason}")]
    IllegalColumnDefinition {
        col: OwnedCol,
        reason: Box<Error>
    },
    #[error("Expected {expected} columns, but found {actual}")]
    BadColumnCount {
        expected: usize,
        actual: usize,
    },
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
}
