use thiserror::Error;
use crate::storage_engine::OpenTableError;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Error creating table")]
    CreateTableError,
    #[error(transparent)]
    OpenTableError(#[from] OpenTableError),
}