
use crate::lexing::{TokenError};
use thiserror::Error;

/// A parse error
#[derive(Debug, Error)]
pub enum ParseQueryError {
    #[error("Incomplete query. (Expected: {1:?}, Found: {0:?})")]
    Incomplete(Vec<String>, Vec<String>),
    #[error("Unexpected token: {0:?}. (Expected: {1:?}, Consumed: {2:?})")]
    UnexpectedToken(String, Vec<String>, Vec<String>),
    #[error(transparent)]
    TokenError(#[from] TokenError),
}
