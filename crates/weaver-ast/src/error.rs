use crate::ast::Query;
use crate::tokens::{Token, TokenError};
use thiserror::Error;

/// A parse error
#[derive(Debug, Error)]
pub enum ParseQueryError<'a> {
    #[error("Incomplete query. (Expected: {1:?}, Found: {0:?})")]
    Incomplete(Vec<Token<'a>>, Vec<String>),
    #[error("Unexpected token: {0:?}. (Expected: {1:?}, Consumed: {2:?})")]
    UnexpectedToken(Token<'a>, Vec<String>, Vec<Token<'a>>),
    #[error(transparent)]
    TokenError(#[from] TokenError)
}
