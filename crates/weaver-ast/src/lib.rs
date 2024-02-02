use crate::error::QueryParseError;

pub mod span;
pub mod tokens;
pub mod error;

#[derive(Debug)]
pub struct QueryParser;

impl QueryParser {
    pub fn parse<S: AsRef<str>>(query: S) -> Result<Query, QueryParseError> {
        todo!()
    }
}

#[cfg(test)]
mod tests {}
