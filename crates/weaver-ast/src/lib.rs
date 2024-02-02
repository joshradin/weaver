use crate::ast::Query;
use crate::error::QueryParseError;

pub mod span;
pub mod tokens;
pub mod error;
pub mod ast;

#[derive(Debug)]
pub struct QueryParser();

impl QueryParser {

    /// Creates a new query parser
    pub fn new() -> Self {
        Self()
    }

    /// Parse a query
    pub fn parse<S: AsRef<str>>(&mut self, query: S) -> Result<Query, QueryParseError> {
        todo!()
    }
}

#[cfg(test)]
mod tests {}
