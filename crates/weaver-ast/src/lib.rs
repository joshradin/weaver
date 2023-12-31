use nom::branch::alt;
use nom::bytes::streaming::tag;
use nom::character::streaming::{alpha0, alpha1, alphanumeric1};
use nom::combinator::recognize;
use nom::multi::{fold_many1, many0_count};
use nom::sequence::pair;
use nom::IResult;

pub mod span;
pub mod tokens;

#[derive(Debug)]
pub struct QueryParser;

impl QueryParser {
    pub fn parse<S: AsRef<str>>(query: S) -> Result<S, ()> {
        todo!()
    }
}

#[cfg(test)]
mod tests {}
