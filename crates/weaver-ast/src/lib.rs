use nom::branch::alt;
use nom::bytes::streaming::tag;
use nom::character::streaming::{alpha0, alpha1, alphanumeric1};
use nom::combinator::recognize;
use nom::IResult;
use nom::multi::{fold_many1, many0_count};
use nom::sequence::pair;

pub mod tokens;
pub mod span;

#[derive(Debug)]
pub struct QueryParser;

impl QueryParser {
    pub fn parse<S: AsRef<str>>(query: S) -> Result<S, ()> {
        todo!()
    }
}




#[cfg(test)]
mod tests {


}