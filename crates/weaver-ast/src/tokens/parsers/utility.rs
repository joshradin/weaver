use nom::bytes::complete::{take_while, take_while1};
use nom::character::complete::{multispace0, one_of};
use nom::combinator::map;
use nom::error::{ErrorKind, ParseError};
use nom::sequence::tuple;
use nom::{
    Compare, CompareResult, IResult, InputLength, InputTake, InputTakeAtPosition, Needed, Parser,
};

pub fn ignore_whitespace<'a, O, E: ParseError<&'a str>, F: Parser<&'a str, O, E>>(
    parser: F,
) -> impl FnMut(&'a str) -> IResult<&'a str, (usize, O), E> {
    tuple((
        map(multispace0, str::len),
        parser
    ))
}

pub fn ignore_case<'a, Error: ParseError<&'a str>>(
    tag: &str,
) -> impl FnMut(&'a str) -> IResult<&'a str, &'a str, Error> + '_ {
    move |i: &'a str| {
        let tag_len = tag.input_len();
        let t = tag.clone();

        let res: IResult<_, _, Error> =
            match (i.to_lowercase().as_str()).compare(t.to_lowercase().as_str()) {
                CompareResult::Ok => Ok(i.take_split(tag_len)),
                CompareResult::Incomplete => {
                    Err(nom::Err::Incomplete(Needed::new(tag_len - i.input_len())))
                }
                CompareResult::Error => {
                    let e: ErrorKind = ErrorKind::Tag;
                    Err(nom::Err::Error(Error::from_error_kind(i, e)))
                }
            };
        res
    }
}

/// Provides a binary parser
pub fn binary0(input: &str) -> IResult<&str, &str> {
    take_while(|c: char| c == '1' || c == '0')(input)
}

/// Provides a binary parser
pub fn binary1(input: &str) -> IResult<&str, &str> {
    take_while1(|c: char| c == '1' || c == '0')(input)
}
