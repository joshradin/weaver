use nom::branch::alt;
use nom::character::complete::{char, satisfy};
use nom::character::streaming::multispace1;
use nom::combinator::{map, recognize, value, verify};
use nom::error::{FromExternalError, ParseError};
use nom::sequence::{delimited, preceded};
use nom::IResult;
use std::borrow::Cow;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StringFragment<'a> {
    Literal(&'a str),
    EscapedChar(char),
    EscapedWs,
}

fn parse_escaped_char<'a, E>(
    delim: char,
) -> impl FnMut(&'a str) -> IResult<&'a str, std::primitive::char, E> + Sized
where
    E: ParseError<&'a str> + FromExternalError<&'a str, std::num::ParseIntError>,
{
    preceded(
        char('\\'),
        alt((
            value('\n', char('n')),
            value('\r', char('r')),
            value('\t', char('t')),
            value('\\', char('\\')),
            value(delim, char(delim)),
        )),
    )
}
fn parse_literal<'a, E>(delim: char) -> impl FnMut(&'a str) -> IResult<&'a str, &'a str, E> + Sized
where
    E: ParseError<&'a str> + FromExternalError<&'a str, std::num::ParseIntError>,
{
    verify(
        recognize(satisfy(move |c| !['\\', delim].contains(&c))),
        |s: &str| !s.is_empty(),
    )
}
fn parse_string_fragment<'a, E>(
    delim: char,
) -> impl FnMut(&'a str) -> IResult<&'a str, StringFragment<'a>, E>
where
    E: ParseError<&'a str> + FromExternalError<&'a str, std::num::ParseIntError>,
{
    move |input| {
        alt((
            map(parse_literal(delim), StringFragment::Literal),
            map(parse_escaped_char(delim), StringFragment::EscapedChar),
            value(StringFragment::EscapedWs, preceded(char('\\'), multispace1)),
        ))(input)
    }
}

/// recognizes the contents of a string literal
pub fn string_literal<'a, E>(
    delim: char,
) -> impl FnMut(&'a str) -> IResult<&'a str, Cow<'a, str>, E>
where
    E: ParseError<&'a str> + FromExternalError<&'a str, std::num::ParseIntError>,
{
    move |input| {
        let build_string = nom::multi::fold_many0(
            parse_string_fragment(delim),
            || Cow::from(&input[..0]),
            |mut string, fragment| {
                match fragment {
                    StringFragment::Literal(lit) => match &mut string {
                        Cow::Borrowed(borrowed) => {
                            *borrowed = &input[1..][..(borrowed.len() + lit.len())];
                        }
                        Cow::Owned(owned) => {
                            owned.push_str(lit);
                        }
                    },
                    StringFragment::EscapedChar(c) => {
                        string.to_mut().push(c);
                    }
                    StringFragment::EscapedWs => {}
                }
                string
            },
        );

        delimited(char(delim), build_string, char(delim))(input)
    }
}

#[cfg(test)]
mod tests {
    use crate::tokens::parsers::strings::string_literal;
    use nom::Finish;
    use std::borrow::Cow;

    #[test]
    fn recognize_string() {
        let s = r#""hello, world""#;
        let (rest, parsed) = string_literal::<nom::error::Error<_>>('"')(s)
            .finish()
            .expect("could not parse");
        assert!(rest.is_empty());
        assert!(
            matches!(parsed, Cow::Borrowed(_)),
            "parsed should be borrowed"
        );
        assert_eq!(parsed, "hello, world");
    }
}
