use std::borrow::Cow;
use std::str::FromStr;

use nom::branch::alt;
use nom::bytes::complete::tag;
use nom::character::complete::{alpha1, alphanumeric1, char, digit1, one_of};
use nom::combinator::{all_consuming, consumed, eof, map, map_parser, recognize, rest, value};
use nom::error::{Error, ErrorKind, FromExternalError, ParseError};
use nom::multi::{many0_count, separated_list1};
use nom::number::complete::recognize_float;
use nom::sequence::{pair, preceded, tuple};
use nom::{Compare, Finish, IResult, InputLength, InputTake, Parser};

use utility::{ignore_case, ignore_whitespace};

use crate::lexing::parsers::utility::binary1;
use crate::lexing::token::Token;

mod strings;
mod utility;

/// IResult is token +

pub fn token(source: &str, start: usize) -> nom::IResult<&str, (usize, Token, usize)> {
    map(
        ignore_whitespace(consumed(alt((
            // EOF MUST BE FIRST
            map(eof, |_| Token::Eof),
            keyword,
            op,
            ident,
            literal,
        )))),
        |(ws_offset, (parsed, token))| {
            let len = parsed.len();
            (start + ws_offset, token, start + ws_offset + len)
        },
    )(source)
}

fn ident(input: &str) -> IResult<&str, Token> {
    map(
        recognize(pair(
            alt((alpha1, tag("_"))),
            many0_count(alt((alphanumeric1, tag("_")))),
        )),
        |r| Token::Ident(Cow::Borrowed(r)),
    )(input)
}

fn keyword(input: &str) -> IResult<&str, Token> {
    alt((
        value(Token::Select, ignore_case("select")),
        value(Token::From, ignore_case("from")),
        value(Token::On, ignore_case("on")),
        value(Token::Join, ignore_case("join")),
        value(Token::Left, ignore_case("left")),
        value(Token::Right, ignore_case("right")),
        value(Token::Outer, ignore_case("outer")),
        value(Token::Inner, ignore_case("inner")),
        value(Token::Full, ignore_case("full")),
        value(Token::Cross, ignore_case("cross")),
        value(Token::Where, ignore_case("where")),
        value(Token::As, ignore_case("as")),
        value(Token::And, ignore_case("and")),
        value(Token::Or, ignore_case("or")),
        value(Token::Not, ignore_case("not")),
        value(Token::Null, ignore_case("null")),
        value(Token::Is, ignore_case("is")),
    ))
    .parse(input)
}

fn literal<'a>(input: &'a str) -> IResult<&str, Token> {
    let string_parser = |input: &'a str, c: char| {
        let token = map(strings::string_literal(c), |inner: Cow<'a, str>| {
            let len = inner.len();
            Token::String(inner)
        })(input);
        token
    };

    println!("checking literal: {input:?}");

    match input.chars().next().unwrap() {
        '0'..='9' => map_parser(
            alt((
                recognize(tuple((tag("0"), one_of("xX"), alphanumeric1))),
                recognize(tuple((tag("0"), one_of("bB"), binary1))),
                recognize_float,
            )),
            differentiate_number,
        )(input),
        c @ ('\'' | '"') => string_parser(input, c),
        _ => Err(nom::Err::Failure(Error::new(input, ErrorKind::Satisfy))),
    }
}

fn differentiate_number(input: &str) -> IResult<&str, Token> {
    alt((
        map(preceded(tag("0x"), alphanumeric1), |str| {
            let i: i64 = i64::from_str_radix(str, 16).expect("should be infallible");
            Token::Int(i)
        }),
        map(preceded(tag("0b"), binary1), |binary_str| {
            let i: i64 = i64::from_str_radix(binary_str, 2).expect("should be infallible");
            Token::Int(i)
        }),
        map(all_consuming(digit1), |str| {
            let i: i64 = i64::from_str(str).expect("should be infallible");
            Token::Int(i)
        }),
        map(rest, |str| {
            let f: f64 = f64::from_str(str).expect("recognize float should not fail");
            Token::Float(f)
        }),
    ))(input)
}

fn op(input: &str) -> IResult<&str, Token> {
    alt((
        value(Token::Comma, char(',')),
        value(Token::Dot, char('.')),
        value(Token::Star, char('*')),
        value(Token::Plus, char('+')),
        value(Token::Minus, char('-')),
        value(Token::Divide, char('/')),
        value(Token::Eq, char('=')),
        value(Token::Neq, tag("<>")),
        value(Token::Neq, tag("!=")),
        value(Token::Less, char('<')),
        value(Token::LessEq, tag("<=")),
        value(Token::Greater, char('>')),
        value(Token::GreaterEq, tag(">=")),
        value(Token::LParen, char('(')),
        value(Token::RParen, char(')')),
        value(Token::Colon, char(':')),
        value(Token::SemiColon, char(';')),
        value(Token::QMark, char('?')),
    ))
    .parse(input)
}

#[cfg(test)]
mod tests {
    use nom::branch::alt;
    use nom::combinator::recognize;
    use nom::multi::many0_count;
    use nom::sequence::pair;
    use nom::{Finish, IResult};

    use crate::lexing::{Token, Tokenizer};

    #[test]
    fn tokenize_eof() {
        let query = "";
        let mut tokenizer = Tokenizer::new(query);
        let (_, token, _) = tokenizer.next().expect("should have next token");
        assert_eq!(token, Token::Eof, "should be eof");
    }

    macro_rules! assert_token {
        ($test:literal, $token_kind:path, $expected:expr) => {
            let query: &str = $test;
            let mut tokenizer = Tokenizer::new(query);
            let (_, token, _) = tokenizer.next().expect("should have next token");
            assert!(
                matches!(token, $token_kind(_)),
                "Got wrong token type: {:?}",
                token
            );
            if let $token_kind(ident) = token {
                assert_eq!(ident, $expected);
            }
        };
    }

    macro_rules! assert_not_token {
        ($test:literal, $token_kind:path, $expected:expr) => {
            let query: &str = $test;
            let mut tokenizer = Tokenizer::new(query);
            let (_, token, _) = tokenizer.next().expect("should have next token");
            if let $token_kind(ident) = token {
                assert_ne!(ident, $expected);
            }
        };
    }

    #[test]
    fn tokenize_ident() {
        assert_token!("user", Token::Ident, "user");
        assert_token!("users.name", Token::Ident, "users");
    }

    #[test]
    fn tokenize_integer() {
        assert_token!("101", Token::Int, 101);
        assert_token!("0", Token::Int, 0);
        assert_token!("0b101", Token::Int, 5);
        assert_token!("0xfff", Token::Int, 0xfff);
    }

    #[test]
    fn tokenize_float() {
        assert_token!("101.", Token::Float, 101.);
        assert_token!("101.23", Token::Float, 101.23);
        assert_token!("5e10", Token::Float, 5e10);
    }

    #[test]
    fn tokenize_string() {
        assert_token!(r#"  "hello, world"  "#, Token::String, "hello, world");
        assert_token!(r#"  'hello, world'  "#, Token::String, "hello, world");
        assert_token!(
            r#"   "hello, \"world\""   "#,
            Token::String,
            "hello, \"world\""
        );
    }

    #[test]
    fn recognize_ident() {
        let query = "user";
        let parser = |input| -> IResult<_, _> {
            recognize(pair(
                alt((
                    nom::character::complete::alpha1,
                    nom::bytes::complete::tag("_"),
                )),
                many0_count(alt((
                    nom::character::complete::alphanumeric1,
                    nom::bytes::complete::tag("_"),
                ))),
            ))(input)
        };
        let (rest, parsed) = parser(query).finish().expect("could not parse");
        assert!(rest.is_empty());
        assert_eq!(parsed, query);
    }
}
