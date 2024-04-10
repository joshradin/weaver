use std::borrow::Cow;
use std::str::FromStr;

use nom::branch::alt;
use nom::bytes::complete::{tag, take_until};
use nom::character::complete::{alpha1, alphanumeric1, char, digit1, one_of};
use nom::combinator::{all_consuming, consumed, eof, map, map_parser, recognize, rest, value};
use nom::error::{Error, ErrorKind};
use nom::multi::many0_count;
use nom::number::complete::recognize_float;
use nom::sequence::{delimited, pair, preceded, tuple};
use nom::{IResult, Parser};

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
        alt((
            delimited(tag("`"), take_until("`"), tag("`")),
            recognize(pair(
                alt((alpha1, tag("_"))),
                many0_count(alt((alphanumeric1, tag("_")))),
            )),
        )),
        |r| Token::Ident(Cow::Borrowed(r)),
    )(input)
}

fn keyword(input: &str) -> IResult<&str, Token> {
    let (rest, token) = alt((
        alt((
            value(Token::Select, ignore_case("select")),
            value(Token::Explain, ignore_case("explain")),
            value(Token::Create, ignore_case("create")),
            value(Token::Drop, ignore_case("drop")),
            value(Token::Insert, ignore_case("insert")),
            value(Token::Delete, ignore_case("delete")),
            value(Token::Table, ignore_case("table")),
            value(Token::Index, ignore_case("index")),
            value(Token::From, ignore_case("from")),
            value(Token::On, ignore_case("on")),
            value(Token::Join, ignore_case("join")),
            value(Token::Left, ignore_case("left")),
            value(Token::Right, ignore_case("right")),
        )),
        alt((
            value(Token::Load, ignore_case("load")),
            value(Token::Data, ignore_case("data")),
            value(Token::Into, ignore_case("into")),
            value(Token::Fields, ignore_case("fields")),
            value(Token::Group, ignore_case("group")),
            value(Token::Order, ignore_case("order")),
            value(Token::Asc, ignore_case("asc")),
            value(Token::Desc, ignore_case("desc")),
            value(Token::Collate, ignore_case("collate")),
            value(Token::Partition, ignore_case("partition")),
            value(Token::By, ignore_case("by")),
            value(Token::Terminated, ignore_case("terminated")),
        )),
        alt((
            value(Token::Limit, ignore_case("limit")),
            value(Token::Offset, ignore_case("offset")),
            value(Token::MetaKill, ignore_case("kill")),
            value(Token::MetaShow, ignore_case("show")),
        )),
        alt((
            value(Token::Values, ignore_case("values")),
            value(Token::Infile, ignore_case("infile")),
            value(Token::Primary, ignore_case("primary")),
            value(Token::Key, ignore_case("key")),
            value(Token::Unique, ignore_case("unique")),
            value(Token::Foreign, ignore_case("foreign")),
            value(Token::AutoIncrement, ignore_case("auto_increment")),
            value(
                Token::IntType,
                alt((
                    ignore_case("int"),
                    ignore_case("integer"),
                    ignore_case("bigint"),
                )),
            ),
            value(
                Token::FloatType,
                alt((
                    ignore_case("float"),
                    ignore_case("real"),
                    ignore_case("double"),
                )),
            ),
            value(Token::VarCharType, ignore_case("varchar")),
            value(Token::VarBinaryType, ignore_case("varbinary")),
        )),
        alt((
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
        )),
    ))
    .parse(input)?;
    if let Some('a'..='z' | 'A'..='Z' | '0'..='9' | '_') = rest.chars().next() {
        Err(nom::Err::Error(Error::new(input, ErrorKind::AlphaNumeric)))
    } else {
        Ok((rest, token))
    }
}

fn literal<'a>(input: &'a str) -> IResult<&str, Token> {
    let string_parser = |input: &'a str, c: char| {
        let token = map(strings::string_literal(c), |inner: Cow<'a, str>| {
            let _len = inner.len();
            Token::String(inner)
        })(input);
        token
    };
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
    use nom::bytes::complete::{tag, take_until};
    use nom::combinator::recognize;
    use nom::multi::many0_count;
    use nom::sequence::{delimited, pair};
    use nom::{Finish, IResult};

    use crate::lexing::{Token, Tokenizer};

    #[test]
    fn tokenize_eof() {
        let query = "";
        let mut tokenizer = Tokenizer::new(query);
        let (_, token, _) = tokenizer.next_token().expect("should have next token");
        assert_eq!(token, Token::Eof, "should be eof");
    }

    macro_rules! assert_token {
        ($test:literal, $token_kind:path, $expected:expr) => {
            let query: &str = $test;
            let mut tokenizer = Tokenizer::new(query);
            let (_, token, _) = tokenizer.next_token().expect("should have next token");
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

    #[allow(unused)]
    macro_rules! assert_not_token {
        ($test:literal, $token_kind:path, $expected:expr) => {
            let query: &str = $test;
            let mut tokenizer = Tokenizer::new(query);
            let (_, token, _) = tokenizer.next_token().expect("should have next token");
            if let $token_kind(ident) = token {
                assert_ne!(ident, $expected);
            }
        };
    }

    #[test]
    fn tokenize_ident() {
        assert_token!("user", Token::Ident, "user");
        assert_token!("users.name", Token::Ident, "users");
        assert_token!("`users`", Token::Ident, "users");
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

    #[test]
    fn recognize_special_ident() {
        let query = "`user name`";
        let parser =
            |input| -> IResult<_, _> { delimited(tag("`"), take_until("`"), tag("`"))(input) };
        let (rest, parsed) = parser(query).finish().expect("could not parse");
        assert!(rest.is_empty());
        assert_eq!(parsed, "user name");
    }
}
