use std::borrow::Cow;

use nom::{Compare, CompareResult, InputLength, InputTake, IResult, Needed, Parser};
use nom::branch::alt;
use nom::bytes::streaming::tag;
use nom::character::complete::multispace0;
use nom::character::streaming::{alpha1, alphanumeric1};
use nom::combinator::{eof, map, recognize};
use nom::error::{ErrorKind, ParseError};
use nom::multi::many0_count;
use nom::sequence::{pair, tuple};
use thiserror::Error;

use crate::span::Span;

#[derive(Debug)]
pub struct Token<'a> {
    kind: TokenKind<'a>,
    span: Span,
}

impl<'a> Token<'a> {
    pub fn new(kind: TokenKind<'a>, span: Span) -> Self {
        Self { kind, span }
    }

    pub fn span(&self) -> &Span {
        &self.span
    }

    pub fn kind(&self) -> &TokenKind {
        &self.kind
    }
}

#[derive(Debug, PartialEq)]
pub enum TokenKind<'a> {
    Select,
    From,
    Join,
    As,
    Inner,
    Where,
    And,
    Or,
    Not,
    Comma,
    Dot,
    LParen,
    RParen,
    Op(Op),
    Ident(Cow<'a, str>),
    Eof,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Op {
    Eq,
    Neq,
}

/// Tokenizer
#[derive(Debug)]
pub struct Tokenizer<'a> {
    src: &'a str,
    consumed: usize,
}

impl<'a> Tokenizer<'a> {
    pub fn new(src: &'a str) -> Self {
        Self { src, consumed: 0 }
    }
    pub fn next(&mut self) -> Result<Option<Token<'a>>, TokenError> {
        let (rest, (l, mut token, r)) = token().parse(self.src)
                                               .map_err::<TokenError, _>(|e| {
                                                   use nom::Err;
                                                   match e {
                                                       Err::Incomplete(i) => { todo!()}
                                                       Err::Error(e) => { todo!("error: {:?}", e); }
                                                       Err::Failure(f) => { todo!("failure: {:?}", f) }
                                                   };
                                               })?;
        let len = token.span.1;
        self.consumed += l;
        token.span.offset(self.consumed as isize);
        self.consumed += len + r;
        self.src = rest;
        Ok(Some(token))
    }
}

#[derive(Debug)]
pub struct TokenizerIter<'a> {
    tokenizer: Tokenizer<'a>,
}

impl<'a> Iterator for TokenizerIter<'a> {
    type Item = Token<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        self.tokenizer.next().ok().flatten()
    }
}

impl<'a> IntoIterator for Tokenizer<'a> {
    type Item = Token<'a>;
    type IntoIter = TokenizerIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        TokenizerIter { tokenizer: self }
    }
}

fn token<'a>() -> impl Parser<&'a str, (usize, Token<'a>, usize), nom::error::Error<&'a str>> {
    ignore_whitespace(
        alt((
            keyword,
            op,
            ident,
            map(eof, |_| {
                println!("eof");
                Token::new(TokenKind::Eof, Span::from_len(0))
            })
        ))
    )
}

fn ignore_whitespace<'a, O, E: ParseError<&'a str>, F: Parser<&'a str, O, E>>(parser: F) -> impl Parser<&'a str, (usize, O, usize), E> {
    tuple(
        (
            map(multispace0, str::len),
         parser,
         map(multispace0, str::len)
        )
    )
}

fn ident(input: &str) -> IResult<&str, Token> {
    map(recognize(
        pair(
            alt((alpha1, tag("_"))),
            many0_count(alt((alphanumeric1, tag("_")))),
        )
    ), |r| Token { kind: TokenKind::Ident(Cow::Borrowed(r)), span: Span(0, r.len()) },
    )(input)
}

fn keyword(input: &str) -> IResult<&str, Token> {
    alt((
        map(ignore_case("select"), |s: &str| Token::new(TokenKind::Select, Span::from_len(s.len()))),
        map(ignore_case("from"), |s: &str| Token::new(TokenKind::Select, Span::from_len(s.len()))),
        map(ignore_case("join"), |s: &str| Token::new(TokenKind::Select, Span::from_len(s.len()))),
        map(ignore_case("where"), |s: &str| Token::new(TokenKind::Select, Span::from_len(s.len()))),
        map(ignore_case("as"), |s: &str| Token::new(TokenKind::Select, Span::from_len(s.len()))),
    ))
        .parse(input)
}

fn op(input: &str) -> IResult<&str, Token> {
    alt((
        map(ignore_case(","), |s: &str| Token::new(TokenKind::Comma, Span::from_len(s.len()))),
        map(ignore_case("."), |s: &str| Token::new(TokenKind::Dot, Span::from_len(s.len()))),
        map(ignore_case("="), |s: &str| Token::new(TokenKind::Op(Op::Eq), Span::from_len(s.len()))),
        map(ignore_case("("), |s: &str| Token::new(TokenKind::LParen, Span::from_len(s.len()))),
        map(ignore_case(")"), |s: &str| Token::new(TokenKind::RParen, Span::from_len(s.len()))),
    ))
        .parse(input)
}

fn ignore_case<'a, Error: ParseError<&'a str>>(tag: &str) -> impl FnMut(&'a str) -> IResult<&'a str, &'a str, Error> + '_
{
    move |i: &'a str| {
        let tag_len = tag.input_len();
        let t = tag.clone();

        let res: IResult<_, _, Error> = match (i.to_lowercase().as_str()).compare(t.to_lowercase().as_str()) {
            CompareResult::Ok => Ok(i.take_split(tag_len)),
            CompareResult::Incomplete => Err(nom::Err::Incomplete(Needed::new(tag_len - i.input_len()))),
            CompareResult::Error => {
                let e: ErrorKind = ErrorKind::Tag;
                Err(nom::Err::Error(Error::from_error_kind(i, e)))
            }
        };
        res
    }
}

#[derive(Debug, Error)]
pub enum TokenError {
    #[error("unexpected EOF")]
    UnexpectedEof
}

#[cfg(test)]
mod test {
    use crate::tokens::{Tokenizer, TokenKind};

    #[test]
    fn tokenize() {
        let query =
            r#"
SELECT user, password, grants FROM users
 JOIN grants on grants.user_id = user.id
        "#;
        let mut tokenizer = Tokenizer::new(query);
        let token = tokenizer.next().expect("should have next token").unwrap();
        assert_eq!(token.kind, TokenKind::Select);
        let token = tokenizer.next().expect("should have next token").unwrap();
        assert!(matches!(token.kind, TokenKind::Ident(_)));

        let mut i = 0;
        for token in tokenizer {
            assert!(token.span().0 >= i);
            assert!(token.span().1 >= token.span().0);
            println!("{} => tok kind: {:?}", token.span().slice(query).unwrap(), token);
            if let TokenKind::Ident(ident) = &token.kind {
                assert_eq!(&query[token.span().to_range()], ident);
            }

            i = token.span().1;
        }
    }
}