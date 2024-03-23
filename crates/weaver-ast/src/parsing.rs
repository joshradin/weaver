//! actual parsing implementation

use crate::ast::{Literal, Query};
use crate::error::ParseQueryError;
use crate::lexing::{Spanned, Token, TokenError, Tokenizer};

use lalrpop_util::{lalrpop_mod, ParseError};
use nom::combinator::all_consuming;

lalrpop_mod!(weaver_query);

#[derive(Debug)]
struct LR1Parser<'a, I: Iterator<Item = Spanned<Token<'a>, usize, TokenError>>> {
    src: &'a str,
    token_stream: I,
}

impl<'a, I: Iterator<Item = Spanned<Token<'a>, usize, TokenError>>> LR1Parser<'a, I> {
    fn new(src: &'a str, stream: I) -> Self {
        Self {
            src,
            token_stream: stream,
        }
    }

    fn parse(mut self) -> Result<Query, ParseQueryError> {
        let mut buffer = vec![];
        let result: Result<Query, lalrpop_util::ParseError<usize, Token<'a>, TokenError>> =
            weaver_query::QueryParser::new().parse(
                self.src,
                (self.token_stream).inspect(|token| {
                    if let Ok((_, token, _)) = token {
                        buffer.push(token.to_string())
                    }
                }),
            );
        result.map_err(|e| match e {
            ParseError::InvalidToken { .. } => {
                todo!()
            }
            ParseError::UnrecognizedEof {
                location: _,
                expected,
            } => ParseQueryError::Incomplete(buffer, expected),
            ParseError::UnrecognizedToken {
                token: (_, token, _),
                expected,
            } => ParseQueryError::UnexpectedToken(token.to_string(), expected, buffer),
            ParseError::ExtraToken { .. } => {
                todo!()
            }
            ParseError::User { error } => error.into(),
        })
    }
}

/// Parse a query from a stream of lexing
///
/// # Return
/// Returns a single, full query AST.
///
/// # Error
/// Returns a [ParseQueryError::Incomplete] if the input is a valid prefix to a query, but not
/// a valid full query.
pub fn parse_query<'a, I: IntoIterator<Item = Spanned<Token<'a>, usize, TokenError>>>(
    src: &'a str,
    tokens: I,
) -> Result<Query, ParseQueryError>
where
    <I as IntoIterator>::IntoIter: 'a,
{
    let mut parser = LR1Parser::new(src, tokens.into_iter());
    let s = parser.parse();
    s
}

pub fn parse_literal(string: &str) -> Result<Literal, ParseQueryError> {
    let tokenizer = Tokenizer::new(string);
    let result = weaver_query::LiteralParser::new().parse(
        string,
        tokenizer.into_iter(),
    );
    result.map_err(|e| match e {
        ParseError::InvalidToken { .. } => {
            todo!()
        }
        ParseError::UnrecognizedEof {
            location: _,
            expected,
        } => ParseQueryError::Incomplete(vec![], expected),
        ParseError::UnrecognizedToken {
            token: (_, token, _),
            expected,
        } => ParseQueryError::UnexpectedToken(token.to_string(), expected, vec![]),
        ParseError::ExtraToken { .. } => {
            todo!()
        }
        ParseError::User { error } => error.into(),
    })
}
