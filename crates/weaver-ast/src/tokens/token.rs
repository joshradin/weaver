use crate::tokens::parsers;
use nom::{Finish, Parser};
use std::borrow::Cow;
use thiserror::Error;

/// The token kind is the lexical meaning of a [Token], and defines how it may be used
#[derive(Debug, PartialEq, Clone)]
pub enum Token<'a> {
    Select,
    From,
    As,
    Join,
    Left,
    Right,
    Outer,
    Inner,
    Full,
    Cross,
    On,

    Where,
    And,
    Or,


    Not,
    Comma,
    Dot,
    LParen,
    RParen,
    Colon,
    SemiColon,

    Qmark,
    Star,
    Eq,
    Neq,

    Ident(Cow<'a, str>),

    String(Cow<'a, str>),
    Binary(Cow<'a, [u8]>),
    Int(i64),
    Float(f64),
    Boolean(bool),
    Null,

    Eof,
}

/// Tokenizer
#[derive(Debug)]
pub struct Tokenizer<'a> {
    src: &'a str,
    consumed: usize,
}
pub type Spanned<Tok, Loc, Error> = Result<(Loc, Tok, Loc), Error>;

impl<'a> Tokenizer<'a> {
    pub fn new(src: &'a str) -> Self {
        Self { src, consumed: 0 }
    }
    pub fn next(&mut self) -> Spanned<Token<'a>, usize, TokenError> {
        let (rest, (l, token, r)) = parsers::token(self.src, self.consumed).finish().map_err(
            |nom::error::Error { input, code }| nom::error::Error {
                input: input.to_string(),
                code,
            },
        )?;
        let len = r - l;
        self.consumed += l + len;
        self.src = rest;
        Ok((l, token, r))
    }
}

#[derive(Debug)]
pub struct TokenizerIter<'a> {
    tokenizer: Tokenizer<'a>,
    eof_reached: bool,
}

impl<'a> Iterator for TokenizerIter<'a> {
    type Item = Spanned<Token<'a>, usize, TokenError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.eof_reached {
            return None;
        }
        let next = self.tokenizer.next();
        if let Ok((_, Token::Eof, _)) = &next {
            self.eof_reached = true;
            return None
        }
        Some(next)
    }
}

impl<'a> IntoIterator for Tokenizer<'a> {
    type Item = Spanned<Token<'a>, usize, TokenError>;
    type IntoIter = TokenizerIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        TokenizerIter {
            tokenizer: self,
            eof_reached: false,
        }
    }
}

#[derive(Debug, Error)]
pub enum TokenError {
    #[error("unexpected EOF")]
    UnexpectedEof,
    #[error(transparent)]
    NomError(#[from] nom::error::Error<String>),
}

#[cfg(test)]
mod test {
    use nom::branch::alt;
    use nom::combinator::recognize;
    use nom::multi::many0_count;
    use nom::sequence::pair;
    use nom::{Finish, IResult};

    use crate::tokens::token::{Token, Tokenizer};

    #[test]
    fn tokenize_query() {
        let query = r#"
SELECT user, password, grants FROM users
 JOIN grants on grants.user_id = 15 and grants.username = "root";
        "#;
        let mut tokenizer = Tokenizer::new(query);
        let token = tokenizer.next().expect("should have next token");
        assert_eq!(token.1, Token::Select);
        let token = tokenizer.next().expect("should have next token");
        assert!(matches!(token, (_, Token::Ident(_), _)));

        let mut i = 0;
        for token in tokenizer {
            let token = token.expect("token error");
            assert!(token.0 >= i);
            assert!(token.2 >= token.0);
            let spanned = &query[token.0..token.2];
            println!(
                "{} => tok kind: {:?}",
                spanned,
                token
            );
            if let (_, Token::Ident(ident), _) = &token {
                assert_eq!(spanned, ident);
            }

            i = token.2;
        }
    }
}
