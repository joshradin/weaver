


use nom::{Finish};

pub use token::*;

mod parsers;
mod token;

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
    pub fn next(&mut self) -> Spanned<Token<'a>, usize, TokenError> {
        let (rest, (l, token, r)) = parsers::token(self.src, self.consumed).finish().map_err(
            |nom::error::Error { input, code }| nom::error::Error {
                input: input.to_string(),
                code,
            },
        )?;
        self.consumed = r;
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
            return None;
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
