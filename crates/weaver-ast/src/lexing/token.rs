use derive_more::Display;
use std::borrow::Cow;
use std::fmt::Formatter;

use nom::{Finish, Parser};
use thiserror::Error;

/// The token kind is the lexical meaning of a [Token], and defines how it may be used
#[derive(Debug, PartialEq, Clone)]
pub enum Token<'a> {
    Select,
    Explain,
    Create,
    Drop,
    Delete,
    Insert,
    Data,
    Infile,
    Fields,
    Values,
    Table,
    Index,
    AutoIncrement,
    Default,
    Key,
    Primary,
    Unique,
    Foreign,
    Constraint,

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
    Is,

    Where,
    And,
    Or,

    Comma,
    Dot,
    LParen,
    RParen,
    Colon,
    SemiColon,
    QMark,

    Not,
    Star,
    Eq,
    Neq,
    Minus,
    Plus,
    Divide,
    Less,
    Greater,
    LessEq,
    GreaterEq,
    Percent,

    Ident(Cow<'a, str>),

    String(Cow<'a, str>),
    VarCharType,
    Binary(Cow<'a, [u8]>),
    VarBinaryType,
    Int(i64),
    IntType,
    Float(f64),
    FloatType,
    Boolean(bool),
    BooleanType,

    Null,

    Eof,
}

impl Display for Token<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}

pub type Spanned<Tok, Loc, Error> = Result<(Loc, Tok, Loc), Error>;

#[derive(Debug, Error)]
pub enum TokenError {
    #[error("unexpected EOF")]
    UnexpectedEof,
    #[error(transparent)]
    NomError(#[from] nom::error::Error<String>),
}

#[cfg(test)]
mod test {
    use crate::lexing::token::Token;
    use crate::lexing::Tokenizer;

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
            if let (_, Token::Ident(ident), _) = &token {
                assert_eq!(spanned, ident);
            }

            i = token.2;
        }
    }

    #[test]
    fn accurate_spans() {
        let names = ["josh", "chris", "jordan", "jon", "desi", "seth"];
        let joined = names.join("  ,  ");
        let mut tokenizer = Tokenizer::new(&joined);
        let mut name_iter = names.iter();
        loop {
            let (l, name, r) = tokenizer.next().expect("could not get next ident");
            let Token::Ident(ident) = name else {
                panic!("expected ident token")
            };
            let expected_name = name_iter.next().expect("should have next name");
            assert_eq!(&*ident, *expected_name, "value in ident should be same");
            let name_from_span = &joined[l..r];
            assert_eq!(
                name_from_span, *expected_name,
                "name from span should be same"
            );

            match tokenizer.next().expect("tokenization failed") {
                (_, Token::Comma, _) => continue,
                (_, Token::Eof, _) => break,
                (_, other, _) => panic!("unexpected token: {other:?}"),
            }
        }
    }
}
