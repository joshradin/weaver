use crate::ast::Query;
use crate::error::ParseQueryError;
use crate::parsing::parse_query;
use lexing::Tokenizer;
use crate::lexing::Token;

pub mod ast;
pub mod error;
pub mod lexing;

mod parsing;

#[derive(Debug)]
pub struct QueryParser();

impl QueryParser {
    /// Creates a new query parser
    pub fn new() -> Self {
        Self()
    }

    /// Parse a query
    pub fn parse(&mut self, query: &str) -> Result<Query, ParseQueryError> {
        let tokenizer = Tokenizer::new(query);
        match parse_query(query, tokenizer) {
            Err(ParseQueryError::Incomplete(buffer,expected)) => {
                if expected.contains(&String::from("\";\"")) {
                    let tokenizer = Tokenizer::new(query);
                    parse_query(query, tokenizer.into_iter().chain([Ok((0, Token::SemiColon, 0))]))
                } else {
                    Err(ParseQueryError::Incomplete(expected, buffer))
                }
            }
            other => other
        }
    }
}

#[cfg(test)]
mod tests {
    mod select {
        use crate::QueryParser;

        #[test]
        fn parse_wildcard() {
            static QUERY: &str = "SELECT * FROM weaver.users;";
            let mut query_parser = QueryParser::new();
            let q = query_parser.parse(QUERY).expect("could not parse");
            println!("{}", serde_json::to_string_pretty(&q).unwrap());
        }

        #[test]
        fn parse_expression() {
            static QUERY: &str = "SELECT 2+3*5, 15 as value2, age;";
            let mut query_parser = QueryParser::new();
            let q = query_parser.parse(QUERY).expect("could not parse");
            println!("{}", serde_json::to_string_pretty(&q).unwrap());
        }

        #[test]
        fn parse_where_param() {
            static QUERY: &str = "SELECT * FROM table";
            let mut query_parser = QueryParser::new();
            let q = query_parser.parse(QUERY).expect("could not parse");
            println!("{}", serde_json::to_string_pretty(&q).unwrap());
        }

        #[test]
        fn parse_joined() {
            static QUERY: &str = r"
            SELECT u.*, p.pid, florg.*
            FROM
                weaver.users AS u
            JOIN
                system.processes AS p ON u.id = p.id
            RIGHT JOIN
                system.florg ON system.florg.id = p.id
                ;";
            let mut query_parser = QueryParser::new();
            let q = query_parser.parse(QUERY).expect("could not parse");
            println!("{}", serde_json::to_string_pretty(&q).unwrap());
        }
    }
}
