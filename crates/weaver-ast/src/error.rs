use thiserror::Error;

/// A parse error
#[derive(Debug, Error)]
pub enum QueryParseError {}