//! The LOAD DATA statement

use std::fmt::{Display, Formatter};
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::ast::{Identifier};

/// The load data statements reads rows from a text file into a table at a high speed
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct LoadData {
    pub infile: PathBuf,
    pub schema: Option<Identifier>,
    pub name: Identifier,
    pub terminated_by: Option<String>,
    pub lines_start: Option<String>,
    pub lines_terminated: Option<String>,
    pub skip: Option<usize>,
    pub columns: Vec<Identifier>
}

impl Display for LoadData {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "LOAD DATA INFILE '{}'", self.infile.to_string_lossy(), )
    }
}