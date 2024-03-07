use std::fmt::{Display, Formatter};
use serde::{Deserialize, Serialize};
use crate::ast::{Expr, FromClause, ResultColumn};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Select {
    pub columns: Vec<ResultColumn>,
    pub from: Option<FromClause>,
    pub condition: Option<Expr>,
    pub limit: Option<u64>,
    pub offset: Option<u64>,
}

impl Display for Select {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "select {}",
            self.columns
                .iter()
                .map(|s| s.to_string())
                .collect::<Vec<_>>()
                .join(", ")
        )?;
        if let Some(from) = &self.from {
            write!(f, " {from}")?;
        }
        if let Some(condition) = &self.condition {
            write!(f, " {condition}")?;
        }
        if let Some(l) = &self.limit {
            write!(f, " limit {l}")?;
        }
        if let Some(l) = &self.offset {
            write!(f, " offset {l}")?;
        }
        Ok(())
    }
}
