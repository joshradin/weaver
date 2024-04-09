use std::fmt::{Display, Formatter};

use serde::{Deserialize, Serialize};

use crate::ast::{Expr, FromClause, ResultColumn};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Select {
    pub columns: Vec<ResultColumn>,
    pub from: Option<FromClause>,
    pub condition: Option<Expr>,
    pub group_by: Option<Vec<Expr>>,
    pub order_by: Option<Vec<OrderBy>>,
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
        if let Some(group_by) = &self.group_by {
            write!(
                f,
                " group by {}",
                group_by
                    .iter()
                    .map(|s| s.to_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            )?;
        }
        if let Some(order_by) = &self.order_by {
            write!(
                f,
                " order by {}",
                order_by
                    .iter()
                    .map(|i| i.to_string())
                    .collect::<Vec<_>>()
                    .join(",")
            )?;
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderBy(pub Expr, pub Option<OrderDirection>);

impl Display for OrderBy {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} {}", self.0, self.1.unwrap_or_default())
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Default, Serialize, Deserialize)]
pub enum OrderDirection {
    #[default]
    Asc,
    Desc,
}

impl Display for OrderDirection {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            OrderDirection::Asc => {
                write!(f, "asc")
            }
            OrderDirection::Desc => {
                write!(f, "desc")
            }
        }
    }
}
