use std::fmt::{Display, Formatter};

use derive_more::{Display as DisplayCustom, From};
use serde::{Deserialize, Serialize};

use crate::ast::{DataType, Identifier};

#[derive(Debug, Clone, Serialize, Deserialize, From, DisplayCustom)]
pub enum Create {
    Table(CreateTable),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateTable {
    pub schema: Option<Identifier>,
    pub name: Identifier,
    pub create_definitions: Vec<CreateDefinition>,
}

impl Display for CreateTable {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "create {schema}`{name}` (",
            schema = self.schema.as_ref().map(|i| format!("`{}`.", i)).unwrap_or_default(),
            name = self.name
        )?;
        if f.alternate() {
            writeln!(f)?;
            write!(f, "\t")?;
        }
        write!(f, "{}",
            self.create_definitions
                .iter()
                .map(|i| i.to_string())
                .collect::<Vec<_>>()
                .join(if f.alternate() { ",\n\t" } else { ", "} )
        )?;
        if f.alternate() {
            writeln!(f)?;
        }
        write!(f, ")")?;

        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, From, DisplayCustom)]
pub enum CreateDefinition {
    Column(ColumnDefinition),
    Constraint(ConstraintDefinition)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnDefinition {
    pub id: Identifier,
    pub data_type: DataType,
    pub non_null: bool,
    pub auto_increment: bool,
    pub unique: bool,
    pub key: bool,
    pub primary: bool,
}

impl Display for ColumnDefinition {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{id} {data_type}{non_null}{auto_increment}{unique}{primary}{key}",
            id = self.id,
            data_type = self.data_type,
            non_null = if self.non_null { " non null" } else { "" },
            auto_increment = if self.auto_increment {
                " auto_increment"
            } else {
                ""
            },
            unique = if self.unique { " unique" } else { "" },
            primary = if self.primary { " primary" } else { "" },
            key = if self.key || self.primary || self.unique {
                " key"
            } else {
                ""
            }
        )
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConstraintDefinition {
    symbol: Option<Identifier>,

}

impl Display for ConstraintDefinition {
    fn fmt(&self, _f: &mut Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}