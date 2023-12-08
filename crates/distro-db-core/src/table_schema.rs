//! A schema describes a table

use std::sync::atomic::AtomicI64;
use crate::storage_engine::{EngineKey, IN_MEMORY_KEY, StorageEngineFactory, Table};
use serde::{Deserialize, Serialize};
use crate::data::{Row, Type, Value};
use crate::error::Error;

/// Table schema
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TableSchema {
    schema: String,
    name: String,
    columns: Vec<ColumnDefinition>,
    keys: Vec<KeyDefinition>,
    engine: EngineKey,
    auto_increment: Option<(String, i64)>
}

impl TableSchema {
    pub fn builder(schema: impl AsRef<str>, name: impl AsRef<str>) -> TableSchemaBuilder {
        TableSchemaBuilder::new(schema, name)
    }
    pub fn schema(&self) -> &str {
        &self.schema
    }
    pub fn name(&self) -> &str {
        &self.name
    }
    pub fn engine(&self) -> &EngineKey {
        &self.engine
    }
    pub fn columns(&self) -> &[ColumnDefinition] {
        &self.columns
    }
    pub fn keys(&self) -> &[KeyDefinition] {
        &self.keys
    }

    pub fn col_idx(&self, name: &str) -> Option<usize> {
        self.columns
            .iter()
            .enumerate()
            .find(|(idx, col)| col.name == name)
            .map(|(idx, ..)| idx)
    }

    pub fn validate<'a>(&self, mut row: Row<'a>, table: &Table) -> Result<Row<'a>, Error> {
        if row.len() != self.columns.len() {
            return Err(todo!())
        }

        row.iter_mut().zip(self.columns.iter())
            .for_each(|(val, col)| {
                if &**val == &Value::Null && col.default_value.is_some() {
                    *val.to_mut() = col.default_value.as_ref().cloned().unwrap();
                }
            });

        if let Some((ref auto_inc, _)) = self.auto_increment {
            let col_idx = self.col_idx(auto_inc).expect("must exist");
            *row[col_idx].to_mut() = Value::Number(table.auto_increment(auto_inc));
        }

        Ok(row)
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ColumnDefinition {
    name: String,
    data_type: Type,
    non_null: bool,
    default_value: Option<Value>
}

impl ColumnDefinition {

    pub fn new(name: impl AsRef<str>, data_type: Type, non_null: bool, default_value: impl Into<Option<Value>>) -> Self {
        Self { name: name.as_ref().to_string(), data_type, non_null, default_value: default_value.into() }
    }


    /// Gets the name of the column
    pub fn name(&self) -> &str {
        &self.name
    }
    pub fn data_type(&self) -> Type {
        self.data_type
    }
    pub fn non_null(&self) -> bool {
        self.non_null
    }
    pub fn default_value(&self) -> Option<&Value> {
        self.default_value.as_ref()
    }

    pub fn validate(&self, value: &Value) -> Result<(), Error> {
        todo!()
    }

}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct KeyDefinition {
    name: String,
    columns: Vec<String>,
    non_null: bool,
    unique: bool
}

impl KeyDefinition {

    /// Create a new key definition
    pub fn new(name: impl AsRef<str>, columns: Vec<String>, non_null: bool, unique: bool) -> Self {
        Self { name: name.as_ref().to_string(), columns, non_null, unique }
    }
    pub fn name(&self) -> &str {
        &self.name
    }
    /// Gets the columns within the key
    pub fn columns(&self) -> &[String] {
        &self.columns
    }
    pub fn non_null(&self) -> bool {
        self.non_null
    }
    pub fn unique(&self) -> bool {
        self.unique
    }

}

#[derive(Debug)]
pub struct TableSchemaBuilder {
    schema: String,
    name: String,
    columns: Vec<ColumnDefinition>,
    keys: Vec<KeyDefinition>,
    engine: Option<EngineKey>,
}

impl TableSchemaBuilder {


    pub fn new(schema: impl AsRef<str>, name: impl AsRef<str>) -> Self {
        Self { schema: schema.as_ref().to_string(), name: name.as_ref().to_string(), columns: vec![], keys: vec![], engine: None }
    }
    pub fn column(mut self, name: impl AsRef<str>, data_type: Type, non_null: bool, default_value: impl Into<Option<Value>>) -> Self {
        self.columns.push(ColumnDefinition::new(name, data_type, non_null, default_value));
        self
    }

    pub fn build(self) -> TableSchema {
        let mut columns = self.columns;
        let mut keys = self.keys;

        if keys.iter().find(|key| {
            key.non_null() && key.unique()
        }).is_none() {
            columns.push(ColumnDefinition::new("@@ROW_ID", Type::Number, true, Value::Number(0)));
            keys.push(KeyDefinition::new("PRIMARY", vec!["PRIMARY".to_string()], true, true));
        }

        TableSchema {
            schema: self.schema,
            name: self.name,
            columns,
            keys,
            engine: self.engine.unwrap_or(EngineKey::new(IN_MEMORY_KEY)),
            auto_increment: None,
        }
    }

}