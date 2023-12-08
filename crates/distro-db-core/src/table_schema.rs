//! A schema describes a table

use std::borrow::Cow;
use std::collections::HashMap;
use std::ops::{Deref, Index};
use std::rc::Rc;
use serde::{Deserialize, Serialize};

use crate::data::{Row, Type, Value};
use crate::dynamic_table::{Col, DynamicTable, EngineKey, IN_MEMORY_KEY, ROW_ID_COLUMN, Table};
use crate::error::Error;
use crate::key::KeyData;
use crate::rows::KeyIndex;
use crate::tx::{Tx, TX_ID_COLUMN};

/// Table schema
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TableSchema {
    schema: String,
    name: String,
    columns: Vec<ColumnDefinition>,
    sys_columns: Vec<ColumnDefinition>,
    keys: Vec<Key>,
    engine: EngineKey,
}

impl TableSchema {
    pub fn get_key(&self, key_name: &str) -> Result<&Key, Error> {
        self.keys
            .iter()
            .find(|key| key.name() == key_name)
            .ok_or(Error::BadKeyName(key_name.to_string()))
    }
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

    /// Gets publicly defined columns
    pub fn columns(&self) -> &[ColumnDefinition] {
        &self.columns
    }

    /// Gets system defined columns
    pub fn sys_columns(&self) -> &[ColumnDefinition] {
        &self.sys_columns
    }

    /// Gets a mutable reference to system defined columns
    pub fn sys_columns_mut(&mut self) -> &mut [ColumnDefinition] {
        &mut self.sys_columns
    }

    /// Add a system column
    pub fn add_sys_column(&mut self, column_definition: ColumnDefinition) -> Result<(), Error> {
        Ok(self.sys_columns.push(column_definition))
    }

    /// Gets *all* columns, including system columns
    pub fn all_columns(&self) -> Vec<&ColumnDefinition> {
        self.columns
            .iter()
            .chain(self.sys_columns.iter())
            .collect()
    }

    pub fn keys(&self) -> &[Key] {
        &self.keys
    }

    pub fn col_idx(&self, name: &str) -> Option<usize> {
        self.all_columns()
            .iter()
            .enumerate()
            .find(|(idx, col)| col.name == name)
            .map(|(idx, ..)| idx)
    }

    /// Gets the primary key of this table
    pub fn primary_key(&self) -> Result<&Key, Error> {
        self.keys.iter().find(|key| {
            key.primary()
        })
            .or_else(|| {
                self.keys.iter().find(
                    |key| key.unique && key.non_null
                )
            })
            .ok_or(Error::NoPrimaryKey)
    }

    pub fn validate<'a, T: DynamicTable>(&self, mut row: Row<'a>, tx: &Tx, table: &T) -> Result<Row<'a>, Error> {
        println!("validating: {:?}", row);
        if row.len() != self.columns().len() {
            return Err(Error::BadColumnCount {
                expected: self.columns().len(),
                actual: row.len(),
            });
        }

        let mut row = {
            let mut sys_modified_row = Row::new(self.all_columns().len());
            for (idx, val) in row.iter().enumerate() {
                sys_modified_row[idx] = dbg!(val.clone());
            }
            dbg!(sys_modified_row)
        };

        row.iter_mut()
            .zip(self.all_columns())
            .map(|(val, col)| {
                match col.name() {
                    name if name == TX_ID_COLUMN => {
                        *val.to_mut() = Value::Integer(tx.id().into());
                    }
                    name if name == ROW_ID_COLUMN => {
                        *val.to_mut() = Value::Integer(table.next_row_id());
                    }
                    _ => {}
                }


                if &**val == &Value::Null && col.default_value.is_some() {
                    *val.to_mut() = col.default_value.as_ref().cloned().unwrap();
                } else if &**val == &Value::Null && col.auto_increment.is_some() {
                    *val.to_mut() = Value::Integer(table.auto_increment(col.name()));
                }
                col.validate(val)
            })
            .collect::<Result<Vec<_>, _>>()?;

        Ok(row)
    }

    pub fn key_data(&self, row: &Row) -> AllKeyData {
        AllKeyData {
            key_data: self.keys()
                     .iter()
                     .map(|key| {
                         println!("getting columns {:?} from row", key.columns());
                         let cols_idxs = key.columns().iter().flat_map(|col| self.col_idx(col));
                         let row = cols_idxs
                             .inspect(|col| {
                                 println!("getting {}", col);
                             })
                             .map(|col_idx| row[col_idx].clone())
                             .collect::<Row>();
                         if row.len() == 0 {
                             panic!("key should never return in empty row");
                         }
                         (key, KeyData::from(row))
                     })
                     .collect::<HashMap<_, _>>()
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ColumnDefinition {
    name: String,
    data_type: Type,
    non_null: bool,
    default_value: Option<Value>,
    auto_increment: Option<i64>,
}

impl ColumnDefinition {
    pub fn new(
        name: impl AsRef<str>,
        data_type: Type,
        non_null: bool,
        default_value: impl Into<Option<Value>>,
        auto_increment: impl Into<Option<i64>>
    ) -> Result<Self, Error> {
        let name = name.as_ref().to_string();
        (|| -> Result<Self, Error> {
            let auto_increment = auto_increment.into();
            if let Some(ref auto_increment) = auto_increment {
                if data_type != Type::Integer {
                    return Err(Error::IllegalAutoIncrement {
                        reason: "only number types can be auto incremented".to_string(),
                    })
                }
            }

            let default_value = default_value.into();
            if let Some(ref default) = default_value {
                if !data_type.validate(default) {
                    return Err(Error::TypeError {
                        expected: data_type,
                        actual: default.clone(),
                    })
                }
            }

            Ok(Self {
                name: name.clone(),
                data_type,
                non_null,
                default_value,
                auto_increment,
            })
        })()
            .map_err(|e| {
                Error::IllegalColumnDefinition {
                    col: name,
                    reason: Box::new(e),
                }
            })
    }



    /// Gets the name of the column
    pub fn name(&self) -> Col {
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

    pub fn auto_increment(&self) -> Option<i64> {
        self.auto_increment
    }

    /// Validates a value
    pub fn validate(&self, value: &mut Cow<Value>) -> Result<(), Error> {
        if !self.data_type().validate(value) {
            return Err(Error::TypeError {
                expected: self.data_type.clone(),
                actual: (&**value).clone(),
            })
        }


        
        Ok(())
    }

}

#[derive(Debug, Serialize, Deserialize, Clone, Eq, PartialEq, Hash)]
pub struct Key {
    name: String,
    columns: Vec<String>,
    non_null: bool,
    unique: bool,
    is_primary: bool
}

impl Key {
    /// Create a new key definition
    pub fn new(name: impl AsRef<str>, columns: Vec<String>, non_null: bool, unique: bool, is_primary: bool) -> Result<Self, Error> {
        if is_primary && !(unique && non_null) {
            return Err(Error::PrimaryKeyMustBeUniqueAndNonNull)
        }

        Ok(Self {
            name: name.as_ref().to_string(),
            columns,
            non_null,
            unique,
            is_primary,
        })
    }

    /// Create a key index over all elements
    pub fn all(&self) -> KeyIndex {
        KeyIndex::all(&self.name)
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

    /// Implies `unique && non_null`
    pub fn primary(&self) -> bool {
        self.is_primary
    }

    /// Can be used as a primary key (always true when `primary`)
    #[inline]
    pub fn primary_eligible(&self) -> bool {
        self.primary() ||( self.unique() && self.non_null())
    }
}

#[derive(Debug)]
pub struct TableSchemaBuilder {
    schema: String,
    name: String,
    columns: Vec<ColumnDefinition>,
    keys: Vec<Key>,
    engine: Option<EngineKey>,
}

impl TableSchemaBuilder {
    pub fn new(schema: impl AsRef<str>, name: impl AsRef<str>) -> Self {
        Self {
            schema: schema.as_ref().to_string(),
            name: name.as_ref().to_string(),
            columns: vec![],
            keys: vec![],
            engine: None,
        }
    }
    pub fn column(
        mut self,
        name: impl AsRef<str>,
        data_type: Type,
        non_null: bool,
        default_value: impl Into<Option<Value>>,
        auto_increment: impl Into<Option<i64>>
    ) -> Result<Self, Error> {
        self.columns.push(ColumnDefinition::new(
            name,
            data_type,
            non_null,
            default_value,
            auto_increment
        )?);
        Ok(self)
    }

    pub fn build(self) -> Result<TableSchema, Error> {
        let mut columns = self.columns;
        let mut sys_columns = vec![];
        let mut keys = self.keys;

        sys_columns.push(ColumnDefinition::new(
            ROW_ID_COLUMN,
            Type::Integer,
            true,
            Value::Integer(0),
            0
        )?);


        if keys
            .iter()
            .find(|key| key.primary_eligible())
            .is_none()
        {
            keys.push(Key::new(
                "PRIMARY",
                vec![ ROW_ID_COLUMN.to_string()],
                true,
                true,
                true
            )?);
        } else if keys.iter().find(|key| key.primary()).is_none() {
            // there may exist some primary key eligible. Find the shortest and first available
            let mut ele_keys = keys.iter_mut().filter(|k| k.primary_eligible()).collect::<Vec<_>>();
            ele_keys.sort_by_key(|l| l.columns.len());

            if let Some(first) = ele_keys.pop() {
                first.is_primary = true;
            } else {
                keys.push(Key::new(
                    "PRIMARY",
                    vec![ ROW_ID_COLUMN.to_string()],
                    true,
                    true,
                    true
                )?);
            }

        }

        Ok(TableSchema {
            schema: self.schema,
            name: self.name,
            columns,
            sys_columns,
            keys,
            engine: self.engine.unwrap_or(EngineKey::new(IN_MEMORY_KEY)),
        }
        )
    }
}

#[derive(Debug)]
pub struct AllKeyData<'a> {
    key_data: HashMap<&'a Key, KeyData>
}

impl<'a> AllKeyData<'a> {
    pub fn primary(&self) -> &KeyData {
        self.key_data.iter()
            .find_map(|(key, data)| {
                if key.primary() {
                    Some(data)
                } else {
                    None
                }
            })
            .expect("primary must always be present")
    }
}

#[derive(Debug)]
pub struct ColumnizedRow<'a> {
    col_to_idx: Rc<HashMap<String, usize>>,
    row: &'a Row<'a>
}

impl<'a> Index<Col<'a>> for ColumnizedRow<'a> {
    type Output = Cow<'a, Value>;

    fn index(&self, index: Col) -> &Self::Output {
        self.get_by_name(index).unwrap()
    }
}

impl<'a> ColumnizedRow<'a> {
    pub fn get_by_name(&self, col: Col) -> Option<&Cow<'a, Value>> {
        self.col_to_idx.get(col)
            .and_then(|&idx| self.row.get(idx))
    }
}

impl<'a> Deref for ColumnizedRow<'a> {
    type Target = Row<'a>;

    fn deref(&self) -> &Self::Target {
        self.row
    }
}

impl<'a> ColumnizedRow<'a> {
    pub fn generator(schema: &'a TableSchema) -> impl Fn(&'a Row) -> ColumnizedRow<'a> {
        let col_to_idx = Rc::new(schema.all_columns()
            .iter()
            .map(|col| {
                (col.name.to_owned(), schema.col_idx(col.name()).unwrap())
            })
            .collect::<HashMap<_, _>>());

        move |row| {
            ColumnizedRow {
                col_to_idx: col_to_idx.clone(),
                row,
            }
        }
    }
}

