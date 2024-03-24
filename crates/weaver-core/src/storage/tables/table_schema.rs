//! A schema describes a table

use std::borrow::Cow;
use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::io;
use std::io::{Read, Write};
use std::ops::{Deref, Index};
use std::rc::Rc;

use serde::{Deserialize, Serialize};
use tracing::{debug, info, trace, warn};

use weaver_ast::ast::{Identifier, ResolvedColumnRef};
use weaver_ast::ToSql;

use crate::data::row::{OwnedRow, Row};
use crate::data::serde::{deserialize_data_untyped, serialize_data_untyped};
use crate::data::types::Type;
use crate::data::values::DbVal;
use crate::dynamic_table::{Col, DynamicTable, EngineKey, ROW_ID_COLUMN};
use crate::error::WeaverError;
use crate::key::KeyData;
use crate::rows::KeyIndex;
use crate::storage::tables::in_memory_table::IN_MEMORY_KEY;
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
    pub fn empty() -> Self {
        Self {
            schema: "".to_string(),
            name: "".to_string(),
            columns: vec![],
            sys_columns: vec![],
            keys: vec![],
            engine: EngineKey::new(IN_MEMORY_KEY),
        }
    }
    
    pub fn get_key(&self, key_name: &str) -> Result<&Key, WeaverError> {
        self.keys
            .iter()
            .find(|key| key.name() == key_name)
            .ok_or(WeaverError::BadKeyName(key_name.to_string()))
    }
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
    pub fn add_sys_column(
        &mut self,
        column_definition: ColumnDefinition,
    ) -> Result<(), WeaverError> {
        Ok(self.sys_columns.push(column_definition))
    }

    /// Removes a system column by index
    ///
    /// # Error
    /// Returns an error if `index >= sys_columns.len()`
    pub fn remove_sys_column(&mut self, index: usize) -> Result<(), WeaverError> {
        if index >= self.sys_columns.len() {
            return Err(WeaverError::OutOfRange);
        }
        self.sys_columns.remove(index);
        Ok(())
    }

    /// Gets *all* columns, including system columns
    pub fn all_columns(&self) -> Vec<&ColumnDefinition> {
        self.columns.iter().chain(self.sys_columns.iter()).collect()
    }

    /// Gets the keys defined in this schema
    pub fn keys(&self) -> &[Key] {
        &self.keys
    }

    /// Gets all non-primary keys
    pub fn secondary_keys(&self) -> Vec<&Key> {
        if let Ok(primary) = self.primary_key() {
            self.keys.iter().filter(|key| key != &primary).collect()
        } else {
            self.keys.iter().collect()
        }
    }

    /// Gets the index of a column.
    ///
    /// Returns `None` if not present
    pub fn column_index(&self, name: &str) -> Option<usize> {
        self.all_columns().iter().position(|col| col.name == name)
    }

    /// Gets the index of a column by source, if present. If not present, matches by name if table
    /// and schema match up.
    ///
    /// Returns `None` if not present
    pub fn column_index_by_source(&self, source: &ResolvedColumnRef) -> Option<usize> {
        if let Some(ret) = self
            .all_columns()
            .iter()
            .position(|col| col.source_column.as_ref() == Some(source))
        {
            return Some(ret);
        }


        if source.schema() == "<select>" {
            return self.column_index(source.column().as_ref())
        }

        if source.schema().as_ref() != self.schema || source.table().as_ref() != self.name {
            warn!("schema doesn't contain {source}");
            return None;
        }
        trace!("resorting to column index for {source}");
        self.column_index(source.column().as_ref())
    }

    /// Gets the index of a column by source, if present. If not present, matches by name if table
    /// and schema match up.
    ///
    /// Returns `None` if not present
    pub fn column_by_source(&self, source: &ResolvedColumnRef) -> Option<&ColumnDefinition> {
        if let Some(ret) = self
            .all_columns()
            .iter()
            .find(|col| col.source_column.as_ref() == Some(source))
        {
            return Some(ret);
        }

        if source.schema().as_ref() != self.schema || source.table().as_ref() != self.name {
            return None;
        }
        self.get_column(source.column().as_ref())
    }

    /// Gets a column definition by name
    pub fn get_column(&self, name: &str) -> Option<&ColumnDefinition> {
        self.all_columns().into_iter().find(|col| col.name == name)
    }

    /// Checks if this schema contains a column by name
    pub fn contains_column(&self, name: &str) -> bool {
        self.all_columns().into_iter().any(|col| col.name == name)
    }

    /// Gets the primary key of this table
    pub fn primary_key(&self) -> Result<&Key, WeaverError> {
        self.keys
            .iter()
            .find(|key| key.primary())
            .or_else(|| self.keys.iter().find(|key| key.primary_eligible()))
            .ok_or(WeaverError::NoPrimaryKey)
    }

    /// Gets the full index.
    ///
    /// This is equivalent to a full range search over the primary key.
    pub fn full_index(&self) -> Result<KeyIndex, WeaverError> {
        self.primary_key().map(|key| KeyIndex::all(key.name()))
    }

    /// Encodes a row
    pub fn encode(&self, row: &Row) -> Box<[u8]> {
        serialize_data_untyped(row.iter().map(|v| v.as_ref())).into_boxed_slice()
    }

    /// Decodes a row
    pub fn decode(&self, bytes: &[u8]) -> Result<OwnedRow, WeaverError> {
        deserialize_data_untyped(bytes, self.all_columns().iter().map(|col| col.data_type))
            .map(|vals| Row::from(vals).to_owned())
            .map_err(|e| e.into())
    }

    /// Gets only public values from this row
    pub fn public_only<'a>(&self, row: Row<'a>) -> Row<'a> {
        let new_len = self.columns().len();
        row.try_slice(..new_len).unwrap_or_else(|| {
            panic!("row {row:?} does not have expected number of columns {new_len}")
        })
    }

    /// Validates and modifies a row for this schema
    pub fn validate<'a, T: DynamicTable>(
        &self,
        mut row: Row<'a>,
        tx: &Tx,
        table: &T,
    ) -> Result<Row<'a>, WeaverError> {
        trace!("validating: {:?}", row);
        if row.len() != self.columns().len() {
            warn!(
                "row {row:?} does match public columns {:?}",
                self.columns().iter().map(|c| &c.name).collect::<Vec<_>>()
            );
            return Err(WeaverError::BadColumnCount {
                expected: self.columns().len(),
                actual: row.len(),
            });
        }

        let mut row = {
            let mut sys_modified_row = Row::new(self.all_columns().len());
            for (idx, val) in row.iter().enumerate() {
                sys_modified_row[idx] = val.clone();
            }
            sys_modified_row
        };

        row.iter_mut()
            .zip(self.all_columns())
            .map(|(val, col)| {
                match col.name() {
                    name if name == TX_ID_COLUMN => {
                        *val.to_mut() = DbVal::Integer(tx.id().into());
                    }
                    name if name == ROW_ID_COLUMN => {
                        *val.to_mut() = DbVal::Integer(table.next_row_id());
                    }
                    _ => {}
                }

                if &**val == &DbVal::Null && col.default_value.is_some() {
                    *val.to_mut() = col.default_value.as_ref().cloned().unwrap();
                } else if &**val == &DbVal::Null && col.auto_increment.is_some() {
                    *val.to_mut() = DbVal::Integer(table.auto_increment(col.name()));
                }
                col.validate(val)
            })
            .collect::<Result<Vec<_>, _>>()?;

        Ok(row)
    }

    /// Gets all key data for a given row.
    ///
    /// This included primary and secondary keys.
    pub fn all_key_data(&self, row: &Row) -> AllKeyData {
        AllKeyData {
            key_data: self
                .keys()
                .iter()
                .map(|key| (key, self.key_data(key, row)))
                .collect::<HashMap<_, _>>(),
        }
    }

    /// Gets the key data for a given row as defined by a given key
    pub fn key_data(&self, key: &Key, row: &Row) -> KeyData {
        trace!("getting columns {:?} from row", key.columns());
        let cols_idxs = key.columns().iter().flat_map(|col| self.column_index(col));
        let row = cols_idxs
            .inspect(|col| {
                trace!("getting {}", col);
            })
            .map(|col_idx| row[col_idx].clone())
            .collect::<Row>();
        KeyData::from(row)
    }

    /// Join two table schemas, one after eachother
    pub fn join(&self, other: &Self) -> TableSchema {
        let mut ret = TableSchema::builder("<query>", "<join>");
        let left_columns = self.columns();
        let right_columns = other.columns();

        for column in left_columns {
            // always tag source
            let mut column = column.clone();
            column.set_source_column(ResolvedColumnRef::new(
                Identifier::new(&self.schema),
                Identifier::new(&self.name),
                Identifier::new(&column.name),
            ));

            if right_columns.iter().any(|c| c.name() == column.name()) {
                let mut col = column.clone();
                col.name = format!("{}.{}", self.name, col.name);
                ret = ret.column_definition(col);
            } else {
                ret = ret.column_definition(column.clone())
            }
        }

        for column in right_columns {
            // always tag source
            let mut column = column.clone();
            column.set_source_column(ResolvedColumnRef::new(
                Identifier::new(&other.schema),
                Identifier::new(&other.name),
                Identifier::new(&column.name),
            ));
            if left_columns.iter().any(|c| c.name() == column.name()) {
                let mut col = column.clone();
                col.name = format!("{}.{}", other.name, col.name);
                ret = ret.column_definition(col);
            } else {
                ret = ret.column_definition(column.clone())
            }
        }

        ret.build().unwrap()
    }
}

impl ToSql for TableSchema {
    fn write_sql<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        write!(writer, "create table {}.{} (", self.schema, self.name)?;
        let cols_and_constraints = self
            .columns
            .iter()
            .map(|col| col.to_sql())
            .chain(self.keys.iter().map(|key| key.to_sql()))
            .map(|s| format!("  {s}"))
            .collect::<Vec<_>>()
            .join(",\n");
        if !cols_and_constraints.is_empty() {
            write!(writer, "\n{cols_and_constraints}\n)")?
        } else {
            write!(writer, ")")?
        }
        write!(writer, " engine={}", self.engine)?;
        Ok(())
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub struct ColumnDefinition {
    name: String,
    data_type: Type,
    non_null: bool,
    default_value: Option<DbVal>,
    auto_increment: Option<i64>,
    source_column: Option<ResolvedColumnRef>,
}

impl ColumnDefinition {
    pub fn new(
        name: impl AsRef<str>,
        data_type: Type,
        non_null: bool,
        default_value: impl Into<Option<DbVal>>,
        auto_increment: impl Into<Option<i64>>,
    ) -> Result<Self, WeaverError> {
        let name = name.as_ref().to_string();
        (|| -> Result<Self, WeaverError> {
            let auto_increment = auto_increment.into();
            if let Some(ref auto_increment) = auto_increment {
                if data_type != Type::Integer {
                    return Err(WeaverError::IllegalAutoIncrement {
                        reason: "only number types can be auto incremented".to_string(),
                    });
                }
            }

            let default_value = default_value.into();
            if let Some(ref default) = default_value {
                if auto_increment.is_some() {
                    return Err(WeaverError::IllegalAutoIncrement {
                        reason: "can not specify both auto increment and default value".to_string(),
                    });
                } else if !data_type.validate(default) {
                    return Err(WeaverError::TypeError {
                        expected: data_type,
                        actual: default.clone(),
                    });
                }
            }

            Ok(Self {
                name: name.clone(),
                data_type,
                non_null,
                default_value,
                auto_increment,
                source_column: None,
            })
        })()
        .map_err(|e| WeaverError::IllegalColumnDefinition {
            col: name,
            reason: Box::new(e),
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
    pub fn default_value(&self) -> Option<&DbVal> {
        self.default_value.as_ref()
    }

    pub fn auto_increment(&self) -> Option<i64> {
        self.auto_increment
    }

    /// Validates a value
    pub fn validate(&self, value: &mut Cow<DbVal>) -> Result<(), WeaverError> {
        if !self.data_type().validate(value) {
            return Err(WeaverError::TypeError {
                expected: self.data_type.clone(),
                actual: (&**value).clone(),
            });
        }
        Ok(())
    }

    /// Alters this column to use a different name
    pub fn with_name(mut self, name: impl AsRef<str>) -> Self {
        self.name = name.as_ref().to_string();
        self
    }

    pub(crate) fn source_column(&self) -> Option<&ResolvedColumnRef> {
        self.source_column.as_ref()
    }

    pub(crate) fn set_source_column(&mut self, source: ResolvedColumnRef) {
        self.source_column = Some(source);
    }
}

impl Debug for ColumnDefinition {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "column {}", self.to_sql())
    }
}

impl ToSql for ColumnDefinition {
    fn write_sql<W: Write>(&self, f: &mut W) -> io::Result<()> {
        write!(f, "`{}` {}", self.name, self.data_type)?;
        if self.non_null {
            write!(f, " not null")?;
        }
        match self.auto_increment {
            None => {}
            Some(0) => {
                write!(f, " autoincrement")?;
            }
            Some(i) => {
                write!(f, " autoincrement({i})")?;
            }
        }
        if let Some(default) = self.default_value.as_ref() {
            write!(f, " default {default}")?;
        }
        if let Some(source_column) = self.source_column() {
            write!(f, " comment \"source-column: {source_column}\"")?;
        }
        Ok(())
    }
}

#[derive(Serialize, Deserialize, Clone, Eq, PartialEq, Hash)]
pub struct Key {
    name: String,
    columns: Vec<String>,
    non_null: bool,
    unique: bool,
    is_primary: bool,
}

impl Debug for Key {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "index `{}` ({})", self.name, self.columns.join(", "))?;
        if self.is_primary {
            write!(f, " primary")?;
        } else {
            if self.non_null {
                write!(f, " not null")?;
            }
            if self.unique {
                write!(f, " unique")?;
            }
        }
        Ok(())
    }
}

impl ToSql for Key {
    fn write_sql<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        write!(writer, "{self:?}")
    }
}

impl Key {
    /// Create a new key definition
    pub fn new(
        name: impl AsRef<str>,
        columns: Vec<String>,
        non_null: bool,
        unique: bool,
        is_primary: bool,
    ) -> Result<Self, WeaverError> {
        if is_primary && !(unique && non_null) {
            return Err(WeaverError::PrimaryKeyMustBeUniqueAndNonNull);
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
        self.primary() || (self.unique() && self.non_null())
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

impl TableSchemaBuilder {}

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
        default_value: impl Into<Option<DbVal>>,
        auto_increment: impl Into<Option<i64>>,
    ) -> Result<Self, WeaverError> {
        self.columns.push(ColumnDefinition::new(
            name,
            data_type,
            non_null,
            default_value,
            auto_increment,
        )?);
        Ok(self)
    }

    pub fn column_definition(mut self, column_definition: ColumnDefinition) -> Self {
        self.columns.push(column_definition);
        self
    }

    /// Sets the primary key
    pub fn primary(mut self, cols: &[&str]) -> Result<Self, WeaverError> {
        self.keys.push(Key::new(
            "PRIMARY",
            cols.into_iter().map(ToString::to_string).collect(),
            true,
            true,
            true,
        )?);

        Ok(self)
    }

    /// Sets the primary key
    pub fn index(mut self, name: &str, cols: &[&str], unique: bool) -> Result<Self, WeaverError> {
        let non_null = cols.iter().try_fold(true, |accum, col| {
            if let Some(col) = self.columns.iter().find(|column| &column.name == col) {
                Ok(col.non_null && accum)
            } else {
                Err(WeaverError::ColumnNotFound(col.to_string()))
            }
        })?;

        self.keys.push(Key::new(
            name.to_string(),
            cols.into_iter().map(ToString::to_string).collect(),
            non_null,
            false,
            false,
        )?);

        Ok(self)
    }

    /// Sets the used engine
    pub fn engine(mut self, engine_key: EngineKey) -> Self {
        self.engine = Some(engine_key);
        self
    }

    #[inline]
    pub fn in_memory(mut self) -> Self {
        self.engine(EngineKey::new(IN_MEMORY_KEY))
    }

    pub fn build(self) -> Result<TableSchema, WeaverError> {
        let mut columns = self.columns;
        let mut sys_columns = vec![];
        let mut keys = self.keys;

        sys_columns.push(ColumnDefinition::new(
            ROW_ID_COLUMN,
            Type::Integer,
            true,
            None,
            0,
        )?);

        if keys.iter().find(|key| key.primary_eligible()).is_none() {
            keys.push(Key::new(
                "PRIMARY",
                vec![ROW_ID_COLUMN.to_string()],
                true,
                true,
                true,
            )?);
        } else if keys.iter().find(|key| key.primary()).is_none() {
            // there may exist some primary key eligible. Find the shortest and first available
            let mut ele_keys = keys
                .iter_mut()
                .filter(|k| k.primary_eligible())
                .collect::<Vec<_>>();
            ele_keys.sort_by_key(|l| l.columns.len());

            if let Some(first) = ele_keys.pop() {
                first.is_primary = true;
            } else {
                keys.push(Key::new(
                    "PRIMARY",
                    vec![ROW_ID_COLUMN.to_string()],
                    true,
                    true,
                    true,
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
        })
    }
}

impl From<TableSchema> for TableSchemaBuilder {
    fn from(value: TableSchema) -> Self {
        Self::from(&value)
    }
}

impl From<&TableSchema> for TableSchemaBuilder {
    fn from(value: &TableSchema) -> Self {
        Self {
            schema: value.schema.clone(),
            name: value.name.clone(),
            columns: value.columns.clone(),
            keys: value.keys.clone(),
            engine: Some(value.engine.clone()),
        }
    }
}

#[derive(Debug)]
pub struct AllKeyData<'a> {
    key_data: HashMap<&'a Key, KeyData>,
}

impl<'a> AllKeyData<'a> {
    pub fn primary(&self) -> &KeyData {
        self.key_data
            .iter()
            .find_map(|(key, data)| if key.primary() { Some(data) } else { None })
            .expect("primary must always be present")
    }
}

#[derive(Debug)]
pub struct ColumnizedRow<'a> {
    col_to_idx: Rc<HashMap<String, usize>>,
    row: &'a Row<'a>,
}

impl<'a> Index<Col<'a>> for ColumnizedRow<'a> {
    type Output = Cow<'a, DbVal>;

    fn index(&self, index: Col) -> &Self::Output {
        self.get_by_name(index).unwrap()
    }
}

impl<'a> ColumnizedRow<'a> {
    pub fn get_by_name(&self, col: Col) -> Option<&Cow<'a, DbVal>> {
        self.col_to_idx.get(col).and_then(|&idx| self.row.get(idx))
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
        let col_to_idx = Rc::new(
            schema
                .all_columns()
                .iter()
                .map(|col| {
                    (
                        col.name.to_owned(),
                        schema.column_index(col.name()).unwrap(),
                    )
                })
                .collect::<HashMap<_, _>>(),
        );

        move |row| ColumnizedRow {
            col_to_idx: col_to_idx.clone(),
            row,
        }
    }
}
