//! An in-memory storage engine

use std::collections::{BTreeMap, HashMap};
use std::collections::btree_map::Entry;
use std::sync::atomic::{AtomicI64, Ordering};

use parking_lot::RwLock;

use crate::data::{OwnedRow, Row};
use crate::dynamic_table::{Col, DynamicTable, OwnedCol};
use crate::error::Error;
use crate::key::KeyData;
use crate::rows::{KeyIndex, KeyIndexKind, Rows};
use crate::table_schema::TableSchema;

#[derive(Debug, Clone, Copy, Ord, PartialOrd, Eq, PartialEq, Hash, Default)]
struct RowId(u64);

/// An in memory table
#[derive(Debug)]
pub struct InMemory {
    schema: TableSchema,
    main_buffer: RwLock<BTreeMap<KeyData, OwnedRow>>,
    auto_incremented: HashMap<OwnedCol, AtomicI64>,
}

impl InMemory {
    pub fn new(schema: TableSchema) -> Self {
        let auto_incremented = schema
            .columns()
            .iter()
            .filter_map(|f| {
                f.auto_increment().map(|i| (f.name(), i))
            })
            .map(|(col, i)| {
                (col.to_owned(), AtomicI64::new(i))
            })
            .collect();
        Self {
            schema,
            main_buffer: RwLock::new(BTreeMap::new()),
            auto_incremented,
        }
    }

    /// Gets the table schema
    pub fn schema(&self) -> &TableSchema {
        &self.schema
    }
}

impl DynamicTable for InMemory {
    fn schema(&self) -> &TableSchema {
        &self.schema
    }

    fn auto_increment(&self, col: Col) -> i64 {
        self.auto_incremented.get(col)
            .expect("auto incremented should be initialized")
            .fetch_add(1, Ordering::SeqCst)
    }


    fn insert(&self, row: Row) -> Result<(), crate::error::Error> {
        let row = self.schema.validate(row, self)?;
        let key_data = self.schema.key_data(&row);
        let primary = key_data.primary().clone();
        match self.main_buffer.write()
                  .entry(primary) {
            Entry::Vacant(v) => {
                v.insert(row.to_owned());
                Ok(())
            }
            Entry::Occupied(_) => {
                todo!()
            }
        }
    }

    fn read(&self, key: &KeyIndex) -> Result<Box<dyn Rows>, Error> {
        let key_def = self.schema.get_key(key.key_name())?;

        if key_def.primary() {
            match key.kind() {
                KeyIndexKind::All => {
                    Ok(self.main_buffer.read().values())
                }
                KeyIndexKind::Range { .. } => {
                    todo!()
                }
                KeyIndexKind::One(id) => {
                    Ok(self.main_buffer.read().get(id))
                }
            }
        } else {
            todo!()
        }
    }

    fn update(&self, row: Row) -> Result<(), crate::error::Error> {
        todo!()
    }

    fn delete(&self, key: &KeyIndex) -> Result<Box<dyn Rows>, Error> {
        todo!()
    }
}

struct AllRows<'a> {

}
