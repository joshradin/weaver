//! An in-memory storage engine

use std::cell::RefCell;
use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, HashMap};
use std::ops::Bound::{Excluded, Unbounded};
use std::sync::atomic::{AtomicI64, Ordering};

use parking_lot::RwLock;
use tracing::info;

use crate::data::row::{OwnedRow, Row};
use crate::data::types::Type;
use crate::dynamic_table::{Col, DynamicTable, OwnedCol, Table};
use crate::error::Error;
use crate::key::KeyData;
use crate::rows::{DefaultOwnedRows, KeyIndex, KeyIndexKind, Rows};
use crate::storage::b_plus_tree::BPlusTree;
use crate::storage::VecPaged;
use crate::tables::table_schema::{ColumnDefinition, TableSchema};
use crate::tx::{Tx, TxId, TX_ID_COLUMN};

#[derive(Debug, Clone, Copy, Ord, PartialOrd, Eq, PartialEq, Hash, Default)]
struct RowId(u64);

/// An in memory table
#[derive(Debug)]
pub struct InMemoryTable {
    schema: TableSchema,
    main_buffer: BPlusTree<VecPaged>,
    auto_incremented: HashMap<OwnedCol, AtomicI64>,
    row_id: AtomicI64,
}

impl InMemoryTable {
    /// Creates a new, empty in memory table
    pub fn new(mut schema: TableSchema) -> Result<Self, Error> {
        if !schema
            .sys_columns()
            .iter()
            .any(|col| col.name() == TX_ID_COLUMN)
        {
            schema.add_sys_column(ColumnDefinition::new(
                TX_ID_COLUMN,
                Type::Integer,
                true,
                None,
                None,
            )?)?;
        }

        let auto_incremented = schema
            .columns()
            .iter()
            .filter_map(|f| f.auto_increment().map(|i| (f.name(), i)))
            .map(|(col, i)| (col.to_owned(), AtomicI64::new(i)))
            .collect();
        Ok(Self {
            schema,
            main_buffer: BPlusTree::new(VecPaged::default()),
            auto_incremented,
            row_id: Default::default(),
        })
    }

    /// Creates an in-memory table from a set of rows and a given schema

    pub fn from_rows<'t>(schema: TableSchema, mut rows: impl Rows<'t>) -> Result<Self, Error> {
        let mut table = Self::new(schema)?;
        let ref tx = Tx::default();
        while let Some(row) = rows.next() {
            table.insert(tx, row)?;
        }
        Ok(table)
    }

    /// Gets the table schema
    pub fn schema(&self) -> &TableSchema {
        &self.schema
    }
}

impl DynamicTable for InMemoryTable {
    fn schema(&self) -> &TableSchema {
        &self.schema
    }

    fn auto_increment(&self, col: Col) -> i64 {
        self.auto_incremented
            .get(col)
            .expect("auto incremented should be initialized")
            .fetch_add(1, Ordering::SeqCst)
    }

    fn next_row_id(&self) -> i64 {
        self.row_id.fetch_add(1, Ordering::SeqCst)
    }

    fn insert(&self, tx: &Tx, row: Row) -> Result<(), crate::error::Error> {
        let row = self.schema.validate(row, tx, self)?;
        info!("validated row: {:?}", row);
        let key_data = self.schema.key_data(&row);
        let primary = key_data.primary().clone();
        info!("validated row primary key: {:?}", primary);
        self.main_buffer.insert(primary, row.to_owned())?;
        Ok(())
    }

    fn read<'tx, 'table: 'tx>(
        &'table self,
        tx: &'tx Tx,
        key: &KeyIndex,
    ) -> Result<Box<dyn Rows<'tx> + 'tx + Send>, Error> {
        let key_def = self.schema.get_key(key.key_name())?;

        if key_def.primary() {
            match key.kind() {
                KeyIndexKind::All => Ok(Box::new(
                    self.main_buffer.all()?
                        .into_iter()
                        .map(|bytes| {
                            self.schema.decode(&bytes)
                        })
                        .filter(|row| {
                            if let Ok(row) = row {
                                row[self.schema.col_idx(TX_ID_COLUMN).unwrap()]
                                    .int_value()
                                    .map(|i| tx.can_see(&TxId::from(i)))
                                    .unwrap_or(false)
                            } else {
                                true
                            }
                        })
                        .collect::<Result<Vec<_>, _>>()
                        .map(|rows| {
                            DefaultOwnedRows::new(self.schema.clone(), rows)
                        })?
                )),
                KeyIndexKind::Range { .. } => {
                    todo!()
                }
                KeyIndexKind::One(id) => Ok(todo!()),
            }
        } else {
            todo!()
        }
    }

    fn update(&self, tx: &Tx, row: Row) -> Result<(), crate::error::Error> {
        todo!()
    }

    fn delete(&self, tx: &Tx, key: &KeyIndex) -> Result<Box<dyn Rows>, Error> {
        todo!()
    }
}
