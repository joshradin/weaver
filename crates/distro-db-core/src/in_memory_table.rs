//! An in-memory storage engine

use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap};
use std::collections::btree_map::Entry;
use std::sync::atomic::{AtomicI64, Ordering};

use parking_lot::RwLock;

use crate::data::{OwnedRow, Row, Type};
use crate::dynamic_table::{Col, DynamicTable, OwnedCol};
use crate::error::Error;
use crate::key::KeyData;
use crate::rows::{KeyIndex, KeyIndexKind, Rows};
use crate::table_schema::{ColumnDefinition, ColumnizedRow, TableSchema};
use crate::tx::{Tx, TX_ID_COLUMN, TxId};

#[derive(Debug, Clone, Copy, Ord, PartialOrd, Eq, PartialEq, Hash, Default)]
struct RowId(u64);

/// An in memory table
#[derive(Debug)]
pub struct InMemoryTable {
    schema: TableSchema,
    main_buffer: RwLock<BTreeMap<KeyData, OwnedRow>>,
    auto_incremented: HashMap<OwnedCol, AtomicI64>,
    row_id: AtomicI64,
}

impl InMemoryTable {
    /// Creates a new, empty in memory table
    pub fn new(mut schema: TableSchema) -> Result<Self, Error> {
        schema.add_sys_column(
            ColumnDefinition::new(
                TX_ID_COLUMN,
                Type::Integer,
                true,
                None,
                None,
            )?
        )?;

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
        Ok(Self {
            schema,
            main_buffer: RwLock::new(BTreeMap::new()),
            auto_incremented,
            row_id: Default::default(),
        }
        )
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
        self.auto_incremented.get(col)
            .expect("auto incremented should be initialized")
            .fetch_add(1, Ordering::SeqCst)
    }

    fn next_row_id(&self) -> i64 {
        self.row_id.fetch_add(1, Ordering::SeqCst)
    }

    fn insert(&self, tx: &Tx, row: Row) -> Result<(), crate::error::Error> {
        let row = self.schema.validate(row, tx, self)?;
        println!("validated row: {:?}", row);
        let key_data = self.schema.key_data(&row);
        let primary = key_data.primary().clone();
        println!("validated row primary key: {:?}", primary);
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

    fn read<'tx, 'table: 'tx>(&'table self, tx: &'tx Tx, key: &KeyIndex) -> Result<Box<dyn Rows + 'tx>, Error> {
        let key_def = self.schema.get_key(key.key_name())?;

        if key_def.primary() {
            match key.kind() {
                KeyIndexKind::All => {
                        Ok(Box::new(AllRows {
                        table: self,
                        tx: tx.id(),
                        look_behind: tx.look_behind(),
                        state: RefCell::new(AllRowsState::Start),
                    }
                    ))
                }
                KeyIndexKind::Range { .. } => {
                    todo!()
                }
                KeyIndexKind::One(id) => {
                    Ok(todo!())
                }
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

struct AllRows<'a> {
    table: &'a InMemoryTable,
    tx: TxId,
    look_behind: TxId,
    state: RefCell<AllRowsState>
}
enum AllRowsState {
    Start,
    InProgress { last: KeyData, },
    Finished
}

impl<'a> Rows for AllRows<'a> {
    fn next(&mut self) -> Option<OwnedRow> {
        let mut state = self.state.borrow_mut();
        match &mut *state {
            state @ AllRowsState::Start => {
                let read = self.table.main_buffer.read();
                let (key, row) = read.iter()
                    .filter(|(_, row)| row[self.table.schema.col_idx(TX_ID_COLUMN).unwrap()]
                        .int_value()
                        .map(|i|
                            TxId::from(i).is_visible_within(&self.tx, &self.look_behind)
                        ).unwrap_or(false)
                    )
                    .map(|(k, row)| (k.clone(), row.clone()))
                    .next()?
                    ;
                let emit = Some(row);
                let key = key.clone();
                *state = AllRowsState::InProgress { last: key };
                emit
            }
            AllRowsState::InProgress { .. } => {
                todo!()
            }
            AllRowsState::Finished => {
                None
            }
        }
    }
}
