//! An in-memory storage engine

use std::cell::RefCell;
use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, HashMap};
use std::ops::Bound::{Excluded, Unbounded};
use std::sync::atomic::{AtomicI64, Ordering};

use parking_lot::RwLock;
use tracing::{debug, info, trace};

use crate::data::row::{OwnedRow, Row};
use crate::data::types::Type;
use crate::dynamic_table::{Col, DynamicTable, OwnedCol, Table};
use crate::error::Error;
use crate::key::{KeyData, KeyDataRange};
use crate::rows::{KeyIndex, KeyIndexKind, OwnedRows, Rows};
use crate::storage::b_plus_tree::BPlusTree;
use crate::storage::{Paged, VecPaged};
use crate::tables::table_schema::{ColumnDefinition, TableSchema};
use crate::tx::{Tx, TxId, TX_ID_COLUMN};

#[derive(Debug, Clone, Copy, Ord, PartialOrd, Eq, PartialEq, Hash, Default)]
struct RowId(u64);

/// An in memory table
#[derive(Debug)]
pub struct UnbufferedTable<P: Paged + Sync + Send> {
    schema: TableSchema,
    main_buffer: BPlusTree<P>,
    auto_incremented: HashMap<OwnedCol, AtomicI64>,
    row_id: AtomicI64,
}

impl<P: Paged + Sync + Send> UnbufferedTable<P> {
    /// Creates a new, empty in memory table
    pub fn new(mut schema: TableSchema, paged: P, transactional: bool) -> Result<Self, Error>
    where
        Error: From<P::Err>,
    {
        if transactional
            && !schema
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
            main_buffer: BPlusTree::new(paged),
            auto_incremented,
            row_id: Default::default(),
        })
    }

    /// Gets the table schema
    pub fn schema(&self) -> &TableSchema {
        &self.schema
    }
}

impl<P: Paged + Sync + Send> DynamicTable for UnbufferedTable<P>
where
    Error: From<P::Err>,
{
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
        let key_data = self.schema.all_key_data(&row);
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
                KeyIndexKind::All => Ok(Box::new(self.all_rows(tx)?)),
                KeyIndexKind::Range { .. } => {
                    todo!()
                }
                KeyIndexKind::One(id) => Ok(todo!()),
            }
        } else {
            let mut all = self.all_rows(tx)?;
            all.retain(|row| {
                let ref row_key_data = self.schema.key_data(key_def, row);
                match key.kind() {
                    KeyIndexKind::All => true,
                    KeyIndexKind::Range { low, high } => {
                        KeyDataRange(low.clone(), high.clone()).contains(row_key_data)
                    }
                    KeyIndexKind::One(id) => id == row_key_data,
                }
            });

            Ok(Box::new(all))
        }
    }

    fn update(&self, tx: &Tx, row: Row) -> Result<(), crate::error::Error> {
        todo!()
    }

    fn delete(&self, tx: &Tx, key: &KeyIndex) -> Result<Box<dyn Rows>, Error> {
        todo!()
    }
}

impl<P: Paged + Sync + Send> UnbufferedTable<P>
where
    Error: From<P::Err>,
{
    fn all_rows(&self, tx: &Tx) -> Result<OwnedRows, Error> {
        self.main_buffer
            .all()?
            .into_iter()
            .map(|bytes| self.schema.decode(&bytes))
            .filter(|row| {
                if let Ok(row) = row {
                    let tx_id = self
                        .schema
                        .col_idx(TX_ID_COLUMN)
                        .and_then(|tx_col| row.get(tx_col))
                        .and_then(|tx| tx.int_value())
                        .map(|tx| TxId::from(tx));
                    let can_see = tx_id.map(|ref i| tx.can_see(i)).unwrap_or(true);
                    trace!(
                        "checking if row {:?} (tx_id: {tx_id:?}) can be seen by tx {} -> {can_see}",
                        row,
                        tx
                    );
                    can_see
                } else {
                    true
                }
            })
            .collect::<Result<Vec<_>, _>>()
            .map(|rows| OwnedRows::new(self.schema.clone(), rows))
    }
}
