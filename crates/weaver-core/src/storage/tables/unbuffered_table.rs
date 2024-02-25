//! An in-memory storage engine

use std::cell::RefCell;
use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, HashMap};
use std::fmt::{Debug, Formatter};
use std::ops::Bound::{Excluded, Unbounded};
use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Instant;

use once_cell::sync::Lazy;
use parking_lot::{Mutex, RwLock};
use tracing::{debug, info, trace};

use crate::data::row::{OwnedRow, Row};
use crate::data::types::Type;
use crate::dynamic_table::{Col, DynamicTable, HasSchema, OwnedCol, Table};
use crate::error::Error;
use crate::key::{KeyData, KeyDataRange};
use crate::monitoring::{monitor_fn, Monitor, MonitorCollector, Monitorable, Stats};
use crate::rows::{KeyIndex, KeyIndexKind, OwnedRows, Rows};
use crate::storage::b_plus_tree::BPlusTree;
use crate::storage::paging::buffered_pager::BufferedPager;
use crate::storage::paging::virtual_pager::{VirtualPager, VirtualPagerTable};
use crate::storage::tables::table_schema::{ColumnDefinition, TableSchema};
use crate::storage::{Pager, VecPager};
use crate::tx::{Tx, TxId, TX_ID_COLUMN};

#[derive(Debug, Clone, Copy, Ord, PartialOrd, Eq, PartialEq, Hash, Default)]
struct RowId(u64);

const MAIN_ROOT: u8 = 0;
const SECONDARY_ROOT: u8 = 1;

/// An in memory table that immediately flushes to storage
pub struct UnbufferedTable<P: Pager + Sync + Send> {
    schema: TableSchema,
    main_buffer: BPlusTree<VirtualPager<u8, BufferedPager<P>>>,
    auto_incremented: HashMap<OwnedCol, OnceLock<AtomicI64>>,
    row_id: AtomicI64,
}

impl<P: Pager + Sync + Send> Debug for UnbufferedTable<P> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UnbufferedTable")
            .field("schema", &self.schema)
            .field(
                "primary_index_nodes",
                &self.main_buffer.nodes().unwrap_or(0),
            )
            .field("auto_incremented", &self.auto_incremented)
            .field("row_id", &self.row_id)
            .finish()
    }
}

impl<P: Pager + Sync + Send> UnbufferedTable<P> {
    /// Creates a new, empty in memory table
    pub fn new(mut schema: TableSchema, paged: P, transactional: bool) -> Result<Self, Error> {
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

        let virtual_pager_table = VirtualPagerTable::<u8, _>::new(BufferedPager::new(paged))?;
        let root = virtual_pager_table.get_or_init(MAIN_ROOT)?;

        let auto_incremented = schema
            .all_columns()
            .iter()
            .filter_map(|f| f.auto_increment().map(|i| (f.name(), i)))
            .map(|(col, i)| (col.to_owned(), OnceLock::new()))
            .collect();
        Ok(Self {
            schema,
            main_buffer: BPlusTree::new(root),
            auto_incremented,
            row_id: Default::default(),
        })
    }

    /// Gets the table schema
    pub fn schema(&self) -> &TableSchema {
        &self.schema
    }

    fn all_rows(&self, tx: &Tx) -> Result<OwnedRows, Error> {
        self.main_buffer
            .all()?
            .into_iter()
            .map(|bytes| self.schema.decode(&bytes))
            .filter(|row| {
                if let Ok(row) = row {
                    let tx_id = self
                        .schema
                        .column_index(TX_ID_COLUMN)
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

impl<P: Pager + Sync + Send> Monitorable for UnbufferedTable<P> {
    fn monitor(&self) -> Box<dyn Monitor> {
        let mut monitor_collector = MonitorCollector::new();
        monitor_collector.push_monitorable(&self.main_buffer);

        Box::new(monitor_fn("UnbufferedTable", move || {
            monitor_collector.all()
        }))
    }
}

impl<P: Pager + Sync + Send> DynamicTable for UnbufferedTable<P>
where
    Error: From<P::Err>,
{
    fn auto_increment(&self, col: Col) -> i64 {
        let lock = self
            .auto_incremented
            .get(col)
            .expect("auto incremented should be initialized");
        lock.get_or_init(|| {
            AtomicI64::new(
                self.schema
                    .get_column(col)
                    .unwrap()
                    .auto_increment()
                    .unwrap_or(0),
            )
        })
        .fetch_add(1, Ordering::SeqCst)
    }

    fn next_row_id(&self) -> i64 {
        self.row_id.fetch_add(1, Ordering::SeqCst)
    }

    fn insert(&self, tx: &Tx, row: Row) -> Result<(), crate::error::Error> {
        let row = self.schema.validate(row, tx, self)?;
        trace!("validated row: {:?}", row);
        let key_data = self.schema.all_key_data(&row);
        let primary = key_data.primary().clone();
        trace!("validated row primary key: {:?}", primary);
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
                KeyIndexKind::Range { low, high } => {
                    let rows = self.main_buffer.range(KeyDataRange(low.clone(), high.clone()))?
                        .into_iter()
                        .map(|bytes| self.schema.decode(&bytes))
                        .filter(|row: &Result<OwnedRow, Error>| {
                            if let Ok(row) = row {
                                println!("row: {row:?}");
                                let tx_id = self
                                    .schema
                                    .column_index(TX_ID_COLUMN)
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
                        .map(|rows| OwnedRows::new(self.schema.clone(), rows))?;
                    Ok(Box::new(rows))
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

impl<P: Pager + Sync + Send> HasSchema for UnbufferedTable<P>
where
    Error: From<P::Err>,
{
    fn schema(&self) -> &TableSchema {
        &self.schema
    }
}
