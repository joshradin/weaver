//! Table in a file

use std::collections::HashMap;
use std::fs::File;
use std::path::{Path, PathBuf};

use tracing::debug;

use crate::data::row::Row;
use crate::db::core::WeaverDbCore;
use crate::dynamic_table::{Col, DynamicTable, HasSchema, Table};
use crate::dynamic_table_factory::DynamicTableFactory;
use crate::error::WeaverError;
use crate::monitoring::{monitor_fn, Monitor, Monitorable};
use crate::rows::{KeyIndex, Rows};
use crate::storage::devices::mmap_file::MMapFile;
use crate::storage::devices::ram_file::RandomAccessFile;
use crate::storage::devices::StorageDevice;
use crate::storage::paging::caching_pager::LruCachingPager;
use crate::storage::paging::file_pager::FilePager;
use crate::storage::paging::virtual_pager::{VirtualPager, VirtualPagerTable};
use crate::storage::tables::table_schema::TableSchema;
use crate::storage::tables::unbuffered_table::UnbufferedTable;
use crate::storage::{Pager, StorageDeviceDelegate};
use crate::tx::Tx;

pub const B_PLUS_TREE_FILE_KEY: &'static str = "weaveBPTF";

/// A table stored in a [FilePager]
#[derive(Debug)]
pub struct BptfTable {
    main_table: UnbufferedTable<LruCachingPager<FilePager<StorageDeviceDelegate>>>,
}
impl DynamicTable for BptfTable {
    fn auto_increment(&self, col: Col) -> i64 {
        self.main_table.auto_increment(col)
    }

    fn next_row_id(&self) -> i64 {
        self.main_table.next_row_id()
    }

    fn insert(&self, tx: &Tx, row: Row) -> Result<(), WeaverError> {
        self.main_table.insert(tx, row)
    }

    fn read<'tx, 'table: 'tx>(
        &'table self,
        tx: &'tx Tx,
        key: &KeyIndex,
    ) -> Result<Box<dyn Rows<'tx> + 'tx + Send>, WeaverError> {
        self.main_table.read(tx, key)
    }

    fn size_estimate(&self, key_index: &KeyIndex) -> Result<u64, WeaverError> {
        self.main_table.size_estimate(key_index)
    }

    fn update(&self, tx: &Tx, row: Row) -> Result<(), WeaverError> {
        self.main_table.update(tx, row)
    }

    fn delete(&self, tx: &Tx, key: &KeyIndex) -> Result<Box<dyn Rows>, WeaverError> {
        self.main_table.delete(tx, key)
    }
}

impl HasSchema for BptfTable {
    fn schema(&self) -> &TableSchema {
        self.main_table.schema()
    }
}

impl Monitorable for BptfTable {
    fn monitor(&self) -> Box<dyn Monitor> {
        self.main_table.monitor()
    }
}

/// Opens tables at a given base directory
#[derive(Debug)]
pub struct BptfTableFactory {
    base_dir: PathBuf,
}

impl BptfTableFactory {
    /// Creates a factory
    pub fn new<P: AsRef<Path>>(path: P) -> Self {
        Self {
            base_dir: path.as_ref().to_path_buf(),
        }
    }

    fn open(&self, schema: &TableSchema) -> Result<BptfTable, WeaverError> {
        let file_location = self.base_dir.join(schema.schema()).join(schema.name());
        if let Some(parent) = file_location.parent() {
            std::fs::create_dir_all(parent)?;
        }

        debug!("opening Bptf table at {file_location:?} if present...");
        let file = if true {
            MMapFile::with_file(
                File::options()
                    .create(true)
                    .write(true)
                    .read(true)
                    .truncate(false)
                    .open(file_location)?,
            )?
            .into_delegate()
        } else {
            RandomAccessFile::open_or_create(file_location)?.into_delegate()
        };

        let file_pager = FilePager::with_file(file);
        let caching_pager = LruCachingPager::new(file_pager, 512);

        Ok(BptfTable {
            main_table: UnbufferedTable::new(schema.clone(), caching_pager, true)?,
        })
    }
}

impl Monitorable for BptfTableFactory {
    fn monitor(&self) -> Box<dyn Monitor> {
        Box::new(monitor_fn("BptfTableFactory", || {}))
    }
}

impl DynamicTableFactory for BptfTableFactory {
    fn open(&self, schema: &TableSchema, _core: &WeaverDbCore) -> Result<Table, WeaverError> {
        self.open(schema).map(|s| Box::new(s) as Table)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::Bound;

    use tempfile::tempdir;

    use crate::data::row::Row;
    use crate::data::types::Type;
    use crate::data::values::DbVal;
    use crate::dynamic_table::{DynamicTable, EngineKey};
    use crate::dynamic_table_factory::DynamicTableFactory;
    use crate::key::KeyData;
    use crate::monitoring::Monitorable;
    use crate::rows::{KeyIndex, KeyIndexKind, Rows};
    use crate::storage::tables::bpt_file_table::{BptfTableFactory, B_PLUS_TREE_FILE_KEY};
    use crate::storage::tables::table_schema::TableSchema;
    use crate::tx::Tx;

    #[test]
    fn table_factory() {
        let temp_dir = tempdir().expect("could not create temp dir");
        let factory = BptfTableFactory::new(temp_dir.path());
        let schema = TableSchema::builder("test", "test")
            .column("first_name", Type::String(16), true, None, None)
            .unwrap()
            .column("last_name", Type::String(16), true, None, None)
            .unwrap()
            .primary(&["first_name", "last_name"])
            .unwrap()
            .index("last_name_idx", &["last_name"], false)
            .unwrap()
            .engine(EngineKey::new(B_PLUS_TREE_FILE_KEY))
            .build()
            .unwrap();

        let table = factory.open(&schema).expect("could not create table");
        println!("table: {table:#?}");

        let tx = Tx::default();
        table
            .insert(&tx, Row::from(["josh", "radin"]))
            .expect("could not insert");
        table
            .insert(&tx, Row::from(["griffen", "radin"]))
            .expect("could not insert");
        let mut result = table
            .read(
                &tx,
                &KeyIndex::new(
                    "PRIMARY",
                    KeyIndexKind::Range {
                        low: Bound::Included(KeyData::from(["josh", "radin"])),
                        high: Bound::Included(KeyData::from(["josh", "radin"])),
                    },
                    None,
                    None,
                ),
            )
            .expect("could not read");
        let row = result.next().unwrap();
        assert_eq!(&*row[0], &DbVal::from("josh"));
        assert_eq!(&*row[1], &DbVal::from("radin"));
        println!("table: {table:#?}");
    }
    #[test]
    fn test_monitor() {
        let temp_dir = tempdir().expect("could not create temp dir");
        let factory = BptfTableFactory::new(temp_dir.path());
        let schema = TableSchema::builder("test", "test")
            .column("age", Type::Integer, true, None, None)
            .unwrap()
            .primary(&["age"])
            .unwrap()
            .engine(EngineKey::new(B_PLUS_TREE_FILE_KEY))
            .build()
            .unwrap();

        let table = factory.open(&schema).expect("could not create table");
        let mut monitor = table.monitor();
        println!("monitor: {:#?}", monitor.stats());

        let ref tx = Tx::default();
        for i in 0..2000 {
            table
                .insert(tx, Row::from([i as i64]))
                .expect("insert failed");
        }
        println!("table: {table:#?}");
        println!("monitor: {:#?}", monitor.stats());
    }
}
