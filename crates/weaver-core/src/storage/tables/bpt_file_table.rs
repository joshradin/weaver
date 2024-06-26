//! Table in a file

use std::fs::File;
use std::path::{Path, PathBuf};

use cfg_if::cfg_if;
use tracing::{debug, trace};

use crate::data::row::Row;
use crate::db::core::WeaverDbCore;
use crate::dynamic_table::{Col, DynamicTable, HasSchema, Table};
use crate::dynamic_table_factory::DynamicTableFactory;
use crate::error::WeaverError;
use crate::monitoring::{monitor_fn, Monitor, Monitorable};
use crate::rows::{KeyIndex, Rows};
#[cfg(feature = "mmap")]
use crate::storage::devices::mmap_file::MMapFile;
use crate::storage::devices::ram_file::RandomAccessFile;
use crate::storage::devices::StorageDevice;
#[cfg(feature = "weaveBPTF-caching")]
use crate::storage::paging::caching_pager::LruCachingPager;
use crate::storage::paging::file_pager::FilePager;
use crate::storage::tables::table_schema::TableSchema;
use crate::storage::tables::unbuffered_table::UnbufferedTable;
use crate::storage::StorageDeviceDelegate;
use crate::tx::Tx;

pub const B_PLUS_TREE_FILE_KEY: &str = "weaveBPTF";

cfg_if! {
    if #[cfg(feature = "weaveBPTF-caching")] {
        type BptfPager = LruCachingPager<FilePager<StorageDeviceDelegate>>;
    } else {
        type BptfPager = FilePager<StorageDeviceDelegate>;
    }
}

/// A table stored in a [FilePager]
#[derive(Debug)]
pub struct BptfTable {
    main_table: UnbufferedTable<BptfPager>,
}
impl DynamicTable for BptfTable {
    fn auto_increment(&self, col: Col) -> i64 {
        self.main_table.auto_increment(col)
    }

    fn next_row_id(&self) -> i64 {
        self.main_table.next_row_id()
    }

    fn commit(&self, tx: &Tx) {
        self.main_table.commit(tx)
    }

    fn rollback(&self, tx: &Tx) {
        self.main_table.commit(tx)
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

    fn all<'tx, 'table: 'tx>(
        &'table self,
        tx: &'tx Tx,
    ) -> Result<Box<dyn Rows<'tx> + 'tx + Send>, WeaverError> {
        self.main_table.all(tx)
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
        let file: StorageDeviceDelegate;

        cfg_if! {
            if #[cfg(feature="mmap")] {
                trace!("using mmaped file");
                file = MMapFile::with_file(
                        File::options()
                        .create(true)
                        .write(true)
                        .read(true)
                        .truncate(false)
                        .open(file_location)?,
                    )?.into_delegate();
            } else {
                trace!("using random access file");
                file= RandomAccessFile::open_or_create(file_location)?.into_delegate()
            }
        }
        let file_pager = FilePager::with_file(file);
        #[cfg(feature = "weaveBPTF-caching")]
        let table =
            UnbufferedTable::new(schema.clone(), LruCachingPager::new(file_pager, 512), true)?;
        #[cfg(not(feature = "weaveBPTF-caching"))]
        let table = UnbufferedTable::new(schema.clone(), file_pager, true)?;

        Ok(BptfTable { main_table: table })
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
    use itertools::Itertools;
    use std::collections::Bound;
    use test_log::test;

    use crate::common::hex_dump::HexDump;
    use tempfile::tempdir;
    use tracing::info;

    use crate::data::row::Row;
    use crate::data::types::Type;
    use crate::data::values::DbVal;
    use crate::dynamic_table::{DynamicTable, EngineKey};
    use crate::error::WeaverError;
    use crate::key::KeyData;
    use crate::monitoring::Monitorable;
    use crate::rows::{KeyIndex, KeyIndexKind, Rows};
    use crate::storage::devices::ram_file::RandomAccessFile;
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

    #[test]
    fn tables_persist() -> Result<(), WeaverError> {
        let temp_dir = tempdir().expect("could not create temp dir");

        let factory = BptfTableFactory::new(temp_dir.path());
        let schema = TableSchema::builder("test", "test")
            .column("id", Type::Integer, true, None, Some(0))?
            .column("first_name", Type::String(32), true, None, None)?
            .column("middle_initial", Type::String(1), false, None, None)?
            .column("last_name", Type::String(32), true, None, None)?
            .column("age", Type::Integer, true, None, None)?
            .primary(&["id"])?
            .build()?;

        let table = factory.open(&schema)?;
        {
            let ref tx = Tx::default();
            table.insert(
                tx,
                Row::from([
                    DbVal::Null,
                    "josh".into(),
                    "e".into(),
                    "radin".into(),
                    25.into(),
                ]),
            )?;
            table.commit(tx);
        }

        let file = RandomAccessFile::open(temp_dir.path().join("test").join("test"))?;
        let bytes = file.bytes().collect::<Vec<_>>();
        assert!(
            bytes.iter().any(|&b| b != 0),
            "nothing was actually written to the page"
        );
        info!("{:#?}", HexDump::new(&bytes));

        Ok(())
    }
}
