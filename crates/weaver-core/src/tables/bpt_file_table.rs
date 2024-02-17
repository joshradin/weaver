//! Table in a file

use std::path::{Path, PathBuf};

use tracing::debug;

use crate::data::row::Row;
use crate::db::core::WeaverDbCore;
use crate::dynamic_table::{Col, DynamicTable, HasSchema, StorageEngineFactory, Table};
use crate::error::Error;
use crate::rows::{KeyIndex, Rows};
use crate::storage::file_pager::FilePager;
use crate::storage::Pager;
use crate::storage::ram_file::RandomAccessFile;
use crate::storage::virtual_pager::{VirtualPager, VirtualPagerTable};
use crate::tables::table_schema::TableSchema;
use crate::tables::unbuffered_table::UnbufferedTable;
use crate::tx::Tx;

pub const B_PLUS_TREE_FILE_KEY: &'static str = "weaveBPTF";

/// A table stored in a [FilePager]
#[derive(Debug)]
pub struct BptfTable {
    main_table: UnbufferedTable<VirtualPager<u8, FilePager>>,
}

impl BptfTable {
    /// Creates a table ifile
    pub fn create<P: AsRef<Path>>(path: P, schema: TableSchema) -> Result<Self, Error> {
        todo!()
    }

    pub fn open<P: AsRef<Path>>(path: P, schema: TableSchema) -> Result<Self, Error> {
        todo!()
    }
}

impl DynamicTable for BptfTable {
    fn auto_increment(&self, col: Col) -> i64 {
        self.main_table.auto_increment(col)
    }

    fn next_row_id(&self) -> i64 {
        self.main_table.next_row_id()
    }

    fn insert(&self, tx: &Tx, row: Row) -> Result<(), Error> {
        self.main_table.insert(tx, row)
    }

    fn read<'tx, 'table: 'tx>(
        &'table self,
        tx: &'tx Tx,
        key: &KeyIndex,
    ) -> Result<Box<dyn Rows<'tx> + 'tx + Send>, Error> {
        self.main_table.read(tx, key)
    }

    fn update(&self, tx: &Tx, row: Row) -> Result<(), Error> {
        self.main_table.update(tx, row)
    }

    fn delete(&self, tx: &Tx, key: &KeyIndex) -> Result<Box<dyn Rows>, Error> {
        self.main_table.delete(tx, key)
    }
}

impl HasSchema for BptfTable {
    fn schema(&self) -> &TableSchema {
        self.main_table.schema()
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

    fn open(&self, schema: &TableSchema) -> Result<BptfTable, Error> {
        let file_location = self.base_dir.join(schema.schema()).join(schema.name());
        if let Some(parent) = file_location.parent() {
            std::fs::create_dir_all(parent)?;
        }

        debug!("opening Bptf table at {file_location:?} if present...");
        let file_pager = FilePager::open(file_location)?;
        let virtual_page_table = VirtualPagerTable::<u8, _>::new(file_pager)?;
        let main_pager = virtual_page_table.get_or_init(MAIN_ROOT)?;

        Ok(BptfTable {
            main_table: UnbufferedTable::new(schema.clone(), main_pager, true)?,
        })
    }
}

const MAIN_ROOT: u8 = 0;

impl StorageEngineFactory for BptfTableFactory {
    fn open(&self, schema: &TableSchema, _core: &WeaverDbCore) -> Result<Table, Error> {
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
    use crate::dynamic_table::{DynamicTable, EngineKey, StorageEngineFactory};
    use crate::key::KeyData;
    use crate::rows::{KeyIndex, KeyIndexKind, Rows};
    use crate::tables::bpt_file_table::{B_PLUS_TREE_FILE_KEY, BptfTableFactory};
    use crate::tables::table_schema::TableSchema;
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
            .engine(EngineKey::new(B_PLUS_TREE_FILE_KEY))
            .build()
            .unwrap();

        let table = factory.open(
            &schema
        ).expect("could not create table");

        let tx = Tx::default();
        table.insert(&tx, Row::from(["josh", "radin"])).expect("could not insert");
        table.insert(&tx, Row::from(["griffen", "radin"])).expect("could not insert");
        let mut result = table.read(&tx, &KeyIndex::new("PRIMARY", KeyIndexKind::Range {
            low: Bound::Included(KeyData::from(["josh", "radin"])),
            high: Bound::Included(KeyData::from(["josh", "radin"])),
        }, None, None)).expect("could not read");
        let row = result.next().unwrap();
        assert_eq!(&*row[0], &DbVal::from("josh"));
        assert_eq!(&*row[1], &DbVal::from("radin"));
        println!("table: {table:#?}");
    }
}
