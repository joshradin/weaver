//! Table in a file

use crate::data::row::Row;
use crate::db::core::WeaverDbCore;
use crate::dynamic_table::{Col, DynamicTable, StorageEngineFactory, Table};
use crate::error::Error;
use crate::rows::{KeyIndex, Rows};
use crate::storage::ram_file::PagedFile;
use crate::tables::table_schema::TableSchema;
use crate::tables::unbuffered_table::UnbufferedTable;
use crate::tx::Tx;
use std::path::{Path, PathBuf};

pub const B_PLUS_TREE_FILE_KEY: &'static str = "weaveBPTF";

/// A table stored in a [PagedFile]
#[derive(Debug)]
pub struct BptfTable(UnbufferedTable<PagedFile>);

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
    fn schema(&self) -> &TableSchema {
        self.0.schema()
    }

    fn auto_increment(&self, col: Col) -> i64 {
        self.0.auto_increment(col)
    }

    fn next_row_id(&self) -> i64 {
        self.0.next_row_id()
    }

    fn insert(&self, tx: &Tx, row: Row) -> Result<(), Error> {
        self.0.insert(tx, row)
    }

    fn read<'tx, 'table: 'tx>(
        &'table self,
        tx: &'tx Tx,
        key: &KeyIndex,
    ) -> Result<Box<dyn Rows<'tx> + 'tx + Send>, Error> {
        self.0.read(tx, key)
    }

    fn update(&self, tx: &Tx, row: Row) -> Result<(), Error> {
        self.0.update(tx, row)
    }

    fn delete(&self, tx: &Tx, key: &KeyIndex) -> Result<Box<dyn Rows>, Error> {
        self.0.delete(tx, key)
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
}

impl StorageEngineFactory for BptfTableFactory {
    fn open(&self, schema: &TableSchema, _core: &WeaverDbCore) -> Result<Table, Error> {
        todo!()
    }
}
