//! Table in a file

use crate::dynamic_table::OwnedCol;
use crate::storage::b_plus_tree::BPlusTree;
use crate::storage::VecPaged;
use crate::tables::table_schema::TableSchema;
use std::collections::HashMap;
use std::sync::atomic::AtomicI64;

#[derive(Debug)]
pub struct TableFile {
    schema: TableSchema,
    main_buffer: BPlusTree<VecPaged>,
    auto_incremented: HashMap<OwnedCol, AtomicI64>,
    row_id: AtomicI64,
}
