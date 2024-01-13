//! Core file system handling

use crate::db::core::WeaverDbCore;
use crate::error::Error;
use std::path::Path;

/// Loads a schema into a weaver db
pub fn load_schema<P: AsRef<Path>>(core: &mut WeaverDbCore, path: P) -> Result<(), Error> {
    todo!()
}
