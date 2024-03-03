//! Initializes the weaver database after both the core has been bootstrapped.

use crate::db::server::WeaverDb;
use crate::error::WeaverError;

pub mod system;
pub mod weaver;

pub mod engines;

/// Initializes the server
pub fn init(server: &mut WeaverDb) -> Result<(), WeaverError> {
    system::init_system_tables(server)?;
    engines::init_engines(server)?;
    Ok(())
}
