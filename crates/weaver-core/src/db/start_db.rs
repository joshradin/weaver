use crate::db::core::WeaverDbCore;
use crate::error::WeaverError;

/// Starts the database
pub fn start_db(_db: &mut WeaverDbCore) -> Result<(), WeaverError> {
    Ok(())
}
