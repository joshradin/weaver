//! Plugin support

use crate::db::server::WeaverDb;
use crate::error::WeaverError;
use std::borrow::Cow;
use thiserror::Error;

/// All plugins must implement this trait
pub trait Module {
    fn name(&self) -> Cow<str>;

    /// Apply the module to the weaver db
    fn apply(&self, weaver_db: &mut WeaverDb) -> Result<(), ModuleError>;
}

#[derive(Debug, Error)]
pub enum ModuleError {
    /// A weaver error
    #[error(transparent)]
    WeaverError(#[from] crate::error::WeaverError),
}
