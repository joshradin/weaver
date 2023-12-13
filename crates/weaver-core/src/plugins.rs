//! Plugin support

use std::borrow::Cow;
use thiserror::Error;
use crate::db::server::WeaverDb;
use crate::error::Error;

/// All plugins must implement this trait
pub trait Plugin {
    fn name(&self) -> Cow<str>;

    /// Apply the plugin to the weaver db
    fn apply(&self, weaver_db: &mut WeaverDb) -> Result<(), PluginError>;
}

#[derive(Debug, Error)]
pub enum PluginError {
    /// A weaver error
    #[error(transparent)]
    WeaverError(#[from] crate::error::Error)
}