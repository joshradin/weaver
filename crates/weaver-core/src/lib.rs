//! # weaver-core
pub use weaver_ast::ast;
pub mod access_control;
pub mod cancellable_task;
pub mod common;
pub mod conversion;
pub mod data;
pub mod db;
pub mod dynamic_table;
pub mod dynamic_table_factory;
pub mod error;
pub mod key;
pub mod modules;
pub mod monitoring;
pub mod queries;
pub mod rows;
pub mod storage;
pub mod tx;

pub use db::server::cnxn;

/// Sealed trait, allowing for traits that can not be implemented outside of this crate
pub(crate) mod sealed {

    /// Seals a trait, preventing outside structs from implementing
    pub trait Sealed {}
}
