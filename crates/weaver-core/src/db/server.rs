use std::fmt::Debug;
use std::net::ToSocketAddrs;
use crate::cnxn::MessageStream;
use serde::{Deserialize, Serialize};
use crate::plugins::Plugin;
use crate::rows::{OwnedRows, OwnedRowsExt, Rows, RowsExt};

mod init_system_tables;
pub mod processes;
mod weaver_db_server;
pub use weaver_db_server::*;
pub mod socket;
pub mod layers;

