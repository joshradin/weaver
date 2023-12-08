//! The db is responsible for building tables

use std::collections::HashMap;
use std::sync::Arc;
use std::thread;
use std::thread::JoinHandle;

use crate::db::start_db::start_db;
use crate::error::Error;
use crossbeam::channel::{unbounded, Receiver, RecvError, SendError, Sender};
use parking_lot::RwLock;
use thiserror::Error;

use crate::dynamic_table::{
    storage_engine_factory, EngineKey, StorageEngineFactory, StorageError, Table, IN_MEMORY_KEY,
};
use crate::in_memory_table::InMemory;
use crate::table_schema::TableSchema;

mod start_db;
mod start_server;

/// A db
pub struct DistroDb {
    engines: HashMap<EngineKey, Box<dyn StorageEngineFactory>>,
    open_tables: RwLock<HashMap<(String, String), Arc<Table>>>,
}

impl DistroDb {
    pub fn new() -> Result<Self, Error> {
        let engines = EngineKey::all()
            .filter_map(|key| match key.as_ref() {
                IN_MEMORY_KEY => Some((
                    key,
                    storage_engine_factory(|schema: &TableSchema| {
                        Ok(Box::new(InMemory::new(schema.clone())))
                    }),
                )),
                _ => None,
            })
            .collect::<HashMap<_, _>>();

        let mut shard = Self {
            engines,
            open_tables: Default::default(),
        };
        start_db(&mut shard)?;
        Ok(shard)
    }

    pub fn open_table(&self, schema: &TableSchema) -> Result<(), Error> {
        let mut open_tables = self.open_tables.write();
        let engine = self
            .engines
            .get(schema.engine())
            .ok_or_else(|| Error::CreateTableError)?;
        let table = engine.open(schema)?;

        open_tables.insert(
            (schema.schema().to_string(), schema.name().to_string()),
            Arc::new(table),
        );

        Ok(())
    }

    /// Gets a table, if preset. The table is responsible for handling shared-access
    pub fn get_table(&self, schema: &str, name: &str) -> Option<Arc<Table>> {
        self.open_tables
            .read()
            .get(&(schema.to_string(), name.to_string()))
            .cloned()
    }
}

pub struct DistroDbServer {
    lock: Arc<RwLock<DistroDb>>,
}

impl DistroDbServer {
    pub fn new(shard: DistroDb) -> Result<Self, Error> {
        let daemon = DistroDbServer {
            lock: Arc::new(RwLock::new(shard)),
        };
        Ok(daemon)
    }

    /// Creates a connection
    pub fn connect(&self) -> DbSocket {
        let (req_send, req_recv) = unbounded::<DbReq>();
        let (resp_send, resp_recv) = unbounded::<DbResp>();
        let shard = self.lock.clone();

        let handle = thread::spawn(move || {
            let shard = shard;

            loop {
                /// Get a request
                let req = match req_recv.recv() {
                    Ok(req) => req,
                    Err(_) => {
                        break;
                    }
                };

                let resp = match req {
                    DbReq::Full(full) => {
                        let mut lock = shard.write();
                        (full)(&mut *lock)
                    }
                    DbReq::Ping => DbResp::Pong,
                };

                match resp_send.send(resp) {
                    Ok(_) => {}
                    Err(_) => {
                        break;
                    }
                }
            }
        });
        DbSocket {
            sender: req_send,
            receiver: resp_recv,
            handle,
        }
    }

    /// Creates a connection
    pub fn connect_system(&self) -> SystemDbSocket {
        let (req_send, req_recv) = unbounded::<DbReq>();
        let shard = self.lock.clone();

        let handle = thread::spawn(move || {
            let shard = shard;

            loop {
                /// Get a request
                let req = match req_recv.recv() {
                    Ok(req) => req,
                    Err(_) => {
                        break;
                    }
                };

                match req {
                    DbReq::Full(full) => {
                        let mut lock = shard.write();
                        (full)(&mut *lock);
                    }
                    DbReq::Ping => {}
                };
            }
        });
        SystemDbSocket {
            sender: req_send,
            handle: Arc::new(handle),
        }
    }
}

pub enum DbReq {
    Full(Box<dyn FnOnce(&mut DistroDb) -> DbResp + Send>),
    Ping,
}

impl DbReq {
    /// Gets full access of db
    pub fn full<F: FnOnce(&mut DistroDb) -> DbResp + Send + 'static>(func: F) -> Self {
        Self::Full(Box::new(func))
    }
}

#[derive(Debug)]
pub enum DbResp {
    Pong,
    Ok,
}

#[derive(Debug)]
pub struct DbSocket {
    sender: Sender<DbReq>,
    receiver: Receiver<DbResp>,
    handle: JoinHandle<()>,
}

impl DbSocket {
    /// Join the socket
    pub fn join(self) {
        let _ = self.handle.join();
    }
}

impl DbSocket {
    /// Communicate with the db
    pub fn send(&self, req: DbReq) -> Result<DbResp, ShardSocketError> {
        self.sender.send(req)?;
        Ok(self.receiver.recv()?)
    }
}

#[derive(Debug, Clone)]
pub struct SystemDbSocket {
    sender: Sender<DbReq>,
    handle: Arc<JoinHandle<()>>,
}

#[derive(Debug, Error)]
pub enum ShardSocketError {
    #[error(transparent)]
    SendError(#[from] SendError<DbReq>),
    #[error(transparent)]
    RecvError(#[from] RecvError),
}
