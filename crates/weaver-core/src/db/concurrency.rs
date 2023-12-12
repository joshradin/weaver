use std::fmt::{Debug, Formatter};
use std::net::{SocketAddr, ToSocketAddrs};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Weak};
use std::thread;
use std::thread::JoinHandle;

use crate::cnxn::cnxn_loop::cnxn_main;
use crate::cnxn::tcp::WeaverTcpListener;
use crate::cnxn::MessageStream;
use crate::db::concurrency::init_system_tables::init_system_tables;
use crate::db::concurrency::processes::{ProcessManager, WeaverProcessInfo};
use crossbeam::channel::{unbounded, Receiver, RecvError, SendError, Sender};
use parking_lot::{Mutex, RwLock};
use serde::{Deserialize, Serialize};
use threadpool_crossbeam_channel::ThreadPool;
use tracing::{info, info_span, warn};

use crate::db::core::WeaverDbCore;
use crate::dynamic_table::{EngineKey, Table, SYSTEM_TABLE_KEY};
use crate::error::Error;
use crate::queries::ast::Query;
use crate::queries::executor::QueryExecutor;
use crate::queries::query_plan::QueryPlan;
use crate::queries::query_plan_factory::QueryPlanFactory;
use crate::rows::{OwnedRows, Rows, RowsExt};
use crate::tables::system_tables::SystemTableFactory;
use crate::tables::TableRef;
use crate::tx::coordinator::TxCoordinator;
use crate::tx::{Tx, TxId};

mod init_system_tables;
pub mod processes;

/// A server that allows for multiple connections.
#[derive(Clone)]
pub struct WeaverDb {
    shared: Arc<WeaverDbShared>,
}

struct WeaverDbShared {
    db: Arc<RwLock<WeaverDbCore>>,
    message_queue: Sender<(DbReq, Sender<DbResp>)>,
    main_handle: Option<JoinHandle<()>>,
    worker_handles: Mutex<Vec<JoinHandle<Result<(), Error>>>>,
    req_worker_pool: ThreadPool,

    /// Should only be bound to TCP once.
    tcp_bound: AtomicBool,
    tcp_local_address: RwLock<Option<SocketAddr>>,

    /// Responsible for managing processes.
    process_manager: RwLock<ProcessManager>,
}

impl WeaverDb {
    pub fn new(workers: usize, shard: WeaverDbCore) -> Result<Self, Error> {
        let inner = Arc::new_cyclic(move |weak| {
            let mut shard = shard;
            shard.tx_coordinator = Some(TxCoordinator::new(WeakWeaverDb(weak.clone()), 0));

            let worker_pool = ThreadPool::new(workers);
            let (sc, rc) = unbounded::<(DbReq, Sender<DbResp>)>();

            let main_handle = {
                let worker_pool = worker_pool.clone();
                let weak_db = weak.clone();
                thread::Builder::new()
                    .name("db-core".to_string())
                    .spawn(move || {
                        let weak_db = weak_db.clone();
                        loop {
                            let Ok((req, response_channel)) = rc.recv() else {
                                info!("Db request channel closed");
                                break;
                            };
                            let weak_db = weak_db.clone();
                            worker_pool.execute(move || {
                                let Some(db) = weak_db.upgrade() else {
                                    warn!("db request processing after db dropped");
                                    return;
                                };
                                let db = WeaverDb { shared: db };

                                let resp = (|| -> Result<DbResp, Error> {
                                    match req {
                                        DbReq::Full(cb) => {
                                            let mut writable = db.shared.db.write();
                                            (cb)(&mut *writable)
                                        }
                                        DbReq::Ping => Ok(DbResp::Pong),
                                        DbReq::TxQuery(tx, ref query) => {
                                            let ref plan = db.to_plan(query)?;
                                            let executor = db.query_executor();
                                            let table = executor.execute(&tx, plan)?;
                                            Ok(DbResp::Rows(tx, table))
                                        }
                                        DbReq::StartTransaction => {
                                            let tx = db.shared.db.read().start_transaction();
                                            Ok(DbResp::Tx(tx))
                                        }
                                        DbReq::Commit(tx) => {
                                            tx.commit();
                                            Ok(DbResp::Ok)
                                        }
                                        DbReq::Rollback(tx) => {
                                            tx.rollback();
                                            Ok(DbResp::Ok)
                                        }
                                    }
                                })();

                                if let Ok(resp) = resp {
                                    let _ = response_channel.send(resp);
                                }
                            })
                        }
                    })
                    .expect("could not start main shard thread")
            };

            let shard = Arc::new(RwLock::new(shard));
            WeaverDbShared {
                db: shard,
                message_queue: sc,
                main_handle: Some(main_handle),
                worker_handles: Mutex::default(),
                req_worker_pool: worker_pool,
                tcp_bound: AtomicBool::new(false),
                tcp_local_address: RwLock::default(),
                process_manager: RwLock::new(ProcessManager::new(WeakWeaverDb(weak.clone()))),
            }
        });

        let mut db = WeaverDb { shared: inner };
        init_system_tables(&mut db)?;
        Ok(db)
    }

    pub fn to_plan(&self, query: &Query) -> Result<QueryPlan, Error> {
        info!("debug query to plan");
        let factory = QueryPlanFactory::new(self.weak());
        let plan = factory.to_plan(query)?;

        Ok(plan)
    }

    /// Bind to a tcp port. Can only be done once
    pub fn bind_tcp<A: ToSocketAddrs>(&mut self, addr: A) -> Result<(), Error> {
        if self
            .shared
            .tcp_bound
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::Relaxed)
            == Ok(false)
        {
            let weak = self.weak();
            let mut listener = WeaverTcpListener::bind(addr, weak)?;
            let _ = self
                .shared
                .tcp_local_address
                .write()
                .insert(listener.local_addr()?);

            let self_clone = self.weak();
            let worker = thread::Builder::new()
                .name("distro-db-tcp".to_string())
                .spawn(move || -> Result<(), Error> {
                    let listener = listener;
                    let distro_db = self_clone;
                    loop {
                        let Ok(mut stream) = listener.accept() else {
                            warn!("Listener closed");
                            break;
                        };

                        let Some(weaver) = distro_db.clone().upgrade() else {
                            warn!("distro db closed");
                            break;
                        };
                        let mut process_manager = weaver.shared.process_manager.write();

                        let _ = process_manager.start(move |child| {
                            let span = info_span!("external-connection", pid = child.pid());
                            let _enter = span.enter();
                            cnxn_main(stream, child)
                        });
                    }
                    info!("Tcp listener shut down");
                    Ok(())
                })?;
            self.shared.worker_handles.lock().push(worker);
            Ok(())
        } else {
            Err(Error::TcpAlreadyBound)
        }
    }

    pub fn query_executor(&self) -> QueryExecutor {
        QueryExecutor::new(Arc::downgrade(&self.shared.db))
    }

    /// Gets the local address of this server, if open on a tcp connection
    pub fn local_addr(&self) -> Option<SocketAddr> {
        self.shared
            .tcp_local_address
            .read()
            .as_ref()
            .map(|s| s.clone())
    }

    /// Creates a connection
    pub fn connect(&self) -> DbSocket {
        let (resp_send, resp_recv) = unbounded::<DbResp>();
        let msg_queue = self.shared.message_queue.clone();

        DbSocket {
            main_queue: msg_queue,
            resp_sender: resp_send,
            receiver: resp_recv,
        }
    }

    /// Creates a weak reference to a distro db server
    pub fn weak(&self) -> WeakWeaverDb {
        WeakWeaverDb(Arc::downgrade(&self.shared))
    }
}

impl Default for WeaverDb {
    fn default() -> Self {
        Self::new(num_cpus::get(), WeaverDbCore::default()).unwrap()
    }
}

impl Drop for WeaverDbShared {
    fn drop(&mut self) {
        info!("Dropping pooled db core");
        info!("Joining request worker pool");
        self.req_worker_pool.join();
    }
}

/// A weak instance of a [`WeaverDb`](WeaverDb).
#[derive(Debug, Clone)]
pub struct WeakWeaverDb(Weak<WeaverDbShared>);

impl WeakWeaverDb {
    pub fn upgrade(&self) -> Option<WeaverDb> {
        self.0.upgrade().map(|db| WeaverDb { shared: db })
    }
}

pub enum DbReq {
    Full(Box<dyn FnOnce(&mut WeaverDbCore) -> Result<DbResp, Error> + Send + Sync>),
    /// Send a query to the request
    TxQuery(Tx, Query),
    Ping,
    StartTransaction,
    Commit(Tx),
    Rollback(Tx),
}

impl Debug for DbReq {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DbReq").finish_non_exhaustive()
    }
}

impl DbReq {
    /// Gets full access of db
    pub fn full<F: FnOnce(&mut WeaverDbCore) -> Result<DbResp, Error> + Send + Sync + 'static>(
        func: F,
    ) -> Self {
        Self::Full(Box::new(func))
    }
}

#[derive(Debug)]
pub enum DbResp {
    Pong,
    Ok,
    Tx(Tx),
    TxTable(Tx, Arc<Table>),
    Rows(Tx, Box<dyn OwnedRows + Send + Sync>),
    Err(String),
}

#[derive(Debug)]
pub struct DbSocket {
    main_queue: Sender<(DbReq, Sender<DbResp>)>,
    resp_sender: Sender<DbResp>,
    receiver: Receiver<DbResp>,
}

impl DbSocket {
    /// Communicate with the db
    pub fn send(&self, req: DbReq) -> Result<DbResp, Error> {
        self.main_queue.send((req, self.resp_sender.clone()))?;
        Ok(self.receiver.recv()?)
    }

    pub fn get_table(&self, (schema, table): &TableRef) -> Result<Arc<Table>, Error> {
        let schema = schema.clone();
        let table = table.clone();
        let tx = Tx::default();
        let DbResp::TxTable(_, table) = self.send(DbReq::full({
            let schema = schema.clone();
            let table = table.clone();
            move |core| {
                let table = core
                    .get_table(&schema, &table)
                    .ok_or(Error::NoTableFound(schema.to_string(), table.to_string()))?;
                Ok(DbResp::TxTable(tx, table))
            }
        }))?
        else {
            return Err(Error::NoTableFound(schema.to_string(), table.to_string()));
        };

        Ok(table)
    }
}

#[derive(Debug, Clone)]
pub struct SystemDbSocket {
    sender: Sender<DbReq>,
    handle: Arc<JoinHandle<()>>,
}

#[derive(Debug, thiserror::Error)]
pub enum ShardSocketError {}
