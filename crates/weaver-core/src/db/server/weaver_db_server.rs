use super::layers::packets::IntoDbResponse;
use crate::cnxn::cnxn_loop::cnxn_main;
use crate::cnxn::tcp::WeaverTcpListener;
use crate::cnxn::{Message, MessageStream, RemoteDbResp};
use crate::db::core::WeaverDbCore;
use crate::db::server::init_system_tables::init_system_tables;
use crate::db::server::layers::packets::{DbReq, DbReqBody, DbResp};
use crate::db::server::layers::{Layer, Layers, Service};
use crate::db::server::processes::{ProcessManager, WeaverProcessChild};
use crate::db::server::socket::DbSocket;
use crate::error::Error;
use crate::plugins::{Plugin, PluginError};
use crate::queries::ast::Query;
use crate::queries::executor::QueryExecutor;
use crate::queries::query_plan::QueryPlan;
use crate::queries::query_plan_factory::QueryPlanFactory;
use crate::tx::coordinator::TxCoordinator;
use crossbeam::channel::{unbounded, Sender};
use parking_lot::{Mutex, RwLock};
use std::net::{SocketAddr, ToSocketAddrs};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Weak};
use std::thread;
use std::thread::{current, JoinHandle};
use threadpool_crossbeam_channel::{Builder, ThreadPool};
use tracing::{debug, error, error_span, info, info_span, warn};

/// A server that allows for multiple connections.
#[derive(Clone)]
pub struct WeaverDb {
    pub(super) shared: Arc<WeaverDbShared>,
}

pub(super) struct WeaverDbShared {
    pub(super) db: Arc<RwLock<WeaverDbCore>>,
    message_queue: Sender<(DbReq, Sender<DbResp>)>,
    main_handle: Option<JoinHandle<()>>,
    worker_handles: Mutex<Vec<JoinHandle<Result<(), Error>>>>,
    req_worker_pool: ThreadPool,

    /// Should only be bound to TCP once.
    tcp_bound: AtomicBool,
    tcp_local_address: RwLock<Option<SocketAddr>>,

    /// Responsible for managing processes.
    process_manager: RwLock<ProcessManager>,

    /// Layered processor
    layers: RwLock<Layers>,
}

impl WeaverDb {
    /// Process a request
    fn base_service(&mut self, req: DbReq) -> Result<DbResp, Error> {
        let (_, body) = req.to_parts();
        match body {
            DbReqBody::OnCore(cb) => {
                let mut writable = self.shared.db.write();
                (cb)(&mut *writable)
            }
            DbReqBody::OnServer(cb) => (cb)(self),
            DbReqBody::Ping => Ok(DbResp::Pong),
            DbReqBody::TxQuery(tx, ref query) => {
                let ref plan = self.to_plan(query)?;
                debug!("created plan: {plan:?}");
                let executor = self.query_executor();
                match executor.execute(&tx, plan) {
                    Ok(rows) => Ok(DbResp::TxRows(tx, rows)),
                    Err(err) => Ok(DbResp::Err(err.to_string())),
                }
            }
            DbReqBody::StartTransaction => {
                let tx = self.shared.db.read().start_transaction();
                Ok(DbResp::Tx(tx))
            }
            DbReqBody::Commit(tx) => {
                tx.commit();
                Ok(DbResp::Ok)
            }
            DbReqBody::Rollback(tx) => {
                tx.rollback();
                Ok(DbResp::Ok)
            }
        }
    }

    pub fn new(workers: usize, shard: WeaverDbCore) -> Result<Self, Error> {
        let inner = Arc::new_cyclic(move |weak| {
            let mut shard = shard;
            shard.tx_coordinator = Some(TxCoordinator::new(WeakWeaverDb(weak.clone()), 0));

            let worker_pool = Builder::new()
                .num_threads(workers)
                .thread_name("worker".to_string())
                .build();
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

                                let mut db = WeaverDb { shared: db };

                                let resp =
                                    thread::spawn(move || db.shared.layers.read().process(req))
                                        .join()
                                        .map_err(|e| {
                                            error!("panic occurred while processing a request");
                                            Error::ThreadPanicked
                                        })
                                        .into_db_resp();

                                let _ = response_channel.send(resp);
                            })
                        }
                    })
                    .expect("could not start main shard thread")
            };

            let layers = {
                let weak_db = weak.clone();
                Layers::new(move |req| {
                    let Some(db) = weak_db.upgrade() else {
                        warn!("db request processing after db dropped");
                        return Error::server_error("no db to connect to").into_db_resp();
                    };
                    let mut db = WeaverDb { shared: db };
                    db.base_service(req).into_db_resp()
                })
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
                layers: RwLock::new(layers),
            }
        });

        let mut db = WeaverDb { shared: inner };
        init_system_tables(&mut db)?;
        Ok(db)
    }

    /// Apply a plugin
    pub fn apply<P: Plugin>(&mut self, plugin: &P) -> Result<(), PluginError> {
        error_span!("plugin-apply", plugin = plugin.name().as_ref()).in_scope(|| {
            match plugin.apply(self) {
                Ok(()) => Ok(()),
                Err(err) => {
                    error!("failed to apply plugin: {}", err);
                    Err(err)
                }
            }
        })
    }

    /// Wraps the db server response mechanism with a new layer
    pub fn wrap_req<L: Layer + 'static>(&mut self, layer: L) {
        self.shared.layers.write().wrap(layer)
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

                        process_manager.start(move |mut child: WeaverProcessChild| {
                            let span = info_span!(
                                "external-connection",
                                peer_addrr = stream.peer_addr().map(|addr| addr.to_string())
                            );
                            let _enter = span.enter();
                            if let Err(e) = cnxn_main(&mut stream, child) {
                                warn!("client connection ended with err: {}", e);
                                stream.write(&Message::Resp(RemoteDbResp::Err(e.to_string())))?;
                                Err(e)
                            } else {
                                Ok(())
                            }
                        })?;
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

    pub fn with_process_manager<F: Fn(&ProcessManager) -> R, R>(&self, cb: F) -> R {
        let pm = &*self.shared.process_manager.read();
        cb(pm)
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

        DbSocket::new(msg_queue, resp_send, resp_recv)
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
