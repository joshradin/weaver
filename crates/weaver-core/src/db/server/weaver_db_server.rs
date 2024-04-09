use std::net::{SocketAddr, ToSocketAddrs};
use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock, Weak};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::thread;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use crossbeam::channel::{Sender, unbounded};
use parking_lot::{Mutex, RwLock};


use tracing::{
    debug, error, error_span, info, Level, Span, span_enabled, trace, trace_span, warn,
};

use weaver_ast::ast::Query;

use crate::access_control::auth::context::AuthContext;
use crate::access_control::auth::init::{AuthConfig, init_auth_context};
use crate::cancellable_task::{CancellableTask, Cancelled, CancelRecv};
use crate::cnxn::{Message, MessageStream, RemoteDbResp, WeaverStreamListener};
use crate::cnxn::cnxn_loop::remote_stream_loop;
use crate::cnxn::interprocess::WeaverLocalSocketListener;
use crate::cnxn::stream::WeaverStream;
use crate::cnxn::tcp::WeaverTcpListener;
use crate::common::stream_support::Stream;
use crate::db::core::{bootstrap, WeaverDbCore};
use crate::db::server::init::engines::init_engines;
use crate::db::server::init::system::init_system_tables;
use crate::db::server::init::weaver::init_weaver_schema;
use crate::db::server::layers::packets::{DbReq, DbReqBody, DbResp};
use crate::db::server::layers::service::Service;
use crate::db::server::lifecycle::{LifecyclePhase, WeaverDbLifecycleService};
use crate::db::server::processes::{
    ProcessManager, WeaverPid, WeaverProcessChild, WeaverProcessInfo,
};
use crate::db::server::socket::{DbSocket, MainQueueItem};
use crate::error::WeaverError;
use crate::modules::{Module, ModuleError};
use crate::monitoring::{Monitor, Monitorable, Stats};
use crate::queries::execution::evaluation::builtins::BUILTIN_FUNCTIONS_REGISTRY;
use crate::queries::execution::evaluation::functions::FunctionRegistry;
use crate::queries::execution::QueryExecutor;
use crate::queries::query_plan::QueryPlan;
use crate::queries::query_plan_factory::QueryPlanFactory;
use crate::queries::query_plan_optimizer::QueryPlanOptimizer;
use crate::rows::OwnedRows;
use crate::tx::coordinator::TxCoordinator;
use crate::tx::Tx;

use super::layers::packets::{IntoDbResponse, Packet};

use crate::db::server::layers::{Layer, Layers};

/// A server that allows for multiple connections.
#[derive(Clone)]
pub struct WeaverDb {
    pub(super) shared: Arc<WeaverDbShared>,
}

pub(super) struct WeaverDbShared {
    core: Arc<RwLock<WeaverDbCore>>,
    message_queue: Sender<MainQueueItem>,
    _main_handle: Option<JoinHandle<()>>,
    worker_continue: Arc<AtomicBool>,
    worker_handles: Mutex<Vec<JoinHandle<Result<(), WeaverError>>>>,

    /// Should only be bound to TCP once.
    tcp_bound: AtomicBool,
    tcp_local_address: OnceLock<SocketAddr>,

    socket_file_bound: AtomicBool,
    socket_file: OnceLock<PathBuf>,
    auth_context: AuthContext,

    function_registry: FunctionRegistry,

    /// Responsible for managing processes.
    process_manager: RwLock<ProcessManager>,

    /// Layered processor
    layers: RwLock<Layers>,

    lifecycle_service: WeaverDbLifecycleService,

    monitor: Arc<OnceLock<WeaverDbMonitor>>,
}

impl WeaverDb {
    #[cfg(test)]
    pub fn in_temp_dir() -> Result<(tempfile::TempDir, Self), WeaverError> {
        let (dir, core) = WeaverDbCore::in_temp_dir()?;
        let auth_config = AuthConfig::in_path(dir.path());
        Self::new(core, auth_config).map(|this| (dir, this))
    }
    pub fn new(shard: WeaverDbCore, auth_config: AuthConfig) -> Result<Self, WeaverError> {
        let path = shard.path().to_path_buf();
        let auth_context = init_auth_context(&auth_config)?;
        let inner = Arc::new_cyclic(move |weak| {
            let mut shard = shard;
            shard.tx_coordinator = Some(TxCoordinator::new(WeakWeaverDb(weak.clone()), 0));

            // let worker_pool = ThreadPoolBuilder::new()
            //     .num_threads(workers)
            //     .thread_name(|c| format!("weaver-core-worker-{c}"))
            //     .build()
            //     .expect("could not build worker pool");
            let (sc, rc) = unbounded::<MainQueueItem>();

            let main_handle = {
                let weak_db = weak.clone();
                thread::Builder::new()
                    .name("db-core".to_string())
                    .spawn(move || {
                        let weak_db = weak_db.clone();
                        error_span!("db-server-main").in_scope(|| loop {
                            let Ok((req, response_channel)) = rc.recv() else {
                                info!("Db request channel closed");
                                break;
                            };
                            debug!("got request packet");
                            let weak_db = weak_db.clone();
                            thread::spawn(move || {
                                let Some(db) = weak_db.upgrade() else {
                                    warn!("db request processing after db dropped");
                                    return;
                                };
                                let db = WeaverDb { shared: db };

                                let req_id = *req.id();
                                let req = req.unwrap();

                                if let Err(e) = CancellableTask::with_cancel({
                                    let response_channel = response_channel.clone();
                                    move |_: (), cancel| {
                                        let span = if span_enabled!(Level::TRACE) {
                                            if let Some(parent) = req.span() {
                                                trace_span!(parent: parent, "packet", id = req_id)
                                            } else {
                                                trace_span!("packet", id = req_id)
                                            }
                                        } else {
                                            if let Some(parent) = req.span() {
                                                parent.clone()
                                            } else {
                                                Span::current()
                                            }
                                        };

                                        span.in_scope(|| {
                                            trace!("packet has begun processing");

                                            let resp =
                                                db.shared.layers.read().process(req, cancel)?;
                                            trace!("packet has finished being processed");
                                            let _ = response_channel
                                                .send(Packet::with_id(resp, req_id));
                                            Ok(())
                                        })
                                    }
                                })
                                .run()
                                .join()
                                .map_err(|_e| {
                                    error!("panic occurred while processing a request");
                                    WeaverError::ThreadPanicked
                                }) {
                                    error!("{}", e);
                                    let resp = e.into_db_resp();
                                    let _ =
                                        response_channel.try_send(Packet::with_id(resp, req_id));
                                }
                            });
                        })
                    })
                    .expect("could not start main shard thread")
            };

            let layers = {
                let weak_db = weak.clone();
                Layers::new(move |req, cancel: &CancelRecv| {
                    let Some(db) = weak_db.upgrade() else {
                        warn!("db request processing after db dropped");
                        return Ok(WeaverError::server_error("no db to connect to").into_db_resp());
                    };
                    let mut db = WeaverDb { shared: db };
                    Ok(db.base_service(req, cancel)?.into_db_resp())
                })
            };

            let shard = Arc::new(RwLock::new(shard));
            WeaverDbShared {
                core: shard,
                message_queue: sc,
                _main_handle: Some(main_handle),
                worker_continue: Arc::new(AtomicBool::new(true)),
                worker_handles: Mutex::default(),
                tcp_bound: AtomicBool::new(false),
                tcp_local_address: Default::default(),

                socket_file_bound: AtomicBool::new(false),
                socket_file: Default::default(),
                auth_context,
                function_registry: BUILTIN_FUNCTIONS_REGISTRY.clone(),
                process_manager: RwLock::new(ProcessManager::new(WeakWeaverDb(weak.clone()))),
                layers: RwLock::new(layers),
                lifecycle_service: WeaverDbLifecycleService::new(WeakWeaverDb(weak.clone())),
                monitor: Arc::new(Default::default()),
            }
        });

        let db = WeaverDb { shared: inner };

        let mut service = db.lifecycle_service();

        let lock_file = path.join("weaver.lock");

        service.on_init(init_engines);
        service.on_init(init_system_tables);
        service.on_bootstrap(move |weaver_db| {
            let weaver_schema_dir = path.join("weaver");
            let socket = weaver_db.connect();
            let _ = socket
                .send(DbReq::on_core(move |core, _| -> Result<(), WeaverError> {
                    // bootstraps weaver schema
                    bootstrap(core, &weaver_schema_dir)?;
                    init_weaver_schema(core)?;
                    Ok(())
                }))
                .join()??
                .to_result()?;
            Ok(())
        });

        service.on_teardown(move |db| {
            let _ = db.shared.worker_continue.compare_exchange(
                true,
                false,
                Ordering::Acquire,
                Ordering::Relaxed,
            );
            let now = Instant::now();

            info!("Giving workers 5 seconds to terminate gracefully");
            let mut workers = db.shared.worker_handles.lock();
            while now.elapsed() < Duration::from_secs(5) && !workers.is_empty() {
                if let Some(finished) = workers.iter().position(|w| w.is_finished()) {
                    let worker = workers.remove(finished);
                    let _ = worker.join();
                }
            }

            if !workers.is_empty() {
                warn!(
                    "unfinished workers: {:?}",
                    workers
                        .iter()
                        .map(|i| i
                            .thread()
                            .name()
                            .map(ToString::to_string)
                            .unwrap_or(format!("{:?}", i.thread().id())))
                        .collect::<Vec<_>>()
                );
            }

            std::fs::remove_file(lock_file)?;
            Ok(())
        });

        Ok(db)
    }

    /// Gets whether the weaver core is initialized
    pub fn is_bootstrapped(&self) -> bool {
        self.shared.core.read().path().join("weaver").exists()
    }

    /// Gets the lifecycle service for this weaver db instance
    pub fn lifecycle_service(&self) -> WeaverDbLifecycleService {
        self.shared.lifecycle_service.clone()
    }

    /// Apply a plugin
    pub fn apply<P: Module>(&mut self, plugin: &P) -> Result<(), ModuleError> {
        error_span!("module-apply", plugin = plugin.name().as_ref()).in_scope(|| {
            match plugin.apply(self) {
                Ok(()) => Ok(()),
                Err(err) => {
                    error!("failed to apply module: {}", err);
                    Err(err)
                }
            }
        })
    }

    pub fn auth_context(&self) -> &AuthContext {
        &self.shared.auth_context
    }

    /// Wraps the db server response mechanism with a new layer
    pub fn wrap_req<L: Layer + 'static>(&mut self, layer: L) {
        self.shared.layers.write().wrap(layer)
    }

    pub fn to_plan<'a>(
        &self,
        tx: &Tx,
        query: &Query,
        plan_context: impl Into<Option<&'a WeaverProcessInfo>>,
    ) -> Result<QueryPlan, WeaverError> {
        info!("query to plan");
        let factory = QueryPlanFactory::new(self.weak());
        debug!("created query factory: {:?}", factory);
        let mut plan = factory.to_plan(tx, query, &self.shared.function_registry, plan_context)?;
        debug!("created initial plan {plan:#?}");
        let optimizer = QueryPlanOptimizer::new(self.weak());
        debug!("created query optimizer: {optimizer:?}");
        optimizer.optimize(tx, &mut plan)?;

        Ok(plan)
    }

    /// Bind to a tcp port. Can only be done once
    pub fn bind_tcp<A: ToSocketAddrs>(&mut self, addr: A) -> Result<(), WeaverError> {
        let phase = self.shared.lifecycle_service.phase();
        if phase != LifecyclePhase::Ready {
            return Err(WeaverError::ServerNotReady(phase));
        }

        if self
            .shared
            .tcp_bound
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::Relaxed)
            == Ok(false)
        {
            let weak = self.weak();
            let listener = WeaverTcpListener::bind(addr, weak)?;
            let _ = self.shared.tcp_local_address.set(listener.local_addr()?);

            let self_clone = self.weak();
            let cont = self.shared.worker_continue.clone();
            let worker = thread::Builder::new()
                .name("distro-db-tcp".to_string())
                .spawn(move || -> Result<(), WeaverError> {
                    Self::tcp_handler(listener, self_clone, cont)
                })?;
            self.shared.worker_handles.lock().push(worker);
            Ok(())
        } else {
            Err(WeaverError::TcpAlreadyBound)
        }
    }

    /// Bind to a tcp port. Can only be done once
    pub fn bind_local_socket<P: AsRef<Path>>(&mut self, path: P) -> Result<(), WeaverError> {
        let phase = self.shared.lifecycle_service.phase();
        if phase != LifecyclePhase::Ready {
            return Err(WeaverError::ServerNotReady(phase));
        }

        let path = path.as_ref().to_path_buf();
        if self.shared.socket_file_bound.compare_exchange(
            false,
            true,
            Ordering::SeqCst,
            Ordering::Relaxed,
        ) == Ok(false)
        {
            let weak = self.weak();
            let _ = self.shared.socket_file.set(path.clone());
            let listener = WeaverLocalSocketListener::bind(&path, weak)?;

            let self_clone = self.weak();
            let cont = self.shared.worker_continue.clone();
            let worker = thread::Builder::new()
                .name("distro-db-local-socket".to_string())
                .spawn(move || -> Result<(), WeaverError> {
                    Self::local_socket_handler(listener, self_clone, cont)
                })?;

            self.shared.worker_handles.lock().push(worker);

            self.lifecycle_service().on_teardown(move |_db| {
                warn!("removing socket file...");
                std::fs::remove_file(path)?;
                Ok(())
            });

            Ok(())
        } else {
            Err(WeaverError::TcpAlreadyBound)
        }
    }

    fn tcp_handler(
        listener: WeaverTcpListener,
        db: WeakWeaverDb,
        cont: Arc<AtomicBool>,
    ) -> Result<(), WeaverError> {
        Self::stream_handler(listener, db, cont)?;
        info!("tcp listener shut down");
        Ok(())
    }
    fn local_socket_handler(
        listener: WeaverLocalSocketListener,
        db: WeakWeaverDb,
        cont: Arc<AtomicBool>,
    ) -> Result<(), WeaverError> {
        Self::stream_handler(listener, db, cont)?;
        info!("local socket listener shut down");
        Ok(())
    }

    fn stream_handler<T: WeaverStreamListener>(
        listener: T,
        db: WeakWeaverDb,
        cont: Arc<AtomicBool>,
    ) -> Result<(), WeaverError>
        where T::Stream : Send + Sync + 'static
    {
        while cont.load(Ordering::Relaxed) {
            let Ok(stream) = listener.try_accept() else {
                warn!("Listener closed");
                break;
            };

            let Some(stream) = stream else {
                continue;
            };

            let Some(weaver) = db.clone().upgrade() else {
                warn!("weaver closed");
                break;
            };

            weaver.handle_connection(stream)?;
        }
        Ok(())
    }

    pub fn handle_connection<T: Stream + Send + Sync + 'static>(
        &self,
        mut stream: WeaverStream<T>,
    ) -> Result<WeaverPid, WeaverError> {
        let mut process_manager = self.shared.process_manager.write();
        let user = stream.user().clone();
        let monitor = self.shared.monitor.clone();

        process_manager.start(
            &user,
            CancellableTask::with_cancel(move |child: WeaverProcessChild, recv| {
                if let Some(monitor) = monitor.get() {
                    monitor.connections.fetch_add(1, Ordering::SeqCst);
                }
                let span = error_span!(
                    "cnxn",
                    peer_addr = stream
                        .peer_addr()
                        .map(|addr| addr.to_string())
                        .unwrap_or_else(|| "<unknown>".to_string()),
                    user = child.info().user,
                    pid = child.pid(),
                );
                let _enter = span.enter();
                let ret = Ok(
                    if let Err(e) = remote_stream_loop(&mut stream, child, recv, &span) {
                        warn!("client connection ended with err: {}", e);
                        if let Err(e) =
                            stream.write(&Message::Resp(RemoteDbResp::Err(e.to_string())))
                        {
                            Err(e)
                        } else {
                            Err(e)
                        }
                    } else {
                        Ok(())
                    },
                );
                if let Some(monitor) = monitor.get() {
                    monitor.connections.fetch_sub(1, Ordering::SeqCst);
                }
                ret
            }),
        )
    }

    pub fn with_process_manager<F: Fn(&ProcessManager) -> R, R>(&self, cb: F) -> R {
        let pm = &*self.shared.process_manager.read();
        cb(pm)
    }

    pub fn query_executor(&self) -> QueryExecutor {
        QueryExecutor::new(Arc::downgrade(&self.shared.core))
    }

    /// Gets the local address of this server, if open on a tcp connection
    pub fn local_addr(&self) -> Option<SocketAddr> {
        self.shared.tcp_local_address.get().map(|s| s.clone())
    }

    /// Creates a connection
    pub fn connect(&self) -> DbSocket {
        let msg_queue = self.shared.message_queue.clone();
        DbSocket::new(msg_queue, None)
    }

    /// Creates a weak reference to a distro db server
    pub fn weak(&self) -> WeakWeaverDb {
        WeakWeaverDb(Arc::downgrade(&self.shared))
    }

    /// Process a request
    fn base_service(&mut self, req: DbReq, cancel_recv: &CancelRecv) -> Result<DbResp, Cancelled> {
        let (_, ctx, body) = req.to_parts();
        trace!("req={:#?}", body);
        match body {
            DbReqBody::OnCoreWrite(cb) => trace_span!("core", mode = "write").in_scope(|| {
                trace!("getting write access to core");
                let mut writable = self.shared.core.write();
                Ok((cb)(&mut *writable, cancel_recv)?.into_db_resp())
            }),
            DbReqBody::OnCore(cb) => trace_span!("core", mode = "read").in_scope(|| {
                trace!("getting read access to core");
                let readable = self.shared.core.read();
                Ok((cb)(&*readable, cancel_recv)?.into_db_resp())
            }),
            DbReqBody::OnServer(cb) => (cb)(self, cancel_recv),
            DbReqBody::Ping => Ok(DbResp::Pong),
            DbReqBody::TxQuery(tx, ref query) => {
                let ref plan = match self.to_plan(&tx, query, ctx.as_ref()).map_err(|e| {
                    error!("creating plan resulted in error: {}", e);
                    e
                }) {
                    Ok(ok) => ok,
                    Err(err) => return Ok(DbResp::Err(err)),
                };
                trace!("created plan: {plan:#?}");
                let executor = self.query_executor();
                let x = match executor.execute(&tx, plan) {
                    Ok(rows) => Ok(DbResp::TxRows(tx, OwnedRows::from(rows))),
                    Err(err) => Ok(DbResp::Err(err)),
                };
                x
            }
            DbReqBody::StartTransaction => error_span!("core", mode = "write").in_scope(|| {
                trace!("getting write access to core");
                let tx = self.shared.core.read().start_transaction();
                trace!("started transaction (id = {:?})", tx.id());
                Ok(DbResp::Tx(tx))
            }),
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
}

impl Default for WeaverDb {
    fn default() -> Self {
        Self::new(WeaverDbCore::default(), AuthConfig::default()).unwrap()
    }
}

impl Monitorable for WeaverDb {
    fn monitor(&self) -> Box<dyn Monitor> {
        Box::new(
            self.shared
                .monitor
                .get_or_init(|| WeaverDbMonitor::new(self.shared.core.read().monitor()))
                .clone(),
        )
    }
}

impl Drop for WeaverDbShared {
    fn drop(&mut self) {
        info!("db server dropped, running teardown if not already");
        let _ = self.lifecycle_service.teardown();
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

/// The main weaver db monitor
#[derive(Debug, Clone)]
pub(super) struct WeaverDbMonitor {
    start_time: Instant,
    pub(super) connections: Arc<AtomicUsize>,
    core_monitor: Arc<Mutex<Box<dyn Monitor>>>,
}

impl Monitor for WeaverDbMonitor {
    fn name(&self) -> &str {
        "WeaverDb"
    }

    fn stats(&mut self) -> Stats {
        Stats::from_iter([
            (
                "elapsed",
                Stats::from(self.start_time.elapsed().as_secs_f64()),
            ),
            (
                "connections",
                Stats::from(self.connections.load(Ordering::Relaxed) as i64),
            ),
            ("core", self.core_monitor.lock().stats()),
        ])
    }
}

impl WeaverDbMonitor {
    fn new(core: Box<dyn Monitor>) -> Self {
        Self {
            start_time: Instant::now(),
            connections: Default::default(),
            core_monitor: Arc::new(Mutex::new(core)),
        }
    }
}
