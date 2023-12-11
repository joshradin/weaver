use std::fmt::{Debug, Formatter};
use std::net::{SocketAddr, ToSocketAddrs};
use std::sync::{Arc, Weak};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::thread;
use std::thread::JoinHandle;

use crossbeam::channel::{Receiver, RecvError, Sender, SendError, unbounded};
use parking_lot::{Mutex, RwLock};
use serde::{Deserialize, Serialize};
use threadpool_crossbeam_channel::ThreadPool;
use tracing::{info, warn};
use crate::cnxn::cnxn_loop::cnxn_main;
use crate::cnxn::MessageStream;
use crate::cnxn::tcp::WeaverTcpListener;

use crate::db::core::WeaverDbCore;
use crate::error::Error;
use crate::tx::coordinator::TxCoordinator;

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
    /// Each process (ie: tcp-connection) is assigned a process identification number
    process_count: Arc<AtomicUsize>,
}

impl WeaverDb {
    pub fn new(workers: usize, shard: WeaverDbCore) -> Result<Self, Error> {
        let inner = Arc::new_cyclic(move |weak| {
            let mut shard = shard;
            shard.tx_coordinator = Some(TxCoordinator::new(
                WeakWeaverDb(weak.clone()),
                0
            ));

            let worker_pool = ThreadPool::new(workers);
            let (sc, rc) = unbounded::<(DbReq, Sender<DbResp>)>();

            let main_handle = {
                let worker_pool = worker_pool.clone();
                let weak_db = weak.clone();
                thread::Builder::new().name("db-shard".to_string()).spawn(move || {
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

                            let resp = match req {
                                DbReq::Full(cb) => {
                                    let mut writable = db.shared.db.write();
                                    (cb)(&mut *writable)
                                }
                                DbReq::Ping => {
                                    Ok(DbResp::Pong)
                                }
                            };

                            if let Ok(resp) = resp {
                                let _ = response_channel.send(resp);
                            }
                        })

                    }
                }).expect("could not start main shard thread")
            };

            WeaverDbShared {
                db: Arc::new(RwLock::new(shard)),
                message_queue: sc,
                main_handle: Some(main_handle),
                worker_handles: Mutex::default(),
                req_worker_pool: worker_pool,
                tcp_bound: AtomicBool::new(false),
                tcp_local_address: RwLock::default(),
                process_count: Arc::new(AtomicUsize::default()),
            }
        });

        let daemon = WeaverDb { shared: inner };
        Ok(daemon)
    }

    /// Bind to a tcp port. Can only be done once
    pub fn bind_tcp<A: ToSocketAddrs>(&mut self, addr: A) -> Result<(), Error> {
        if self.shared.tcp_bound.compare_exchange(false, true, Ordering::SeqCst, Ordering::Relaxed) == Ok(false) {
            let weak = self.weak();
            let mut listener = WeaverTcpListener::bind(addr, weak)?;
            let _ = self.shared.tcp_local_address.write().insert(listener.local_addr()?);
            let process_count = self.shared.process_count.clone();
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

                        let pid = process_count.fetch_add(1, Ordering::SeqCst);
                        let distro_db = distro_db.clone();
                        thread::Builder::new().name(format!("distro-db-process-{}", pid)).spawn(move || {
                            let ref distro_db = distro_db;
                            cnxn_main(stream, pid, distro_db)
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


    /// Gets the local address of this server, if open on a tcp connection
    pub fn local_addr(&self) -> Option<SocketAddr> {
        self.shared.tcp_local_address.read().as_ref().map(|s| s.clone())
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

    /// Creates a connection
    pub fn connect_system(&self) -> SystemDbSocket {
        let (req_send, req_recv) = unbounded::<DbReq>();
        let shard = self.shared.db.clone();

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
                        let _ = (full)(&mut *lock);
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


#[derive(Serialize, Deserialize)]
pub enum DbReq {
    #[serde(skip)]
    Full(Box<dyn FnOnce(&mut WeaverDbCore) -> Result<DbResp, Error> + Send>),
    Ping,
}

impl Debug for DbReq {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DbReq").finish_non_exhaustive()
    }
}

impl DbReq {
    /// Gets full access of db
    pub fn full<F: FnOnce(&mut WeaverDbCore) -> Result<DbResp, Error> + Send + 'static>(func: F) -> Self {
        Self::Full(Box::new(func))
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub enum DbResp {
    Pong,
    Ok,
}

#[derive(Debug)]
pub struct DbSocket {
    main_queue: Sender<(DbReq, Sender<DbResp>)>,
    resp_sender: Sender<DbResp>,
    receiver: Receiver<DbResp>,
}

impl DbSocket {

    /// Communicate with the db
    pub fn send(&self, req: DbReq) -> Result<DbResp, ShardSocketError> {
        self.main_queue.send((req, self.resp_sender.clone()))?;
        Ok(self.receiver.recv()?)
    }
}

#[derive(Debug, Clone)]
pub struct SystemDbSocket {
    sender: Sender<DbReq>,
    handle: Arc<JoinHandle<()>>,
}

#[derive(Debug, thiserror::Error)]
pub enum ShardSocketError {
    #[error(transparent)]
    SendError(#[from] SendError<(DbReq, Sender<DbResp>)>),
    #[error(transparent)]
    RecvError(#[from] RecvError),
}
