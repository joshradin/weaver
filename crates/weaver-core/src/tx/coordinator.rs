use crate::db::server::WeakWeaverDb;
use crate::tx::behavior::{TxCompletion, TxDropBehavior};
use crate::tx::{Tx, TxId};
use crossbeam::channel::{unbounded, Sender};
use parking_lot::{Mutex, RwLock};
use std::collections::BTreeSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::thread::JoinHandle;
use tracing::{debug, error, info, span, warn, Level};

/// The transaction coordinator creates transactions and is responsible
/// to responding to the completion of transactions.
///
/// Coordinators are only useful
/// in concurrent setups, and are initialized when a [`WeaverDbServer`](crate::db::server::WeaverDb) is
/// started
#[derive(Debug)]
pub struct TxCoordinator {
    tx_lock: Arc<Mutex<()>>,
    primary_msg_sender: Sender<TxCompletionToken>,
    server: WeakWeaverDb,
    _handle: JoinHandle<()>,
    next_tx_id: Arc<AtomicU64>,
    committed_to: Arc<AtomicU64>,
    active_txs: Arc<RwLock<BTreeSet<TxId>>>,
    committed_txs: Arc<RwLock<BTreeSet<TxId>>>,
    on_drop: TxDropBehavior,
}

impl TxCoordinator {
    /// Creates a new transaction coordinator for a weak distro db server
    pub fn new(server: WeakWeaverDb, committed: u64) -> Self {
        let (sc, rc) = unbounded::<TxCompletionToken>();
        let next_tx_id = Arc::new(AtomicU64::new(committed + 1));
        let committed_to = Arc::new(AtomicU64::new(committed));
        let active_txs: Arc<RwLock<BTreeSet<TxId>>> = Default::default();
        let committed_txs: Arc<RwLock<BTreeSet<TxId>>> = Default::default();
        let tx_lock = Arc::new(Mutex::default());
        let handle = {
            let server = server.clone();
            let committed_to = committed_to.clone();
            let active_txs = active_txs.clone();
            let committed_txs = committed_txs.clone();
            thread::spawn(move || {
                let span = span!(Level::INFO, "tx-coordinator");
                let _enter = span.enter();
                loop {
                    let TxCompletionToken {
                        tx_id,
                        completion,
                        ack,
                    } = match rc.recv() {
                        Ok(msg) => msg,
                        Err(..) => {
                            break;
                        }
                    };
                    let Some(_server) = server.upgrade() else {
                        warn!(
                            "Could not upgrade server weak ptr but received completion token: {:?}",
                            (tx_id, completion)
                        );
                        if completion == TxCompletion::Commit {
                            error!("Transaction with commit request {tx_id:?} will be lost.");
                        }
                        if ack.send(()).is_err() {
                            error!("could not send ack")
                        }
                        break;
                    };

                    info!("transaction {:?} completed with {:?}", tx_id, completion);
                    info!("active txs: {:?}", active_txs);
                    info!("committed txs: {:?}", committed_txs);

                    match completion {
                        TxCompletion::Rollback => {
                            // perform rollback
                            if !active_txs.write().remove(&tx_id) {
                                if ack.send(()).is_err() {
                                    error!("could not send ack")
                                }
                                panic!("tx should be present in active set")
                            }
                        }
                        TxCompletion::Commit => {
                            if !active_txs.write().remove(&tx_id) {
                                if ack.send(()).is_err() {
                                    error!("could not send ack")
                                }
                                panic!("tx should be present in active set")
                            }
                            match active_txs.read().iter().min() {
                                None => {
                                    let prev = committed_to.load(Ordering::SeqCst);
                                    let mut committed_txs = committed_txs.write();
                                    if let Some(&max) = committed_txs.iter().max() {
                                        let max: u64 = max.into();
                                        if max > prev {
                                            let res = committed_to.compare_exchange(
                                                prev,
                                                max,
                                                Ordering::SeqCst,
                                                Ordering::Relaxed,
                                            );
                                            if let Ok(res) = res {
                                                debug!(
                                                    "increased committed_to to {} (prev: {res})",
                                                    max
                                                );
                                                committed_txs.clear();
                                            } else {
                                                warn!("failed to update committed_to to {}", max)
                                            }
                                        }
                                    }
                                }
                                Some(min_active) => {
                                    let mut committed_txs = committed_txs.write();
                                    committed_txs.insert(tx_id);
                                    let prev = committed_to.load(Ordering::SeqCst);
                                    if let Some(&max) =
                                        committed_txs.iter().filter(|tx| tx < &min_active).max()
                                    {
                                        let max: u64 = max.into();
                                        if max > prev {
                                            let res = committed_to.compare_exchange(
                                                prev,
                                                max,
                                                Ordering::SeqCst,
                                                Ordering::Relaxed,
                                            );
                                            if let Ok(res) = res {
                                                debug!(
                                                    "increased committed_to to {} (prev: {res})",
                                                    max
                                                );
                                                committed_txs.retain(|tx| tx >= min_active);
                                            } else {
                                                warn!("failed to update committed_to to {}", max)
                                            }
                                        }
                                    }
                                }
                            }
                            debug!(
                                "current tx look-behind: {} after {} completed",
                                committed_to.load(Ordering::SeqCst),
                                tx_id
                            );
                        }
                    }
                    if ack.send(()).is_err() {
                        error!("could not send ack")
                    }
                }
                info!("tx-coordinator has completed");
            })
        };

        Self {
            tx_lock,
            primary_msg_sender: sc,
            server,
            _handle: handle,
            next_tx_id,
            committed_to,
            active_txs,
            committed_txs,
            on_drop: Default::default(),
        }
    }

    /// Starts the next transaction
    pub fn next(&self) -> Tx {
        let lock = self.tx_lock.clone();
        let tx = {
            let _lock = self.tx_lock.lock();
            debug!("locked tx_lock for completion for tx creation");
            let tx = Tx {
                id: TxId(self.next_tx_id.fetch_add(1, Ordering::SeqCst)),
                look_behind: TxId(self.committed_to.load(Ordering::SeqCst)),
                visible: self.committed_txs.read().clone(),
                completed: false,
                drop_behavior: self.on_drop,
                msg_sender: Some(self.primary_msg_sender.clone()),
                _server_ref: Some(self.server.upgrade().expect("no server").into()),
                lock,
            };
            self.active_txs.write().insert(tx.id);
            tx
        };
        debug!("started tx {}", tx);
        tx
    }

    pub fn on_drop(&self) -> TxDropBehavior {
        self.on_drop
    }

    pub fn on_drop_mut(&mut self) -> &mut TxDropBehavior {
        &mut self.on_drop
    }
}

/// A completion token
#[derive(Debug)]
pub(super) struct TxCompletionToken {
    pub tx_id: TxId,
    pub completion: TxCompletion,
    pub ack: Sender<()>,
}
