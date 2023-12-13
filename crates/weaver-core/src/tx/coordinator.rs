use crate::db::server::WeakWeaverDb;
use crate::tx::behavior::{TxCompletion, TxDropBehavior};
use crate::tx::{Tx, TxId};
use crossbeam::channel::{unbounded, Sender};
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::thread::JoinHandle;
use tracing::{error, info, span, warn, Level};

/// The transaction coordinator creates transactions and is responsible
/// to responding to the completion of transactions.
///
/// Coordinators are only useful
/// in concurrent setups, and are initialized when a [`WeaverDbServer`](crate::db::server::WeaverDb) is
/// started
#[derive(Debug)]
pub struct TxCoordinator {
    primary_msg_sender: Sender<TxCompletionToken>,
    server: WeakWeaverDb,
    handle: JoinHandle<()>,
    next_tx_id: AtomicU64,
    committed_to: AtomicU64,
    on_drop: TxDropBehavior,
}

impl TxCoordinator {
    /// Creates a new transaction coordinator for a weak distro db server
    pub fn new(server: WeakWeaverDb, committed: u64) -> Self {
        let (sc, rc) = unbounded::<TxCompletionToken>();
        let handle = {
            let server = server.clone();
            thread::spawn(move || {
                let span = span!(Level::INFO, "tx-coordinator");
                let _enter = span.enter();
                loop {
                    let TxCompletionToken { tx_id, completion } = match rc.recv() {
                        Ok(msg) => msg,
                        Err(..) => {
                            break;
                        }
                    };
                    let Some(mut server) = server.upgrade() else {
                        warn!(
                            "Could not upgrade server weak ptr but received completion token: {:?}",
                            (tx_id, completion)
                        );
                        if completion == TxCompletion::Commit {
                            error!("Transaction with commit request {tx_id:?} will be lost.");
                        }
                        break;
                    };
                    let cnxn = server.connect();

                    info!("transaction {:?} completed with {:?}", tx_id, completion);
                    match completion {
                        TxCompletion::Rollback => {}
                        TxCompletion::Commit => {}
                    }
                }
                info!("tx-coordinator has completed");
            })
        };

        Self {
            primary_msg_sender: sc,
            server,
            handle,
            next_tx_id: AtomicU64::new(committed + 1),
            committed_to: AtomicU64::new(committed),
            on_drop: Default::default(),
        }
    }

    /// Starts the next transaction
    pub fn next(&self) -> Tx {
        Tx {
            id: TxId(self.next_tx_id.fetch_add(1, Ordering::SeqCst)),
            look_behind: TxId(self.committed_to.load(Ordering::SeqCst)),
            completed: false,
            drop_behavior: self.on_drop.clone(),
            msg_sender: Some(self.primary_msg_sender.clone()),
            _server_ref: Some(self.server.upgrade().expect("no server").into()),
        }
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
}
