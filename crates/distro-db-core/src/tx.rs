//! Transactions

use std::sync::atomic::AtomicU64;
use std::thread;
use std::thread::JoinHandle;
use crossbeam::channel::{RecvError, Sender, unbounded};
use log::info;
use serde::{Deserialize, Serialize};
use crate::db::{DistroDbServer, WeakDistroDbServer};
use crate::dynamic_table::Col;

/// Behavior when a transaction drops
#[derive(Debug, Default, Eq, PartialEq, Copy, Clone)]
pub struct TxDropBehavior(pub TxCompletion);

#[derive(Debug, Default, Eq, PartialEq, Copy, Clone)]
pub enum TxCompletion {
    #[default]
    Rollback,
    Commit
}

pub static TX_ID_COLUMN: Col<'static> = "@@TX_ID";

/// The transaction coordinator creates transactions and is responsible
/// to responding to the completion of transactions.
///
/// Coordinators are only useful
/// in concurrent setups, and are initialized when a [`DistroDbServer`](crate::db::DistroDbServer) is
/// started
#[derive(Debug)]
pub struct TxCoordinator {
    primary_msg_sender: Sender<TxCompletionToken>,
    server: WeakDistroDbServer,
    handle: JoinHandle<()>,
    next_tx_id: AtomicU64,
    committed_to: AtomicU64
}

impl TxCoordinator {
    /// Creates a new transaction coordinator for a weak distro db server
    pub fn new(server: WeakDistroDbServer, committed: u64) -> Self {
        let (sc, rc) = unbounded::<TxCompletionToken>();

        let handle = thread::spawn(move || {
            loop {
                let TxCompletionToken{ tx_id, completion } = match rc.recv() {
                    Ok(msg) => {msg}
                    Err(..) => {
                        break
                    }
                };

                info!("transaction {:?} completed with {:?}", tx_id, completion);
            }
        });

        Self {
            primary_msg_sender: sc,
            server,
            handle,
            next_tx_id: AtomicU64::new(committed + 1),
            committed_to: AtomicU64::new(committed),
        }
    }

    /// Starts the next transaction
    pub fn next(&self) -> Tx {
        todo!()
    }
}

#[derive(Debug)]
struct TxCompletionToken {
    tx_id: TxId,
    completion: TxCompletion
}

/// The id of the a transaction
#[derive(Debug, Default, Ord, PartialOrd, Eq, PartialEq, Hash, Copy, Clone, Serialize, Deserialize)]
pub struct TxId(u64);

impl TxId {
    /// Checks if this id would be visible within a transaction
    pub fn is_visible_in(&self, tx: &Tx) -> bool {
        self <= &tx.id && self >= &tx.look_behind
    }

    /// Checks if this id would be visible within a transaction
    pub fn is_visible_within(&self, tx: &TxId, look_behind: &TxId) -> bool {
        self <= tx && self >= look_behind
    }
}

impl From<TxId> for i64 {
    fn from(value: TxId) -> Self {
        value.0 as i64
    }
}

impl From<i64> for TxId {
    fn from(value: i64) -> Self {
        Self(value as u64)
    }
}


/// A transaction within the database. All transactions contain an identifier which are generated sequentially.
///
/// A transaction is able to see any the results of any other transaction that's been completed before
/// this transaction was created
#[derive(Debug, Default)]
pub struct Tx {
    id: TxId,
    look_behind: TxId,
    completed: bool,
    drop_behavior: TxDropBehavior,
    msg_sender: Option<Sender<TxCompletionToken>>,
}

impl Tx {

}

impl Tx {

    /// Gets the identifier of this transaction
    pub fn id(&self) -> TxId {
        self.id
    }

    pub fn look_behind(&self) -> TxId {
       self.look_behind
    }

    /// Checks if this transaction can see any data involved with a given id
    pub fn can_see(&self, id: &TxId) -> bool {
        id.is_visible_in(self)
    }

    fn _rollback(&mut self) {
        if let Some(ref msg_sender) = self.msg_sender {
            let _ = msg_sender.send(TxCompletionToken {
                tx_id: self.id,
                completion: TxCompletion::Rollback
            });
        }
    }
    fn _commit(&mut self) {
        if let Some(ref msg_sender) = self.msg_sender {
            let _ = msg_sender.send(TxCompletionToken {
                tx_id: self.id,
                completion: TxCompletion::Rollback
            });
        }
    }
    pub fn rollback(mut self) {
        self.completed = true;
        self._rollback();
    }
    pub fn commit(mut self) {
        self.completed = true;
        self._commit();
    }
}

impl Drop for Tx {
    fn drop(&mut self) {
        if !self.completed {
            match self.drop_behavior.0 {
                TxCompletion::Rollback => {
                    self._rollback()
                }
                TxCompletion::Commit => {
                    self._commit()
                }
            }
        }
    }
}

