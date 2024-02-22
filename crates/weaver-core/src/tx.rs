//! Transactions

use std::collections::BTreeSet;
use std::fmt::{Debug, Display, Formatter};
use std::sync::Arc;

use crossbeam::channel::{bounded, Receiver, Sender};
use derive_more::{Display, From, Into};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use tracing::{debug, info, trace};

use behavior::{TxCompletion, TxDropBehavior};
use coordinator::TxCompletionToken;

use crate::common::opaque::Opaque;
use crate::db::server::WeaverDb;
use crate::dynamic_table::Col;

pub mod behavior;
pub mod coordinator;

pub static TX_ID_COLUMN: Col<'static> = "@@TX_ID";

/// The id of the a transaction
#[derive(
    Default,
    Ord,
    PartialOrd,
    Eq,
    PartialEq,
    Hash,
    Copy,
    Clone,
    Serialize,
    Deserialize,
    From,
    Into,
    Display,
)]
pub struct TxId(u64);

impl Debug for TxId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "TxId({})", self.0)
    }
}

impl TxId {
    /// Checks if this id would be visible within a transaction
    pub fn is_visible_in(&self, tx: &Tx) -> bool {
        self.is_visible_within(&tx.id, &tx.look_behind) || tx.visible.contains(self)
    }

    /// Checks if this id would be visible within a transaction
    pub fn is_visible_within(&self, tx: &TxId, look_behind: &TxId) -> bool {
        self <= look_behind || tx == self
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
    /// Tx ids that are ahead of the look behind and are committed
    visible: BTreeSet<TxId>,
    completed: bool,
    drop_behavior: TxDropBehavior,
    msg_sender: Option<Sender<TxCompletionToken>>,
    _server_ref: Option<Opaque<WeaverDb>>,

    lock: Arc<Mutex<()>>,
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
        self.send_complete(TxCompletion::Rollback)
    }
    fn _commit(&mut self) {
        self.send_complete(TxCompletion::Commit);
    }

    fn send_complete(&mut self, completion: TxCompletion) {
        if let Some(ref msg_sender) = self.msg_sender {
            let _lock = self.lock.lock();
            trace!("tx {} locked tx_lock for completion", self);
            let (ack_send, ack_recv) = bounded(0);
            let _ = msg_sender.send(TxCompletionToken {
                tx_id: self.id,
                completion,
                ack: ack_send,
            });
            let _ = ack_recv.recv();
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

    pub fn as_ref(&self) -> TxRef {
        TxRef { id: self.id }
    }
}

impl Display for Tx {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Tx[{}]{{ look_behind: {}, visible: {:?} }}",
            self.id,
            self.look_behind,
            self.visible
                .iter()
                .map(|s| s.to_string())
                .collect::<Vec<_>>()
        )
    }
}

impl Drop for Tx {
    fn drop(&mut self) {
        info!("dropping transaction {:?}", self);
        if !self.completed {
            match self.drop_behavior.0 {
                TxCompletion::Rollback => self._rollback(),
                TxCompletion::Commit => self._commit(),
            }
        }
    }
}

/// A reference to a transaction
#[derive(Debug, Eq, PartialEq, Hash)]
pub struct TxRef {
    id: TxId,
}
