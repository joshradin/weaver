//! Transactions

use crate::common::opaque::Opaque;
use behavior::{TxCompletion, TxDropBehavior};
use coordinator::TxCompletionToken;
use crossbeam::channel::Sender;
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};
use tracing::info;

use crate::db::server::WeaverDb;
use crate::dynamic_table::Col;

pub mod behavior;
pub mod coordinator;

pub static TX_ID_COLUMN: Col<'static> = "@@TX_ID";

/// The id of the a transaction
#[derive(
    Debug, Default, Ord, PartialOrd, Eq, PartialEq, Hash, Copy, Clone, Serialize, Deserialize,
)]
pub struct TxId(u64);

impl TxId {
    /// Checks if this id would be visible within a transaction
    pub fn is_visible_in(&self, tx: &Tx) -> bool {
        self.is_visible_within(&tx.id, &tx.look_behind)
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
    completed: bool,
    drop_behavior: TxDropBehavior,
    msg_sender: Option<Sender<TxCompletionToken>>,
    _server_ref: Option<Opaque<WeaverDb>>,
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
                completion: TxCompletion::Rollback,
            });
        }
    }
    fn _commit(&mut self) {
        if let Some(ref msg_sender) = self.msg_sender {
            let _ = msg_sender.send(TxCompletionToken {
                tx_id: self.id,
                completion: TxCompletion::Commit,
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

    pub fn as_ref(&self) -> TxRef {
        TxRef { id: self.id }
    }
}

impl Display for Tx {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("Tx").field(&self.id).finish()
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
    id: TxId
}