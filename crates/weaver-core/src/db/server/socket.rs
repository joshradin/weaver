use crate::db::server::layers::packets::{DbReq, DbReqBody, DbResp, Packet, PacketId};
use crate::db::server::processes::WeaverPid;
use crate::dynamic_table::Table;
use crate::error::Error;
use crate::tables::TableRef;
use crate::tx::Tx;
use crossbeam::channel::{unbounded, Receiver, RecvError, Sender, TryRecvError};
use parking_lot::{Mutex, RwLock};
use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::Arc;
use std::{hint, thread};
use tracing::{debug, error_span, trace, warn};

pub type MainQueueItem = (Packet<DbReq>, Sender<Packet<DbResp>>);

#[derive(Debug)]
pub struct DbSocket {
    pid: Option<WeaverPid>,
    main_queue: Sender<MainQueueItem>,
    resp_sender: Sender<Packet<DbResp>>,
    receiver: Receiver<Packet<DbResp>>,
    buffer: Mutex<HashMap<PacketId, Packet<DbResp>>>,
}

impl DbSocket {
    /// Creates a new socket associated with an optional [pid](WeaverPid)
    pub(super) fn new(
        main_queue: Sender<MainQueueItem>,
        pid: impl Into<Option<WeaverPid>>,
    ) -> Self {
        let (resp_sender, receiver) = unbounded::<Packet<DbResp>>();
        Self {
            pid: pid.into(),
            main_queue,
            resp_sender,
            receiver,
            buffer: Default::default(),
        }
    }

    /// Communicate with the db
    pub fn send(&self, req: impl Into<DbReq>) -> Result<DbResp, Error> {
        error_span!("req-resp", pid = self.pid).in_scope(|| {
            let packet = Packet::new(req.into());
            trace!("packet={:#?}", packet);
            let &id = packet.id();
            self.main_queue.send((packet, self.resp_sender.clone()))?;
            trace!("sent packet to main queue");
            trace!("waiting for packet response...");
            let packet = self.get_resp(id)?;
            trace!("got response packet");
            Ok(packet.unwrap())
        })
    }

    fn get_resp(&self, id: PacketId) -> Result<Packet<DbResp>, Error> {
        loop {
            match self.receiver.try_recv() {
                Ok(recv) => {
                    trace!("received response packet with id {}", recv.id());
                    if recv.id() == &id {
                        trace!("ids matched, sending to owner");
                        break Ok(recv);
                    } else {
                        trace!("id mismatch, saving in buffer");
                        self.buffer.lock().insert(*recv.id(), recv);
                    }
                }
                Err(TryRecvError::Empty) => {
                    // trace!("no responses in channel, looking for packet with id {id}");
                    if let Some(packet) = self.buffer.lock().remove(&id) {
                        trace!("found matching packet in buffer");
                        break Ok(packet);
                    }
                    hint::spin_loop();
                }
                Err(err) => {
                    warn!("channel returned error: {}", err);;
                    break Err(Error::RecvError(RecvError));
                }
            }
        }
    }

    pub fn get_table(&self, (schema, table): &TableRef) -> Result<Arc<Table>, Error> {
        let schema = schema.clone();
        let table = table.clone();
        let tx = Tx::default();
        let DbResp::TxTable(_, table) = self.send(DbReqBody::on_core({
            let schema = schema.clone();
            let table = table.clone();
            move |core| {
                debug!("getting table {:?}", (&schema, &table));
                let table = core
                    .get_table(&schema, &table)
                    .ok_or(Error::NoTableFound(schema.to_string(), table.to_string()))?;
                Ok(DbResp::TxTable(tx, table))
            }
        }))? else {
            return Err(Error::NoTableFound(schema.to_string(), table.to_string()));
        };

        Ok(table)
    }

    pub fn start_tx(&self) -> Result<Tx, Error> {
        let DbResp::Tx(tx) = self.send(DbReqBody::StartTransaction)?
            else {
                return Err(Error::NoTransaction);
            };

        Ok(tx)
    }

    pub fn commit_tx(&self, tx: Tx) -> Result<(), Error> {
        let DbResp::Ok = self.send(DbReqBody::Commit(tx))?
            else {
                return Err(Error::NoTransaction);
            };

        Ok(())
    }

    pub fn rollback_tx(&self, tx: Tx) -> Result<(), Error> {
        let DbResp::Ok = self.send(DbReqBody::Rollback(tx))?
            else {
                return Err(Error::NoTransaction);
            };

        Ok(())
    }
}
