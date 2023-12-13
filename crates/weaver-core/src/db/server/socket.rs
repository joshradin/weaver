use crate::db::server::layers::packets::{DbReq, DbReqBody, DbResp};
use crate::dynamic_table::Table;
use crate::error::Error;
use crate::tables::TableRef;
use crate::tx::Tx;
use crossbeam::channel::{Receiver, Sender};
use std::sync::Arc;
use tracing::error_span;

#[derive(Debug)]
pub struct DbSocket {
    main_queue: Sender<(DbReq, Sender<DbResp>)>,
    resp_sender: Sender<DbResp>,
    receiver: Receiver<DbResp>,
}

impl DbSocket {
    pub(super) fn new(
        main_queue: Sender<(DbReq, Sender<DbResp>)>,
        resp_sender: Sender<DbResp>,
        receiver: Receiver<DbResp>,
    ) -> Self {
        Self {
            main_queue,
            resp_sender,
            receiver,
        }
    }

    /// Communicate with the db
    pub fn send(&self, req: impl Into<DbReq>) -> Result<DbResp, Error> {
        error_span!("req-resp").in_scope(|| {
            self.main_queue
                .send((req.into(), self.resp_sender.clone()))?;
            Ok(self.receiver.recv()?)
        })
    }

    pub fn get_table(&self, (schema, table): &TableRef) -> Result<Arc<Table>, Error> {
        let schema = schema.clone();
        let table = table.clone();
        let tx = Tx::default();
        let DbResp::TxTable(_, table) = self.send(DbReqBody::on_core({
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
