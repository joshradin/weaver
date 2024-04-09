




pub use layer_impl::*;
use service::Service;

use crate::cancellable_task::{CancelRecv, Cancelled};
use crate::db::server::layers::packets::{DbReq, DbResp};
use crate::db::server::layers::service::FromFnService;

mod layer_impl;
pub mod packets;
pub mod service;

/// A stack of layers.
pub struct Layers {
    /// The base service
    layer_count: usize,
    inner: Option<Box<dyn Service>>,
}

impl Layers {
    /// Creates a new, empty layer stack
    pub fn new<S: Service + 'static>(service: S) -> Self {
        let func = FromFnService::new(move |req, cancel| service.process(req, cancel));
        Self {
            layer_count: 1,
            inner: Some(Box::new(func)),
        }
    }

    /// Wraps the layers
    pub fn wrap<L: Layer + 'static>(&mut self, layer: L) {
        let inner = self.inner.take().expect("should always have inner server");
        let next = Next { inner };
        self.layer_count += 1;
        let new_service =
            FromFnService::new(move |db_req: DbReq, cancel| layer.process(db_req, cancel, &next));
        self.inner = Some(Box::new(new_service))
    }
}

impl Service for Layers {
    fn process(&self, db_req: DbReq, cancel_recv: &CancelRecv) -> Result<DbResp, Cancelled> {
        self.inner
            .as_ref()
            .expect("should always have base service")
            .process(db_req, cancel_recv)
    }
}

#[cfg(test)]
mod tests {
    use service::Service;

    use crate::cancellable_task::CancellableTask;

    use super::packets::*;
    use super::*;

    #[test]
    fn layered_processing() {
        let mut layers = Layers::new(FromFnService::new(|_req, _cancel| Ok(DbResp::Pong)));
        CancellableTask::spawn(move |cancel| {
            assert!(matches!(
                layers.process(DbReq::from(DbReqBody::Ping), cancel)?,
                DbResp::Pong
            ));
            layers.wrap(from_fn(|req, next, cancel| {
                let resp = next.process(req, cancel);
                if matches!(resp, Ok(DbResp::Pong)) {
                    Ok(DbResp::Ok)
                } else {
                    resp
                }
            }));
            assert!(matches!(
                layers.process(DbReq::from(DbReqBody::Ping), cancel)?,
                DbResp::Ok
            ));
            Ok(())
        })
        .join()
        .unwrap();
    }
}
