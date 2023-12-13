use std::collections::VecDeque;
use std::fmt::{Debug, Formatter};
use std::marker::PhantomData;
use serde::Deserialize;
use tracing::{error_span, info_span, span};
use crate::db::server::layers::packets::{DbReq, DbResp, IntoDbResponse};
use crate::error::Error;
use crate::rows::OwnedRows;

pub mod packets;

/// A service
pub trait Service : Send + Sync {

    fn process(&self, db_req: DbReq) -> DbResp;
}

/// A layer
pub trait Layer : Send + Sync {
    fn process(&self, db_req: DbReq, next: &Next) -> DbResp;
}

/// The next layer
pub struct Next {
    inner: Box<dyn Service>
}

impl Service for Next {
    fn process(&self, db_req: DbReq) -> DbResp {
        self.inner.process(db_req)
    }
}


/// A layer created from a function
pub struct FromFn<F, R>
    where F : Fn(DbReq, &Next) -> R,
        R : IntoDbResponse
{
    func: F,
    _ret: PhantomData<R>
}

impl<F, R> Debug for FromFn<F, R> where F: Fn(DbReq,&Next) -> R,
                                    R: IntoDbResponse {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FromFn").finish_non_exhaustive()
    }
}


impl<F, R> Layer for FromFn<F, R> where
    F: Fn(DbReq,&Next) -> R,
    F: Send + Sync,
    R: IntoDbResponse + Send + Sync {
    fn process(&self, db_req: DbReq, next: &Next) -> DbResp {
        (self.func)(db_req, next).into_db_resp()
    }
}


/// Create a layer from a function
pub fn from_fn<F, R>(cb: F) -> FromFn<F, R>
    where F : Fn(DbReq, &Next) -> R,
                             R : IntoDbResponse {
    FromFn {
        func: cb,
        _ret: PhantomData,
    }
}

/// A stack of layers.
pub struct Layers {
    /// The base service
    layer_count: usize,
    inner: Option<Box<dyn Service>>,
}

impl Layers {

    /// Creates a new, empty layer stack
    pub fn new<S: Service + 'static>(service: S) -> Self {
        Self {
            layer_count: 1,
            inner: Some(Box::new(move |req| {
                service.process(req)
            })),
        }
    }

    /// Wraps the layers
    pub fn wrap<L : Layer + 'static>(&mut self, layer: L) {
        let inner = self.inner.take().expect("should always have inner server");
        let next = Next { inner };
        self.layer_count += 1;
        let new_service = move |db_req: DbReq| {
            layer.process(db_req, &next)
        };
        self.inner = Some(Box::new(new_service))
    }
}

impl Service for Layers {
    fn process(&self, db_req: DbReq) -> DbResp {
        error_span!("request-handling").in_scope(|| {
            self.inner.as_ref().expect("should always have base service").process(db_req)
        })
    }
}

impl<F : Fn(DbReq) -> R + 'static, R : IntoDbResponse + 'static> Service for F
    where F : Send + Sync
{
    fn process(&self, db_req: DbReq) -> DbResp {
        (self)(db_req).into_db_resp()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::packets::*;

    #[test]
    fn layered_processing() {
        let mut layers = Layers::new(|req| {
            DbResp::Pong
        });
        assert!(matches!(layers.process(DbReq::from(DbReqBody::Ping)), DbResp::Pong));
        layers.wrap(from_fn(|req, next| {
            let resp = next.process(req);
            if matches!(resp, DbResp::Pong) {
                DbResp::Ok
            } else {
                resp
            }
        }));
        assert!(matches!(layers.process(DbReq::from(DbReqBody::Ping)), DbResp::Ok));
    }
}
