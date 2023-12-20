use crate::cancellable_task::{CancelRecv, Cancelled};
use crate::db::server::layers::packets::{DbReq, DbResp, IntoDbResponse};
use crate::db::server::layers::service::Service;
use std::fmt::{Debug, Formatter};
use std::marker::PhantomData;

/// A layer
pub trait Layer: Send + Sync {
    fn process(
        &self,
        db_req: DbReq,
        cancel_recv: &CancelRecv,
        next: &Next,
    ) -> Result<DbResp, Cancelled>;
}

/// The next layer
pub struct Next {
    pub(crate) inner: Box<dyn Service>,
}

impl Service for Next {
    fn process(&self, db_req: DbReq, cancel_recv: &CancelRecv) -> Result<DbResp, Cancelled> {
        self.inner.process(db_req, cancel_recv)
    }
}

/// A layer created from a function
pub struct FromFn<F, R>
where
    F: Fn(DbReq, &Next, &CancelRecv) -> Result<R, Cancelled>,
    R: IntoDbResponse,
{
    func: F,
    _ret: PhantomData<R>,
}

impl<F, R> Debug for FromFn<F, R>
where
    F: Fn(DbReq, &Next, &CancelRecv) -> Result<R, Cancelled>,
    R: IntoDbResponse,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FromFn").finish_non_exhaustive()
    }
}

impl<F, R> Layer for FromFn<F, R>
where
    F: Fn(DbReq, &Next, &CancelRecv) -> Result<R, Cancelled>,
    F: Send + Sync,
    R: IntoDbResponse + Send + Sync,
{
    fn process(
        &self,
        db_req: DbReq,
        cancel_recv: &CancelRecv,
        next: &Next,
    ) -> Result<DbResp, Cancelled> {
        Ok((self.func)(db_req, next, cancel_recv)?.into_db_resp())
    }
}

/// Create a layer from a function
pub fn from_fn<F, R>(cb: F) -> FromFn<F, R>
where
    F: Fn(DbReq, &Next, &CancelRecv) -> Result<R, Cancelled>,
    R: IntoDbResponse,
{
    FromFn {
        func: cb,
        _ret: PhantomData,
    }
}
