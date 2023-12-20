use std::marker::PhantomData;
use crate::cancellable_task::{CancelRecv, Cancelled};
use crate::db::server::layers::packets::{DbReq, DbResp, IntoDbResponse};

/// A service
pub trait Service: Send + Sync {
    fn process(&self, db_req: DbReq, cancel_recv: &CancelRecv) -> Result<DbResp, Cancelled>;
}

impl<F, R: IntoDbResponse + 'static> Service for F
where
    F: Send + Sync,
    for<'a> F: Fn(DbReq, &'a CancelRecv) -> Result<R, Cancelled>,
{
    fn process(&self, db_req: DbReq, cancel_recv: &CancelRecv) -> Result<DbResp, Cancelled> {
        let ret = (self)(db_req, cancel_recv)?;
        Ok(ret.into_db_resp())
    }
}


pub struct FromFnService<F, R>
    where F: Send + Sync,
          F: Fn(DbReq, &CancelRecv) -> Result<R, Cancelled>,
    R : Send + Sync + 'static,
    R : IntoDbResponse
{
    func: F,
    _ret: PhantomData<R>
}

impl<F, R> Service for FromFnService<F, R> where F: Send + Sync,
                                           F: Fn(DbReq, &CancelRecv) -> Result<R, Cancelled>,
                                           R: Send + Sync + 'static,
                                           R: IntoDbResponse {
    fn process(&self, db_req: DbReq, cancel_recv: &CancelRecv) -> Result<DbResp, Cancelled> {
        (self.func)(db_req, cancel_recv).map(|ret| ret.into_db_resp())
    }
}

impl<F, R> FromFnService<F, R> where F: Send + Sync,
                                     F: Fn(DbReq, &CancelRecv) -> Result<R, Cancelled>,
                                     R: Send + Sync + 'static,
                                     R: IntoDbResponse {
    pub fn new(func: F) -> Self {
        Self { func, _ret: PhantomData }
    }
}

