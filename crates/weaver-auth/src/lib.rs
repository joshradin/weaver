use crate::access_control::users::UserTable;
use std::borrow::Cow;
use weaver_core::db::server::layers::packets::{DbReq, DbResp};
use weaver_core::db::server::layers::{from_fn, Service};
use weaver_core::db::server::WeaverDb;
use weaver_core::plugins::{Plugin, PluginError};

pub mod access_control;

#[derive(Default, Debug)]
pub struct AuthPlugin {}

impl Plugin for AuthPlugin {
    fn name(&self) -> Cow<str> {
        Cow::Borrowed("auth")
    }

    fn apply(&self, weaver_db: &mut WeaverDb) -> Result<(), PluginError> {
        let connection = weaver_db.connect();
        connection.send(DbReq::on_core(
            |core| -> Result<(), weaver_core::error::Error> {
                core.add_table(UserTable::default())?;

                Ok(())
            },
        ))?;

        weaver_db.wrap_req(from_fn(|req, next| next.process(req)));
        Ok(())
    }
}

pub fn apply(db: &mut WeaverDb) -> Result<(), PluginError> {
    db.apply(&AuthPlugin::default())
}
