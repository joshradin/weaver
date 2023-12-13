use std::thread;
use weaver_core::db::server::layers::packets::{DbReqBody, DbResp};
use weaver_core::db::server::WeaverDb;

#[test]
fn connect() {
    let mut shard = WeaverDb::default();

    let handle1 = {
        let socket = shard.connect();
        thread::spawn(move || {
            let pong = socket
                .send(DbReqBody::Ping)
                .expect("could not get response");
            assert!(matches!(pong, DbResp::Pong));
        })
    };
    let handle2 = {
        let socket = shard.connect();
        thread::spawn(move || {
            let pong = socket
                .send(DbReqBody::Ping)
                .expect("could not get response");
            assert!(matches!(pong, DbResp::Pong));
        })
    };
    handle1.join().unwrap();
    handle2.join().unwrap();
}
