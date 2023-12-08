use distro_db_core::db::{DbReq, DbResp, DistroDb, DistroDbServer};
use std::thread;

#[test]
fn connect() {
    let mut shard = DistroDbServer::new(DistroDb::new().unwrap()).expect("couldn't create daemon");

    let handle1 = {
        let socket = shard.connect();
        thread::spawn(move || {
            let pong = socket.send(DbReq::Ping).expect("could not get response");
            assert!(matches!(pong, DbResp::Pong));
        })
    };
    let handle2 = {
        let socket = shard.connect();
        thread::spawn(move || {
            let pong = socket.send(DbReq::Ping).expect("could not get response");
            assert!(matches!(pong, DbResp::Pong));
        })
    };
    handle1.join().unwrap();
    handle2.join().unwrap();
}
