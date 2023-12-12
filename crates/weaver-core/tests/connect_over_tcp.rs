use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Duration;
use tracing::level_filters::LevelFilter;
use weaver_core::cnxn::tcp::{WeaverTcpListener, WeaverTcpStream};
use weaver_core::db::concurrency::WeaverDb;

#[test]
fn can_handshake() {
    tracing_subscriber::fmt()
        .with_max_level(LevelFilter::TRACE)
        .with_thread_ids(true)
        .init();
    let server = WeaverDb::default();

    let listener = WeaverTcpListener::bind("localhost:0", server.weak()).expect("couldnt create listener");
    let port = listener.local_addr().unwrap();

    let barrier = Arc::new(Barrier::new(2));
    let connect_thread = {
        let barrier = barrier.clone();
        thread::spawn(move || {
            barrier.wait();
            listener.accept().expect("could not connect")
        })
    };

    barrier.wait();
    let _ = WeaverTcpStream::connect_timeout(port, Duration::from_secs(10)).expect("failed to connect tcp stream");

    connect_thread.join().expect("listener thread panicked");
}