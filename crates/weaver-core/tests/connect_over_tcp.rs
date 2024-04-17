use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Duration;
use tempfile::TempDir;
use tracing::level_filters::LevelFilter;
use weaver_core::access_control::auth::LoginContext;
use weaver_core::cnxn::stream::WeaverStream;
use weaver_core::cnxn::tcp::WeaverTcpListener;
use weaver_core::cnxn::WeaverStreamListener;
use weaver_core::db::server::WeaverDb;

#[test]
fn can_handshake() {
    tracing_subscriber::fmt()
        .with_max_level(LevelFilter::TRACE)
        .with_thread_ids(true)
        .init();
    let dir = TempDir::new().unwrap();
    let server = WeaverDb::at_path(&dir).unwrap();
    server.lifecycle_service().startup().expect("could not startup");

    let listener =
        WeaverTcpListener::bind("localhost:0", server.weak()).expect("couldnt create listener");
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
    let _ = WeaverStream::connect_timeout(port, Duration::from_secs(10), LoginContext::new())
        .expect("failed to connect tcp stream");

    connect_thread.join().expect("listener thread panicked");
}
