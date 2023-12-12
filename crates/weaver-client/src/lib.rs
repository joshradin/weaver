use weaver_core::cnxn::tcp::WeaverTcpStream;

/// A client to attach to a weaver instance
#[derive(Debug)]
pub struct WeaverClient {
    stream: WeaverTcpStream,
    pid: usize,
}
