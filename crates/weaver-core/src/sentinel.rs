//! A sentinel is used to ensure that everything that has a reference to a sentinel is dropped before
//! The object containing the sentinel is dropped itself

#[derive(Debug)]
pub struct Sentinel {





}

impl Sentinel {

    pub fn new_ref(&self) -> SentinelRef {
        todo!()
    }
}

#[derive(Debug)]
pub struct SentinelRef {}
