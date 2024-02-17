//! Common types and traits
pub mod batched;
pub mod consistent_hasher;
pub mod dual_result;
pub mod hex_dump;
mod iter_ext;
pub mod linked_list;
pub mod opaque;
pub mod pretty_bytes;
pub mod read_only;
pub mod stream_support;
pub mod track_dirty;
pub mod window;

pub use iter_ext::IteratorExt;
