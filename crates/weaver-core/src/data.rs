//! The data that is actually stored

use std::borrow::Borrow;
use std::hash::{Hash, Hasher};
use std::ops::{Deref, DerefMut, Index, IndexMut, RangeBounds};
use std::slice::SliceIndex;
use values::Literal;

pub mod charset;
pub mod row;
pub mod types;
pub mod values;

pub mod serde;
