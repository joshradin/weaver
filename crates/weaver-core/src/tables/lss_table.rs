//! # Log-Structured Storage Tables
//!
//! Immutable storage structures do not allow for modifications to existing files; tables are written
//! once and are never modified again. Instead, new records are appended to a new file and, to find the final value
//! or conclude its absence, records have to be reconstructed from multiple files.
