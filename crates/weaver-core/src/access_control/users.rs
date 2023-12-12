//! Users are what connect to the database

/// A user struct is useful for access control
#[derive(Debug)]
pub struct User {
    name: String,
    host: String,
}
