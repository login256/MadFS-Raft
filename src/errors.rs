//! ChiselStore errors.

use thiserror::Error;
use serde::{Deserialize, Serialize};
use std::{fmt, error};

#[derive(Debug,Serialize, Deserialize)]
pub struct SQLiteError {
    /// The error code.
    pub code: Option<isize>,
    /// The error message.
    pub message: Option<String>,
}
impl fmt::Display for SQLiteError {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        match (self.code, &self.message) {
            (Some(code), &Some(ref message)) => write!(formatter, "{} (code {})", message, code),
            (Some(code), _) => write!(formatter, "an SQLite error (code {})", code),
            (_, &Some(ref message)) => message.fmt(formatter),
            _ => write!(formatter, "an SQLite error"),
        }
    }
}

impl error::Error for SQLiteError {
    fn description(&self) -> &str {
        match self.message {
            Some(ref message) => message,
            _ => "an SQLite error",
        }
    }
}
/// Errors encountered in the store layer.
#[derive(Error, Debug)]
#[derive(Serialize, Deserialize)]
pub enum StoreError {
    /// SQLite error.
    #[error("SQLite error: {0}")]
    SQLiteError(SQLiteError),
    /// This node is not a leader and cannot therefore execute the command.
    #[error("Node is not a leader")]
    NotLeader,
}

impl From<sqlite::Error> for StoreError {
    fn from(e: sqlite::Error) -> Self {
        StoreError::SQLiteError(SQLiteError{
            code: e.code,
            message: e.message,
        })
    }
}