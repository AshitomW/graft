//! Shared storage engine and Write-Ahead Log for multi-group multiplexing.
#![forbid(unsafe_code)]

pub mod wal;

pub use wal::{SharedWal, WalError};
