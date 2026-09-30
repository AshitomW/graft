//! High-performance linearizable read paths: ReadIndex, Leases, and Closed Timestamps.
#![forbid(unsafe_code)]

pub mod closed_timestamp;
pub mod lease;
pub mod read_index;

pub use closed_timestamp::{ClosedTimestampManager, TimestampError};
pub use lease::{LeaseConfig, LeaseManager};
pub use read_index::{PendingRead, ReadIndexManager};
