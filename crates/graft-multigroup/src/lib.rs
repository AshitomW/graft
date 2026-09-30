//! Multi-group Raft coordination, shared coalesced transport, quiescing, and range metadata.
#![forbid(unsafe_code)]

pub mod coalesced;
pub mod group;
pub mod metadata;
pub mod router;

pub use coalesced::CoalescedBatch;
pub use group::{GroupId, GroupState, GroupStatus};
pub use metadata::{MetadataRouter, RangeDescriptor, RangeError};
pub use router::MultiGroupRouter;
