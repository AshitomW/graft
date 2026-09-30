//! Core consensus protocol types and deterministic state machine.
#![forbid(unsafe_code)]

pub mod config;
pub mod engine;
pub mod log;
pub mod message;
pub mod pipeline;
pub mod quorum;
pub mod types;

pub use config::{ConfigError, RaftConfig};
pub use engine::RaftEngine;
pub use log::{LogError, RaftLog};
pub use message::{Message, MessagePayload};
pub use pipeline::CommitApplyPipeline;
pub use quorum::{QuorumError, QuorumModel, ReplicaRole};
pub use types::{EntryType, HardState, Index, LogEntry, NodeId, Role, Term};
