//! In-memory Raft log implementation with index and term tracking.

use thiserror::Error;
use crate::types::{Index, LogEntry, Term};

/// Errors encountered during log operations.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum LogError {
    /// Attempted to append non-contiguous entries.
    #[error("non-contiguous log append: expected index {expected}, got {actual}")]
    NonContiguous {
        /// Expected entry index.
        expected: Index,
        /// Actual entry index provided.
        actual: Index,
    },
}

/// Raft sequential log.
#[derive(Debug, Clone)]
pub struct RaftLog {
    entries: Vec<LogEntry>,
}

impl RaftLog {
    /// Creates a new empty Raft log initialized with a sentinel at index 0.
    pub fn new() -> Self {
        Self {
            entries: vec![LogEntry {
                term: 0,
                index: 0,
                entry_type: crate::types::EntryType::NoOp,
                data: Vec::new(),
            }],
        }
    }

    /// Returns the highest index present in the log.
    pub fn last_index(&self) -> Index {
        self.entries.last().map(|e| e.index).unwrap_or(0)
    }

    /// Returns the term of the highest index present in the log.
    pub fn last_term(&self) -> Term {
        self.entries.last().map(|e| e.term).unwrap_or(0)
    }

    /// Returns the term of the entry at the specified index, if present.
    pub fn term_at(&self, index: Index) -> Option<Term> {
        let idx = index as usize;
        self.entries.get(idx).map(|e| e.term)
    }

    /// Returns a reference to the entry at the specified index, if present.
    pub fn entry_at(&self, index: Index) -> Option<&LogEntry> {
        let idx = index as usize;
        self.entries.get(idx)
    }

    /// Appends entries to the log after validating contiguous indexing.
    pub fn append(&mut self, new_entries: Vec<LogEntry>) -> Result<(), LogError> {
        let mut expected = self.last_index() + 1;
        for entry in &new_entries {
            if entry.index != expected {
                return Err(LogError::NonContiguous {
                    expected,
                    actual: entry.index,
                });
            }
            expected += 1;
        }
        self.entries.extend(new_entries);
        Ok(())
    }

    /// Truncates all entries with index strictly greater than the given index.
    pub fn truncate_after(&mut self, index: Index) {
        let idx = (index + 1) as usize;
        if idx < self.entries.len() {
            self.entries.truncate(idx);
        }
    }

    /// Returns a slice of entries starting from `from_index` up to `max_entries`.
    pub fn slice(&self, from_index: Index, max_entries: usize) -> Vec<LogEntry> {
        let start = from_index as usize;
        if start >= self.entries.len() {
            return Vec::new();
        }
        let end = (start + max_entries).min(self.entries.len());
        self.entries[start..end].to_vec()
    }
}

impl Default for RaftLog {
    fn default() -> Self {
        Self::new()
    }
}
