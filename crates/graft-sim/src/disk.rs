//! Deterministic simulated storage with crash and torn write simulation.

use std::collections::BTreeMap;
use thiserror::Error;

/// Storage errors that can occur during disk operations.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum DiskError {
    /// Injected sync failure.
    #[error("simulated disk sync failure")]
    SyncFailed,
    /// Corrupt data read from disk.
    #[error("simulated disk data corruption")]
    DataCorrupted,
}

/// Simulated persistent disk device.
#[derive(Debug, Clone, Default)]
pub struct SimDisk {
    persisted: BTreeMap<Vec<u8>, Vec<u8>>,
    unflushed: BTreeMap<Vec<u8>, Vec<u8>>,
    fail_next_sync: bool,
    torn_write_on_crash: bool,
}

impl SimDisk {
    /// Creates a new empty simulated disk.
    pub fn new() -> Self {
        Self::default()
    }

    /// Writes a key-value entry to the disk cache.
    pub fn write(&mut self, key: Vec<u8>, value: Vec<u8>) {
        self.unflushed.insert(key, value);
    }

    /// Reads a key-value entry from disk, checking unflushed cache then persisted storage.
    pub fn read(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.unflushed
            .get(key)
            .or_else(|| self.persisted.get(key))
            .cloned()
    }

    /// Reads an entry strictly from persisted storage.
    pub fn read_persisted(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.persisted.get(key).cloned()
    }

    /// Synchronizes unflushed writes to persistent storage.
    pub fn sync(&mut self) -> Result<(), DiskError> {
        if self.fail_next_sync {
            self.fail_next_sync = false;
            return Err(DiskError::SyncFailed);
        }

        for (k, v) in self.unflushed.split_off(&Vec::new()) {
            self.persisted.insert(k, v);
        }
        Ok(())
    }

    /// Configures the disk to fail the next sync invocation.
    pub fn set_fail_next_sync(&mut self, fail: bool) {
        self.fail_next_sync = fail;
    }

    /// Configures whether an unflushed entry is torn on crash.
    pub fn set_torn_write_on_crash(&mut self, enabled: bool) {
        self.torn_write_on_crash = enabled;
    }

    /// Simulates a crash and restart, discarding un-flushed writes.
    pub fn crash_and_restart(&mut self) {
        if self.torn_write_on_crash {
            if let Some((k, mut v)) = self.unflushed.pop_first() {
                v.truncate(v.len() / 2);
                self.persisted.insert(k, v);
            }
        }
        self.unflushed.clear();
        self.fail_next_sync = false;
    }

    /// Returns the total number of persisted entries.
    pub fn persisted_count(&self) -> usize {
        self.persisted.len()
    }
}
