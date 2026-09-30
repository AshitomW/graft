//! Separation of large blob payloads from consensus logs via digest references.

use serde::{Deserialize, Serialize};
use thiserror::Error;
use crate::cas::{BlobDigest, ContentAddressedStore};

/// Errors encountered during blob payload operations.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum BlobError {
    /// Referenced blob chunk was not found in content-addressed store.
    #[error("blob chunk not found for digest: {0:?}")]
    ChunkNotFound(BlobDigest),
    /// Corrupt blob reference serialization.
    #[error("failed to deserialize blob reference: {0}")]
    Serialization(String),
}

/// Metadata reference stored inside the consensus log in lieu of the raw payload.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlobReference {
    /// Content digest of the out-of-band blob.
    pub digest: BlobDigest,
    /// Size of raw payload in bytes.
    pub byte_size: u64,
}

/// Inbound/outbound pipeline managing data and consensus separation.
pub struct PayloadManager {
    threshold_bytes: usize,
    store: ContentAddressedStore,
}

impl PayloadManager {
    /// Creates a payload manager with the specified size threshold.
    pub fn new(threshold_bytes: usize) -> Self {
        Self {
            threshold_bytes: threshold_bytes.max(1024),
            store: ContentAddressedStore::new(),
        }
    }

    /// Processes an inbound payload: stores out-of-band if exceeding threshold, returning log bytes.
    pub fn prepare_proposal(&mut self, payload: Vec<u8>) -> Vec<u8> {
        if payload.len() > self.threshold_bytes {
            let digest = self.store.put(&payload);
            let reference = BlobReference {
                digest,
                byte_size: payload.len() as u64,
            };
            let mut encoded = vec![0x01u8];
            let serialized = serde_json::to_vec(&reference).unwrap_or_default();
            encoded.extend_from_slice(&serialized);
            encoded
        } else {
            let mut encoded = Vec::with_capacity(1 + payload.len());
            encoded.push(0x00u8);
            encoded.extend_from_slice(&payload);
            encoded
        }
    }

    /// Resolves an entry from the consensus log, fetching from chunk store if referenced.
    pub fn resolve_entry(&self, entry_data: &[u8]) -> Result<Vec<u8>, BlobError> {
        if entry_data.is_empty() {
            return Ok(Vec::new());
        }

        match entry_data[0] {
            0x00 => Ok(entry_data[1..].to_vec()),
            0x01 => {
                let reference: BlobReference = serde_json::from_slice(&entry_data[1..])
                    .map_err(|e| BlobError::Serialization(e.to_string()))?;

                self.store
                    .get(&reference.digest)
                    .map(|slice| slice.to_vec())
                    .ok_or(BlobError::ChunkNotFound(reference.digest))
            }
            _ => Ok(entry_data.to_vec()),
        }
    }

    /// Returns a reference to the underlying chunk store.
    pub fn store(&self) -> &ContentAddressedStore {
        &self.store
    }

    /// Returns a mutable reference to the underlying chunk store.
    pub fn store_mut(&mut self) -> &mut ContentAddressedStore {
        &mut self.store
    }
}
