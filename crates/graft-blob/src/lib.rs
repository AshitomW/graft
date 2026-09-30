//! Data and consensus separation via out-of-band content-addressed chunk store.
#![forbid(unsafe_code)]

pub mod cas;
pub mod payload;

pub use cas::{BlobDigest, ContentAddressedStore};
pub use payload::{BlobError, BlobReference, PayloadManager};
