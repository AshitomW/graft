//! Shared sequential append-only Write-Ahead Log for multi-group multiplexing.

use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use thiserror::Error;
use graft_core::{EntryType, Index, LogEntry};

/// WAL storage and recovery errors.
#[derive(Debug, Error)]
pub enum WalError {
    /// Standard I/O failure.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    /// Corrupt record detected.
    #[error("corrupt WAL entry at offset {offset}")]
    CorruptRecord {
        /// Byte offset in WAL file.
        offset: u64,
    },
}

/// Disk-backed shared Write-Ahead Log storing interleaved entries for multiple groups.
pub struct SharedWal {
    path: PathBuf,
    file: File,
    index: BTreeMap<u64, Vec<LogEntry>>,
}

impl SharedWal {
    /// Opens or creates a shared WAL file at the given filesystem path.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, WalError> {
        let path_buf = path.as_ref().to_path_buf();
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path_buf)?;

        let mut index: BTreeMap<u64, Vec<LogEntry>> = BTreeMap::new();
        let mut offset = 0u64;
        let file_len = file.metadata()?.len();

        file.seek(SeekFrom::Start(0))?;

        while offset < file_len {
            let mut header = [0u8; 25];
            match file.read_exact(&mut header) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                    file.set_len(offset)?;
                    break;
                }
                Err(e) => return Err(WalError::Io(e)),
            }

            let group_id = u64::from_be_bytes(header[0..8].try_into().unwrap());
            let term = u64::from_be_bytes(header[8..16].try_into().unwrap());
            let log_index = u64::from_be_bytes(header[16..24].try_into().unwrap());
            let entry_type_byte = header[24];

            let mut len_buf = [0u8; 4];
            if let Err(_) = file.read_exact(&mut len_buf) {
                file.set_len(offset)?;
                break;
            }
            let payload_len = u32::from_be_bytes(len_buf) as usize;

            let mut data = vec![0u8; payload_len];
            if let Err(_) = file.read_exact(&mut data) {
                file.set_len(offset)?;
                break;
            }

            let entry_type = match entry_type_byte {
                0 => EntryType::Normal,
                1 => EntryType::Configuration,
                _ => EntryType::NoOp,
            };

            let entry = LogEntry {
                term,
                index: log_index,
                entry_type,
                data,
            };

            index.entry(group_id).or_default().push(entry);
            offset += 25 + 4 + (payload_len as u64);
        }

        file.seek(SeekFrom::End(0))?;

        Ok(Self {
            path: path_buf,
            file,
            index,
        })
    }

    /// Appends a batch of entries belonging to the specified group to the WAL.
    pub fn append(&mut self, group_id: u64, entries: &[LogEntry]) -> Result<(), WalError> {
        for entry in entries {
            let mut buf = Vec::with_capacity(29 + entry.data.len());
            buf.extend_from_slice(&group_id.to_be_bytes());
            buf.extend_from_slice(&entry.term.to_be_bytes());
            buf.extend_from_slice(&entry.index.to_be_bytes());

            let type_byte = match entry.entry_type {
                EntryType::Normal => 0u8,
                EntryType::Configuration => 1u8,
                EntryType::NoOp => 2u8,
            };
            buf.push(type_byte);

            let len = entry.data.len() as u32;
            buf.extend_from_slice(&len.to_be_bytes());
            buf.extend_from_slice(&entry.data);

            self.file.write_all(&buf)?;
            self.index
                .entry(group_id)
                .or_default()
                .push(entry.clone());
        }
        Ok(())
    }

    /// Forces physical synchronization of all pending writes to disk (`fdatasync`).
    pub fn sync(&mut self) -> Result<(), WalError> {
        self.file.sync_all()?;
        Ok(())
    }

    /// Retrieves all entries recorded for the specified group.
    pub fn read_group(&self, group_id: u64) -> Vec<LogEntry> {
        self.index.get(&group_id).cloned().unwrap_or_default()
    }

    /// Truncates entries for a group strictly after the specified index.
    pub fn truncate_group_after(&mut self, group_id: u64, index: Index) {
        if let Some(entries) = self.index.get_mut(&group_id) {
            entries.retain(|e| e.index <= index);
        }
    }

    /// Returns the filesystem path to the WAL file.
    pub fn path(&self) -> &Path {
        &self.path
    }
}
