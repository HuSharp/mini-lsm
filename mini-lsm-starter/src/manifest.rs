use std::fs::OpenOptions;
use std::io::Read;
use std::path::Path;
use std::sync::Arc;
use std::{fs::File, io::Write};

use anyhow::{Context, Result};
use bytes::{Buf, BufMut};
use parking_lot::{Mutex, MutexGuard};
use serde::{Deserialize, Serialize};

use crate::compact::CompactionTask;

// | len | JSON record | checksum | len | JSON record | checksum | len | JSON record | checksum |
pub struct Manifest {
    file: Arc<Mutex<File>>,
}

#[derive(Serialize, Deserialize)]
pub enum ManifestRecord {
    Flush(usize),
    NewMemtable(usize),
    Compaction(CompactionTask, Vec<usize>),
}

impl Manifest {
    pub fn create(path: impl AsRef<Path>) -> Result<Self> {
        Ok(Self {
            file: Arc::new(Mutex::new(
                OpenOptions::new()
                    .read(true)
                    .create_new(true)
                    .write(true)
                    .open(path)
                    .context("[manifest.create] failed to create manifest")?,
            )),
        })
    }

    pub fn recover(path: impl AsRef<Path>) -> Result<(Self, Vec<ManifestRecord>)> {
        let mut file = OpenOptions::new()
            .read(true)
            .append(true)
            .open(path)
            .context("failed to recover manifest")?;
        let mut buf = Vec::new();
        file.read_to_end(&mut buf)?;
        // need to check all records checksum
        let mut stream = buf.as_slice();
        let mut records = Vec::new();
        while stream.has_remaining() {
            let len = stream.get_u64() as usize;
            let record = stream.copy_to_bytes(len);
            let checksum = stream.get_u32();
            if crc32fast::hash(&record) != checksum {
                return Err(anyhow::anyhow!("Manifest record checksum mismatch"));
            }
            records.push(serde_json::from_slice(&record)?);
        }
        Ok((
            Self {
                file: Arc::new(Mutex::new(file)),
            },
            records,
        ))
    }

    pub fn add_record(
        &self,
        _state_lock_observer: &MutexGuard<()>,
        record: ManifestRecord,
    ) -> Result<()> {
        self.add_record_when_init(record)
    }

    pub fn add_record_when_init(&self, record: ManifestRecord) -> Result<()> {
        let mut file = self.file.lock();
        let mut buf = serde_json::to_vec(&record)?;
        file.write_all(&(buf.len() as u64).to_be_bytes())?;
        // add checksum
        let checksum = crc32fast::hash(&buf);
        buf.put_u32(checksum);
        file.write_all(&buf)?;
        file.sync_all()?;
        Ok(())
    }
}
