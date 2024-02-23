#![allow(dead_code)] // REMOVE THIS LINE after fully implementing this functionality

use std::fs::{File, OpenOptions};
use std::hash::Hasher;
use std::io::{BufWriter, Read, Write};
use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result};
use bytes::{Buf, BufMut, Bytes};
use crossbeam_skiplist::SkipMap;
use parking_lot::Mutex;

// | key_len | key | value_len | value | checksum |
#[derive(Debug)]
pub struct Wal {
    file: Arc<Mutex<BufWriter<File>>>,
}

impl Wal {
    pub fn create(path: impl AsRef<Path>) -> Result<Self> {
        Ok(Self {
            file: Arc::new(Mutex::new(BufWriter::new(
                OpenOptions::new()
                    .read(true)
                    .create_new(true)
                    .write(true)
                    .open(path)
                    .context("[WAL.create] failed to create WAL")?,
            ))),
        })
    }

    pub fn recover(path: impl AsRef<Path>, skiplist: &SkipMap<Bytes, Bytes>) -> Result<Self> {
        let path = path.as_ref();
        let mut file = OpenOptions::new()
            .read(true)
            .append(true)
            .open(path)
            .context("[WAL.recover] failed to recover from WAL")?;
        let mut buf = Vec::new();
        file.read_to_end(&mut buf)?;
        // read buf to insert into skiplist
        let mut buf = buf.as_slice();
        while !buf.is_empty() {
            let mut hasher = crc32fast::Hasher::new();
            let key_len = buf.get_u16() as usize;
            hasher.write_u16(key_len as u16);
            let key = Bytes::copy_from_slice(&buf[..key_len]);
            hasher.write(&key);
            buf.advance(key_len);
            let value_len = buf.get_u16() as usize;
            hasher.write_u16(value_len as u16);
            let value = Bytes::copy_from_slice(&buf[..value_len]);
            hasher.write(&value);
            buf.advance(value_len);
            let checksum = buf.get_u32();
            if checksum != hasher.finalize() {
                return Err(anyhow::anyhow!("WAL checksum mismatch"));
            }
            skiplist.insert(key, value);
        }
        Ok(Self {
            file: Arc::new(Mutex::new(BufWriter::new(file))),
        })
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let mut file = self.file.lock();
        let mut buf: Vec<u8> = Vec::with_capacity(
            key.len() + value.len() + std::mem::size_of::<u16>() + std::mem::size_of::<u32>(),
        );
        //  use a crc32fast::Hasher to compute the checksum incrementally on each field.
        let mut hasher = crc32fast::Hasher::new();
        buf.put_u16(key.len() as u16);
        hasher.write_u16(key.len() as u16);
        buf.put_slice(key);
        hasher.write(key);
        buf.put_u16(value.len() as u16);
        hasher.write_u16(value.len() as u16);
        buf.put_slice(value);
        hasher.write(value);
        buf.put_u32(hasher.finalize());
        file.write_all(&buf)?;
        Ok(())
    }

    pub fn sync(&self) -> Result<()> {
        let mut file = self.file.lock();
        file.flush()?;
        file.get_mut().sync_all()?;
        Ok(())
    }
}
