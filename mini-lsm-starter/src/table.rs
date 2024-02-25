#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

pub(crate) mod bloom;
mod builder;
mod iterator;

use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use anyhow::{Ok, Result};
pub use builder::SsTableBuilder;
use bytes::{Buf, BufMut};
pub use iterator::SsTableIterator;

use crate::block::Block;
use crate::key::{KeyBytes, KeySlice};
use crate::lsm_storage::BlockCache;

use self::bloom::Bloom;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlockMeta {
    /// Offset of this data block.
    pub offset: usize,
    /// The first key of the data block.
    pub first_key: KeyBytes,
    /// The last key of the data block.
    pub last_key: KeyBytes,
}

/*
-----------------------------------------------------------------------
|                           block meta                           | ... |
-----------------------------------------------------------------------
| offset(4B) | first_key_len (2B) | first_key (keylen) ts | last_key_len (2B) | last_key (keylen) ts | ... |
-----------------------------------------------------------------------
*/
impl BlockMeta {
    /// Encode block meta to a buffer.
    /// You may add extra fields to the buffer,
    /// in order to help keep track of `first_key` when decoding from the same buffer in the future.
    pub fn encode_block_metas(block_meta: &[BlockMeta], buf: &mut Vec<u8>, max_ts: u64) {
        let original_len = buf.len();
        let meta_len = block_meta.len();
        buf.put_u32(block_meta.len() as u32);
        for meta in block_meta {
            buf.put_u32(meta.offset as u32);
            buf.put_u16(meta.first_key.key_len() as u16);
            buf.put_slice(meta.first_key.key_ref());
            buf.put_u64(meta.first_key.ts());
            buf.put_u16(meta.last_key.key_len() as u16);
            buf.put_slice(meta.last_key.key_ref());
            buf.put_u64(meta.last_key.ts());
        }
        // put max ts
        buf.put_u64(max_ts);
        // add checksum
        let checksum = crc32fast::hash(&buf[original_len + 4..]);
        buf.put_u32(checksum);
    }

    /// Decode block meta from a buffer.
    pub fn decode_block_metas(mut buf: &[u8]) -> Result<(Vec<BlockMeta>, u64)> {
        // get meta data len
        let meta_len = buf.get_u32() as usize;
        // cal checksum
        let checksum = crc32fast::hash(&buf[..buf.remaining() - 4]);
        let mut block_meta = vec![];
        for _ in 0..meta_len {
            let offset = buf.get_u32() as usize;
            let first_key_len = buf.get_u16() as usize;
            let first_key =
                KeyBytes::from_bytes_with_ts(buf.copy_to_bytes(first_key_len), buf.get_u64());
            let last_key_len = buf.get_u16() as usize;
            let last_key =
                KeyBytes::from_bytes_with_ts(buf.copy_to_bytes(last_key_len), buf.get_u64());
            block_meta.push(BlockMeta {
                offset,
                first_key,
                last_key,
            });
        }
        let max_ts = buf.get_u64();
        // checksum
        if checksum != buf.get_u32() {
            return Err(anyhow::anyhow!("BlockMeta checksum mismatch"));
        }

        Ok((block_meta, max_ts))
    }
}

/// A file object.
pub struct FileObject(Option<File>, u64);

impl FileObject {
    pub fn read(&self, offset: u64, len: u64) -> Result<Vec<u8>> {
        use std::os::unix::fs::FileExt;
        let mut data = vec![0; len as usize];
        self.0
            .as_ref()
            .unwrap()
            .read_exact_at(&mut data[..], offset)?;
        Ok(data)
    }

    pub fn size(&self) -> u64 {
        self.1
    }

    /// Create a new file object (day 2) and write the file to the disk (day 4).
    pub fn create(path: &Path, data: Vec<u8>) -> Result<Self> {
        std::fs::write(path, &data)?;
        File::open(path)?.sync_all()?;
        Ok(FileObject(
            Some(File::options().read(true).write(false).open(path)?),
            data.len() as u64,
        ))
    }

    pub fn open(path: &Path) -> Result<Self> {
        let file = File::options().read(true).write(false).open(path)?;
        let size = file.metadata()?.len();
        Ok(FileObject(Some(file), size))
    }
}

/// An SSTable.
pub struct SsTable {
    id: usize,
    /// The actual storage unit of SsTable, the format is as above.
    pub(crate) file: FileObject,
    /// The meta blocks that hold info for data blocks.
    pub(crate) block_metas: Vec<BlockMeta>,
    /// The offset that indicates the start point of meta blocks in `file`.
    pub(crate) block_meta_offset: usize,
    first_key: KeyBytes,
    last_key: KeyBytes,
    block_cache: Option<Arc<BlockCache>>,
    pub(crate) bloom: Option<Bloom>,
    /// The maximum timestamp stored in this SST, implemented in week 3.
    max_ts: u64,
}

impl SsTable {
    #[cfg(test)]
    pub(crate) fn open_for_test(file: FileObject) -> Result<Self> {
        Self::open(0, None, file)
    }

    /// Open SSTable from a file.
    pub fn open(id: usize, block_cache: Option<Arc<BlockCache>>, file: FileObject) -> Result<Self> {
        let len = file.size();
        // decode bloom filter
        // u32 for extra info
        let raw_bloom_offset = file.read(len - 4, 4)?;
        let bloom_offset = (&raw_bloom_offset[..]).get_u32() as u64;
        let raw_bloom = file.read(bloom_offset, len - bloom_offset - 4 /* extra size */)?;
        let bloom = Bloom::decode(raw_bloom.as_slice())?;
        println!("decode bloom size: {}, k={}", bloom.filter.len(), bloom.k);

        // decode block meta
        // u32 for extra info
        let raw_block_meta_offset = file.read(bloom_offset - 4, 4)?;
        let block_meta_offset = (&raw_block_meta_offset[..]).get_u32() as u64;
        let raw_block_meta = file.read(block_meta_offset, bloom_offset - 4 - block_meta_offset)?;
        let (block_metas, max_ts) = BlockMeta::decode_block_metas(raw_block_meta.as_slice())?;

        // decode data blocks
        let raw_data = file.read(0, block_meta_offset)?;

        let sst_table = SsTable {
            id,
            file,
            block_meta_offset: block_meta_offset as usize,
            first_key: block_metas.first().unwrap().first_key.clone(),
            last_key: block_metas.last().unwrap().last_key.clone(),
            block_metas,
            block_cache,
            bloom: Some(bloom),
            max_ts,
        };
        Ok(sst_table)
    }

    /// Create a mock SST with only first key + last key metadata
    pub fn create_meta_only(
        id: usize,
        file_size: u64,
        first_key: KeyBytes,
        last_key: KeyBytes,
    ) -> Self {
        Self {
            file: FileObject(None, file_size),
            block_metas: vec![],
            block_meta_offset: 0,
            id,
            block_cache: None,
            first_key,
            last_key,
            bloom: None,
            max_ts: 0,
        }
    }

    /// Read a block from the disk.
    pub fn read_block(&self, block_idx: usize) -> Result<Arc<Block>> {
        let offset = self.block_metas[block_idx].offset as u64;
        let block_end = self
            .block_metas
            .get(block_idx + 1)
            .map_or(self.block_meta_offset, |x| x.offset) as u64;
        // get all data including checksum
        let block_len = block_end - offset - 4;
        let data_including_checksum = self.file.read(offset, block_end - offset)?;
        let checksum = (&data_including_checksum[block_len as usize..]).get_u32();
        let block_data = &data_including_checksum[..block_len as usize];
        // check checksum
        if checksum != crc32fast::hash(block_data) {
            return Err(anyhow::anyhow!("checksum mismatch"));
        }

        Ok(Arc::new(Block::decode(block_data)))
    }

    /// Read a block from disk, with block cache. (Day 4)
    pub fn read_block_cached(&self, block_idx: usize) -> Result<Arc<Block>> {
        if let Some(ref block_cache) = self.block_cache {
            let block = block_cache
                .try_get_with((self.id, block_idx), || self.read_block(block_idx))
                .map_err(|e| anyhow::anyhow!("block cache error: {:?}", e))?;
            Ok(block)
        } else {
            self.read_block(block_idx)
        }
    }

    /*
    --------------------------------------
    | block 1 | block 2 |   block meta   |
    --------------------------------------
    | a, b, c | e, f, g | 1: a/c, 2: e/g |
    --------------------------------------
    */
    /// Find the block that may contain `key`.
    /// make use of the `first_key` stored in `BlockMeta`.
    /// because for example, if we want to get `b`,
    /// we can directly we can know block 1 contains keys a <= keys < e.
    pub fn find_block_idx(&self, key: KeySlice) -> usize {
        self.block_metas
            .partition_point(|meta| meta.first_key.as_key_slice() <= key)
            .saturating_sub(1)
    }

    /// Get number of data blocks.
    pub fn num_of_blocks(&self) -> usize {
        self.block_metas.len()
    }

    pub fn first_key(&self) -> &KeyBytes {
        &self.first_key
    }

    pub fn last_key(&self) -> &KeyBytes {
        &self.last_key
    }

    pub fn table_size(&self) -> u64 {
        self.file.1
    }

    pub fn sst_id(&self) -> usize {
        self.id
    }

    pub fn max_ts(&self) -> u64 {
        self.max_ts
    }
}
