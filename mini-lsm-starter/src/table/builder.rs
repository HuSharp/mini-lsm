use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use bytes::BufMut;

use super::{bloom::Bloom, BlockMeta, FileObject, SsTable};
use crate::{
    block::BlockBuilder,
    key::{KeySlice, KeyVec},
    lsm_storage::BlockCache,
};

/// Builds an SSTable from key-value pairs.
pub struct SsTableBuilder {
    block_builder: BlockBuilder,
    first_key: KeyVec,
    last_key: KeyVec,
    data: Vec<u8>,
    pub(crate) metas: Vec<BlockMeta>,
    block_size: usize,
    key_hashes: Vec<u32>,
}

/*
-----------------------------------------------------------------------------------------------------
|         Block Section         |                            Meta Section                           |
-----------------------------------------------------------------------------------------------------
| data block | ... | data block | metadata | meta block offset | bloom filter  | bloom filter offset  |
|                               |  varlen  |         u32       |    varlen    |        u32          |
-----------------------------------------------------------------------------------------------------
*/
impl SsTableBuilder {
    /// Create a builder based on target block size.
    pub fn new(block_size: usize) -> Self {
        Self {
            block_builder: BlockBuilder::new(block_size),
            first_key: KeyVec::new(),
            last_key: KeyVec::new(),
            data: Vec::new(),
            metas: Vec::new(),
            block_size,
            key_hashes: Vec::new(),
        }
    }

    /// Adds a key-value pair to SSTable.
    ///
    /// we should split a new block when the current block is full.
    pub fn add(&mut self, key: KeySlice, value: &[u8]) {
        // if the first time add to this block, set the first key
        if self.first_key.is_empty() {
            self.first_key.set_from_slice(key);
        }

        // add the key hash to the bloom filter
        self.key_hashes.push(farmhash::fingerprint32(key.raw_ref()));
        // block builder returns false when the block is full.
        if self.block_builder.add(key, value) {
            self.last_key.set_from_slice(key);
            return;
        }

        // if the block is full, build the block and add the block to the data.
        self.finalize_block_to_sst();

        // add (key, value) to next block
        assert!(self.block_builder.add(key, value));
        self.first_key.set_from_slice(key);
        self.last_key.set_from_slice(key);
    }

    // finalize the block and add it to the data.
    fn finalize_block_to_sst(&mut self) {
        let block = std::mem::replace(&mut self.block_builder, BlockBuilder::new(self.block_size));
        self.metas.push(BlockMeta {
            offset: self.data.len(), /* previous data len */
            first_key: std::mem::take(&mut self.first_key).into_key_bytes(),
            last_key: std::mem::take(&mut self.last_key).into_key_bytes(),
        });
        self.data.extend_from_slice(&block.build().encode());
    }

    /// Get the estimated size of the SSTable.
    ///
    /// Since the data blocks contain much more data than meta blocks, just return the size of data
    /// blocks here.
    pub fn estimated_size(&self) -> usize {
        self.data.len()
    }

    /// Builds the SSTable and writes it to the given path. Use the `FileObject` structure to manipulate the disk objects.
    pub fn build(
        mut self,
        id: usize,
        block_cache: Option<Arc<BlockCache>>,
        path: impl AsRef<Path>,
    ) -> Result<SsTable> {
        // finalize the last block whatever it is full or not.
        self.finalize_block_to_sst();
        // encode the block meta and write it to the disk.
        let meta_len = self.metas.len();
        let block_meta_offset = self.data.len();
        let mut buf = self.data;
        BlockMeta::encode_block_metas(&self.metas, &mut buf);
        // extra info for the meta block offset
        buf.put_u32(block_meta_offset as u32);
        // create bloom filter and encode it
        let bits_per_key = Bloom::bloom_bits_per_key(self.key_hashes.len(), 0.01);
        println!("encode bits per key: {}", bits_per_key);
        let bloom = Bloom::build_from_key_hashes(&self.key_hashes, bits_per_key);
        println!("encode bloom size: {}, k={}", bloom.filter.len(), bloom.k);
        let bloom_offset = buf.len();
        bloom.encode(&mut buf);
        buf.put_u32(bloom_offset as u32);

        let file = FileObject::create(path.as_ref(), buf)?;
        let sst_table = SsTable {
            id,
            file,
            first_key: self.metas.first().unwrap().first_key.clone(),
            last_key: self.metas.last().unwrap().last_key.clone(),
            block_metas: self.metas,
            block_meta_offset,
            block_cache,
            bloom: Some(bloom),
            max_ts: 0,
        };
        Ok(sst_table)
    }

    #[cfg(test)]
    pub(crate) fn build_for_test(self, path: impl AsRef<Path>) -> Result<SsTable> {
        self.build(0, None, path)
    }
}
