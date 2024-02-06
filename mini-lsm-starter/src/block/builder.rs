use bytes::BufMut;

use crate::key::{KeySlice, KeyVec};

use super::{Block, SIZEOF_U16};

/// Builds a block.
/*
----------------------------------------------------------------------------------------------------
|             Data Section             |              Offset Section             |      Extra      |
----------------------------------------------------------------------------------------------------
| Entry #1 | Entry #2 | ... | Entry #N | Offset #1 | Offset #2 | ... | Offset #N | num_of_elements |
----------------------------------------------------------------------------------------------------
*/
pub struct BlockBuilder {
    /// Offsets of each key-value entries.
    offsets: Vec<u16>,
    /// All serialized key-value pairs in the block.
    data: Vec<u8>,
    /// The expected block size.
    block_size: usize,
    /// The first key in the block
    first_key: KeyVec,
}

fn compute_redundant_key(first_key: KeySlice, cur_key: KeySlice) -> usize {
    let mut i = 0;
    while i < first_key.len() && i < cur_key.len() {
        if first_key.raw_ref()[i] == cur_key.raw_ref()[i] {
            i += 1;
        } else {
            break;
        }
    }
    i
}

impl BlockBuilder {
    /// Creates a new block builder.
    pub fn new(block_size: usize) -> Self {
        Self {
            offsets: Vec::new(),
            data: Vec::new(),
            block_size,
            first_key: KeyVec::new(),
        }
    }

    /* Each entry is a key-value pair.
    -----------------------------------------------------------------------
    |                           Entry #1                            | ... |
    -----------------------------------------------------------------------
    | redundant_key(first_key_index) | key_len (2B) | key (keylen) | value_len (2B) | value (varlen) | ... |
    -----------------------------------------------------------------------
    */
    /// Adds a key-value pair to the block. Returns false when the block is full.
    #[must_use]
    pub fn add(&mut self, key: KeySlice, value: &[u8]) -> bool {
        if self.data.len() + self.offsets.len() + SIZEOF_U16 /* num of elements */
            + key.len() + value.len() + SIZEOF_U16 /* key len */ + SIZEOF_U16 /* value len */
            > self.block_size
            && !self.is_empty()
        {
            return false;
        }
        // Add the offset of the data into the offset array.
        self.offsets.push(self.data.len() as u16);
        // compute redundant key
        // when is the first key, the redundant key is 0
        let redundant_index = compute_redundant_key(self.first_key.as_key_slice(), key);
        self.data.put_u16(redundant_index as u16);
        // rest of the key
        self.data.put_u16((key.len() - redundant_index) as u16);
        self.data.put(&key.raw_ref()[redundant_index..]);

        self.data.put_u16(value.len() as u16);
        self.data.put(value);

        if self.first_key.is_empty() {
            self.first_key = key.to_key_vec();
        }

        true
    }

    /// Check if there is no key-value pair in the block.
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Finalize the block.
    pub fn build(self) -> Block {
        Block {
            data: self.data,
            offsets: self.offsets,
        }
    }
}
