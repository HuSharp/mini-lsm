mod builder;
mod iterator;

use std::u8;

pub use builder::BlockBuilder;
use bytes::{Buf, BufMut, Bytes};
pub use iterator::BlockIterator;

use crate::key::KeyVec;

pub(crate) const SIZEOF_U16: usize = std::mem::size_of::<u16>();

/// A block is the smallest unit of read and caching in LSM tree. It is a collection of sorted key-value pairs.
pub struct Block {
    pub(crate) data: Vec<u8>,
    pub(crate) offsets: Vec<u16>,
}

impl Block {
    /// Encode the internal data to the data layout illustrated in the tutorial
    /// Note: You may want to recheck if any of the expected field is missing from your output
    pub fn encode(&self) -> Bytes {
        let mut buf = self.data.clone();
        for offset in &self.offsets {
            buf.put_u16(*offset);
        }
        // num of elements
        buf.put_u16(self.offsets.len() as u16);
        buf.into()
    }

    /// Decode from the data layout, transform the input `data` to a single `Block`
    pub fn decode(data: &[u8]) -> Self {
        let num_of_elements = (&data[data.len() - SIZEOF_U16..]).get_u16();
        // transform offsets
        let data_end = data.len() - SIZEOF_U16 - SIZEOF_U16 * (num_of_elements as usize);
        let offsets = data[data_end..data.len() - SIZEOF_U16]
            .chunks(SIZEOF_U16)
            .map(|mut x| x.get_u16())
            .collect();
        // transform data
        let data = data[..data_end].to_vec();
        Self { data, offsets }
    }

    fn get_first_key(&self) -> KeyVec {
        // The first key is the first entry in the data section
        // redundant key is 0
        let mut entry = &self.data[0..];
        entry.get_u16();
        let key_len = entry.get_u16() as usize;
        let key = &entry[..key_len];
        entry.advance(key_len);
        KeyVec::from_vec_with_ts(key.to_vec(), entry.get_u64())
    }
}
