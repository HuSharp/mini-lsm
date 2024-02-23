// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use anyhow::Result;
use bytes::{Buf, BufMut, Bytes, BytesMut};

/// Bloom implements bloom filter functionalities over
/// a bit-slice of data.
pub struct Bloom {
    /// data of filter in bits
    pub(crate) filter: Bytes,
    /// number of hash functions
    pub(crate) k: u8,
}

pub trait BitSlice {
    fn get_bit(&self, idx: usize) -> bool;
    fn bit_len(&self) -> usize;
}

pub trait BitSliceMut {
    fn set_bit(&mut self, idx: usize, val: bool);
}

impl<T: AsRef<[u8]>> BitSlice for T {
    fn get_bit(&self, idx: usize) -> bool {
        let pos = idx / 8;
        let offset = idx % 8;
        (self.as_ref()[pos] & (1 << offset)) != 0
    }

    fn bit_len(&self) -> usize {
        self.as_ref().len() * 8
    }
}

impl<T: AsMut<[u8]>> BitSliceMut for T {
    fn set_bit(&mut self, idx: usize, val: bool) {
        let pos = idx / 8;
        let offset = idx % 8;
        if val {
            self.as_mut()[pos] |= 1 << offset;
        } else {
            self.as_mut()[pos] &= !(1 << offset);
        }
    }
}

impl Bloom {
    /// Decode a bloom filter
    pub fn decode(buf: &[u8]) -> Result<Self> {
        // get checksum as 4 bytes
        let checksum = (&buf[buf.len() - 4..buf.len()]).get_u32();
        let all_data = &buf[..buf.len() - 4];
        if checksum != crc32fast::hash(all_data) {
            return Err(anyhow::anyhow!("Bloom filter checksum mismatch"));
        }
        let filter = &buf[..buf.len() - 1 - 4];
        let k = buf[buf.len() - 1 - 4];
        Ok(Self {
            filter: filter.to_vec().into(),
            k,
        })
    }

    /// Encode a bloom filter
    pub fn encode(&self, buf: &mut Vec<u8>) {
        let offset = buf.len();
        buf.extend(&self.filter);
        buf.put_u8(self.k);
        // add checksum
        let checksum = crc32fast::hash(buf[offset..].as_ref());
        buf.put_u32(checksum);
    }

    /// Get bloom filter bits per key from entries count and FPR
    pub fn bloom_bits_per_key(hashs_count: usize, false_positive_rate: f64) -> usize {
        let size =
            -1.0 * (hashs_count as f64) * false_positive_rate.ln() / std::f64::consts::LN_2.powi(2);
        let locs = (std::f64::consts::LN_2 * size / (hashs_count as f64)).ceil();
        locs as usize
    }

    /// Build bloom filter from key hashes
    pub fn build_from_key_hashes(keys: &[u32], bits_per_key: usize) -> Self {
        let k = (bits_per_key as f64 * 0.69) as u32;
        let k = k.min(30).max(1);
        let nbits = (keys.len() * bits_per_key).max(64);
        let nbytes = (nbits + 7) / 8;
        let nbits = nbytes * 8;
        let mut filter = BytesMut::with_capacity(nbytes);
        filter.resize(nbytes, 0);

        keys.iter().for_each(|h| {
            /* h is the key hash */
            let mut h = *h;
            let delta = (h >> 17) | (h << 15);
            for j in 0..k {
                let bitpos = delta as usize % nbits;
                filter.set_bit(bitpos, true);
                // we can add delta to uniformly distribute the bits
                h = h.wrapping_add(delta);
            }
        });

        Self {
            filter: filter.freeze(),
            k: k as u8,
        }
    }

    /// Check if a bloom filter may contain some data
    pub fn may_contain(&self, mut h: u32) -> bool {
        if self.k > 30 {
            // potential new encoding for short bloom filters
            true
        } else {
            let nbits = self.filter.bit_len();
            let delta = (h >> 17) | (h << 15);

            for j in 0..self.k {
                let bitpos = delta as usize % nbits;
                if !self.filter.get_bit(bitpos) {
                    return false;
                }
                // we can add delta to uniformly distribute the bits
                h = h.wrapping_add(delta);
            }

            true
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_small_bloom_filter() {
        let hash: Vec<u32> = vec![b"hello".to_vec(), b"world".to_vec()]
            .into_iter()
            .map(|x| farmhash::fingerprint32(&x))
            .collect();
        let bloom = Bloom::build_from_key_hashes(&hash, 10);

        let check_hash: Vec<u32> = vec![
            b"hello".to_vec(),
            b"world".to_vec(),
            b"x".to_vec(),
            b"fool".to_vec(),
        ]
        .into_iter()
        .map(|x| farmhash::fingerprint32(&x))
        .collect();

        assert!(bloom.may_contain(check_hash[0]));
        assert!(bloom.may_contain(check_hash[1]));
        assert!(!bloom.may_contain(check_hash[2]));
        assert!(!bloom.may_contain(check_hash[3]));
    }
}
