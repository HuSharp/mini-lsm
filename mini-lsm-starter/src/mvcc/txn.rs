use std::{
    collections::HashSet,
    ops::Bound,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

use anyhow::Result;
use bytes::Bytes;
use crossbeam_skiplist::{map::Entry, SkipMap};
use ouroboros::self_referencing;
use parking_lot::Mutex;

use crate::{
    iterators::{two_merge_iterator::TwoMergeIterator, StorageIterator},
    lsm_iterator::{FusedIterator, LsmIterator},
    lsm_storage::{LsmStorageInner, WriteBatchRecord},
    mem_table::map_bound,
};

use super::CommittedTxnData;

pub struct Transaction {
    pub(crate) read_ts: u64,
    pub(crate) inner: Arc<LsmStorageInner>,
    pub(crate) local_storage: Arc<SkipMap<Bytes, Bytes>>,
    pub(crate) committed: Arc<AtomicBool>,
    /// Write set and read set
    pub(crate) key_hashes: Option<Mutex<(HashSet<u32>, HashSet<u32>)>>,
}

impl Transaction {
    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        if self.committed.load(Ordering::SeqCst) {
            panic!("cannot operate on committed txn!");
        }

        if let Some(guard) = &self.key_hashes {
            let mut guard = guard.lock();
            let (_, read_set) = &mut *guard;
            read_set.insert(farmhash::hash32(key));
        }

        if let Some(entry) = self.local_storage.get(key) {
            if entry.value().is_empty() {
                return Ok(None);
            }
            return Ok(Some(entry.value().clone()));
        }
        self.inner.get_with_txn(key, self.read_ts)
    }

    pub fn scan(self: &Arc<Self>, lower: Bound<&[u8]>, upper: Bound<&[u8]>) -> Result<TxnIterator> {
        if self.committed.load(Ordering::SeqCst) {
            panic!("cannot operate on committed txn!");
        }
        let mut local_iter = TxnLocalIteratorBuilder {
            map: self.local_storage.clone(),
            iter_builder: |map| map.range((map_bound(lower), map_bound(upper))),
            item: (Bytes::new(), Bytes::new()),
        }
        .build();

        // handle deletions
        let next = local_iter.with_iter_mut(|iter| TxnLocalIterator::entry_to_item(iter.next()));
        local_iter.with_mut(|x| *x.item = next);

        TxnIterator::create(
            self.clone(),
            TwoMergeIterator::create(
                local_iter,
                self.inner.scan_with_txn(lower, upper, self.read_ts)?,
            )?,
        )
    }

    pub fn put(&self, key: &[u8], value: &[u8]) {
        if self.committed.load(Ordering::SeqCst) {
            panic!("cannot operate on committed txn!");
        }
        if let Some(key_hashes) = &self.key_hashes {
            let mut key_hashes = key_hashes.lock();
            let (write_hashes, _) = &mut *key_hashes;
            write_hashes.insert(farmhash::hash32(key));
        }
        self.local_storage
            .insert(Bytes::copy_from_slice(key), Bytes::copy_from_slice(value));
    }

    pub fn delete(&self, key: &[u8]) {
        if self.committed.load(Ordering::SeqCst) {
            panic!("cannot operate on committed txn!");
        }
        if let Some(key_hashes) = &self.key_hashes {
            let mut key_hashes = key_hashes.lock();
            let (write_hashes, _) = &mut *key_hashes;
            write_hashes.insert(farmhash::hash32(key));
        }
        self.local_storage
            .insert(Bytes::copy_from_slice(key), Bytes::new());
    }

    pub fn commit(&self) -> Result<()> {
        self.committed
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .expect("cannot operate on committed txn!");
        // ensures only one transaction goes into the transaction verification and commit phase.
        let _commit_lock = self.inner.mvcc().commit_lock.lock();
        if let Some(guard) = &self.key_hashes {
            let guard = guard.lock();
            let (write_set, read_set) = &*guard;
            println!(
                "commit txn: write_set: {:?}, read_set: {:?}",
                write_set, read_set
            );
            if !write_set.is_empty() {
                let committed_txns = self.inner.mvcc().committed_txns.lock();
                // go through all transactions with commit timestamp within range (read_ts, expected_commit_ts) (both excluded bounds)
                for (_, txn_data) in committed_txns.range(self.read_ts + 1..) {
                    for key_hash in read_set {
                        // if the read set of the current transaction overlaps with the write set of any transaction.
                        if txn_data.key_hashes.contains(key_hash) {
                            println!(
                                "txn conflict detected: {:?} {:?}",
                                write_set, txn_data.key_hashes
                            );
                            return Err(anyhow::anyhow!("txn conflict detected"));
                        }
                    }
                }
            }
        }

        let batch = self
            .local_storage
            .iter()
            .map(|entry| {
                if entry.value().is_empty() {
                    WriteBatchRecord::Del(entry.key().clone())
                } else {
                    WriteBatchRecord::Put(entry.key().clone(), entry.value().clone())
                }
            })
            .collect::<Vec<_>>();
        let commit_ts = self.inner.write_batch_inner(&batch)?;

        // insert the write set into the committed_txns
        if let Some(_) = &self.key_hashes {
            let mut committed_txns = self.inner.mvcc().committed_txns.lock();
            let mut key_hashes = self.key_hashes.as_ref().unwrap().lock();
            let (write_set, _) = &mut *key_hashes;
            committed_txns.insert(
                commit_ts,
                CommittedTxnData {
                    key_hashes: std::mem::take(write_set),
                    read_ts: self.read_ts,
                    commit_ts,
                },
            );

            // remove all transactions below the watermark
            let watermark = self.inner.mvcc().watermark();
            while let Some(entry) = committed_txns.first_entry() {
                if *entry.key() < watermark {
                    entry.remove();
                } else {
                    break;
                }
            }
        }

        Ok(())
    }
}

impl Drop for Transaction {
    fn drop(&mut self) {
        self.inner.mvcc().ts.lock().1.remove_reader(self.read_ts);
    }
}

type SkipMapRangeIter<'a> =
    crossbeam_skiplist::map::Range<'a, Bytes, (Bound<Bytes>, Bound<Bytes>), Bytes, Bytes>;

#[self_referencing]
pub struct TxnLocalIterator {
    /// Stores a reference to the skipmap.
    map: Arc<SkipMap<Bytes, Bytes>>,
    /// Stores a skipmap iterator that refers to the lifetime of `MemTableIterator` itself.
    #[borrows(map)]
    #[not_covariant]
    iter: SkipMapRangeIter<'this>,
    /// Stores the current key-value pair.
    item: (Bytes, Bytes),
}

impl TxnLocalIterator {
    // This function is used to convert a `SkipMap` entry to a key-value pair.
    fn entry_to_item(entry: Option<Entry<'_, Bytes, Bytes>>) -> (Bytes, Bytes) {
        entry
            .map(|x| (x.key().clone(), x.value().clone()))
            .unwrap_or_else(|| (Bytes::new(), Bytes::new()))
    }
}

impl StorageIterator for TxnLocalIterator {
    type KeyType<'a> = &'a [u8];

    fn value(&self) -> &[u8] {
        &self.borrow_item().1
    }

    fn key(&self) -> &[u8] {
        &self.borrow_item().0[..]
    }

    fn is_valid(&self) -> bool {
        !self.borrow_item().0.is_empty()
    }

    fn next(&mut self) -> Result<()> {
        let next = self.with_iter_mut(|iter| TxnLocalIterator::entry_to_item(iter.next()));
        self.with_item_mut(|item| *item = next);
        Ok(())
    }
}

pub struct TxnIterator {
    txn: Arc<Transaction>,
    iter: TwoMergeIterator<TxnLocalIterator, FusedIterator<LsmIterator>>,
}

impl TxnIterator {
    pub fn create(
        txn: Arc<Transaction>,
        iter: TwoMergeIterator<TxnLocalIterator, FusedIterator<LsmIterator>>,
    ) -> Result<Self> {
        let mut iter = Self { txn, iter };
        iter.skip_deletes()?;
        if iter.is_valid() {
            iter.add_to_read_set(iter.key());
        }
        Ok(iter)
    }

    // TwoMergeIterator will retain the deletion markers in the child iterators,
    // we need to modify your TxnIterator implementation to correctly handle deletions.
    fn skip_deletes(&mut self) -> Result<()> {
        while self.iter.is_valid() && self.iter.value().is_empty() {
            self.iter.next()?;
        }
        Ok(())
    }

    fn add_to_read_set(&self, key: &[u8]) {
        if let Some(guard) = &self.txn.key_hashes {
            let mut guard = guard.lock();
            let (_, read_set) = &mut *guard;
            read_set.insert(farmhash::hash32(key));
        }
    }
}

impl StorageIterator for TxnIterator {
    type KeyType<'a> = &'a [u8] where Self: 'a;

    fn value(&self) -> &[u8] {
        self.iter.value()
    }

    fn key(&self) -> Self::KeyType<'_> {
        self.iter.key()
    }

    fn is_valid(&self) -> bool {
        self.iter.is_valid()
    }

    fn next(&mut self) -> Result<()> {
        self.iter.next()?;
        self.skip_deletes()
    }

    fn num_active_iterators(&self) -> usize {
        self.iter.num_active_iterators()
    }
}
