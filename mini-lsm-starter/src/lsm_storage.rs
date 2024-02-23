#![allow(dead_code)] // REMOVE THIS LINE after fully implementing this functionality

use std::collections::{BTreeSet, HashMap};
use std::fs::File;
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

use anyhow::{Context, Ok, Result};
use bytes::Bytes;
use parking_lot::{Mutex, MutexGuard, RwLock};

use crate::block::Block;
use crate::compact::{
    CompactionController, CompactionOptions, LeveledCompactionController, LeveledCompactionOptions,
    SimpleLeveledCompactionController, SimpleLeveledCompactionOptions, TieredCompactionController,
};
use crate::iterators::concat_iterator::SstConcatIterator;
use crate::iterators::merge_iterator::MergeIterator;
use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::iterators::StorageIterator;
use crate::key::KeySlice;
use crate::lsm_iterator::{FusedIterator, LsmIterator};
use crate::manifest::{Manifest, ManifestRecord};
use crate::mem_table::{map_bound, MemTable};
use crate::mvcc::LsmMvccInner;
use crate::table::{FileObject, SsTable, SsTableBuilder, SsTableIterator};

pub type BlockCache = moka::sync::Cache<(usize, usize), Arc<Block>>;

/// Represents the state of the storage engine.
#[derive(Clone)]
pub struct LsmStorageState {
    /// The current memtable.
    pub memtable: Arc<MemTable>,
    /// Immutable memtables, from latest to earliest.
    pub imm_memtables: Vec<Arc<MemTable>>,
    /// L0 SSTs, from latest to earliest.
    pub l0_sstables: Vec<usize>,
    /// SsTables sorted by key range; L1 - L_max for leveled compaction, or tiers for tiered
    /// compaction.
    pub levels: Vec<(usize, Vec<usize>)>,
    /// SST objects.
    pub sstables: HashMap<usize, Arc<SsTable>>,
}

pub enum WriteBatchRecord<T: AsRef<[u8]>> {
    Put(T, T),
    Del(T),
}

impl LsmStorageState {
    fn create(options: &LsmStorageOptions) -> Self {
        let levels = match &options.compaction_options {
            CompactionOptions::Leveled(LeveledCompactionOptions { max_levels, .. })
            | CompactionOptions::Simple(SimpleLeveledCompactionOptions { max_levels, .. }) => (1
                ..=*max_levels)
                .map(|level| (level, Vec::new()))
                .collect::<Vec<_>>(),
            CompactionOptions::Tiered(_) => Vec::new(),
            CompactionOptions::NoCompaction => vec![(1, Vec::new())],
        };
        Self {
            memtable: Arc::new(MemTable::create(0)),
            imm_memtables: Vec::new(),
            l0_sstables: Vec::new(),
            levels,
            sstables: Default::default(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct LsmStorageOptions {
    // Block size in bytes
    pub block_size: usize,
    // SST size in bytes, also the approximate memtable capacity limit
    pub target_sst_size: usize,
    // Maximum number of memtables in memory, flush to L0 when exceeding this limit
    pub num_memtable_limit: usize,
    pub compaction_options: CompactionOptions,
    pub enable_wal: bool,
    pub serializable: bool,
}

impl LsmStorageOptions {
    pub fn default_for_week1_test() -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 2 << 20,
            compaction_options: CompactionOptions::NoCompaction,
            enable_wal: false,
            num_memtable_limit: 50,
            serializable: false,
        }
    }

    pub fn default_for_week1_day6_test() -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 2 << 20,
            compaction_options: CompactionOptions::NoCompaction,
            enable_wal: false,
            num_memtable_limit: 2,
            serializable: false,
        }
    }

    pub fn default_for_week2_test(compaction_options: CompactionOptions) -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 1 << 20, // 1MB
            compaction_options,
            enable_wal: false,
            num_memtable_limit: 2,
            serializable: false,
        }
    }
}

/// The storage interface of the LSM tree.
pub(crate) struct LsmStorageInner {
    pub(crate) state: Arc<RwLock<Arc<LsmStorageState>>>,
    pub(crate) state_lock: Mutex<()>,
    path: PathBuf,
    pub(crate) block_cache: Arc<BlockCache>,
    next_sst_id: AtomicUsize,
    pub(crate) options: Arc<LsmStorageOptions>,
    pub(crate) compaction_controller: CompactionController,
    pub(crate) manifest: Option<Manifest>,
    pub(crate) mvcc: Option<LsmMvccInner>,
}

/// A thin wrapper for `LsmStorageInner` and the user interface for MiniLSM.
pub struct MiniLsm {
    pub(crate) inner: Arc<LsmStorageInner>,
    /// Notifies the L0 flush thread to stop working. (In week 1 day 6)
    flush_notifier: crossbeam_channel::Sender<()>,
    /// The handle for the compaction thread. (In week 1 day 6)
    flush_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
    /// Notifies the compaction thread to stop working. (In week 2)
    compaction_notifier: crossbeam_channel::Sender<()>,
    /// The handle for the compaction thread. (In week 2)
    compaction_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
}

impl Drop for MiniLsm {
    fn drop(&mut self) {
        self.compaction_notifier.send(()).ok();
        self.flush_notifier.send(()).ok();
    }
}

impl MiniLsm {
    pub fn close(&self) -> Result<()> {
        self.inner.sync_dir()?;
        // flush and compaction threads should be stopped
        self.flush_notifier.send(()).ok();
        self.compaction_notifier.send(()).ok();

        // close the compaction thread and flush thread
        if let Some(handle) = self.flush_thread.lock().take() {
            handle.join().ok();
        }
        if let Some(handle) = self.compaction_thread.lock().take() {
            handle.join().ok();
        }

        // sync wal
        if self.inner.options.enable_wal {
            self.inner.sync()?;
            self.inner.sync_dir()?;
            return Ok(());
        }

        // flush memtable and imm_memtables
        if !self.inner.state.read().memtable.is_empty() {
            self.inner
                .force_freeze_memtable(&self.inner.state_lock.lock())?;
        }

        while !self.inner.state.read().imm_memtables.is_empty() {
            self.inner.force_flush_next_imm_memtable()?;
        }

        self.inner.sync_dir()?;

        Ok(())
    }

    /// Start the storage engine by either loading an existing directory or creating a new one if the directory does
    /// not exist.
    pub fn open(path: impl AsRef<Path>, options: LsmStorageOptions) -> Result<Arc<Self>> {
        let inner = Arc::new(LsmStorageInner::open(path, options)?);
        let (tx1, rx) = crossbeam_channel::unbounded();
        let compaction_thread = inner.spawn_compaction_thread(rx)?;
        let (tx2, rx) = crossbeam_channel::unbounded();
        let flush_thread = inner.spawn_flush_thread(rx)?;
        Ok(Arc::new(Self {
            inner,
            flush_notifier: tx2,
            flush_thread: Mutex::new(flush_thread),
            compaction_notifier: tx1,
            compaction_thread: Mutex::new(compaction_thread),
        }))
    }

    pub fn new_txn(&self) -> Result<()> {
        self.inner.new_txn()
    }

    pub fn write_batch<T: AsRef<[u8]>>(&self, batch: &[WriteBatchRecord<T>]) -> Result<()> {
        self.inner.write_batch(batch)
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        self.inner.get(key)
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.inner.put(key, value)
    }

    pub fn delete(&self, key: &[u8]) -> Result<()> {
        self.inner.delete(key)
    }

    pub fn sync(&self) -> Result<()> {
        self.inner.sync()
    }

    pub fn scan(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> Result<FusedIterator<LsmIterator>> {
        self.inner.scan(lower, upper)
    }

    /// Only call this in test cases due to race conditions
    pub fn force_flush(&self) -> Result<()> {
        if !self.inner.state.read().memtable.is_empty() {
            self.inner
                .force_freeze_memtable(&self.inner.state_lock.lock())?;
        }
        if !self.inner.state.read().imm_memtables.is_empty() {
            self.inner.force_flush_next_imm_memtable()?;
        }
        Ok(())
    }

    pub fn force_full_compaction(&self) -> Result<()> {
        self.inner.force_full_compaction()
    }
}

impl LsmStorageInner {
    pub(crate) fn next_sst_id(&self) -> usize {
        self.next_sst_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }

    /// Start the storage engine by either loading an existing directory or creating a new one if the directory does
    /// not exist.
    pub(crate) fn open(path: impl AsRef<Path>, options: LsmStorageOptions) -> Result<Self> {
        let path = path.as_ref();
        if !path.exists() {
            std::fs::create_dir_all(path)
                .context("[LsmStorageInner.open] failed to create DB directory")?;
        }

        let block_cache = Arc::new(BlockCache::new(1 << 20)); // 4GB block cache,

        let compaction_controller = match &options.compaction_options {
            CompactionOptions::Leveled(options) => {
                CompactionController::Leveled(LeveledCompactionController::new(options.clone()))
            }
            CompactionOptions::Tiered(options) => {
                CompactionController::Tiered(TieredCompactionController::new(options.clone()))
            }
            CompactionOptions::Simple(options) => CompactionController::Simple(
                SimpleLeveledCompactionController::new(options.clone()),
            ),
            CompactionOptions::NoCompaction => CompactionController::NoCompaction,
        };

        let mut state = LsmStorageState::create(&options);
        let manifest;
        let mut next_sst_id = 1;
        // recover from MANIFEST, `<path>/MANIFEST`
        let manifest_path = path.join("MANIFEST");
        if !manifest_path.exists() {
            if options.enable_wal {
                let memtable = Arc::new(MemTable::create_with_wal(
                    state.memtable.id(),
                    Self::path_of_wal_static(path, state.memtable.id()),
                )?);
                state.memtable = memtable;
            }
            manifest = Manifest::create(&manifest_path)
                .context("[LsmStorageInner.open] failed to create manifest")?;
            manifest.add_record_when_init(ManifestRecord::NewMemtable(state.memtable.id()))?;
        } else {
            let (m, records) = Manifest::recover(manifest_path)?;
            let mut memtables = BTreeSet::new();
            for record in records {
                match record {
                    ManifestRecord::Flush(sst_id) => {
                        let res = memtables.remove(&sst_id);
                        assert!(res, "memtable not exist?");
                        if compaction_controller.flush_to_l0() {
                            state.l0_sstables.insert(0, sst_id);
                        } else {
                            state.levels.insert(0, (sst_id, vec![sst_id]));
                        }
                        next_sst_id = next_sst_id.max(sst_id);
                    }
                    ManifestRecord::NewMemtable(x) => {
                        next_sst_id = next_sst_id.max(x);
                        memtables.insert(x);
                    }
                    ManifestRecord::Compaction(task, new_ssts) => {
                        let (new_state, _) =
                            compaction_controller.apply_compaction_result(&state, &task, &new_ssts);
                        state = new_state;
                        next_sst_id = next_sst_id.max(new_ssts.iter().max().copied().unwrap());
                    }
                }
            }

            // recover SSTs
            let mut sst_cnt = 0;
            for &sst_id in state
                .l0_sstables
                .iter()
                .chain(state.levels.iter().flat_map(|(_, files)| files))
            {
                let sst = SsTable::open(
                    sst_id,
                    Some(block_cache.clone()),
                    FileObject::open(&Self::path_of_sst_static(path, sst_id))
                        .context("failed to open SST")?,
                )?;
                state.sstables.insert(sst_id, Arc::new(sst));
                next_sst_id = next_sst_id.max(sst_id);
                sst_cnt += 1;
            }
            println!("recovered {} SSTs", sst_cnt);
            next_sst_id += 1;

            // recover memtables
            if options.enable_wal {
                let mut wal_cnt = 0;
                for id in memtables {
                    let memtable =
                        MemTable::recover_from_wal(id, Self::path_of_wal_static(path, id))?;
                    if !memtable.is_empty() {
                        state.imm_memtables.insert(0, Arc::new(memtable));
                        wal_cnt += 1;
                    }
                }
                println!("recovered {} memtables from WAL", wal_cnt);
                state.memtable = Arc::new(MemTable::create_with_wal(
                    next_sst_id,
                    Self::path_of_wal_static(path, next_sst_id),
                )?);
            } else {
                state.memtable = Arc::new(MemTable::create(next_sst_id));
            }
            m.add_record_when_init(ManifestRecord::NewMemtable(next_sst_id))?;

            next_sst_id += 1;
            manifest = m;
        }

        let storage = Self {
            state: Arc::new(RwLock::new(Arc::new(state))),
            state_lock: Mutex::new(()),
            path: path.to_path_buf(),
            block_cache,
            next_sst_id: AtomicUsize::new(next_sst_id),
            compaction_controller,
            manifest: Some(manifest),
            options: options.into(),
            mvcc: None,
        };

        storage.sync_dir()?;

        Ok(storage)
    }

    pub fn sync(&self) -> Result<()> {
        self.state.read().memtable.sync_wal()
    }

    pub fn try_freeze(&self, size: usize) -> Result<()> {
        // using double check for concurrency
        if size >= self.options.target_sst_size {
            let state_lock = self.state_lock.lock();
            let snapshot = self.state.read();

            if snapshot.memtable.approximate_size() >= self.options.target_sst_size {
                drop(snapshot);
                self.force_freeze_memtable(&state_lock)?;
            }
        }
        Ok(())
    }

    pub(crate) fn path_of_sst_static(path: impl AsRef<Path>, id: usize) -> PathBuf {
        path.as_ref().join(format!("{:05}.sst", id))
    }

    pub(crate) fn path_of_sst(&self, id: usize) -> PathBuf {
        Self::path_of_sst_static(&self.path, id)
    }

    pub(crate) fn path(&self) -> &Path {
        &self.path
    }

    pub(crate) fn path_of_wal_static(path: impl AsRef<Path>, id: usize) -> PathBuf {
        path.as_ref().join(format!("{:05}.wal", id))
    }

    pub(crate) fn path_of_wal(&self, id: usize) -> PathBuf {
        Self::path_of_wal_static(&self.path, id)
    }

    pub(super) fn sync_dir(&self) -> Result<()> {
        File::open(&self.path)?.sync_all()?;
        Ok(())
    }

    /// Force freeze the current memtable to an immutable memtable
    pub fn force_freeze_memtable(&self, state_lock_observer: &MutexGuard<'_, ()>) -> Result<()> {
        let new_memtable_id = self.next_sst_id();
        let new_memtable = if self.options.enable_wal {
            Arc::new(MemTable::create_with_wal(
                new_memtable_id,
                self.path_of_wal(new_memtable_id),
            )?)
        } else {
            Arc::new(MemTable::create(new_memtable_id))
        };

        {
            let mut guard = self.state.write();
            let mut snapshot = guard.as_ref().clone();
            let old_memtable = std::mem::replace(&mut snapshot.memtable, new_memtable);
            // imm_memtables.first() should be the last frozen memtable
            snapshot.imm_memtables.insert(0, old_memtable.clone());
            *guard = Arc::new(snapshot);
            drop(guard);
            old_memtable.sync_wal()?;
        }

        // add NewMemtable record to manifest
        self.manifest.as_ref().unwrap().add_record(
            state_lock_observer,
            ManifestRecord::NewMemtable(new_memtable_id),
        )?;
        Ok(())
    }

    /// Force flush the earliest-created immutable memtable to disk
    pub fn force_flush_next_imm_memtable(&self) -> Result<()> {
        let state_lock = self.state_lock.lock();

        let last_imm_memtable;
        {
            let snapshot = self.state.read();
            last_imm_memtable = snapshot
                .imm_memtables
                .last()
                .expect("no imm memtables!")
                .clone();
        }

        // build new sstable
        let mut builder = SsTableBuilder::new(self.options.block_size);
        last_imm_memtable.flush(&mut builder)?;
        let sst_id = last_imm_memtable.id();
        let new_sst = Arc::new(builder.build(
            sst_id,
            Some(self.block_cache.clone()),
            self.path_of_sst(sst_id),
        )?);

        // Add the new SST to the storage
        {
            let mut guard = self.state.write();
            let mut snapshot = guard.as_ref().clone();
            let sst_id = snapshot.imm_memtables.pop().unwrap().id();
            println!("flushed {}.sst with size={}", sst_id, new_sst.table_size());
            snapshot.sstables.insert(sst_id, new_sst);
            if self.compaction_controller.flush_to_l0() {
                // In leveled compaction or no compaction, simply flush to L0
                // L0 SSTs are sorted by creation time, from latest to earliest.
                snapshot.l0_sstables.insert(0, sst_id);
            } else {
                // In tiered compaction, create a new tier
                snapshot.levels.insert(0, (sst_id, vec![sst_id]));
            }
            *guard = Arc::new(snapshot);
        }

        if self.options.enable_wal {
            std::fs::remove_file(self.path_of_wal(sst_id))?;
        }

        // update manifest
        self.manifest
            .as_ref()
            .unwrap()
            .add_record(&state_lock, ManifestRecord::Flush(sst_id))?;

        Ok(())
    }

    pub fn new_txn(&self) -> Result<()> {
        // no-op
        Ok(())
    }

    /// Get a key from the storage. In week1 day7, this can be further optimized by using a bloom filter.
    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        let snapshot = self.state.read();
        // search memtable firstly
        if let Some(value) = snapshot.memtable.get(key) {
            if value.is_empty() {
                return Ok(None);
            }
            return Ok(Some(value));
        }

        // traverse imm-memtable
        for memtable in snapshot.imm_memtables.iter() {
            if let Some(value) = memtable.get(key) {
                if value.is_empty() {
                    return Ok(None);
                }
                return Ok(Some(value));
            }
        }

        let check_sst = |key: &[u8], sst: &SsTable| {
            // check if the key is within the SST's key range
            if key_within(key, sst.first_key().raw_ref(), sst.last_key().raw_ref()) {
                // bloom filter check
                if let Some(bloom) = &sst.bloom {
                    if bloom.may_contain(farmhash::fingerprint32(key)) {
                        return true;
                    }
                } else {
                    return true;
                }
            }
            false
        };

        // create merge iterator for l0_sstables
        let mut l0_iters = Vec::with_capacity(snapshot.l0_sstables.len());
        for sst_id in snapshot.l0_sstables.iter() {
            let sst = snapshot.sstables[sst_id].clone();
            if check_sst(key, &sst) {
                let iter = SsTableIterator::create_and_seek_to_key(
                    sst.clone(),
                    KeySlice::from_slice(key),
                )?;
                l0_iters.push(Box::new(iter));
            }
        }
        let merge_l0_sstable_iter = MergeIterator::create(l0_iters);

        // create merge iterator for multi-level sstables
        let mut level_iters = Vec::with_capacity(snapshot.levels.len());
        for (_, level_sst_ids) in &snapshot.levels {
            let mut ssts = Vec::with_capacity(level_sst_ids.len());
            for sst_id in level_sst_ids {
                let sst = snapshot.sstables[sst_id].clone();
                if check_sst(key, &sst) {
                    ssts.push(sst.clone());
                }
            }
            let level_iter =
                SstConcatIterator::create_and_seek_to_key(ssts, KeySlice::from_slice(key))?;
            level_iters.push(Box::new(level_iter));
        }
        let merge_iter = MergeIterator::create(level_iters);

        let two_merge_iterator = TwoMergeIterator::create(merge_l0_sstable_iter, merge_iter)?;
        if two_merge_iterator.is_valid()
            && two_merge_iterator.key() == KeySlice::from_slice(key)
            && !two_merge_iterator.value().is_empty()
        {
            return Ok(Some(Bytes::copy_from_slice(two_merge_iterator.value())));
        }

        Ok(None)
    }

    /// Create an iterator over a range of keys.
    pub fn scan(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> Result<FusedIterator<LsmIterator>> {
        let snapshot = {
            let guard = self.state.read();
            Arc::clone(&guard)
        }; // drop global lock here

        // need to get all memtables and imm_memtables
        let mut memtable_iters = Vec::with_capacity(snapshot.imm_memtables.len() + 1);
        memtable_iters.push(Box::new(snapshot.memtable.scan(lower, upper)));
        for imm_memtable in snapshot.imm_memtables.iter() {
            memtable_iters.push(Box::new(imm_memtable.scan(lower, upper)));
        }
        // using merge iterator to merge all memtables and imm_memtables iters
        let merge_memtable_iter = MergeIterator::create(memtable_iters);

        // using merge iterator to merge all sstables iters
        let mut l0_iters = Vec::with_capacity(snapshot.l0_sstables.len());
        for sst_id in snapshot.l0_sstables.iter() {
            let sst = snapshot.sstables[sst_id].clone();
            println!("sst_id: {}, scan range: {:?} {:?}", sst_id, lower, upper);
            // filter out some SSTs that do not contain the key range
            if check_intersect_of_range(
                lower,
                upper,
                sst.first_key().raw_ref(),
                sst.last_key().raw_ref(),
            ) {
                // SST iterator does not support passing an end bound to it.
                // Therefore, need to handle the end_bound manually in LsmIterator
                let iter = match lower {
                    Bound::Included(key) => {
                        SsTableIterator::create_and_seek_to_key(sst, KeySlice::from_slice(key))?
                    }
                    Bound::Excluded(key) => {
                        let mut iter = SsTableIterator::create_and_seek_to_key(
                            sst,
                            KeySlice::from_slice(key),
                        )?;
                        if iter.is_valid() && iter.key() == KeySlice::from_slice(key) {
                            iter.next()?;
                        }
                        iter
                    }
                    Bound::Unbounded => SsTableIterator::create_and_seek_to_first(sst)?,
                };

                l0_iters.push(Box::new(iter));
            }
        }
        let merge_l0_sstable_iter = MergeIterator::create(l0_iters);
        // memtables and imm_memtables are merged first, then the result is merged with L0 SSTs
        let two_merge_sst_iter =
            TwoMergeIterator::create(merge_memtable_iter, merge_l0_sstable_iter)?;

        // concat multi-level sstables
        let mut sst_concat_iters = Vec::with_capacity(snapshot.levels.len());
        for (level, level_sst_ids) in &snapshot.levels {
            println!("level: {}", level);
            let mut ssts = Vec::with_capacity(level_sst_ids.len());
            for sst_id in level_sst_ids.iter() {
                ssts.push(snapshot.sstables[sst_id].clone());
            }
            let concat_iter = match lower {
                Bound::Included(key) => {
                    SstConcatIterator::create_and_seek_to_key(ssts, KeySlice::from_slice(key))?
                }
                Bound::Excluded(key) => {
                    let mut iter =
                        SstConcatIterator::create_and_seek_to_key(ssts, KeySlice::from_slice(key))?;
                    if iter.is_valid() && iter.key() == KeySlice::from_slice(key) {
                        iter.next()?;
                    }
                    iter
                }
                Bound::Unbounded => SstConcatIterator::create_and_seek_to_first(ssts)?,
            };
            sst_concat_iters.push(Box::new(concat_iter));
        }
        let merge_sst_iter = MergeIterator::create(sst_concat_iters);

        // finally, the result is merged with L1 SSTs
        let two_merge_iter = TwoMergeIterator::create(two_merge_sst_iter, merge_sst_iter)?;

        Ok(FusedIterator::new(LsmIterator::new(
            two_merge_iter,
            map_bound(upper),
        )?))
    }

    /// Write a batch of data into the storage. Implement in week 2 day 7.
    pub fn write_batch<T: AsRef<[u8]>>(&self, batch: &[WriteBatchRecord<T>]) -> Result<()> {
        for record in batch {
            match record {
                WriteBatchRecord::Put(key, value) => {
                    let size;
                    {
                        let key = key.as_ref();
                        let value = value.as_ref();
                        assert!(!key.is_empty(), "key cannot be empty");
                        assert!(!value.is_empty(), "value cannot be empty");
                        let snapshot = self.state.write();
                        snapshot.memtable.put(key, value)?;
                        size = snapshot.memtable.approximate_size();
                    }

                    self.try_freeze(size)?;
                }
                WriteBatchRecord::Del(key) => {
                    let size;
                    {
                        let key = key.as_ref();
                        let snapshot = self.state.write();
                        snapshot.memtable.put(key, b"")?;
                        size = snapshot.memtable.approximate_size();
                    }

                    self.try_freeze(size)?;
                }
            }
        }
        Ok(())
    }

    /// Put a key-value pair into the storage by writing into the current memtable.
    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.write_batch(&[WriteBatchRecord::Put(key, value)])
    }

    /// Remove a key from the storage by writing an empty value.
    pub fn delete(&self, key: &[u8]) -> Result<()> {
        self.write_batch(&[WriteBatchRecord::Del(key)])
    }
}

// utils

// key_within checks if the user's key is within the SST's key range.
fn key_within(key: &[u8], sst_begin: &[u8], sst_end: &[u8]) -> bool {
    sst_begin <= key && key <= sst_end
}

// Check if user's `key_range` intersects with the SST's key range.
fn check_intersect_of_range(
    begin: Bound<&[u8]>,
    end: Bound<&[u8]>,
    sst_begin: &[u8],
    sst_end: &[u8],
) -> bool {
    println!(
        "intersected: {:?} {:?}, sst: {:?} {:?}",
        begin, end, sst_begin, sst_end
    );
    match end {
        Bound::Excluded(key) if key <= sst_begin => {
            return false;
        }
        Bound::Included(key) if key < sst_begin => {
            return false;
        }
        _ => {}
    }
    match begin {
        Bound::Excluded(key) if sst_end <= key => {
            return false;
        }
        Bound::Included(key) if sst_end < key => {
            return false;
        }
        _ => {}
    }

    true
}
