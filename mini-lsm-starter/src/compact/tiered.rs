use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::lsm_storage::LsmStorageState;

#[derive(Debug, Serialize, Deserialize)]
pub struct TieredCompactionTask {
    pub tiers: Vec<(usize, Vec<usize>)>,
    pub bottom_tier_included: bool,
}

#[derive(Debug, Clone)]
pub struct TieredCompactionOptions {
    pub num_tiers: usize,
    pub max_size_amplification_percent: usize,
    pub size_ratio: usize,
    pub min_merge_width: usize,
}

pub struct TieredCompactionController {
    options: TieredCompactionOptions,
}

impl TieredCompactionController {
    pub fn new(options: TieredCompactionOptions) -> Self {
        Self { options }
    }

    // rely on https://github.com/facebook/rocksdb/wiki/Universal-Compaction
    pub fn generate_compaction_task(
        &self,
        snapshot: &LsmStorageState,
    ) -> Option<TieredCompactionTask> {
        assert!(
            snapshot.l0_sstables.is_empty(),
            "should not add l0 ssts in tiered compaction"
        );
        if snapshot.levels.len() < self.options.num_tiers {
            return None;
        }
        // compaction triggered by space amplification ratio
        // all levels except last level size / last level size
        let mut size = 0;
        for id in 0..(snapshot.levels.len() - 1) {
            size += snapshot.levels[id].1.len();
        }
        let space_amp_ratio =
            (size as f64) / (snapshot.levels.last().unwrap().1.len() as f64) * 100.0;
        if space_amp_ratio >= self.options.max_size_amplification_percent as f64 {
            println!(
                "compaction triggered by space amplification ratio: {}",
                space_amp_ratio
            );
            return Some(TieredCompactionTask {
                tiers: snapshot.levels.clone(),
                bottom_tier_included: true,
            });
        }

        // size of all previous tiers / this tier >= (1 + size_ratio) * 100%
        // compaction triggered by size ratio(number of sorted runs)
        let size_ratio_trigger = (100.0 + self.options.size_ratio as f64) / 100.0;
        let mut size = 0;
        for id in 0..(snapshot.levels.len() - 1) {
            size += snapshot.levels[id].1.len();
            let cur_tier = snapshot.levels[id + 1].1.len();
            let cur_size_ratio = size as f64 / cur_tier as f64;
            // compaction num need exceed `self.options.min_merge_width`
            if cur_size_ratio >= size_ratio_trigger && id + 2 >= self.options.min_merge_width {
                println!(
                    "compaction triggered by size ratio: {}",
                    cur_size_ratio * 100.0
                );
                return Some(TieredCompactionTask {
                    tiers: snapshot
                        .levels
                        .iter()
                        .take(id + 2)
                        .cloned()
                        .collect::<Vec<_>>(),
                    bottom_tier_included: id + 2 >= snapshot.levels.len(),
                });
            }
        }
        // trying to reduce sorted runs without respecting size ratio
        // to make sure we have exactly `num_tiers` tiers
        println!("compaction triggered by reducing sorted runs");
        let num_tiers_to_take = snapshot.levels.len() - self.options.num_tiers + 2;
        Some(TieredCompactionTask {
            tiers: snapshot
                .levels
                .iter()
                .take(num_tiers_to_take)
                .cloned()
                .collect::<Vec<_>>(),
            bottom_tier_included: snapshot.levels.len() >= num_tiers_to_take,
        })
    }

    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &TieredCompactionTask,
        output: &[usize],
    ) -> (LsmStorageState, Vec<usize>) {
        assert!(
            snapshot.l0_sstables.is_empty(),
            "should not add l0 ssts in tiered compaction"
        );
        let mut new_snapshot = snapshot.clone();
        let mut tier_to_remove = task
            .tiers
            .iter()
            .map(|(x, y)| (*x, y))
            .collect::<HashMap<_, _>>();
        let mut files_to_remove = Vec::new();
        let mut levels = Vec::new();
        let mut new_tier_added = false;
        for (tier_id, files) in &new_snapshot.levels {
            if let Some(remove_files) = tier_to_remove.remove(tier_id) {
                // the tier should be removed
                assert_eq!(
                    remove_files, files,
                    "file changed after issuing compaction task"
                );
                files_to_remove.extend(files.iter().cloned());
            } else {
                // retain the tier
                levels.push((*tier_id, files.clone()));
            }
            // ?
            if tier_to_remove.is_empty() && !new_tier_added {
                // add the compacted tier to the LSM tree
                new_tier_added = true;
                levels.push((output[0], output.to_vec()));
            }
        }
        if !tier_to_remove.is_empty() {
            unreachable!("some tiers not found??");
        }
        new_snapshot.levels = levels;
        (new_snapshot, files_to_remove)
    }
}
