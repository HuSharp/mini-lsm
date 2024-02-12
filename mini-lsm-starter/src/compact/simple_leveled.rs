use std::collections::HashSet;

use serde::{Deserialize, Serialize};

use crate::lsm_storage::LsmStorageState;

#[derive(Debug, Clone)]
pub struct SimpleLeveledCompactionOptions {
    pub size_ratio_percent: usize,
    pub level0_file_num_compaction_trigger: usize,
    pub max_levels: usize,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SimpleLeveledCompactionTask {
    // if upper_level is `None`, then it is L0 compaction
    pub upper_level: Option<usize>,
    pub upper_level_sst_ids: Vec<usize>,
    pub lower_level: usize,
    pub lower_level_sst_ids: Vec<usize>,
    pub is_lower_level_bottom_level: bool,
}

pub struct SimpleLeveledCompactionController {
    options: SimpleLeveledCompactionOptions,
}

impl SimpleLeveledCompactionController {
    pub fn new(options: SimpleLeveledCompactionOptions) -> Self {
        Self { options }
    }

    /// Generates a compaction task.
    ///
    /// Returns `None` if no compaction needs to be scheduled. The order of SSTs in the compaction task id vector matters.
    pub fn generate_compaction_task(
        &self,
        snapshot: &LsmStorageState,
    ) -> Option<SimpleLeveledCompactionTask> {
        // check if the size ratio is satisfied
        for i in 0..self.options.max_levels {
            // trigger a compaction of L0 and L1
            if i == 0
                && snapshot.l0_sstables.len() < self.options.level0_file_num_compaction_trigger
            {
                continue;
            }
            let upper_level_size = if i == 0 {
                snapshot.l0_sstables.len()
            } else {
                // levels start from 0
                snapshot.levels[i - 1].1.len()
            };
            let lower_level_size = snapshot.levels[i].1.len();
            let size_ratio = lower_level_size as f64 / upper_level_size as f64;
            if size_ratio < self.options.size_ratio_percent as f64 / 100.0 {
                let message = if i == 0 {
                    "L0 to L1".to_string()
                } else {
                    format!("L{} to L{}", i, i + 1)
                };
                println!(
                    "compaction triggered at {} with size ratio {}",
                    message, size_ratio,
                );
                return Some(SimpleLeveledCompactionTask {
                    upper_level: if i == 0 { None } else { Some(i - 1) },
                    upper_level_sst_ids: if i == 0 {
                        snapshot.l0_sstables.clone()
                    } else {
                        snapshot.levels[i - 1].1.clone()
                    },
                    lower_level: i,
                    lower_level_sst_ids: snapshot.levels[i].1.clone(),
                    is_lower_level_bottom_level: i == self.options.max_levels - 1,
                });
            }
        }
        None
    }

    /// Apply the compaction result.
    ///
    /// The compactor will call this function with the compaction task and the list of SST ids generated. This function applies the
    /// result and generates a new LSM state. The functions should only change `l0_sstables` and `levels` without changing memtables
    /// and `sstables` hash map. Though there should only be one thread running compaction jobs, you should think about the case
    /// where an L0 SST gets flushed while the compactor generates new SSTs, and with that in mind, you should do some sanity checks
    /// in your implementation.
    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &SimpleLeveledCompactionTask,
        output: &[usize],
    ) -> (LsmStorageState, Vec<usize>) {
        let mut files_to_remove = Vec::new();
        let mut new_snapshot = snapshot.clone();
        if let Some(upper_level) = task.upper_level {
            // L1+ compaction
            assert_eq!(
                task.upper_level_sst_ids, snapshot.levels[upper_level].1,
                "sst mismatched"
            );
            println!("L{} compaction output: {:?}", upper_level + 1, output);
            files_to_remove.extend(&task.upper_level_sst_ids);
            new_snapshot.levels[upper_level].1.clear();
        } else {
            // L0 compaction
            println!("L0 compaction output: {:?}", output);
            files_to_remove.extend(&task.upper_level_sst_ids);
            let mut l0_ssts_compacted = task
                .upper_level_sst_ids
                .iter()
                .copied()
                .collect::<HashSet<_>>();
            let new_l0_sstables = snapshot
                .l0_sstables
                .iter()
                .copied()
                .filter(|x| !l0_ssts_compacted.remove(x))
                .collect::<Vec<_>>();
            assert!(l0_ssts_compacted.is_empty());
            new_snapshot.l0_sstables = new_l0_sstables;
        }

        files_to_remove.extend(&new_snapshot.levels[task.lower_level].1);
        new_snapshot.levels[task.lower_level].1 = output.to_vec();
        (new_snapshot, files_to_remove)
    }
}
