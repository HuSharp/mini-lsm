use crate::lsm_storage::{LsmStorageInner, MiniLsm};

impl LsmStorageInner {
    pub fn dump_structure(&self) {
        println!("----------------- Dump Structure -----------------");
        let snapshot = self.state.read();
        // print mem-table
        if !snapshot.memtable.is_empty() {
            println!("Mem-table: {:?}", snapshot.memtable);
        }
        if !snapshot.imm_memtables.is_empty() {
            println!("Imm-mem-tables: {:?}", snapshot.imm_memtables);
        }
        if !snapshot.l0_sstables.is_empty() {
            println!(
                "L0 ({}): {:?}",
                snapshot.l0_sstables.len(),
                snapshot.l0_sstables,
            );
        }
        for (level, files) in &snapshot.levels {
            println!("L{level} ({}): {:?}", files.len(), files);
        }
        println!("----------------- Dump Structure finished -----------------");
    }
}

impl MiniLsm {
    pub fn dump_structure(&self) {
        self.inner.dump_structure()
    }
}
