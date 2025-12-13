use std::{ops::Range, sync::{Arc, Condvar, Mutex, MutexGuard}};

struct RwMapSyncInner {
    next_claim_id: u64,
    pending_claims: Vec<Claim>,
    hold_claims: Vec<Claim>,
}

struct RwMapInner {
    sync: Mutex<RwMapSyncInner>,
    condvar: Condvar,
}

#[derive(Clone)]
pub struct RwMap {
    inner: Arc<RwMapInner>,
}

struct Claim {
    id: u64,
    range: Range<u64>,
    writer: bool,
}

impl RwMap {
    pub fn new() -> Self {
        RwMap {
            inner: Arc::new(RwMapInner {
                sync: Mutex::new(RwMapSyncInner {
                    next_claim_id: 0,
                    pending_claims: Vec::new(),
                    hold_claims: Vec::new(),
                }),
                condvar: Condvar::new(),
            }),
        }
    }
}

pub struct RwMapLock {
    map: RwMap,
    claim_id: u64,
    range: Range<u64>,
    writer: bool,
}

impl RwMap {
    pub fn lock(&self, range: Range<u64>, writer: bool) -> RwMapLock {
        let claim_id = self.claim(range.clone(), writer);
        self.wait_lock(claim_id);
        RwMapLock {
            map: self.clone(),
            claim_id,
            range,
            writer,
        }
    }

    pub fn try_lock(&self, range: Range<u64>, writer: bool) -> Option<RwMapLock> {
        let claim_id = self.claim(range.clone(), writer);
        let (locked, mut sync) = self.check_lock(claim_id);
        if locked {
            drop(sync);
            Some(RwMapLock {
                map: self.clone(),
                claim_id,
                range,
                writer,
            })
        } else {
            let claim_index = sync.pending_claims.iter().position(|c| c.id == claim_id).unwrap();
            sync.pending_claims.swap_remove(claim_index);
            drop(sync);
            None
        }
    }

    fn claim(&self, range: Range<u64>, writer: bool) -> u64 {
        if range.start >= range.end {
            panic!("Invalid range for RwMapLock");
        }
        let mut sync = self.inner.sync.lock().unwrap();
        let claim_id = sync.next_claim_id;
        // TODO: handle wraparound
        sync.next_claim_id += 1;
        sync.pending_claims.push(Claim {
            id: claim_id,
            range: range.clone(),
            writer,
        });
        drop(sync);
        claim_id
    }

    fn check_pending(&self) {
        let mut sync = self.inner.sync.lock().unwrap();
        let mut changes = false;
        loop {
            let next_pending_claim_id = sync.pending_claims.iter().map(|c| c.id).min();
            let Some(next_id) = next_pending_claim_id else { break; };
            let claim_index = sync.pending_claims.iter().position(|c| c.id == next_id).unwrap();
            let claim = &sync.pending_claims[claim_index];
            let conflict = sync.hold_claims.iter().any(|held| {
                if claim.writer || held.writer {
                    !(claim.range.end <= held.range.start || claim.range.start >= held.range.end)
                } else {
                    false
                }
            });
            if conflict { break; }
            let claim = sync.pending_claims.swap_remove(claim_index);
            sync.hold_claims.push(claim);
            changes = true;
        }
        if changes {
            self.inner.condvar.notify_all();
        }
    }

    fn check_lock(&self, claim_id: u64) -> (bool, MutexGuard<'_, RwMapSyncInner>) {
        self.check_pending();
        let sync = self.inner.sync.lock().unwrap();
        let result = sync.hold_claims.iter().any(|c| c.id == claim_id);
        (result, sync)
    }

    fn wait_lock(&self, claim_id: u64) {
        self.check_pending();
        let mut sync = self.inner.sync.lock().unwrap();
        while !sync.hold_claims.iter().any(|c| c.id == claim_id) {
            sync = self.inner.condvar.wait(sync).unwrap();
        }
    }

    fn release_lock(&self, claim_id: u64) {
        let mut sync = self.inner.sync.lock().unwrap();
        let claim_index = sync.hold_claims.iter().position(|c| c.id == claim_id).unwrap();
        sync.hold_claims.swap_remove(claim_index);
        drop(sync);
        self.check_pending();
    }
}

impl Drop for RwMapLock {
    fn drop(&mut self) {
        self.map.release_lock(self.claim_id);
    }
}

impl RwMapLock {
    pub fn release(self) {
        drop(self)
    }

    pub fn range(&self) -> &Range<u64> {
        &self.range
    }

    pub fn is_writer(&self) -> bool {
        self.writer
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rwmap_locking() {
        let rwmap = RwMap::new();

        // Test multiple readers on same range
        let lock1 = rwmap.lock(0..10, false);
        assert!(!lock1.is_writer());

        let lock2 = rwmap.try_lock(0..10, false);
        assert!(lock2.is_some()); // Multiple readers allowed

        let lock3 = rwmap.try_lock(5..15, false);
        assert!(lock3.is_some()); // Overlapping readers allowed

        // Test writer blocks when readers exist
        let lock4 = rwmap.try_lock(0..10, true);
        assert!(lock4.is_none()); // Writer blocked by readers

        drop(lock1);
        drop(lock2);
        drop(lock3);

        // Test writer succeeds when no readers
        let lock5 = rwmap.try_lock(0..10, true);
        assert!(lock5.is_some());

        // Test reader blocked by writer
        let lock6 = rwmap.try_lock(5..15, false);
        assert!(lock6.is_none()); // Reader blocked by writer

        drop(lock5);

        // Test non-overlapping ranges work independently
        let _lock7 = rwmap.lock(0..10, false);
        let lock8 = rwmap.try_lock(10..20, true);
        assert!(lock8.is_some()); // Non-overlapping writer allowed
    }
}
