use crate::models::PoolMetrics;
use parking_lot::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

#[derive(Clone, Default)]
pub struct PoolTracker {
    inner: Arc<PoolTrackerInner>,
}

#[derive(Default)]
struct PoolTrackerInner {
    waiters: AtomicU64,
    timeouts: AtomicU64,
    acquired_total: AtomicU64,
    observed_active: Mutex<u64>,
}

pub struct WaiterGuard {
    tracker: PoolTracker,
}

impl Drop for WaiterGuard {
    fn drop(&mut self) {
        self.tracker
            .inner
            .waiters
            .fetch_sub(1, Ordering::Relaxed);
    }
}

impl PoolTracker {
    pub fn waiter(&self) -> WaiterGuard {
        self.inner.waiters.fetch_add(1, Ordering::Relaxed);
        WaiterGuard {
            tracker: self.clone(),
        }
    }

    pub fn acquired(&self) {
        self.inner.acquired_total.fetch_add(1, Ordering::Relaxed);
    }

    pub fn timeout(&self) {
        self.inner.timeouts.fetch_add(1, Ordering::Relaxed);
    }

    pub fn observe_active(&self, active: u64) -> u64 {
        let mut observed = self.inner.observed_active.lock();
        *observed = (*observed).max(active);
        *observed
    }

    pub fn metrics(&self, total: u64, idle: u64, max_size: u32) -> PoolMetrics {
        let active = total.saturating_sub(idle);
        PoolMetrics {
            active,
            idle,
            total,
            waiters: self.inner.waiters.load(Ordering::Relaxed),
            timeouts: self.inner.timeouts.load(Ordering::Relaxed),
            max_size,
            acquired_total: self.inner.acquired_total.load(Ordering::Relaxed),
        }
    }
}
