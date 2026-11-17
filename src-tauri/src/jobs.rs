use crate::error::{AppError, AppResult};
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Instant;

/// Long-running work (transfer, diff scan) answers `status` from a snapshot instead of holding
/// the UI request open, so progress, cancellation and a reload all work the same way.
pub const MAX_TRACKED_JOBS: usize = 24;

#[derive(Debug, Clone)]
pub struct ActiveOperation {
    pub connection_id: String,
    pub operation_id: String,
}

pub struct Job<P> {
    payload: Mutex<P>,
    cancelled: AtomicBool,
    finished: AtomicBool,
    started: Instant,
    /// Slots stay reserved so a nested guard can remove its own entry by index.
    active: Mutex<Vec<Option<ActiveOperation>>>,
}

impl<P: Clone> Job<P> {
    pub fn new(payload: P) -> Self {
        Self {
            payload: Mutex::new(payload),
            cancelled: AtomicBool::new(false),
            finished: AtomicBool::new(false),
            started: Instant::now(),
            active: Mutex::new(Vec::new()),
        }
    }

    pub fn snapshot(&self) -> P {
        self.payload.lock().clone()
    }

    pub fn edit<T>(&self, update: impl FnOnce(&mut P) -> T) -> T {
        update(&mut self.payload.lock())
    }

    pub fn elapsed_ms(&self) -> u64 {
        self.started.elapsed().as_millis() as u64
    }

    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Relaxed)
    }

    pub fn is_finished(&self) -> bool {
        self.finished.load(Ordering::Relaxed)
    }

    pub fn mark_finished(&self) {
        self.finished.store(true, Ordering::SeqCst);
    }

    /// Registers a driver operation so `cancel` can interrupt the batch that is in flight.
    pub fn track(&self, operation: ActiveOperation) -> ActiveGuard<'_, P> {
        let mut active = self.active.lock();
        active.push(Some(operation));
        ActiveGuard {
            job: self,
            index: active.len() - 1,
        }
    }

    fn take_active(&self) -> Vec<ActiveOperation> {
        self.active
            .lock()
            .iter_mut()
            .filter_map(|slot| slot.take())
            .collect()
    }
}

pub struct ActiveGuard<'a, P> {
    job: &'a Job<P>,
    index: usize,
}

impl<P> Drop for ActiveGuard<'_, P> {
    fn drop(&mut self) {
        let mut active = self.job.active.lock();
        if let Some(slot) = active.get_mut(self.index) {
            *slot = None;
        }
    }
}

pub struct JobTable<P> {
    jobs: Mutex<HashMap<String, Arc<Job<P>>>>,
}

impl<P: Clone> Default for JobTable<P> {
    fn default() -> Self {
        Self {
            jobs: Mutex::new(HashMap::new()),
        }
    }
}

impl<P: Clone> JobTable<P> {
    pub fn register(&self, id: impl Into<String>, payload: P) -> Arc<Job<P>> {
        let job = Arc::new(Job::new(payload));
        let mut jobs = self.jobs.lock();
        jobs.insert(id.into(), Arc::clone(&job));
        if jobs.len() > MAX_TRACKED_JOBS {
            let finished = jobs
                .iter()
                .filter(|(_, entry)| entry.is_finished())
                .map(|(key, _)| key.clone())
                .collect::<Vec<_>>();
            for key in finished.iter().take(jobs.len().saturating_sub(MAX_TRACKED_JOBS / 2)) {
                jobs.remove(key);
            }
        }
        job
    }

    pub fn get(&self, id: &str) -> AppResult<Arc<Job<P>>> {
        self.jobs
            .lock()
            .get(id)
            .cloned()
            .ok_or_else(|| AppError::validation("This operation is no longer tracked. Start it again to continue."))
    }

    pub fn cancel(&self, id: &str) -> AppResult<Vec<ActiveOperation>> {
        let job = self.get(id)?;
        job.cancelled.store(true, Ordering::SeqCst);
        Ok(job.take_active())
    }
}

#[cfg(test)]
mod job_tests {
    use super::*;

    #[derive(Clone, Debug, PartialEq)]
    struct Probe {
        done: u64,
    }

    #[test]
    fn registers_snapshots_and_cancels_a_job() {
        let table: JobTable<Probe> = JobTable::default();
        let job = table.register("op-1", Probe { done: 3 });
        job.edit(|payload| payload.done += 1);
        assert_eq!(job.snapshot(), Probe { done: 4 });
        assert_eq!(table.get("op-1").unwrap().snapshot(), Probe { done: 4 });
        assert!(table.cancel("op-1").unwrap().is_empty());
        assert!(job.is_cancelled());
        assert!(table.get("missing").is_err());
    }

    #[test]
    fn active_operations_are_reported_once_and_released_on_drop() {
        let table: JobTable<Probe> = JobTable::default();
        let job = table.register("op-2", Probe { done: 0 });
        {
            let _guard = job.track(ActiveOperation {
                connection_id: "conn".into(),
                operation_id: "conn-batch-1".into(),
            });
            let pending = table.cancel("op-2").unwrap();
            assert_eq!(pending.len(), 1);
            assert_eq!(pending[0].operation_id, "conn-batch-1");
            assert!(table.cancel("op-2").unwrap().is_empty());
        }
        assert_eq!(job.take_active().len(), 0);
    }

    #[test]
    fn prunes_finished_jobs_past_the_cap() {
        let table: JobTable<Probe> = JobTable::default();
        for index in 0..MAX_TRACKED_JOBS + 10 {
            let job = table.register(format!("op-{index}"), Probe { done: 0 });
            if index % 2 == 0 {
                job.mark_finished();
            }
        }
        let remaining = table.jobs.lock().len();
        assert!(
            remaining <= MAX_TRACKED_JOBS,
            "finished jobs should have been pruned, found {remaining}"
        );
    }
}
