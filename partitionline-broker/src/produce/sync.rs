use super::MAX_SYNC_CONCURRENCY;
use std::{
    io,
    panic::{AssertUnwindSafe, catch_unwind},
    sync::mpsc::{Receiver, SyncSender, sync_channel},
    thread::{Builder, JoinHandle},
};

const HELPER_STACK_BYTES: usize = 256 * 1024;

type Operation<T> = fn(&T) -> io::Result<()>;

fn failed<T>(job: &T, operation: Operation<T>) -> bool {
    !matches!(
        catch_unwind(AssertUnwindSafe(|| operation(job))),
        Ok(Ok(()))
    )
}

struct Helper<T> {
    submit: SyncSender<T>,
    complete: Receiver<(T, bool)>,
    thread: JoinHandle<()>,
}

impl<T: Send + 'static> Helper<T> {
    fn start(operation: Operation<T>) -> io::Result<Self> {
        let (submit, jobs) = sync_channel(1);
        let (completed, complete) = sync_channel(1);
        let thread = Builder::new()
            .name("pl-sync".to_owned())
            .stack_size(HELPER_STACK_BYTES)
            .spawn(move || {
                while let Ok(job) = jobs.recv() {
                    let failed = failed(&job, operation);
                    if completed.send((job, failed)).is_err() {
                        break;
                    }
                }
            })?;
        Ok(Self {
            submit,
            complete,
            thread,
        })
    }

    fn stop(self) {
        drop(self.submit);
        // Each helper has at most one outstanding reply, so its reply queue
        // can hold that file until the thread has exited.
        let _joined = self.thread.join();
        drop(self.complete);
    }
}

/// The actor syncs the first file itself and lends each helper one file per wave.
/// A helper returns the file along with its outcome so the staged append still
/// holds its lease through commit.
pub(super) struct Pool<T: Send + 'static> {
    helpers: Vec<Option<Helper<T>>>,
    operation: Operation<T>,
}

impl<T: Send + 'static> Pool<T> {
    pub(super) fn new(operation: Operation<T>) -> Self {
        Self {
            helpers: Vec::new(),
            operation,
        }
    }

    pub(super) fn wave<Work: ?Sized>(
        &mut self,
        work: &mut Work,
        width: usize,
        slot: impl Fn(&mut Work, usize) -> Option<&mut Option<T>>,
        allow_start: impl Fn(usize) -> bool,
    ) -> [bool; MAX_SYNC_CONCURRENCY] {
        let mut failures = [true; MAX_SYNC_CONCURRENCY];
        let width = width.min(MAX_SYNC_CONCURRENCY);
        match width {
            0 => return failures,
            1 => {
                if let Some(Some(first)) = slot(work, 0) {
                    failures[0] = failed(first, self.operation);
                }
                return failures;
            }
            _ => {}
        }
        let needed = width - 1;
        if needed > self.helpers.len()
            && self.helpers.try_reserve(needed - self.helpers.len()).is_ok()
        {
            self.helpers.resize_with(needed, || None);
        }

        let mut submitted = [false; MAX_SYNC_CONCURRENCY];
        for (index, submitted) in submitted.iter_mut().enumerate().take(width).skip(1) {
            let Some(slot) = slot(work, index) else {
                continue;
            };
            let Some(job) = slot.take() else { continue };
            let result = if allow_start(index) {
                self.submit(index - 1, job)
            } else {
                Err(job)
            };
            match result {
                Ok(()) => *submitted = true,
                Err(job) => {
                    failures[index] = failed(&job, self.operation);
                    *slot = Some(job);
                }
            }
        }
        if let Some(Some(first)) = slot(work, 0) {
            failures[0] = failed(first, self.operation);
        }
        for (index, submitted) in submitted.into_iter().enumerate().take(width).skip(1) {
            if submitted {
                // Even if the slot is missing, drain the helper before we
                // return. Missing completions keep that partition failed.
                let result = self.complete(index - 1);
                if let (Some(slot), Some((job, failed))) = (slot(work, index), result) {
                    *slot = Some(job);
                    failures[index] = failed;
                }
            }
        }
        failures
    }

    fn submit(&mut self, lane: usize, job: T) -> Result<(), T> {
        let Some(slot) = self.helpers.get_mut(lane) else {
            return Err(job);
        };
        if slot.is_none() {
            *slot = Helper::start(self.operation).ok();
        }
        let Some(helper) = slot else {
            return Err(job);
        };
        match helper.submit.send(job) {
            Ok(()) => Ok(()),
            Err(closed) => {
                if let Some(exited) = slot.take() {
                    exited.stop();
                }
                Err(closed.0)
            }
        }
    }

    fn complete(&mut self, lane: usize) -> Option<(T, bool)> {
        let slot = self.helpers.get_mut(lane)?;
        match slot.as_ref()?.complete.recv() {
            Ok(completion) => Some(completion),
            Err(_) => {
                if let Some(exited) = slot.take() {
                    exited.stop();
                }
                None
            }
        }
    }
}

impl<T: Send + 'static> Drop for Pool<T> {
    fn drop(&mut self) {
        for helper in self.helpers.drain(..).flatten() {
            helper.stop();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::mpsc::{TryRecvError, channel};

    #[test]
    fn single_file_creates_no_helper() {
        let mut pool = Pool::new(|_: &u8| Ok(()));
        let mut files = [Some(4)];
        assert!(!pool.wave(&mut files, 1, |files, i| files.get_mut(i), |_| true)[0]);
        assert!(pool.helpers.is_empty());
        assert_eq!(files, [Some(4)]);
    }

    #[test]
    fn drop_waits_for_helpers_and_closes_their_queues() {
        struct Job {
            started: SyncSender<()>,
            release: Receiver<()>,
        }
        let (started, running) = sync_channel(0);
        let (resume, release) = sync_channel(0);
        let mut pool = Pool::new(|job: &Job| {
            job.started.send(()).map_err(io::Error::other)?;
            job.release.recv().map_err(io::Error::other)
        });
        pool.helpers.resize_with(1, || None);
        assert!(pool.submit(0, Job { started, release }).is_ok());
        assert_eq!(running.recv(), Ok(()));

        let (shutdown, stopped) = channel();
        std::thread::scope(|scope| {
            let handle = scope.spawn(move || {
                drop(pool);
                assert_eq!(shutdown.send(()), Ok(()));
            });
            assert_eq!(stopped.try_recv(), Err(TryRecvError::Empty));
            assert_eq!(resume.send(()), Ok(()));
            assert_eq!(stopped.recv(), Ok(()));
            assert!(handle.join().is_ok());
        });
    }

    #[test]
    fn disconnected_helpers_discard_missing_completions_and_can_restart() {
        let mut pool = Pool::new(|_: &u8| Ok(()));
        let (submit, jobs) = sync_channel(1);
        let (completed, complete) = sync_channel(1);
        let thread = std::thread::spawn(move || {
            let _ = jobs.recv();
            drop(completed);
        });
        pool.helpers.push(Some(Helper {
            submit,
            complete,
            thread,
        }));

        let mut files = [Some(4), Some(9)];
        let results = pool.wave(&mut files, 2, |files, i| files.get_mut(i), |_| true);
        assert_eq!(results[..2], [false, true]);
        assert_eq!(files, [Some(4), None]);
        assert!(pool.helpers[0].is_none());

        files[1] = Some(12);
        let results = pool.wave(&mut files, 2, |files, i| files.get_mut(i), |_| true);
        assert_eq!(results[..2], [false, false]);
        assert_eq!(files, [Some(4), Some(12)]);
    }
}
