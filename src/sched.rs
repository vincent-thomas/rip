//! State shared by all threads: work queues, pending count, parking.

use std::{
  io::{self},
  sync::{
    Condvar, Mutex,
    atomic::{AtomicBool, AtomicUsize, Ordering},
  },
  time::Duration,
};

use crossbeam_deque::{Injector, Steal, Stealer, Worker};
use lio::api::resource::Resource;

use crate::{
  cli::Config,
  walk::{DirWork, Work},
};

/// Lets idle threads sleep until new work arrives.
/// Whether lrg runs on several threads. A WASI component has one thread, so
/// there lrg runs one worker on the main thread and no lister threads.
pub(crate) const THREADS: bool = cfg!(not(target_os = "wasi"));

pub(crate) struct Parker {
  pub(crate) lock: Mutex<()>,
  pub(crate) cv: Condvar,
  pub(crate) sleepers: AtomicUsize,
}

impl Parker {
  pub(crate) fn new() -> Self {
    Self {
      lock: Mutex::new(()),
      cv: Condvar::new(),
      sleepers: AtomicUsize::new(0),
    }
  }

  /// Sleeps until woken or `timeout` passes. The timeout bounds the delay
  /// from a wake-up that races with going to sleep.
  pub(crate) fn park(&self, timeout: Duration) {
    let guard = self.lock.lock().unwrap_or_else(|e| e.into_inner());
    self.sleepers.fetch_add(1, Ordering::SeqCst);
    let _ = self.cv.wait_timeout(guard, timeout);
    self.sleepers.fetch_sub(1, Ordering::SeqCst);
  }

  pub(crate) fn wake(&self) {
    if self.sleepers.load(Ordering::SeqCst) > 0 {
      self.cv.notify_all();
    }
  }

  pub(crate) fn wake_one(&self) {
    if self.sleepers.load(Ordering::SeqCst) > 0 {
      self.cv.notify_one();
    }
  }
}

pub(crate) struct Shared {
  pub(crate) cfg: Config,
  pub(crate) injector: Injector<Work>,
  pub(crate) stealers: Vec<Stealer<Work>>,
  /// Opened directories waiting for a lister thread.
  pub(crate) listings: Injector<(DirWork, Resource)>,
  pub(crate) lister_park: Parker,
  /// Set once a worker needs listers; the main thread then starts them.
  pub(crate) want_listers: AtomicBool,
  pub(crate) main_park: Parker,
  /// Work items (directories and files) pushed and not yet finished.
  pub(crate) pending: AtomicUsize,
  pub(crate) park: Parker,
  pub(crate) matched: AtomicBool,
  pub(crate) errored: AtomicBool,
}

impl Shared {
  pub(crate) fn done(&self) -> bool {
    self.pending.load(Ordering::Acquire) == 0
  }

  /// Marks one work item finished. The last one wakes every parked thread so
  /// they can exit.
  pub(crate) fn finish_item(&self) {
    if self.pending.fetch_sub(1, Ordering::AcqRel) == 1 {
      self.park.wake();
      self.lister_park.wake();
      self.main_park.wake();
    }
  }

  pub(crate) fn find_work(&self, local: &Worker<Work>) -> Option<Work> {
    if let Some(w) = local.pop() {
      return Some(w);
    }
    loop {
      let s = self.injector.steal_batch_and_pop(local).or_else(|| {
        self.stealers.iter().map(|s| s.steal_batch_and_pop(local)).collect()
      });
      match s {
        Steal::Success(w) => return Some(w),
        Steal::Empty => return None,
        Steal::Retry => {}
      }
    }
  }

  pub(crate) fn error(&self, path: &[u8], err: &io::Error) {
    self.errored.store(true, Ordering::Relaxed);
    let shown = if path.is_empty() { b".".as_slice() } else { path };
    eprintln!("lrg: {}: {err}", String::from_utf8_lossy(shown));
  }
}
