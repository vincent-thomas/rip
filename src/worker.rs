//! Worker threads: one lio ring each, many opens and reads in flight.

use std::{
  cell::RefCell,
  io::{self},
  rc::Rc,
  sync::{Arc, atomic::Ordering},
  time::{Duration, Instant},
};

use crossbeam_deque::Worker;
use lio::{
  BufResult, Lio,
  api::{self, FileMode, FileType, OpenFlags, ReadDirBuf, resource::Resource},
};
use regex::bytes::Regex;

use crate::{
  bigfile::{BigFile, MAX_RANGES, RANGE, RANGE_SLACK, RangeResult},
  output::{OUT_FLUSH, finish, flush},
  sched::{Shared, THREADS},
  search::{SearchState, search_lines},
  walk::{DirWork, Work, children},
};

/// Size of each read buffer. Most source files fit in one read; larger
/// files stream through the buffer.
pub(crate) const READ_CAP: usize = 256 * 1024;

/// How far a worker raises its depth while it waits on the disk.
pub(crate) const DEPTH_GROWTH: usize = 16;

/// A wait for completions at least this long counts as waiting on the disk.
pub(crate) const DISK_WAIT: Duration = Duration::from_micros(250);

/// Buffers larger than this (grown for very long lines) are not kept.
pub(crate) const POOL_MAX_CAP: usize = 64 << 20;

/// Directory listing buffer: bytes for names, and entries, per call. Most
/// directories fit in one call. (Measured: 8 KiB was much slower.)
pub(crate) const RDBUF_BYTES: usize = 32 * 1024;

pub(crate) const RDBUF_ENTRIES: usize = 1024;

/// A read buffer that the kernel fills after its current length, so the
/// partial last line of one block stays in place in front of the next.
pub(crate) struct Tail(pub(crate) Vec<u8>);

// SAFETY: the vector does not move its allocation under a shared borrow.
unsafe impl lio::IoBuf for Tail {
  fn as_slice(&self) -> &[u8] {
    &self.0
  }
}

// SAFETY: `as_uninit_mut` returns the spare capacity of the vector, and
// `commit` only extends the length by bytes the kernel wrote there.
unsafe impl lio::IoBufMut for Tail {
  fn as_uninit_mut(&mut self) -> &mut [std::mem::MaybeUninit<u8>] {
    self.0.spare_capacity_mut()
  }

  fn commit(&mut self, initialized: lio::Initialized<'_>) {
    let len = self.0.len();
    assert!(
      initialized.as_ptr() == self.0.as_mut_ptr().wrapping_add(len)
        && initialized.len() <= self.0.capacity() - len,
      "initialized proof does not match this buffer"
    );
    // SAFETY: the kernel initialized `initialized.len()` bytes right after
    // the current length, within capacity (checked above).
    unsafe { self.0.set_len(len + initialized.len()) }
  }
}

pub(crate) enum Ev {
  DirOpen(usize, io::Result<Resource>),
  DirRead(usize, io::Result<ReadDirBuf>),
  FileOpen(usize, io::Result<Resource>),
  FileRead(usize, BufResult<usize, Tail>),
  FileStat(usize, io::Result<api::FileStat>),
  RangeRead(usize, BufResult<usize, Tail>),
}

pub(crate) enum Slot {
  Dir {
    work: DirWork,
    fd: Option<Resource>,
    /// Names of the entries read so far, back to back.
    names: Vec<u8>,
    /// (start, length, type) of each entry in `names`.
    entries: Vec<(usize, usize, Option<FileType>)>,
  },
  File {
    path: Vec<u8>,
    fd: Option<Resource>,
    st: SearchState,
    /// File offset of the next read.
    off: u64,
    /// The first block, held while the file's size is checked.
    held: Vec<u8>,
    explicit: bool,
  },
  Range(Arc<BigFile>, usize),
}

impl Slot {
  pub(crate) fn path(&self) -> &[u8] {
    match self {
      Slot::Dir { work, .. } => &work.path,
      Slot::File { path, .. } => path,
      Slot::Range(big, _) => &big.path,
    }
  }
}

pub(crate) struct WorkerThread<'a> {
  pub(crate) shared: &'a Shared,
  /// A clone per thread keeps the regex cache on its fast path.
  pub(crate) re: Regex,
  pub(crate) local: Worker<Work>,
  pub(crate) lio: Lio,
  pub(crate) evq: Rc<RefCell<Vec<Ev>>>,
  pub(crate) slots: Vec<Option<Slot>>,
  pub(crate) free: Vec<usize>,
  pub(crate) inflight: usize,
  /// Current depth limit, between the configured depth and
  /// `DEPTH_GROWTH` times it.
  pub(crate) depth: usize,
  pub(crate) pool: Vec<Vec<u8>>,
  /// Buffers for ranges of big files.
  pub(crate) range_pool: Vec<Vec<u8>>,
  /// Ranges in flight.
  pub(crate) ranges: usize,
  pub(crate) rdbufs: Vec<ReadDirBuf>,
  /// Children of the directory being finished.
  pub(crate) children: Vec<Work>,
  pub(crate) out: Vec<u8>,
}

impl WorkerThread<'_> {
  pub(crate) fn alloc(&mut self, slot: Slot) -> usize {
    self.inflight += 1;
    if let Some(id) = self.free.pop() {
      self.slots[id] = Some(slot);
      id
    } else {
      self.slots.push(Some(slot));
      self.slots.len() - 1
    }
  }

  /// Frees slot `id`, without finishing its work item.
  pub(crate) fn take(&mut self, id: usize) -> Slot {
    self.inflight -= 1;
    self.free.push(id);
    let Some(slot) = self.slots[id].take() else {
      unreachable!("released an empty slot");
    };
    slot
  }

  /// Frees slot `id` and finishes its work item.
  pub(crate) fn release(&mut self, id: usize) -> Slot {
    let slot = self.take(id);
    self.shared.finish_item();
    slot
  }

  #[hotpath::measure]
  pub(crate) fn start(&mut self, work: Work) {
    let q = self.evq.clone();
    match work {
      Work::Dir(d) => {
        let (parent, name) = (d.parent.clone(), d.name.clone());
        let id = self.alloc(Slot::Dir {
          work: d,
          fd: None,
          names: Vec::new(),
          entries: Vec::new(),
        });
        api::openat(
          &parent,
          name,
          OpenFlags::READ | OpenFlags::DIRECTORY,
          FileMode::empty(),
        )
        .with_lio(&self.lio)
        .when_done(move |r| q.borrow_mut().push(Ev::DirOpen(id, r)));
      }
      Work::File(f) => {
        let slot = Slot::File {
          path: f.path,
          fd: None,
          st: SearchState::default(),
          off: 0,
          held: Vec::new(),
          explicit: f.explicit,
        };
        let id = self.alloc(slot);
        api::openat(&f.parent, f.name, OpenFlags::READ, FileMode::empty())
          .with_lio(&self.lio)
          .when_done(move |r| q.borrow_mut().push(Ev::FileOpen(id, r)));
      }
      Work::Range(big, idx) => {
        let start = idx as u64 * RANGE;
        let end = (start + RANGE).min(big.size);
        // Read from one byte before the range, to see whether a line starts
        // at its first byte.
        let base = start.saturating_sub(1);
        let cap = (end - base) as usize + RANGE_SLACK;
        let mut buf = self.range_pool.pop().unwrap_or_default();
        buf.clear();
        buf.reserve_exact(cap);
        let fd = big.fd.clone();
        self.ranges += 1;
        let id = self.alloc(Slot::Range(big, idx));
        self.read_range(id, &fd, buf, base);
      }
    }
  }

  pub(crate) fn read_dir(&mut self, id: usize, fd: &Resource, buf: ReadDirBuf) {
    let q = self.evq.clone();
    api::readdir(fd, buf)
      .with_lio(&self.lio)
      .when_done(move |r| q.borrow_mut().push(Ev::DirRead(id, r)));
  }

  pub(crate) fn read_file(
    &mut self,
    id: usize,
    fd: &Resource,
    buf: Vec<u8>,
    offset: u64,
  ) {
    let q = self.evq.clone();
    api::read_at(fd, Tail(buf), offset)
      .with_lio(&self.lio)
      .when_done(move |r| q.borrow_mut().push(Ev::FileRead(id, r)));
  }

  pub(crate) fn recycle(&mut self, mut buf: Vec<u8>) {
    if buf.capacity() <= POOL_MAX_CAP {
      buf.clear();
      self.pool.push(buf);
    }
  }

  pub(crate) fn handle(&mut self, ev: Ev) {
    match ev {
      Ev::DirOpen(id, Ok(fd))
        if THREADS && self.depth > self.shared.cfg.depth =>
      {
        // This thread waits on the disk, so listing may block too: hand the
        // directory to a lister thread and keep the ring running.
        let Slot::Dir { work, .. } = self.take(id) else {
          unreachable!("directory event for a non-directory slot");
        };
        self.shared.listings.push((work, fd));
        if self.shared.want_listers.swap(true, Ordering::AcqRel) {
          self.shared.lister_park.wake_one();
        } else {
          self.shared.main_park.wake();
        }
      }
      Ev::DirOpen(id, Ok(fd)) => {
        let Some(Slot::Dir { fd: slot_fd, .. }) = &mut self.slots[id] else {
          unreachable!("directory event for a non-directory slot");
        };
        *slot_fd = Some(fd.clone());
        let buf = self.rdbufs.pop().unwrap_or_else(|| {
          ReadDirBuf::with_capacity(RDBUF_BYTES, RDBUF_ENTRIES)
        });
        self.read_dir(id, &fd, buf);
      }
      Ev::DirRead(id, Ok(buf)) => {
        let Some(Slot::Dir { fd: Some(fd), names, entries, .. }) =
          &mut self.slots[id]
        else {
          unreachable!("directory event for an unopened directory slot");
        };
        for e in buf.iter() {
          entries.push((names.len(), e.name.len(), e.file_type));
          names.extend_from_slice(e.name);
        }
        if buf.result.eof {
          self.rdbufs.push(buf);
          self.finish_dir(id);
        } else {
          let fd = fd.clone();
          self.read_dir(id, &fd, buf);
        }
      }
      Ev::FileOpen(id, Ok(fd)) => {
        let Some(Slot::File { fd: slot_fd, .. }) = &mut self.slots[id] else {
          unreachable!("file event for a non-file slot");
        };
        *slot_fd = Some(fd.clone());
        let buf =
          self.pool.pop().unwrap_or_else(|| Vec::with_capacity(READ_CAP));
        self.read_file(id, &fd, buf, 0);
      }
      Ev::FileRead(id, (Ok(n), Tail(buf))) => self.file_read(id, n, buf),
      Ev::FileStat(id, stat) => self.file_stat(id, stat),
      Ev::RangeRead(id, (Ok(n), Tail(buf))) => self.range_read(id, n, buf),
      Ev::RangeRead(id, (Err(e), Tail(buf))) => {
        self.range_pool.push(buf);
        self.ranges -= 1;
        let Slot::Range(big, idx) = self.release(id) else {
          unreachable!("range event for a non-range slot");
        };
        self.shared.error(&big.path, &e);
        // Finish the range with no output, so the file is still written.
        self.range_done(&big, idx, RangeResult::default());
      }
      Ev::DirOpen(id, Err(e))
      | Ev::DirRead(id, Err(e))
      | Ev::FileOpen(id, Err(e)) => {
        let slot = self.release(id);
        self.shared.error(slot.path(), &e);
      }
      Ev::FileRead(id, (Err(e), Tail(buf))) => {
        self.recycle(buf);
        let slot = self.release(id);
        self.shared.error(slot.path(), &e);
      }
    }
  }

  #[hotpath::measure]
  pub(crate) fn file_read(&mut self, id: usize, n: usize, data: Vec<u8>) {
    let threads = self.shared.cfg.threads;
    let Some(Slot::File { fd: Some(fd), off, held, .. }) = &mut self.slots[id]
    else {
      unreachable!("file read for an unopened file slot");
    };
    *off += n as u64;
    if n > 0 && data.len() == data.capacity() {
      if *off == n as u64 && threads > 1 {
        // The first block is full: check the size, since a big file is
        // faster searched in ranges on several threads.
        *held = data;
        let q = self.evq.clone();
        api::fstat(fd)
          .with_lio(&self.lio)
          .when_done(move |r| q.borrow_mut().push(Ev::FileStat(id, r)));
        return;
      }
      self.file_more(id, data);
    } else {
      self.file_done(id, data, false);
    }
  }

  /// Handles a full block: searches its whole lines, keeps the partial last
  /// line, and reads the next block after it.
  #[hotpath::measure]
  pub(crate) fn file_more(&mut self, id: usize, mut data: Vec<u8>) {
    let cfg = &self.shared.cfg;
    let Some(Slot::File { path, fd: Some(fd), st, off, explicit, .. }) =
      &mut self.slots[id]
    else {
      unreachable!("file read for an unopened file slot");
    };
    match memchr::memrchr(b'\n', &data) {
      // One line is longer than the buffer: grow it.
      None => data.reserve(data.capacity()),
      Some(nl) => {
        let block_off = *off - data.len() as u64;
        if !search_lines(
          cfg,
          &self.re,
          path,
          &data[..=nl],
          block_off,
          false,
          *explicit,
          st,
          &mut self.out,
        ) {
          self.file_done(id, data, true);
          return;
        }
        data.copy_within(nl + 1.., 0);
        data.truncate(data.len() - nl - 1);
      }
    }
    let (fd, off) = (fd.clone(), *off);
    self.read_file(id, &fd, data, off);
  }

  /// Finishes a file: searches what is left in `data` (unless the search
  /// `stopped`) and writes the per-file output.
  #[hotpath::measure]
  pub(crate) fn file_done(&mut self, id: usize, data: Vec<u8>, stopped: bool) {
    let cfg = &self.shared.cfg;
    let Slot::File { path, fd, mut st, off, explicit, .. } = self.release(id)
    else {
      unreachable!("file read for a non-file slot");
    };
    // Closes the file.
    drop(fd);
    if !stopped {
      // The buffer holds the whole file if no earlier block was searched.
      let whole = off == data.len() as u64;
      let data_off = off - data.len() as u64;
      search_lines(
        cfg,
        &self.re,
        &path,
        &data,
        data_off,
        whole,
        explicit,
        &mut st,
        &mut self.out,
      );
    }
    if finish(cfg, &path, explicit, &st, &mut self.out) {
      self.shared.matched.store(true, Ordering::Relaxed);
    }
    self.recycle(data);
    if self.out.len() >= OUT_FLUSH {
      flush(&mut self.out);
    }
  }

  /// Queues the children of a listed directory.
  #[hotpath::measure]
  pub(crate) fn finish_dir(&mut self, id: usize) {
    let Some(Slot::Dir { work, fd: Some(fd), names, entries }) =
      &self.slots[id]
    else {
      unreachable!("finished an unopened directory slot");
    };
    children(&self.shared.cfg, work, fd, names, entries, &mut self.children);
    let n = self.children.len();
    for w in self.children.drain(..) {
      self.local.push(w);
    }
    self.shared.pending.fetch_add(n, Ordering::AcqRel);
    if n > 1 {
      // Other threads may be parked: let them steal.
      self.shared.park.wake();
    }
    self.release(id);
  }

  pub(crate) fn run(&mut self) {
    let mut events = Vec::new();
    loop {
      while self.inflight < self.depth {
        match self.shared.find_work(&self.local) {
          Some(w @ Work::Range(..)) if self.ranges >= MAX_RANGES => {
            // Enough large buffers in flight: leave it for another thread.
            self.local.push(w);
            self.shared.park.wake();
            break;
          }
          Some(w) => self.start(w),
          None => break,
        }
      }
      if self.inflight == 0 {
        if self.shared.done() {
          break;
        }
        // Nothing to do now: write what we have, then wait for work.
        flush(&mut self.out);
        self.shared.park.park(Duration::from_micros(500));
        continue;
      }
      // lio delivers a result the backend has at once during the submit
      // itself, so some may be here already. Those are not waiting, and with
      // them in hand the thread must not block: their follow-up operations
      // keep the disk busy.
      let ready = self.evq.borrow().len();
      let waiting = self.inflight - ready;
      let t0 = Instant::now();
      // The kernel does the I/O here.
      let ran = if ready > 0 {
        hotpath::measure_block!("lio_run", self.lio.try_run())
      } else {
        hotpath::measure_block!("lio_run", self.lio.run())
      };
      if let Err(e) = ran {
        eprintln!("lrg: io driver: {e}");
        std::process::exit(2);
      }
      let waited = t0.elapsed();
      std::mem::swap(&mut events, &mut *self.evq.borrow_mut());
      // When most operations finish at once, data comes from the cache and a
      // small depth keeps work spread across threads. When few finish after
      // a long wait, the thread waits on the disk: more operations in flight
      // keep it busy. (A short wait is the kernel finishing a cached open on
      // a helper thread, not the disk.) Only what the run delivered counts.
      let base = self.shared.cfg.depth;
      let delivered = events.len() - ready;
      if delivered * 2 < waiting && waited >= DISK_WAIT {
        self.depth = (self.depth * 2).min(base * DEPTH_GROWTH);
      } else if delivered >= waiting {
        self.depth = (self.depth / 2).max(base);
      }
      for ev in events.drain(..) {
        self.handle(ev);
      }
    }
    flush(&mut self.out);
  }
}
