//! Lister threads: list cold directories so a worker's ring keeps running.

use std::{
  io::{self},
  sync::atomic::Ordering,
  time::Duration,
};

use crossbeam_deque::Steal;
use lio::{
  Lio,
  api::{self, FileType, ReadDirBuf, resource::Resource},
};

use crate::{
  sched::Shared,
  walk::{Work, children},
  worker::{RDBUF_BYTES, RDBUF_ENTRIES},
};

/// Runs `lio` until the one operation behind `rx` completes.
pub(crate) fn wait_one<T>(
  lio: &Lio,
  mut rx: api::Receiver<T>,
) -> io::Result<T> {
  loop {
    if let Some(v) = rx.try_recv() {
      return Ok(v);
    }
    lio.run()?;
  }
}

/// Lists directories that workers opened, so a cold directory blocks a
/// lister and not a worker's ring.
pub(crate) struct Lister<'a> {
  pub(crate) shared: &'a Shared,
  pub(crate) lio: Lio,
  pub(crate) rdbuf: Option<ReadDirBuf>,
  pub(crate) names: Vec<u8>,
  pub(crate) entries: Vec<(usize, usize, Option<FileType>)>,
  pub(crate) work: Vec<Work>,
}

impl Lister<'_> {
  pub(crate) fn run(&mut self) {
    let shared = self.shared;
    loop {
      let (d, fd) = match shared.listings.steal() {
        Steal::Success(v) => v,
        Steal::Retry => continue,
        Steal::Empty => {
          if shared.done() {
            return;
          }
          shared.lister_park.park(Duration::from_millis(20));
          continue;
        }
      };
      match self.list(&fd) {
        Ok(()) => {
          children(
            &shared.cfg,
            &d,
            &fd,
            &self.names,
            &self.entries,
            &mut self.work,
          );
          let n = self.work.len();
          if n > 0 {
            shared.pending.fetch_add(n, Ordering::AcqRel);
            for w in self.work.drain(..) {
              shared.injector.push(w);
            }
            shared.park.wake();
          }
        }
        Err(e) => shared.error(&d.path, &e),
      }
      shared.finish_item();
    }
  }

  /// Reads all entries of directory `fd` into `names` and `entries`.
  #[hotpath::measure]
  pub(crate) fn list(&mut self, fd: &Resource) -> io::Result<()> {
    self.names.clear();
    self.entries.clear();
    loop {
      let buf = self.rdbuf.take().unwrap_or_else(|| {
        ReadDirBuf::with_capacity(RDBUF_BYTES, RDBUF_ENTRIES)
      });
      let rx = api::readdir(fd, buf).with_lio(&self.lio).send();
      let buf = wait_one(&self.lio, rx)??;
      for e in buf.iter() {
        self.entries.push((self.names.len(), e.name.len(), e.file_type));
        self.names.extend_from_slice(e.name);
      }
      let eof = buf.result.eof;
      self.rdbuf = Some(buf);
      if eof {
        return Ok(());
      }
    }
  }
}
