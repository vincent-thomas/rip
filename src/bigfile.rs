//! Big files: split into ranges that all workers search, output in order.

use std::{
  io::{self},
  sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
  },
};

use lio::api::{self, resource::Resource};

use crate::{
  cli::Mode,
  output::{OUT_FLUSH, finish, flush, write_line},
  search::{SearchState, binary_cut, scan},
  walk::Work,
  worker::{Ev, Slot, Tail, WorkerThread},
};

/// Files at least this large are searched in ranges, on several threads.
pub(crate) const BIG_FILE: u64 = 8 << 20;

/// Size of one range of a big file.
pub(crate) const RANGE: u64 = 1 << 20;

/// Extra room in a range buffer for the end of the range's last line.
pub(crate) const RANGE_SLACK: usize = 64 * 1024;

/// Ranges one worker keeps in flight (their buffers are large).
pub(crate) const MAX_RANGES: usize = 4;

/// A file large enough to search in ranges on several threads. Each range
/// owns the lines that start inside it.
pub(crate) struct BigFile {
  pub(crate) path: Vec<u8>,
  pub(crate) explicit: bool,
  pub(crate) fd: Resource,
  pub(crate) size: u64,
  /// The result of each range, filled as ranges finish in any order.
  pub(crate) results: Vec<Mutex<Option<RangeResult>>>,
  /// Ranges not yet finished. The thread that finishes the last one writes
  /// the output, in file order.
  pub(crate) left: AtomicUsize,
}

#[derive(Default)]
pub(crate) struct RangeResult {
  /// Matching lines.
  pub(crate) count: u64,
  /// Lines in the range (counted only with line numbers).
  pub(crate) newlines: u64,
  /// The matching lines, each ended by a newline.
  pub(crate) lines: Vec<u8>,
  /// The line number of each matching line, within the range.
  pub(crate) line_nos: Vec<u64>,
  /// File offset of a NUL byte in the range: output stops there, as in
  /// ripgrep.
  pub(crate) binary_at: Option<u64>,
  /// Matching lines from the NUL byte on (not shown).
  pub(crate) binary_hits: u64,
}

/// Whether `data`, read from file offset `base`, holds all the lines of the
/// range that ends at `end`: it reaches `end`, and the line that holds byte
/// `end - 1` has ended. (Also complete at the end of the file.)
pub(crate) fn range_complete(data: &[u8], base: u64, end: u64) -> bool {
  let e = (end - base) as usize;
  data.len() >= e && memchr::memchr(b'\n', &data[e - 1..]).is_some()
}

/// The part of `data` (read from file offset `base`) that holds the lines
/// of the range `start..end` of a file of `size` bytes. A range owns the
/// lines that start inside it: from the first line start at or after
/// `start`, to the end of the line that holds byte `end - 1`. `base` is
/// `start - 1` (or 0 for the first range), so `data[0]` tells whether a line
/// starts at `start`.
pub(crate) fn owned_lines(
  data: &[u8],
  start: u64,
  end: u64,
  base: u64,
  size: u64,
) -> std::ops::Range<usize> {
  // Index of `end` in `data`.
  let e = (end - base) as usize;
  let from = if start == 0 {
    0
  } else {
    memchr::memchr(b'\n', data).map_or(data.len(), |j| j + 1)
  };
  let to = if end >= size {
    data.len()
  } else {
    memchr::memchr(b'\n', &data[e - 1..]).map_or(data.len(), |k| e - 1 + k + 1)
  };
  // No line starts in the range: one long line covers all of it.
  if from < e && from < to { from..to } else { 0..0 }
}

/// The parts of a worker that search a big file in ranges.
impl WorkerThread<'_> {
  pub(crate) fn read_range(
    &mut self,
    id: usize,
    fd: &Resource,
    buf: Vec<u8>,
    offset: u64,
  ) {
    let q = self.evq.clone();
    api::read_at(fd, Tail(buf), offset)
      .with_lio(&self.lio)
      .when_done(move |r| q.borrow_mut().push(Ev::RangeRead(id, r)));
  }

  pub(crate) fn file_stat(
    &mut self,
    id: usize,
    stat: io::Result<api::FileStat>,
  ) {
    let Some(Slot::File { path, fd: Some(fd), held, explicit, .. }) =
      &mut self.slots[id]
    else {
      unreachable!("file stat for an unopened file slot");
    };
    let data = std::mem::take(held);
    let size = match stat {
      Ok(st) if st.size >= BIG_FILE => st.size,
      // Not big (or unknown): go on block by block.
      _ => return self.file_more(id, data),
    };
    // Split the file into ranges that any worker can take.
    let n = size.div_ceil(RANGE) as usize;
    let big = Arc::new(BigFile {
      path: std::mem::take(path),
      explicit: *explicit,
      fd: fd.clone(),
      size,
      results: (0..n).map(|_| Mutex::new(None)).collect(),
      left: AtomicUsize::new(n),
    });
    self.recycle(data);
    self.shared.pending.fetch_add(n, Ordering::AcqRel);
    for idx in (0..n).rev() {
      self.local.push(Work::Range(big.clone(), idx));
    }
    self.shared.park.wake();
    self.release(id);
  }

  #[hotpath::measure]
  pub(crate) fn range_read(&mut self, id: usize, n: usize, data: Vec<u8>) {
    let Some(Slot::Range(big, idx)) = &self.slots[id] else {
      unreachable!("range read for a non-range slot");
    };
    let (big, idx) = (big.clone(), *idx);
    let start = idx as u64 * RANGE;
    let end = (start + RANGE).min(big.size);
    let base = start.saturating_sub(1);
    let eof = n == 0 || base + data.len() as u64 >= big.size;
    if !eof && !range_complete(&data, base, end) {
      let mut data = data;
      if data.len() == data.capacity() {
        data.reserve(RANGE_SLACK);
      }
      let offset = base + data.len() as u64;
      let fd = big.fd.clone();
      self.read_range(id, &fd, data, offset);
      return;
    }
    self.ranges -= 1;
    self.release(id);

    let lines = owned_lines(&data, start, end, base, big.size);
    let from = lines.start;
    let mut region = &data[lines];

    let cfg = &self.shared.cfg;
    let mut r = RangeResult::default();
    if !cfg.text
      && !(big.explicit && cfg.mode == Mode::Count)
      && let Some(z) = memchr::memchr(0, region)
    {
      // Binary: search only the lines before the NUL byte.
      let region_off = base + from as u64;
      r.binary_at = Some(region_off + z as u64);
      let cut = binary_cut(region, region_off, z);
      if big.explicit {
        let mut rest = SearchState::default();
        scan(cfg, &self.re, &region[cut..], &mut rest, &mut |_, _| {});
        r.binary_hits = rest.count;
      }
      region = &region[..cut];
    }
    let mut st = SearchState::default();
    let (lines, line_nos) = (&mut r.lines, &mut r.line_nos);
    scan(cfg, &self.re, region, &mut st, &mut |line_no, line| {
      line_nos.push(line_no);
      lines.extend_from_slice(line);
      lines.push(b'\n');
    });
    r.count = st.count;
    r.newlines = st.line_no;
    self.range_pool.push(data);
    self.range_done(&big, idx, r);
  }

  /// Stores the result of range `idx`. After the last range, writes the
  /// file's output in order.
  #[hotpath::measure]
  pub(crate) fn range_done(
    &mut self,
    big: &BigFile,
    idx: usize,
    r: RangeResult,
  ) {
    *big.results[idx].lock().unwrap_or_else(|e| e.into_inner()) = Some(r);
    if big.left.fetch_sub(1, Ordering::AcqRel) != 1 {
      return;
    }
    let cfg = &self.shared.cfg;
    let mut lines_before = 0;
    let mut count = 0;
    let mut binary_at = None;
    let mut binary_hits = 0;
    let mut headed = false;
    for slot in &big.results {
      let Some(r) = slot.lock().unwrap_or_else(|e| e.into_inner()).take()
      else {
        unreachable!("a range of a finished big file has no result");
      };
      count += r.count;
      if cfg.mode == Mode::Lines {
        let mut rest = &r.lines[..];
        for &no in &r.line_nos {
          let Some(nl) = memchr::memchr(b'\n', rest) else {
            unreachable!("a stored line has no newline");
          };
          write_line(
            cfg,
            &self.re,
            &big.path,
            lines_before + no,
            &rest[..nl],
            &mut headed,
            &mut self.out,
          );
          rest = &rest[nl + 1..];
        }
      }
      lines_before += r.newlines;
      if r.binary_at.is_some() {
        binary_at = r.binary_at;
        binary_hits = r.binary_hits;
        break;
      }
    }
    let st = SearchState { count, line_no: 0, binary_at, binary_hits, headed };
    if finish(cfg, &big.path, big.explicit, &st, &mut self.out) {
      self.shared.matched.store(true, Ordering::Relaxed);
    }
    if self.out.len() >= OUT_FLUSH {
      flush(&mut self.out);
    }
  }
}

#[cfg(test)]
mod tests {
  use super::{owned_lines, range_complete};

  /// The lines a range owns in `file`: reads it as a worker does (from one
  /// byte before `start`) and returns the owned text.
  fn owned(file: &[u8], start: u64, end: u64) -> &[u8] {
    let base = start.saturating_sub(1);
    let data = &file[base as usize..];
    let r = owned_lines(data, start, end, base, file.len() as u64);
    &data[r]
  }

  #[test]
  fn ranges_own_the_lines_that_start_in_them() {
    let file = b"aa\nbbbb\ncc\ndddd\n";
    // Byte offsets: "aa\n" 0..3, "bbbb\n" 3..8, "cc\n" 8..11, "dddd\n" 11..16.
    assert_eq!(owned(file, 0, 5), b"aa\nbbbb\n"); // "bbbb" starts at 3 < 5
    assert_eq!(owned(file, 5, 10), b"cc\n"); // "bbbb" started before 5
    assert_eq!(owned(file, 10, 16), b"dddd\n");
    // A line that starts exactly at the range start belongs to the range.
    assert_eq!(owned(file, 3, 8), b"bbbb\n");
    assert_eq!(owned(file, 8, 11), b"cc\n");
  }

  #[test]
  fn every_line_is_owned_by_exactly_one_range() {
    let mut file = Vec::new();
    for i in 0..500 {
      file.extend(std::iter::repeat_n(b'x', i * 37 % 91));
      file.push(b'\n');
    }
    file.extend_from_slice(b"last line, no newline");
    for size in [1, 7, 64, 100, 1000] {
      let mut joined = Vec::new();
      let mut start = 0;
      while start < file.len() as u64 {
        let end = (start + size).min(file.len() as u64);
        joined.extend_from_slice(owned(&file, start, end));
        start = end;
      }
      assert!(joined == file, "range size {size}");
    }
  }

  #[test]
  fn a_line_longer_than_the_range_leaves_it_empty() {
    let file = b"a\nxxxxxxxxxxxxxxxxxxxx\nb\n";
    assert_eq!(owned(file, 4, 12), b"");
  }

  #[test]
  fn complete_once_the_last_line_ends() {
    // Range 2..6 read from base 1: needs data through the newline after
    // byte 5.
    assert!(!range_complete(b"bcd", 1, 6)); // does not reach `end`
    assert!(!range_complete(b"bcdefg", 1, 6)); // the line has not ended
    assert!(range_complete(b"bcdef\n", 1, 6));
    assert!(range_complete(b"bcde\nxyz", 1, 6));
  }
}
