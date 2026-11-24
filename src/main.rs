//! lrg: a recursive line searcher built on lio.
//!
//! Each worker thread owns one `Lio` driver (one io_uring ring on Linux) and
//! keeps many directory and file operations in flight, so one kernel entry
//! submits and reaps work for many files. It then searches what arrived.
//! Directories and files sit in work-stealing deques, so idle threads take
//! work from busy ones, and park when there is none.
//!
//! Each worker adapts how many operations it keeps in flight: few while data
//! comes from the cache (to spread work across threads), many while it waits
//! on the disk (to keep the disk queue full).
//!
//! Listing a directory (`getdents64`) has no io_uring operation and blocks
//! when the directory is not cached. While a worker waits on the disk, it
//! hands opened directories to a pool of lister threads instead, so its ring
//! keeps running and many cold directories load in parallel.

use std::{
  cell::RefCell,
  ffi::{CString, OsStr},
  io::{self, IsTerminal, Write},
  os::unix::ffi::OsStrExt,
  path::Path,
  process::ExitCode,
  rc::Rc,
  sync::{
    Arc, Condvar, Mutex,
    atomic::{AtomicBool, AtomicUsize, Ordering},
  },
  time::{Duration, Instant},
};

use crossbeam_deque::{Injector, Steal, Stealer, Worker};
use ignore::{
  Match,
  gitignore::{Gitignore, GitignoreBuilder},
};
use lio::{
  BufResult, Lio,
  api::{self, FileMode, FileType, OpenFlags, ReadDirBuf, resource::Resource},
};
use regex::bytes::{Regex, RegexBuilder};

// Work items are allocated by one thread and often freed by another. glibc's
// allocator serializes that on arena locks; mimalloc does not.
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

/// Size of each read buffer. Most source files fit in one read; larger
/// files stream through the buffer.
const READ_CAP: usize = 256 * 1024;
/// Default number of operations one worker keeps in flight while data comes
/// from the cache.
const DEFAULT_DEPTH: usize = 16;
/// How far a worker raises its depth while it waits on the disk.
const DEPTH_GROWTH: usize = 16;
/// A wait for completions at least this long counts as waiting on the disk.
const DISK_WAIT: Duration = Duration::from_micros(250);
/// Buffers larger than this (grown for very long lines) are not kept.
const POOL_MAX_CAP: usize = 64 << 20;
/// Directory listing buffer: bytes for names, and entries, per call. Most
/// directories fit in one call. (Measured: 8 KiB was much slower.)
const RDBUF_BYTES: usize = 32 * 1024;
const RDBUF_ENTRIES: usize = 1024;
/// Files at least this large are searched in ranges, on several threads.
const BIG_FILE: u64 = 8 << 20;
/// Size of one range of a big file.
const RANGE: u64 = 1 << 20;
/// Extra room in a range buffer for the end of the range's last line.
const RANGE_SLACK: usize = 64 * 1024;
/// Ranges one worker keeps in flight (their buffers are large).
const MAX_RANGES: usize = 4;
/// Output is written once a thread has buffered this much.
const OUT_FLUSH: usize = 64 * 1024;

/// A read buffer that the kernel fills after its current length, so the
/// partial last line of one block stays in place in front of the next.
struct Tail(Vec<u8>);

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

#[derive(Clone, Copy, PartialEq, Eq)]
enum Mode {
  Lines,
  Count,
  Files,
}

struct Config {
  re: Regex,
  mode: Mode,
  line_numbers: bool,
  /// Show each file's path once, above its matches (as ripgrep on a
  /// terminal).
  heading: bool,
  /// Color paths, line numbers and matches.
  color: bool,
  with_filename: bool,
  hidden: bool,
  no_ignore: bool,
  text: bool,
  threads: usize,
  /// Directory lister threads.
  listers: usize,
  /// Operations one worker keeps in flight.
  depth: usize,
}

// ---------------------------------------------------------------------------
// Ignore rules
// ---------------------------------------------------------------------------

/// The ignore rules of one directory, linked to the rules of its parent.
struct IgnNode {
  parent: Option<Arc<IgnNode>>,
  gi: Gitignore,
  /// Length of the directory's display path prefix (with its separator).
  prefix: usize,
}

/// `Some(true)` if an ignore rule ignores `path`, `Some(false)` if a rule
/// whitelists it, `None` if no rule matches.
fn ignore_match(
  mut node: Option<&Arc<IgnNode>>,
  path: &[u8],
  is_dir: bool,
) -> Option<bool> {
  while let Some(n) = node {
    let rel = &path[n.prefix.min(path.len())..];
    match n.gi.matched(Path::new(OsStr::from_bytes(rel)), is_dir) {
      Match::Ignore(_) => return Some(true),
      Match::Whitelist(_) => return Some(false),
      Match::None => node = n.parent.as_ref(),
    }
  }
  None
}

// ---------------------------------------------------------------------------
// Work items and shared state
// ---------------------------------------------------------------------------

struct DirWork {
  parent: Resource,
  name: CString,
  path: Vec<u8>,
  ign: Option<Arc<IgnNode>>,
  in_git: bool,
}

struct FileWork {
  parent: Resource,
  name: CString,
  path: Vec<u8>,
  /// Named on the command line (binary files are handled differently).
  explicit: bool,
}

/// A file large enough to search in ranges on several threads. Each range
/// owns the lines that start inside it.
struct BigFile {
  path: Vec<u8>,
  explicit: bool,
  fd: Resource,
  size: u64,
  /// The result of each range, filled as ranges finish in any order.
  results: Vec<Mutex<Option<RangeResult>>>,
  /// Ranges not yet finished. The thread that finishes the last one writes
  /// the output, in file order.
  left: AtomicUsize,
}

#[derive(Default)]
struct RangeResult {
  /// Matching lines.
  count: u64,
  /// Lines in the range (counted only with line numbers).
  newlines: u64,
  /// The matching lines, each ended by a newline.
  lines: Vec<u8>,
  /// The line number of each matching line, within the range.
  line_nos: Vec<u64>,
  /// File offset of a NUL byte in the range: output stops there, as in
  /// ripgrep.
  binary_at: Option<u64>,
  /// Matching lines from the NUL byte on (not shown).
  binary_hits: u64,
}

/// `dir` + `/` + `name`, without a leading separator for the empty root.
fn join(dir: &[u8], name: &[u8]) -> Vec<u8> {
  let mut p = Vec::with_capacity(dir.len() + name.len() + 1);
  p.extend_from_slice(dir);
  if !dir.is_empty() && !dir.ends_with(b"/") {
    p.push(b'/');
  }
  p.extend_from_slice(name);
  p
}

/// Lets idle threads sleep until new work arrives.
struct Parker {
  lock: Mutex<()>,
  cv: Condvar,
  sleepers: AtomicUsize,
}

impl Parker {
  fn new() -> Self {
    Self {
      lock: Mutex::new(()),
      cv: Condvar::new(),
      sleepers: AtomicUsize::new(0),
    }
  }

  /// Sleeps until woken or `timeout` passes. The timeout bounds the delay
  /// from a wake-up that races with going to sleep.
  fn park(&self, timeout: Duration) {
    let guard = self.lock.lock().unwrap_or_else(|e| e.into_inner());
    self.sleepers.fetch_add(1, Ordering::SeqCst);
    let _ = self.cv.wait_timeout(guard, timeout);
    self.sleepers.fetch_sub(1, Ordering::SeqCst);
  }

  fn wake(&self) {
    if self.sleepers.load(Ordering::SeqCst) > 0 {
      self.cv.notify_all();
    }
  }

  fn wake_one(&self) {
    if self.sleepers.load(Ordering::SeqCst) > 0 {
      self.cv.notify_one();
    }
  }
}

enum Work {
  Dir(DirWork),
  File(FileWork),
  /// Range `.1` of a big file.
  Range(Arc<BigFile>, usize),
}

struct Shared {
  cfg: Config,
  injector: Injector<Work>,
  stealers: Vec<Stealer<Work>>,
  /// Opened directories waiting for a lister thread.
  listings: Injector<(DirWork, Resource)>,
  lister_park: Parker,
  /// Set once a worker needs listers; the main thread then starts them.
  want_listers: AtomicBool,
  main_park: Parker,
  /// Work items (directories and files) pushed and not yet finished.
  pending: AtomicUsize,
  park: Parker,
  matched: AtomicBool,
  errored: AtomicBool,
}

impl Shared {
  fn done(&self) -> bool {
    self.pending.load(Ordering::Acquire) == 0
  }

  /// Marks one work item finished. The last one wakes every parked thread so
  /// they can exit.
  fn finish_item(&self) {
    if self.pending.fetch_sub(1, Ordering::AcqRel) == 1 {
      self.park.wake();
      self.lister_park.wake();
      self.main_park.wake();
    }
  }

  fn find_work(&self, local: &Worker<Work>) -> Option<Work> {
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

  fn error(&self, path: &[u8], err: &io::Error) {
    self.errored.store(true, Ordering::Relaxed);
    let shown = if path.is_empty() { b".".as_slice() } else { path };
    eprintln!("lrg: {}: {err}", String::from_utf8_lossy(shown));
  }
}

/// Set once any output was written.
static WROTE: AtomicBool = AtomicBool::new(false);
/// Output uses headings (set once at startup).
static HEADING: AtomicBool = AtomicBool::new(false);

/// Writes `out` to stdout and clears it.
#[hotpath::measure]
fn flush(out: &mut Vec<u8>) {
  if out.is_empty() {
    return;
  }
  let mut stdout = io::stdout().lock();
  // Under headings, each file starts with a blank separator line; the first
  // file of the output does not need it.
  let first = !WROTE.swap(true, Ordering::Relaxed);
  let skip =
    usize::from(first && HEADING.load(Ordering::Relaxed) && out[0] == b'\n');
  if let Err(e) = stdout.write_all(&out[skip..]).and_then(|()| stdout.flush()) {
    if e.kind() == io::ErrorKind::BrokenPipe {
      std::process::exit(0);
    }
    eprintln!("lrg: {e}");
    std::process::exit(2);
  }
  out.clear();
}

// ---------------------------------------------------------------------------
// Searching
// ---------------------------------------------------------------------------

/// Search progress through one file, carried across the blocks of a file
/// that is read in more than one part.
#[derive(Default)]
struct SearchState {
  /// Matching lines so far.
  count: u64,
  /// Lines before the current block (counted only with line numbers).
  line_no: u64,
  /// File offset of the first NUL byte, once the search stopped there.
  binary_at: Option<u64>,
  /// Matching lines from the binary data on (not shown).
  binary_hits: u64,
  /// The heading for this file was written.
  headed: bool,
}

/// Searches `buf`, which holds whole lines (the last one may lack its
/// newline) and starts at file offset `off`, and appends output for `path`
/// to `out`.
///
/// A NUL byte makes the file binary. As in ripgrep, the search then stops
/// before the line that holds it. `whole` says that `buf` is the whole file:
/// then the scan for a NUL byte runs only if a line matched, since most files
/// do not match and their output does not depend on it.
///
/// Returns `false` when the rest of the file need not be searched: the file
/// is binary, or one match is enough.
#[allow(clippy::too_many_arguments)]
#[hotpath::measure]
fn search_lines(
  cfg: &Config,
  re: &Regex,
  path: &[u8],
  buf: &[u8],
  off: u64,
  whole: bool,
  explicit: bool,
  st: &mut SearchState,
  out: &mut Vec<u8>,
) -> bool {
  let mut headed = st.headed;
  // ripgrep counts all matches of a file named on the command line.
  if cfg.text || (explicit && cfg.mode == Mode::Count) {
    let more = scan(cfg, re, buf, st, &mut |n, l| {
      write_line(cfg, re, path, n, l, &mut headed, out)
    });
    st.headed = headed;
    return more;
  }
  if whole {
    let mark = (out.len(), st.count, st.line_no, headed);
    let more = scan(cfg, re, buf, st, &mut |n, l| {
      write_line(cfg, re, path, n, l, &mut headed, out)
    });
    st.headed = headed;
    if st.count == mark.1 {
      return more;
    }
    let Some(z) = memchr::memchr(0, buf) else { return more };
    // Binary: take back the output and search again up to the NUL byte.
    out.truncate(mark.0);
    (st.count, st.line_no, st.headed) = (mark.1, mark.2, mark.3);
    return stop_at_binary(cfg, re, path, buf, off, z, explicit, st, out);
  }
  match memchr::memchr(0, buf) {
    None => {
      let more = scan(cfg, re, buf, st, &mut |n, l| {
        write_line(cfg, re, path, n, l, &mut headed, out)
      });
      st.headed = headed;
      more
    }
    Some(z) => stop_at_binary(cfg, re, path, buf, off, z, explicit, st, out),
  }
}

/// ripgrep reads files in blocks of this size and drops the whole block that
/// holds a NUL byte. A NUL in the first block hides all of a file's lines.
const BINARY_HEAD: u64 = 64 * 1024;

/// Where output stops for a NUL byte at index `z` of `buf`, which starts at
/// file offset `off`: before the line that holds it, or before everything
/// when it is in the file's first block.
fn binary_cut(buf: &[u8], off: u64, z: usize) -> usize {
  if off + (z as u64) < BINARY_HEAD {
    return 0;
  }
  memchr::memrchr(b'\n', &buf[..z]).map_or(0, |i| i + 1)
}

/// Searches the lines of `buf` before its binary data (NUL byte at `z`),
/// and records where that starts. For a file named on the command line, it
/// also counts the matches in the rest of `buf`, which ripgrep reports as
/// "binary file matches". Returns `false`.
#[allow(clippy::too_many_arguments)]
fn stop_at_binary(
  cfg: &Config,
  re: &Regex,
  path: &[u8],
  buf: &[u8],
  off: u64,
  z: usize,
  explicit: bool,
  st: &mut SearchState,
  out: &mut Vec<u8>,
) -> bool {
  let cut = binary_cut(buf, off, z);
  let mut headed = st.headed;
  scan(cfg, re, &buf[..cut], st, &mut |n, l| {
    write_line(cfg, re, path, n, l, &mut headed, out)
  });
  st.headed = headed;
  if explicit {
    let mut rest = SearchState::default();
    scan(cfg, re, &buf[cut..], &mut rest, &mut |_, _| {});
    st.binary_hits += rest.count;
  }
  st.binary_at = Some(off + z as u64);
  false
}

const PATH_COLOR: &[u8] = b"\x1b[0m\x1b[35m";
const LINE_NO_COLOR: &[u8] = b"\x1b[0m\x1b[32m";
const MATCH_COLOR: &[u8] = b"\x1b[0m\x1b[1m\x1b[31m";
const RESET: &[u8] = b"\x1b[0m";

/// Appends `part`, in `color` if colors are on.
fn colored(cfg: &Config, color: &[u8], part: &[u8], out: &mut Vec<u8>) {
  if cfg.color {
    out.extend_from_slice(color);
    out.extend_from_slice(part);
    out.extend_from_slice(RESET);
  } else {
    out.extend_from_slice(part);
  }
}

/// Appends one matching line: `path:line_no:line`, or under a heading, with
/// the parts that `cfg` asks for. `headed` tracks whether this file's
/// heading was written.
#[allow(clippy::too_many_arguments)]
fn write_line(
  cfg: &Config,
  re: &Regex,
  path: &[u8],
  line_no: u64,
  line: &[u8],
  headed: &mut bool,
  out: &mut Vec<u8>,
) {
  if cfg.heading {
    if !*headed {
      *headed = true;
      // A blank line between files. `flush` drops it before the first.
      out.push(b'\n');
      colored(cfg, PATH_COLOR, path, out);
      out.push(b'\n');
    }
  } else if cfg.with_filename {
    colored(cfg, PATH_COLOR, path, out);
    out.push(b':');
  }
  if cfg.line_numbers {
    colored(cfg, LINE_NO_COLOR, itoa(line_no, &mut [0; 20]), out);
    out.push(b':');
  }
  if cfg.color {
    let mut at = 0;
    for m in re.find_iter(line) {
      if m.is_empty() {
        continue;
      }
      out.extend_from_slice(&line[at..m.start()]);
      colored(cfg, MATCH_COLOR, m.as_bytes(), out);
      at = m.end();
    }
    out.extend_from_slice(&line[at..]);
  } else {
    out.extend_from_slice(line);
  }
  out.push(b'\n');
}

/// The search loop. Counts matching lines in `st` and, in `Lines` mode,
/// passes each one to `emit` with its line number (1-based, after the
/// `st.line_no` lines before `buf`; counted only with line numbers).
#[hotpath::measure]
fn scan(
  cfg: &Config,
  re: &Regex,
  buf: &[u8],
  st: &mut SearchState,
  emit: &mut dyn FnMut(u64, &[u8]),
) -> bool {
  let len = buf.len();
  let mut pos = 0;
  let mut counted_to = 0;

  while pos < len {
    let Some(m) = re.find_at(buf, pos) else { break };
    let start = m.start();
    let ls =
      memchr::memrchr(b'\n', &buf[pos..start]).map_or(pos, |i| pos + i + 1);
    if ls >= len {
      break;
    }
    let le = memchr::memchr(b'\n', &buf[start..]).map_or(len, |i| start + i);
    // A match that runs past the end of its line counts only if the line
    // matches by itself.
    let hit = m.end() <= le || re.is_match(&buf[ls..le]);
    if hit {
      st.count += 1;
      match cfg.mode {
        Mode::Files => return false,
        Mode::Count => {}
        Mode::Lines => {
          if cfg.line_numbers {
            st.line_no +=
              memchr::memchr_iter(b'\n', &buf[counted_to..ls]).count() as u64;
            counted_to = ls;
          }
          emit(st.line_no + 1, &buf[ls..le]);
        }
      }
    }
    pos = le + 1;
  }
  if cfg.line_numbers {
    st.line_no += memchr::memchr_iter(b'\n', &buf[counted_to..]).count() as u64;
  }
  true
}

/// Writes the per-file output (for `-c` and `-l`, and the note that ends
/// the matches of a binary file). Returns whether the file matched.
fn finish(
  cfg: &Config,
  path: &[u8],
  explicit: bool,
  st: &SearchState,
  out: &mut Vec<u8>,
) -> bool {
  // For a file named on the command line, matches in binary data count.
  let hits = if explicit { st.count + st.binary_hits } else { st.count };
  if hits == 0 {
    return false;
  }
  match cfg.mode {
    Mode::Lines => {
      if let Some(z) = st.binary_at.filter(|_| !explicit || st.binary_hits > 0)
      {
        // As ripgrep, on stdout.
        if cfg.with_filename {
          colored(cfg, PATH_COLOR, path, out);
          out.extend_from_slice(b": ");
        }
        if !explicit {
          out.extend_from_slice(
            b"WARNING: stopped searching binary file after match",
          );
        } else {
          out.extend_from_slice(b"binary file matches");
        }
        out.extend_from_slice(b" (found \"\\0\" byte around offset ");
        out.extend_from_slice(itoa(z, &mut [0; 20]));
        out.extend_from_slice(b")\n");
      }
    }
    // ripgrep leaves a binary file out of the counts of a recursive search.
    Mode::Count if st.binary_at.is_some() => return false,
    Mode::Files => {
      colored(cfg, PATH_COLOR, path, out);
      out.push(b'\n');
    }
    Mode::Count => {
      if cfg.with_filename {
        colored(cfg, PATH_COLOR, path, out);
        out.push(b':');
      }
      out.extend_from_slice(itoa(st.count, &mut [0; 20]));
      out.push(b'\n');
    }
  }
  true
}

fn itoa(mut n: u64, buf: &mut [u8; 20]) -> &[u8] {
  let mut i = buf.len();
  loop {
    i -= 1;
    buf[i] = b'0' + (n % 10) as u8;
    n /= 10;
    if n == 0 {
      return &buf[i..];
    }
  }
}

// ---------------------------------------------------------------------------
// Directory entries
// ---------------------------------------------------------------------------

/// Filters the entries of directory `d` (open as `fd`) and returns the work
/// for its children.
#[hotpath::measure]
fn children(
  cfg: &Config,
  d: &DirWork,
  fd: &Resource,
  names: &[u8],
  entries: &[(usize, usize, Option<FileType>)],
  out: &mut Vec<Work>,
) {
  let name = |i: usize| -> &[u8] {
    let (s, l, _) = entries[i];
    &names[s..s + l]
  };
  let has = |n: &[u8]| (0..entries.len()).any(|i| name(i) == n);
  let in_git = d.in_git || has(b".git");
  let ign = if cfg.no_ignore {
    d.ign.clone()
  } else {
    load_ignores(&d.path, &has, in_git, d.ign.clone())
  };

  for (i, entry) in entries.iter().enumerate() {
    let n = name(i);
    let child = join(&d.path, n);
    let ft = match entry.2 {
      Some(FileType::Unknown) | None => {
        match std::fs::symlink_metadata(OsStr::from_bytes(&child)) {
          Ok(m) if m.is_dir() => FileType::Directory,
          Ok(m) if m.is_file() => FileType::File,
          _ => continue,
        }
      }
      Some(ft) => ft,
    };
    let is_dir = match ft {
      FileType::Directory => true,
      FileType::File => false,
      _ => continue,
    };
    // A whitelist rule wins over the hidden-file rule, as in ripgrep.
    match ignore_match(ign.as_ref(), &child, is_dir) {
      Some(true) => continue,
      Some(false) => {}
      None if !cfg.hidden && n.first() == Some(&b'.') => continue,
      None => {}
    }
    let Ok(cname) = CString::new(n) else { continue };
    out.push(if is_dir {
      Work::Dir(DirWork {
        parent: fd.clone(),
        name: cname,
        path: child,
        ign: ign.clone(),
        in_git,
      })
    } else {
      Work::File(FileWork {
        parent: fd.clone(),
        name: cname,
        path: child,
        explicit: false,
      })
    });
  }
}

// ---------------------------------------------------------------------------
// Lister threads
// ---------------------------------------------------------------------------

/// Runs `lio` until the one operation behind `rx` completes.
fn wait_one<T>(lio: &Lio, mut rx: api::Receiver<T>) -> io::Result<T> {
  loop {
    if let Some(v) = rx.try_recv() {
      return Ok(v);
    }
    lio.run()?;
  }
}

/// Lists directories that workers opened, so a cold directory blocks a
/// lister and not a worker's ring.
struct Lister<'a> {
  shared: &'a Shared,
  lio: Lio,
  rdbuf: Option<ReadDirBuf>,
  names: Vec<u8>,
  entries: Vec<(usize, usize, Option<FileType>)>,
  work: Vec<Work>,
}

impl Lister<'_> {
  fn run(&mut self) {
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
  fn list(&mut self, fd: &Resource) -> io::Result<()> {
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

// ---------------------------------------------------------------------------
// Worker threads
// ---------------------------------------------------------------------------

enum Ev {
  DirOpen(usize, io::Result<Resource>),
  DirRead(usize, io::Result<ReadDirBuf>),
  FileOpen(usize, io::Result<Resource>),
  FileRead(usize, BufResult<usize, Tail>),
  FileStat(usize, io::Result<api::FileStat>),
  RangeRead(usize, BufResult<usize, Tail>),
}

enum Slot {
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
  fn path(&self) -> &[u8] {
    match self {
      Slot::Dir { work, .. } => &work.path,
      Slot::File { path, .. } => path,
      Slot::Range(big, _) => &big.path,
    }
  }
}

struct WorkerThread<'a> {
  shared: &'a Shared,
  /// A clone per thread keeps the regex cache on its fast path.
  re: Regex,
  local: Worker<Work>,
  lio: Lio,
  evq: Rc<RefCell<Vec<Ev>>>,
  slots: Vec<Option<Slot>>,
  free: Vec<usize>,
  inflight: usize,
  /// Current depth limit, between the configured depth and
  /// `DEPTH_GROWTH` times it.
  depth: usize,
  pool: Vec<Vec<u8>>,
  /// Buffers for ranges of big files.
  range_pool: Vec<Vec<u8>>,
  /// Ranges in flight.
  ranges: usize,
  rdbufs: Vec<ReadDirBuf>,
  /// Children of the directory being finished.
  children: Vec<Work>,
  out: Vec<u8>,
}

impl WorkerThread<'_> {
  fn alloc(&mut self, slot: Slot) -> usize {
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
  fn take(&mut self, id: usize) -> Slot {
    self.inflight -= 1;
    self.free.push(id);
    let Some(slot) = self.slots[id].take() else {
      unreachable!("released an empty slot");
    };
    slot
  }

  /// Frees slot `id` and finishes its work item.
  fn release(&mut self, id: usize) -> Slot {
    let slot = self.take(id);
    self.shared.finish_item();
    slot
  }

  #[hotpath::measure]
  fn start(&mut self, work: Work) {
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

  fn read_range(
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

  fn read_dir(&mut self, id: usize, fd: &Resource, buf: ReadDirBuf) {
    let q = self.evq.clone();
    api::readdir(fd, buf)
      .with_lio(&self.lio)
      .when_done(move |r| q.borrow_mut().push(Ev::DirRead(id, r)));
  }

  fn read_file(&mut self, id: usize, fd: &Resource, buf: Vec<u8>, offset: u64) {
    let q = self.evq.clone();
    api::read_at(fd, Tail(buf), offset)
      .with_lio(&self.lio)
      .when_done(move |r| q.borrow_mut().push(Ev::FileRead(id, r)));
  }

  fn recycle(&mut self, mut buf: Vec<u8>) {
    if buf.capacity() <= POOL_MAX_CAP {
      buf.clear();
      self.pool.push(buf);
    }
  }

  fn handle(&mut self, ev: Ev) {
    match ev {
      Ev::DirOpen(id, Ok(fd)) if self.depth > self.shared.cfg.depth => {
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
  fn file_read(&mut self, id: usize, n: usize, data: Vec<u8>) {
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
  fn file_more(&mut self, id: usize, mut data: Vec<u8>) {
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
  fn file_done(&mut self, id: usize, data: Vec<u8>, stopped: bool) {
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

  fn file_stat(&mut self, id: usize, stat: io::Result<api::FileStat>) {
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
  fn range_read(&mut self, id: usize, n: usize, data: Vec<u8>) {
    let Some(Slot::Range(big, idx)) = &self.slots[id] else {
      unreachable!("range read for a non-range slot");
    };
    let (big, idx) = (big.clone(), *idx);
    let start = idx as u64 * RANGE;
    let end = (start + RANGE).min(big.size);
    let base = start.saturating_sub(1);
    let eof = n == 0 || base + data.len() as u64 >= big.size;
    // Index of `end` in `data`.
    let e = (end - base) as usize;
    // The range is complete once it reaches `end` and the line that holds
    // byte `end - 1` has ended, or the file has.
    let complete = eof
      || (data.len() >= e && memchr::memchr(b'\n', &data[e - 1..]).is_some());
    if !complete {
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

    // The lines this range owns: from the first line start at or after
    // `start`, to the end of the line that holds byte `end - 1`.
    let from = if start == 0 {
      0
    } else {
      memchr::memchr(b'\n', &data).map_or(data.len(), |j| j + 1)
    };
    let to = if end >= big.size {
      data.len()
    } else {
      memchr::memchr(b'\n', &data[e - 1..])
        .map_or(data.len(), |k| e - 1 + k + 1)
    };
    let region = if from < e && from < to { &data[from..to] } else { &[][..] };

    let cfg = &self.shared.cfg;
    let mut r = RangeResult::default();
    let mut region = region;
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
  fn range_done(&mut self, big: &BigFile, idx: usize, r: RangeResult) {
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

  /// Queues the children of a listed directory.
  #[hotpath::measure]
  fn finish_dir(&mut self, id: usize) {
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

  fn run(&mut self) {
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
      let before = self.inflight;
      let t0 = Instant::now();
      // The kernel does the I/O here.
      if let Err(e) = hotpath::measure_block!("lio_run", self.lio.run()) {
        eprintln!("lrg: io driver: {e}");
        std::process::exit(2);
      }
      let waited = t0.elapsed();
      std::mem::swap(&mut events, &mut *self.evq.borrow_mut());
      // When most operations finish at once, data comes from the cache and a
      // small depth keeps work spread across threads. When few finish after
      // a long wait, the thread waits on the disk: more operations in flight
      // keep it busy. (A short wait is the kernel finishing a cached open on
      // a helper thread, not the disk.)
      let base = self.shared.cfg.depth;
      if events.len() * 2 < before && waited >= DISK_WAIT {
        self.depth = (self.depth * 2).min(base * DEPTH_GROWTH);
      } else if events.len() >= before {
        self.depth = (self.depth / 2).max(base);
      }
      for ev in events.drain(..) {
        self.handle(ev);
      }
    }
    flush(&mut self.out);
  }
}

/// Builds the ignore rules for directory `path` from its ignore files.
#[hotpath::measure]
fn load_ignores(
  path: &[u8],
  has: &dyn Fn(&[u8]) -> bool,
  in_git: bool,
  parent: Option<Arc<IgnNode>>,
) -> Option<Arc<IgnNode>> {
  // Lowest precedence first: later rules win.
  let mut files: Vec<&[u8]> = Vec::new();
  if in_git && has(b".git") {
    files.push(b".git/info/exclude");
  }
  if in_git && has(b".gitignore") {
    files.push(b".gitignore");
  }
  if has(b".ignore") {
    files.push(b".ignore");
  }
  if has(b".rgignore") {
    files.push(b".rgignore");
  }
  if files.is_empty() {
    return parent;
  }
  let mut b = GitignoreBuilder::new("");
  for f in files {
    let p = join(path, f);
    let p = if p.is_empty() { b".".to_vec() } else { p };
    // A missing or unreadable ignore file adds no rules.
    let _ = b.add(Path::new(OsStr::from_bytes(&p)));
  }
  match b.build() {
    Ok(gi) if !gi.is_empty() => {
      Some(Arc::new(IgnNode { parent, gi, prefix: join(path, b"").len() }))
    }
    _ => parent,
  }
}

// ---------------------------------------------------------------------------
// Command line
// ---------------------------------------------------------------------------

/// `parse_args` returns this as its error when help was asked for.
const HELP: &str = "help";

const USAGE: &str = "\
usage: lrg [OPTIONS] PATTERN [PATH ...]
       lrg [OPTIONS] -e PATTERN ... [PATH ...]

options:
  -e, --regexp PATTERN     a pattern to search for (repeatable)
  -F, --fixed-strings      treat patterns as literal strings
  -i, --ignore-case        case insensitive search
  -S, --smart-case         case insensitive unless the pattern has uppercase
  -w, --word-regexp        match only whole words
  -n, --line-number        show line numbers (default on a terminal)
  -N, --no-line-number     do not show line numbers
      --heading            show each path once, above its matches
                           (default on a terminal)
      --no-heading         show the path on each line
      --color WHEN         color output: always, never or auto (default)
  -c, --count              show the count of matching lines per file
  -l, --files-with-matches show only the paths of files that match
  -H, --with-filename      show the file path for each match
  -I, --no-filename        do not show file paths
  -., --hidden             search hidden files and directories
      --no-ignore          do not use .gitignore, .ignore or .rgignore
  -u                       -u: --no-ignore, -uu: also --hidden, -uuu: also -a
  -a, --text               search binary files as text
  -j, --threads N          number of worker threads (default: CPUs)
      --depth N            I/O operations each thread keeps in flight
                           while data is cached (default: 16)
      --listers N          threads that list directories while the disk is
                           slow (default: 4 x threads)
  -h, --help               show this help
";

fn parse_args() -> Result<(Config, Vec<Vec<u8>>), String> {
  let args: Vec<Vec<u8>> =
    std::env::args_os().skip(1).map(|a| a.as_bytes().to_vec()).collect();
  let mut patterns: Vec<String> = Vec::new();
  let mut positional: Vec<Vec<u8>> = Vec::new();
  let (mut fixed, mut icase, mut smart, mut word) =
    (false, false, false, false);
  let mut line_numbers: Option<bool> = None;
  let mut heading: Option<bool> = None;
  let mut color: Option<bool> = None;
  let mut mode = Mode::Lines;
  let mut with_filename: Option<bool> = None;
  let (mut hidden, mut no_ignore, mut text) = (false, false, false);
  let mut threads = std::thread::available_parallelism().map_or(4, |n| n.get());
  let mut depth = DEFAULT_DEPTH;
  let mut listers: Option<usize> = None;

  let mut i = 0;
  let mut only_positional = false;
  let take = |i: &mut usize, flag: &str| -> Result<Vec<u8>, String> {
    *i += 1;
    args.get(*i).cloned().ok_or_else(|| format!("{flag} needs a value"))
  };
  let utf8 = |v: Vec<u8>| {
    String::from_utf8(v).map_err(|_| "pattern is not valid UTF-8".to_string())
  };

  while i < args.len() {
    let a = args[i].clone();
    if only_positional || a.first() != Some(&b'-') || a == b"-" {
      positional.push(a);
      i += 1;
      continue;
    }
    match a.as_slice() {
      b"--" => only_positional = true,
      b"--regexp" => patterns.push(utf8(take(&mut i, "--regexp")?)?),
      b"--fixed-strings" => fixed = true,
      b"--ignore-case" => icase = true,
      b"--smart-case" => smart = true,
      b"--word-regexp" => word = true,
      b"--line-number" => line_numbers = Some(true),
      b"--no-line-number" => line_numbers = Some(false),
      b"--heading" => heading = Some(true),
      b"--no-heading" => heading = Some(false),
      b"--color" => {
        color = match take(&mut i, "--color")?.as_slice() {
          b"always" => Some(true),
          b"never" => Some(false),
          b"auto" => None,
          _ => return Err("--color needs always, never or auto".to_string()),
        }
      }
      b"--count" => mode = Mode::Count,
      b"--files-with-matches" => mode = Mode::Files,
      b"--with-filename" => with_filename = Some(true),
      b"--no-filename" => with_filename = Some(false),
      b"--hidden" => hidden = true,
      b"--no-ignore" => no_ignore = true,
      b"--text" => text = true,
      b"--threads" => {
        threads = String::from_utf8_lossy(&take(&mut i, "--threads")?)
          .parse()
          .map_err(|_| "--threads needs a number".to_string())?
      }
      b"--depth" => {
        depth = String::from_utf8_lossy(&take(&mut i, "--depth")?)
          .parse()
          .map_err(|_| "--depth needs a number".to_string())?
      }
      b"--listers" => {
        listers = Some(
          String::from_utf8_lossy(&take(&mut i, "--listers")?)
            .parse()
            .map_err(|_| "--listers needs a number".to_string())?,
        )
      }
      b"--help" => return Err(HELP.to_string()),
      long if long.starts_with(b"--") => {
        return Err(format!(
          "unknown option {}",
          String::from_utf8_lossy(long)
        ));
      }
      short => {
        // A cluster of short flags, such as -inw or -uu.
        let mut j = 1;
        while j < short.len() {
          match short[j] {
            b'F' => fixed = true,
            b'i' => icase = true,
            b'S' => smart = true,
            b'w' => word = true,
            b'n' => line_numbers = Some(true),
            b'N' => line_numbers = Some(false),
            b'c' => mode = Mode::Count,
            b'l' => mode = Mode::Files,
            b'H' => with_filename = Some(true),
            b'I' => with_filename = Some(false),
            b'.' => hidden = true,
            b'a' => text = true,
            b'u' => {
              if no_ignore && hidden {
                text = true;
              } else if no_ignore {
                hidden = true;
              }
              no_ignore = true;
            }
            b'h' => return Err(HELP.to_string()),
            b'e' | b'j' => {
              let flag = short[j];
              let rest = &short[j + 1..];
              let val = if rest.is_empty() {
                take(&mut i, "-e/-j")?
              } else {
                rest.to_vec()
              };
              if flag == b'e' {
                patterns.push(utf8(val)?);
              } else {
                threads = String::from_utf8_lossy(&val)
                  .parse()
                  .map_err(|_| "-j needs a number".to_string())?;
              }
              break;
            }
            c => return Err(format!("unknown option -{}", c as char)),
          }
          j += 1;
        }
      }
    }
    i += 1;
  }

  if patterns.is_empty() {
    if positional.is_empty() {
      return Err(String::new());
    }
    patterns.push(utf8(positional.remove(0))?);
  }
  let parts: Vec<String> = patterns
    .iter()
    .map(|p| if fixed { regex::escape(p) } else { p.clone() })
    .map(|p| if word { format!(r"\b(?:{p})\b") } else { p })
    .collect();
  let pattern = if parts.len() == 1 {
    parts.into_iter().next().unwrap_or_default()
  } else {
    parts.iter().map(|p| format!("(?:{p})")).collect::<Vec<_>>().join("|")
  };
  if smart && !icase {
    icase = !patterns.iter().any(|p| p.chars().any(char::is_uppercase));
  }
  let re = RegexBuilder::new(&pattern)
    .multi_line(true)
    .case_insensitive(icase)
    .size_limit(100 << 20)
    .dfa_size_limit(1000 << 20)
    .build()
    .map_err(|e| e.to_string())?;

  // Without a filename choice, show names unless the only path is one file.
  let with_filename = with_filename.unwrap_or_else(|| {
    !(positional.len() == 1
      && std::fs::metadata(OsStr::from_bytes(&positional[0]))
        .is_ok_and(|m| !m.is_dir()))
  });

  let tty = io::stdout().is_terminal();
  let cfg = Config {
    re,
    mode,
    // As ripgrep: line numbers, headings and colors on a terminal.
    line_numbers: line_numbers.unwrap_or(tty),
    heading: heading.unwrap_or(tty) && with_filename && mode == Mode::Lines,
    color: color.unwrap_or(tty),
    with_filename,
    hidden,
    no_ignore,
    text,
    threads: threads.max(1),
    depth: depth.max(1),
    listers: listers.unwrap_or(4 * threads).max(1),
  };
  Ok((cfg, positional))
}

/// Whether some ancestor of `path` (not `path` itself) holds a `.git`.
fn parent_in_git(path: &Path) -> bool {
  let Ok(abs) = std::fs::canonicalize(path) else { return false };
  abs.ancestors().skip(1).any(|a| a.join(".git").exists())
}

#[hotpath::main]
fn main() -> ExitCode {
  let (cfg, roots) = match parse_args() {
    Ok(v) => v,
    Err(msg) => {
      if msg == HELP {
        print!("{USAGE}");
        return ExitCode::SUCCESS;
      }
      if msg.is_empty() {
        eprint!("{USAGE}");
        return ExitCode::from(2);
      }
      eprintln!("lrg: {msg}");
      return ExitCode::from(2);
    }
  };

  let injector = Injector::new();
  let mut pending = 0;
  let mut early_error = false;
  // No path means the current directory, shown without a "./" prefix.
  let roots: Vec<(Vec<u8>, Vec<u8>)> = if roots.is_empty() {
    vec![(b".".to_vec(), Vec::new())]
  } else {
    roots.into_iter().map(|r| (r.clone(), r)).collect()
  };
  for (open_path, display) in roots {
    let os = OsStr::from_bytes(&open_path);
    let Ok(name) = CString::new(open_path.clone()) else { continue };
    match std::fs::metadata(os) {
      Ok(m) if m.is_dir() => {
        let in_git = parent_in_git(Path::new(os));
        injector.push(Work::Dir(DirWork {
          parent: Resource::cwd(),
          name,
          path: display,
          ign: None,
          in_git,
        }));
        pending += 1;
      }
      Ok(_) => {
        injector.push(Work::File(FileWork {
          parent: Resource::cwd(),
          name,
          path: display,
          explicit: true,
        }));
        pending += 1;
      }
      Err(e) => {
        eprintln!("lrg: {}: {e}", String::from_utf8_lossy(&open_path));
        early_error = true;
      }
    }
  }

  let workers: Vec<Worker<Work>> =
    (0..cfg.threads).map(|_| Worker::new_lifo()).collect();
  let listers = cfg.listers;
  HEADING.store(cfg.heading, Ordering::Relaxed);
  let shared = Shared {
    cfg,
    injector,
    listings: Injector::new(),
    lister_park: Parker::new(),
    want_listers: AtomicBool::new(false),
    main_park: Parker::new(),
    stealers: workers.iter().map(Worker::stealer).collect(),
    pending: AtomicUsize::new(pending),
    park: Parker::new(),
    matched: AtomicBool::new(false),
    errored: AtomicBool::new(early_error),
  };

  let new_lio = |cap: usize| match Lio::new(cap) {
    Ok(l) => l,
    Err(e) => {
      eprintln!("lrg: cannot start io driver: {e}");
      std::process::exit(2);
    }
  };

  std::thread::scope(|s| {
    for local in workers {
      let shared = &shared;
      s.spawn(move || {
        let depth = shared.cfg.depth;
        let lio = new_lio(depth * DEPTH_GROWTH * 2);
        let mut t = WorkerThread {
          shared,
          re: shared.cfg.re.clone(),
          local,
          lio,
          evq: Rc::new(RefCell::new(Vec::with_capacity(depth))),
          slots: Vec::with_capacity(depth),
          free: Vec::new(),
          inflight: 0,
          depth,
          pool: Vec::new(),
          range_pool: Vec::new(),
          ranges: 0,
          rdbufs: Vec::new(),
          children: Vec::new(),
          out: Vec::with_capacity(2 * OUT_FLUSH),
        };
        t.run();
        // The process exits next and frees everything. Tearing down the ring
        // and buffers here would only add a few milliseconds at the end.
        std::mem::forget(t);
      });
    }
    // Start the listers only once a worker waits on the disk: a search of
    // cached data never needs them.
    while !shared.done() && !shared.want_listers.load(Ordering::Acquire) {
      shared.main_park.park(Duration::from_millis(50));
    }
    if shared.want_listers.load(Ordering::Acquire) {
      for _ in 0..listers {
        let shared = &shared;
        s.spawn(move || {
          let mut t = Lister {
            shared,
            lio: new_lio(4),
            rdbuf: None,
            names: Vec::new(),
            entries: Vec::new(),
            work: Vec::new(),
          };
          t.run();
          std::mem::forget(t);
        });
      }
    }
  });

  // As ripgrep: 2 after any error, else 0 on a match, else 1.
  if shared.errored.load(Ordering::Relaxed) {
    ExitCode::from(2)
  } else if shared.matched.load(Ordering::Relaxed) {
    ExitCode::SUCCESS
  } else {
    ExitCode::from(1)
  }
}
