//! Formatting matches (paths, line numbers, colors) and writing to stdout.

use std::{
  io::{self, Write},
  sync::atomic::{AtomicBool, Ordering},
};

use regex::bytes::Regex;

use crate::{
  cli::{Config, Mode},
  search::SearchState,
};

/// Output is written once a thread has buffered this much.
pub(crate) const OUT_FLUSH: usize = 64 * 1024;

/// Set once any output was written.
pub(crate) static WROTE: AtomicBool = AtomicBool::new(false);

/// Output uses headings (set once at startup).
pub(crate) static HEADING: AtomicBool = AtomicBool::new(false);

/// Writes `out` to stdout and clears it.
#[hotpath::measure]
pub(crate) fn flush(out: &mut Vec<u8>) {
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

pub(crate) const PATH_COLOR: &[u8] = b"\x1b[0m\x1b[35m";

pub(crate) const LINE_NO_COLOR: &[u8] = b"\x1b[0m\x1b[32m";

pub(crate) const MATCH_COLOR: &[u8] = b"\x1b[0m\x1b[1m\x1b[31m";

pub(crate) const RESET: &[u8] = b"\x1b[0m";

/// Appends `part`, in `color` if colors are on.
pub(crate) fn colored(
  cfg: &Config,
  color: &[u8],
  part: &[u8],
  out: &mut Vec<u8>,
) {
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
pub(crate) fn write_line(
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

/// Writes the per-file output (for `-c` and `-l`, and the note that ends
/// the matches of a binary file). Returns whether the file matched.
pub(crate) fn finish(
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

pub(crate) fn itoa(mut n: u64, buf: &mut [u8; 20]) -> &[u8] {
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
