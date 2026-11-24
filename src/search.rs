//! Searching a buffer of lines, and binary-file handling as in ripgrep.

use regex::bytes::Regex;

use crate::{
  cli::{Config, Mode},
  output::write_line,
};

/// Search progress through one file, carried across the blocks of a file
/// that is read in more than one part.
#[derive(Default)]
pub(crate) struct SearchState {
  /// Matching lines so far.
  pub(crate) count: u64,
  /// Lines before the current block (counted only with line numbers).
  pub(crate) line_no: u64,
  /// File offset of the first NUL byte, once the search stopped there.
  pub(crate) binary_at: Option<u64>,
  /// Matching lines from the binary data on (not shown).
  pub(crate) binary_hits: u64,
  /// The heading for this file was written.
  pub(crate) headed: bool,
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
pub(crate) fn search_lines(
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
pub(crate) const BINARY_HEAD: u64 = 64 * 1024;

/// Where output stops for a NUL byte at index `z` of `buf`, which starts at
/// file offset `off`: before the line that holds it, or before everything
/// when it is in the file's first block.
pub(crate) fn binary_cut(buf: &[u8], off: u64, z: usize) -> usize {
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
pub(crate) fn stop_at_binary(
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

/// The search loop. Counts matching lines in `st` and, in `Lines` mode,
/// passes each one to `emit` with its line number (1-based, after the
/// `st.line_no` lines before `buf`; counted only with line numbers).
#[hotpath::measure]
pub(crate) fn scan(
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

#[cfg(test)]
mod tests {
  use super::{SearchState, binary_cut, scan, search_lines};
  use crate::cli::{Config, Mode};

  /// The matching lines of `buf` with their line numbers.
  fn lines(pattern: &str, buf: &[u8]) -> Vec<(u64, String)> {
    let cfg = Config::for_test(pattern, Mode::Lines);
    let mut found = Vec::new();
    let mut st = SearchState::default();
    scan(&cfg, &cfg.re, buf, &mut st, &mut |n, l| {
      found.push((n, String::from_utf8_lossy(l).into_owned()))
    });
    assert_eq!(st.count as usize, found.len());
    found
  }

  #[test]
  fn finds_each_matching_line_once() {
    let buf = b"one\ntwo two\nthree\ntwo";
    assert_eq!(lines("two", buf), [(2, "two two".into()), (4, "two".into())]);
  }

  #[test]
  fn anchors_apply_per_line() {
    assert_eq!(lines("^b$", b"a\nb\nab\n"), [(2, "b".into())]);
  }

  #[test]
  fn a_match_across_lines_counts_only_if_a_line_matches_alone() {
    // `a\sb` matches "a\nb" across the line break; no single line matches.
    assert_eq!(lines(r"a\sb", b"xa\nbx\n"), []);
    // The next line still gets searched.
    assert_eq!(lines(r"a\sb", b"xa\nbx\na b\n"), [(3, "a b".into())]);
  }

  #[test]
  fn line_numbers_continue_across_blocks() {
    let cfg = Config::for_test("x", Mode::Lines);
    let mut st = SearchState::default();
    let mut nos = Vec::new();
    for block in [&b"a\nx\n"[..], b"b\nc\nx\n"] {
      scan(&cfg, &cfg.re, block, &mut st, &mut |n, _| nos.push(n));
    }
    assert_eq!(nos, [2, 5]);
  }

  #[test]
  fn binary_cut_hides_the_first_block() {
    // A NUL in the first 64 KiB hides every line, as in ripgrep.
    assert_eq!(binary_cut(b"line\n\0", 0, 5), 0);
    // Later, output stops before the line that holds the NUL.
    assert_eq!(binary_cut(b"line\nab\0", 1 << 20, 7), 5);
  }

  #[test]
  fn binary_file_found_by_the_walk_is_skipped() {
    let cfg = Config::for_test("needle", Mode::Lines);
    let mut st = SearchState::default();
    let mut out = Vec::new();
    let buf = b"needle\n\0\n";
    let more =
      search_lines(&cfg, &cfg.re, b"f", buf, 0, true, false, &mut st, &mut out);
    assert!(!more);
    assert!(out.is_empty());
    assert_eq!(st.binary_at, Some(7));
  }
}
