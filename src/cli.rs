//! Command-line arguments and the search configuration.

use std::{
  ffi::OsStr,
  io::{self, IsTerminal},
  os::unix::ffi::OsStrExt,
};

use regex::bytes::{Regex, RegexBuilder};

/// Default number of operations one worker keeps in flight while data comes
/// from the cache.
pub(crate) const DEFAULT_DEPTH: usize = 16;

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum Mode {
  Lines,
  Count,
  Files,
}

pub(crate) struct Config {
  pub(crate) re: Regex,
  pub(crate) mode: Mode,
  pub(crate) line_numbers: bool,
  /// Show each file's path once, above its matches (as ripgrep on a
  /// terminal).
  pub(crate) heading: bool,
  /// Color paths, line numbers and matches.
  pub(crate) color: bool,
  pub(crate) with_filename: bool,
  pub(crate) hidden: bool,
  pub(crate) no_ignore: bool,
  pub(crate) text: bool,
  pub(crate) threads: usize,
  /// Directory lister threads.
  pub(crate) listers: usize,
  /// Operations one worker keeps in flight.
  pub(crate) depth: usize,
}

/// `parse_args` returns this as its error when help was asked for.
pub(crate) const HELP: &str = "help";

pub(crate) const USAGE: &str = "\
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

pub(crate) fn parse_args() -> Result<(Config, Vec<Vec<u8>>), String> {
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

#[cfg(test)]
impl Config {
  /// A configuration for unit tests: `pattern` in `mode`, piped output.
  pub(crate) fn for_test(pattern: &str, mode: Mode) -> Self {
    Config {
      re: RegexBuilder::new(pattern).multi_line(true).build().unwrap(),
      mode,
      line_numbers: true,
      heading: false,
      color: false,
      with_filename: true,
      hidden: false,
      no_ignore: false,
      text: false,
      threads: 1,
      listers: 1,
      depth: DEFAULT_DEPTH,
    }
  }
}
