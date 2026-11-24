//! End-to-end tests: run the `lrg` binary on generated directory trees.

use std::{
  fs,
  path::{Path, PathBuf},
  process::{Command, Output},
  sync::atomic::{AtomicUsize, Ordering},
};

/// A temporary directory, removed on drop.
struct Tree(PathBuf);

impl Tree {
  fn new() -> Self {
    static N: AtomicUsize = AtomicUsize::new(0);
    let dir = std::env::temp_dir().join(format!(
      "lrg-test-{}-{}",
      std::process::id(),
      N.fetch_add(1, Ordering::Relaxed)
    ));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).unwrap();
    Tree(dir)
  }

  fn file(&self, rel: &str, data: impl AsRef<[u8]>) -> &Self {
    let p = self.0.join(rel);
    fs::create_dir_all(p.parent().unwrap()).unwrap();
    fs::write(p, data).unwrap();
    self
  }

  fn dir(&self, rel: &str) -> &Self {
    fs::create_dir_all(self.0.join(rel)).unwrap();
    self
  }

  fn run(&self, args: &[&str]) -> Output {
    run_in(&self.0, args)
  }
}

impl Drop for Tree {
  fn drop(&mut self) {
    let _ = fs::remove_dir_all(&self.0);
  }
}

fn run_in(dir: &Path, args: &[&str]) -> Output {
  Command::new(env!("CARGO_BIN_EXE_lrg"))
    .args(args)
    .current_dir(dir)
    .output()
    .unwrap()
}

/// Stdout lines, sorted: threads finish files in any order.
fn lines(out: &Output) -> Vec<String> {
  let mut v: Vec<String> =
    String::from_utf8_lossy(&out.stdout).lines().map(str::to_owned).collect();
  v.sort();
  v
}

#[test]
fn finds_lines_in_a_tree() {
  let t = Tree::new();
  t.file("a.txt", "one\nneedle two\nthree\n")
    .file("sub/b.txt", "needle\nno\nneedle again")
    .file("sub/c.txt", "nothing here\n");
  let out = t.run(&["needle"]);
  assert_eq!(out.status.code(), Some(0));
  assert_eq!(
    lines(&out),
    ["a.txt:needle two", "sub/b.txt:needle", "sub/b.txt:needle again"]
  );

  let out = t.run(&["-n", "needle"]);
  assert_eq!(
    lines(&out),
    ["a.txt:2:needle two", "sub/b.txt:1:needle", "sub/b.txt:3:needle again"]
  );
  assert_eq!(lines(&t.run(&["-c", "needle"])), ["a.txt:1", "sub/b.txt:2"]);
  assert_eq!(lines(&t.run(&["-l", "needle"])), ["a.txt", "sub/b.txt"]);
}

#[test]
fn options_and_exit_codes() {
  let t = Tree::new();
  t.file("a.txt", "Foo bar\nfoobar\nfoo.bar\n");
  assert_eq!(lines(&t.run(&["-i", "^foo bar$", "a.txt"])), ["Foo bar"]);
  assert_eq!(lines(&t.run(&["-F", "foo.bar", "a.txt"])), ["foo.bar"]);
  assert_eq!(lines(&t.run(&["-w", "foo", "a.txt"])), ["foo.bar"]);
  assert_eq!(
    lines(&t.run(&["-e", "Foo", "-e", "foobar", "a.txt"])),
    ["Foo bar", "foobar"]
  );
  assert_eq!(t.run(&["zzz"]).status.code(), Some(1));
  assert_eq!(t.run(&["x", "missing.txt"]).status.code(), Some(2));
  assert_eq!(t.run(&["(unclosed"]).status.code(), Some(2));
}

#[test]
fn honors_ignore_files_and_hidden_files() {
  let t = Tree::new();
  t.dir(".git")
    .file(".gitignore", "*.log\nbuild/\n!.keep\n")
    .file("a.txt", "needle\n")
    .file("b.log", "needle\n")
    .file("build/c.txt", "needle\n")
    .file(".hidden", "needle\n")
    .file(".keep", "needle\n")
    .file("sub/.ignore", "d.txt\n")
    .file("sub/d.txt", "needle\n")
    .file("sub/e.txt", "needle\n");
  // A whitelist rule wins over the hidden-file rule.
  assert_eq!(lines(&t.run(&["-l", "needle"])), [".keep", "a.txt", "sub/e.txt"]);
  assert_eq!(
    lines(&t.run(&["-l", "--no-ignore", "needle"])),
    ["a.txt", "b.log", "build/c.txt", "sub/d.txt", "sub/e.txt"]
  );
  assert_eq!(
    lines(&t.run(&["-l", "-uu", "needle"])),
    [
      ".hidden",
      ".keep",
      "a.txt",
      "b.log",
      "build/c.txt",
      "sub/d.txt",
      "sub/e.txt"
    ]
  );
}

#[test]
fn gitignore_needs_a_git_repository() {
  let t = Tree::new();
  t.file(".gitignore", "*.log\n").file("b.log", "needle\n");
  assert_eq!(lines(&t.run(&["-l", "needle"])), ["b.log"]);
}

#[test]
fn binary_files() {
  let t = Tree::new();
  t.file("text.txt", "needle\n").file("bin.dat", b"needle\n\0\nneedle\n");
  // Found by the walk: skipped without a word.
  assert_eq!(lines(&t.run(&["needle"])), ["text.txt:needle"]);
  // Named on the command line: reported.
  let out = t.run(&["needle", "bin.dat"]);
  assert_eq!(
    lines(&out),
    [r#"binary file matches (found "\0" byte around offset 7)"#]
  );
  assert_eq!(lines(&t.run(&["-c", "needle", "bin.dat"])), ["2"]);
  assert_eq!(
    lines(&t.run(&["-a", "-c", "needle"])),
    ["bin.dat:2", "text.txt:1"]
  );
}

/// A file large enough to be searched in ranges must give the same output
/// as the single-thread path, which reads it block by block.
#[test]
fn big_file_ranges_match_streaming() {
  let t = Tree::new();
  let mut data = Vec::new();
  let mut i = 0u64;
  while data.len() < 20 << 20 {
    // Lines of varied length, some with the pattern, and one very long
    // line that crosses range borders.
    i += 1;
    let len = (i * 7919 % 300) as usize;
    data.extend_from_slice(format!("{i} ").as_bytes());
    data.extend(std::iter::repeat_n(b'a' + (i % 26) as u8, len));
    if i.is_multiple_of(13) {
      data.extend_from_slice(b" needle");
    }
    if i == 30_000 {
      data.extend(std::iter::repeat_n(b'z', 3 << 20));
      data.extend_from_slice(b" needle");
    }
    data.push(b'\n');
  }
  data.extend_from_slice(b"needle without a final newline");
  t.file("big.txt", &data);

  for args in [
    &["-n", "needle"][..],
    &["-c", "needle"],
    &["-n", "^1[0-9]*5 "],
    &["-c", ""],
  ] {
    let mut multi = args.to_vec();
    multi.extend(["-j", "4", "big.txt"]);
    let mut single = args.to_vec();
    single.extend(["-j", "1", "big.txt"]);
    let (m, s) = (t.run(&multi), t.run(&single));
    assert_eq!(m.status.code(), Some(0), "{args:?}");
    // Output is in file order, so compare it as is.
    assert!(m.stdout == s.stdout, "ranges differ from streaming for {args:?}");
  }
}

#[test]
fn long_lines_and_files_larger_than_one_read() {
  let t = Tree::new();
  let mut data = Vec::new();
  for i in 0..20_000 {
    data.extend_from_slice(
      format!("line {i} filler filler filler filler filler\n").as_bytes(),
    );
  }
  data.extend(std::iter::repeat_n(b'x', 600_000));
  data.extend_from_slice(b" needle\nlast needle\n");
  t.file("f.txt", &data);
  assert_eq!(lines(&t.run(&["-n", "-c", "needle", "f.txt"])), ["2"]);
  let out = t.run(&["-n", "^last", "f.txt"]);
  assert_eq!(lines(&out), ["20002:last needle"]);
}

#[test]
fn headings_and_colors() {
  let t = Tree::new();
  t.file("a.txt", "x needle y\n").file("b.txt", "needle\n");
  let out =
    t.run(&["--heading", "-n", "--color", "never", "needle", "a.txt", "b.txt"]);
  let s = String::from_utf8(out.stdout).unwrap();
  // Two groups separated by one blank line, in either order.
  let mut groups: Vec<&str> = s.split("\n\n").collect();
  groups.sort();
  assert_eq!(groups, ["a.txt\n1:x needle y", "b.txt\n1:needle\n"]);

  let out = t.run(&["--color", "always", "needle", "a.txt"]);
  assert_eq!(
    String::from_utf8(out.stdout).unwrap(),
    "x \x1b[0m\x1b[1m\x1b[31mneedle\x1b[0m y\n"
  );
}
