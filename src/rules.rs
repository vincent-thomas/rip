//! Ignore rules: `.gitignore`, `.ignore` and `.rgignore` files.

#[cfg(unix)]
use std::os::unix::ffi::OsStrExt;
#[cfg(target_os = "wasi")]
use std::os::wasi::ffi::OsStrExt;
use std::{ffi::OsStr, path::Path, sync::Arc};

use ignore::{
  Match,
  gitignore::{Gitignore, GitignoreBuilder},
};

use crate::walk::join;

/// The ignore rules of one directory, linked to the rules of its parent.
pub(crate) struct IgnNode {
  pub(crate) parent: Option<Arc<IgnNode>>,
  pub(crate) gi: Gitignore,
  /// Length of the directory's display path prefix (with its separator).
  pub(crate) prefix: usize,
}

/// `Some(true)` if an ignore rule ignores `path`, `Some(false)` if a rule
/// whitelists it, `None` if no rule matches.
pub(crate) fn ignore_match(
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

/// Builds the ignore rules for directory `path` from its ignore files.
#[hotpath::measure]
pub(crate) fn load_ignores(
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

/// Whether some ancestor of `path` (not `path` itself) holds a `.git`.
pub(crate) fn parent_in_git(path: &Path) -> bool {
  let Ok(abs) = std::fs::canonicalize(path) else { return false };
  abs.ancestors().skip(1).any(|a| a.join(".git").exists())
}
