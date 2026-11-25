//! Work items, and turning a directory listing into work for its children.

use std::{
  ffi::{CString, OsStr},
  os::unix::ffi::OsStrExt,
  sync::Arc,
};

use lio::api::{FileType, resource::Resource};

use crate::{
  bigfile::BigFile,
  cli::Config,
  rules::{IgnNode, ignore_match, load_ignores},
};

pub(crate) struct DirWork {
  pub(crate) parent: Resource,
  pub(crate) name: CString,
  pub(crate) path: Vec<u8>,
  pub(crate) ign: Option<Arc<IgnNode>>,
  pub(crate) in_git: bool,
}

pub(crate) struct FileWork {
  pub(crate) parent: Resource,
  pub(crate) name: CString,
  pub(crate) path: Vec<u8>,
  /// Named on the command line (binary files are handled differently).
  pub(crate) explicit: bool,
}

/// `dir` + `/` + `name`, without a leading separator for the empty root.
pub(crate) fn join(dir: &[u8], name: &[u8]) -> Vec<u8> {
  let mut p = Vec::with_capacity(dir.len() + name.len() + 1);
  p.extend_from_slice(dir);
  if !dir.is_empty() && !dir.ends_with(b"/") {
    p.push(b'/');
  }
  p.extend_from_slice(name);
  p
}

pub(crate) enum Work {
  Dir(DirWork),
  File(FileWork),
  /// Range `.1` of a big file.
  Range(Arc<BigFile>, usize),
}

/// Filters the entries of directory `d` (open as `fd`) and returns the work
/// for its children.
#[hotpath::measure]
pub(crate) fn children(
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
