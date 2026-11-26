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
//!
//! Module map:
//!
//! - [`cli`]: arguments and the search configuration.
//! - [`sched`]: state shared by all threads (queues, pending count, parking).
//! - [`walk`] and [`rules`]: directory entries, and the ignore rules applied
//!   to them.
//! - [`worker`]: the worker threads and their io_uring rings.
//! - [`lister`]: threads that list cold directories.
//! - [`bigfile`]: big files searched in ranges on all workers.
//! - [`search`] and [`output`]: searching buffers, and formatting the output.

mod bigfile;
mod cli;
mod lister;
mod output;
mod rules;
mod sched;
mod search;
mod walk;
mod worker;

#[cfg(unix)]
use std::os::unix::ffi::OsStrExt;
#[cfg(target_os = "wasi")]
use std::os::wasi::ffi::OsStrExt;
use std::{
  cell::RefCell,
  ffi::{CString, OsStr},
  path::Path,
  process::ExitCode,
  rc::Rc,
  sync::atomic::{AtomicBool, AtomicUsize, Ordering},
  time::Duration,
};

use crossbeam_deque::{Injector, Worker};
use lio::{Lio, api::resource::Resource};

use crate::{
  cli::{HELP, USAGE, parse_args},
  lister::Lister,
  output::{HEADING, OUT_FLUSH},
  rules::parent_in_git,
  sched::{Parker, Shared, THREADS},
  walk::{DirWork, FileWork, Work},
  worker::{DEPTH_GROWTH, WorkerThread},
};

// Work items are allocated by one thread and often freed by another. glibc's
// allocator serializes that on arena locks; mimalloc does not.
#[cfg(not(target_os = "wasi"))]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

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

  let run_worker = |local: Worker<Work>| {
    let depth = shared.cfg.depth;
    let lio = new_lio(depth * DEPTH_GROWTH * 2);
    let mut t = WorkerThread {
      shared: &shared,
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
  };

  if !THREADS {
    // One worker (`threads` is 1), on this thread.
    for local in workers {
      run_worker(local);
    }
  } else {
    std::thread::scope(|s| {
      for local in workers {
        s.spawn(move || run_worker(local));
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
  }

  // As ripgrep: 2 after any error, else 0 on a match, else 1.
  if shared.errored.load(Ordering::Relaxed) {
    ExitCode::from(2)
  } else if shared.matched.load(Ordering::Relaxed) {
    ExitCode::SUCCESS
  } else {
    ExitCode::from(1)
  }
}
