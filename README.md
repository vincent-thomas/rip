# lrg

A recursive line searcher in the style of ripgrep, built on `lio`.

```sh
cargo build --release
target/release/lrg [OPTIONS] PATTERN [PATH ...]
```

`lrg --help` lists the options. Output, exit codes, `.gitignore` /
`.ignore` / `.rgignore` handling, hidden files and binary files follow
ripgrep 14. On a terminal, lrg shows headings, line numbers and colors, as
ripgrep does.

## How it works

- **One io_uring ring per worker thread.** Each worker owns one `Lio` driver
  and keeps many directory opens, file opens and file reads in flight. One
  kernel entry submits and reaps work for many files.
- **Adaptive depth.** While data comes from the page cache, a worker keeps
  about 16 operations in flight, so work stays spread across threads. When
  waits get long (the disk), it raises that to 256 to keep the disk queue
  full. ripgrep keeps about 2 reads in flight on a cold cache.
- **Lister threads for cold directories.** `getdents64` has no io_uring
  operation and blocks on a cold directory. A worker that waits on the disk
  hands opened directories to lister threads. They start only when needed.
- **Large files in parallel.** A file of 8 MiB or more is split into 1 MiB
  ranges that all workers search. Output is written in file order.
- **Streaming reads.** Other files go through one reused 256 KiB buffer per
  operation. A file larger than the buffer streams through it.
- **mimalloc.** Work items are allocated on one thread and freed on another.
  glibc's allocator serializes that on arena locks.

## Results

4 vCPUs, Linux 6.18, virtio disk (2 GB/s sequential). Corpus: the Linux
source tree (97k files, 1.4 GB) and one 1.15 GB text file. Median of 6 to 12
alternating runs (`scripts/bench.py`). ripgrep 14.1.0.

| Case | ripgrep | lrg | Speedup |
| --- | --- | --- | --- |
| Tree, cold cache | 2.5 to 3.1 s | 0.93 to 1.14 s | about 2.7x |
| Tree, warm cache (7 patterns) | 248 to 791 ms | 253 to 692 ms | 0.98x to 1.14x |
| One 1.15 GB file, warm | 163 to 1552 ms | 79 to 368 ms | 2.0x to 4.2x |
| One 1.15 GB file, cold | 0.60 s | 0.31 to 0.40 s | 1.5x to 2.0x |

On a warm tree, lrg uses 8% to 19% less CPU time than ripgrep, but its
wall time is close to ripgrep's: from 2% slower to 14% faster. Results
change by a few percent between runs and between machine restarts.

Where the limits are:

- **Cold tree:** lrg reads at 1.9 GB/s, the same as this disk's sequential
  speed. It cannot go much faster on this hardware.
- **Warm tree:** most time goes to work both tools must do: the kernel copies
  file data (about 28%), path lookups, and the pattern scan. io_uring removes
  syscall overhead, but that is a small part. A large gain here would need an
  index, which is a different kind of tool.
- **Large single files:** ripgrep searches one file on one thread. lrg uses
  all cores.

## Known differences from ripgrep

- A NUL byte deep inside a large file: ripgrep stops at the border of its
  64 KiB read buffer, lrg at the line before the NUL. The output can differ
  by a few lines. NUL bytes in the first 64 KiB behave the same.
- Not implemented: global gitignore files, `.gitignore` files in parent
  directories of the search root, following symlinks, context lines (`-A`,
  `-B`, `-C`), replacements, file type filters (`-t`), globs (`-g`), PCRE2,
  and multiline search.
- A pattern that contains a NUL byte is not rejected as ripgrep does.
- Linux only in practice: the speed comes from the io_uring backend.

## Code layout

| File | What it holds |
| --- | --- |
| `src/main.rs` | Start-up: roots, threads, exit code. |
| `src/cli.rs` | Arguments, `--help`, and the search configuration. |
| `src/sched.rs` | State all threads share: work queues, pending count, parking. |
| `src/walk.rs` | Work items, and turning a directory listing into work. |
| `src/rules.rs` | Ignore rules (`.gitignore`, `.ignore`, `.rgignore`). |
| `src/worker.rs` | Worker threads: one lio ring each, the main I/O loop. |
| `src/lister.rs` | Threads that list cold directories. |
| `src/bigfile.rs` | Big files: split into ranges, output put back in order. |
| `src/search.rs` | Searching a buffer of lines, and binary files. |
| `src/output.rs` | Output format (paths, line numbers, colors) and stdout. |

## Development

Rust 1.88 or later, on Linux.

```sh
cargo test --release
```

Unit tests cover the line search, binary handling and how big-file ranges
split lines. Integration tests (`tests/cli.rs`) run the binary on generated
trees. One of them checks that the parallel range search gives the same
output as the single-thread path.

Compare with ripgrep (needs `rg`; run it in a directory to search). It
checks that the sorted output and the exit code are the same:

```sh
scripts/compare.sh -n 'fn \w+'
```

Compare speed (the first command is the baseline):

```sh
scripts/bench.py 10 rg=rg lrg=target/release/lrg -- -c 'fn \w+'
```

Profile with [hotpath](https://hotpath.rs). The feature times the main
functions and the time spent in `lio.run()`. Without it, the profiling
macros compile to nothing:

```sh
cargo build --release --features hotpath
HOTPATH_OUTPUT_PATH=report.txt target/release/lrg PATTERN
```

CI (`.github/workflows/ci.yml`) checks formatting, clippy (warnings are
errors), the tests, the hotpath build, and the build on Rust 1.88.
