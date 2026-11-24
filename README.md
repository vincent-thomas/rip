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
alternating runs. ripgrep 14.1.0.

| Case | ripgrep | lrg | Speedup |
| --- | --- | --- | --- |
| Tree, cold cache | 2.55 s | 1.05 s | 2.4x |
| Tree, warm cache (9 patterns) | 281 to 1016 ms | 259 to 861 ms | 1.03x to 1.19x |
| One 1.15 GB file, warm | 184 to 2017 ms | 78 to 502 ms | 2.4x to 4.0x |
| One 1.15 GB file, cold | 0.53 s | 0.48 s | 1.1x |

On a warm tree, lrg also uses 12% to 24% less CPU time than ripgrep.

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

## Tests

```sh
cargo test --release
```

The tests run the binary on generated trees. One test checks that the
parallel range search of a large file gives the same output as the
single-thread path.
