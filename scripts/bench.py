#!/usr/bin/env python3
"""Compares the speed of search commands on the same arguments.

Runs the commands in turn, RUNS times each, in the current directory, and
prints the median wall time and the median CPU time (user + system) of each.
Taking turns spreads changes in machine load over all commands.

Output goes to a temporary file, not /dev/null: ripgrep notices /dev/null and
does less work.

usage: scripts/bench.py RUNS NAME=COMMAND... -- ARGS...
example: scripts/bench.py 10 rg=rg lrg=target/release/lrg -- -c 'fn \\w+'
"""
import os
import shlex
import statistics
import sys
import tempfile
import time


def run(argv, out, null):
    start = time.perf_counter()
    pid = os.fork()
    if pid == 0:
        os.dup2(null, 0)
        os.dup2(out, 1)
        os.execvp(argv[0], argv)
    _, _, usage = os.wait4(pid, 0)
    wall = (time.perf_counter() - start) * 1000
    return wall, (usage.ru_utime + usage.ru_stime) * 1000


def main():
    if len(sys.argv) < 4 or "--" not in sys.argv:
        sys.exit(__doc__)
    runs = int(sys.argv[1])
    split = sys.argv.index("--")
    commands = [c.split("=", 1) for c in sys.argv[2:split]]
    args = sys.argv[split + 1:]
    null = os.open(os.devnull, os.O_RDONLY)
    with tempfile.TemporaryFile() as f:
        out = f.fileno()
        argvs = {name: shlex.split(cmd) + args for name, cmd in commands}
        for argv in argvs.values():
            run(argv, out, null)  # Warm the cache.
        times = {name: [] for name in argvs}
        for _ in range(runs):
            for name, argv in argvs.items():
                os.ftruncate(out, 0)
                times[name].append(run(argv, out, null))
    base = None
    cols = []
    for name, ts in times.items():
        wall = statistics.median(t[0] for t in ts)
        cpu = statistics.median(t[1] for t in ts)
        base = base or wall
        cols.append(f"{name} {wall:7.1f} ms, cpu {cpu:6.0f} ms ({base / wall:.2f}x)")
    print(f"{' '.join(args)[:30]:30} | " + " | ".join(cols))


if __name__ == "__main__":
    main()
