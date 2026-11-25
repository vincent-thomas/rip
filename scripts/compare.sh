#!/usr/bin/env bash
# Compares lrg with ripgrep: runs both with the same arguments in the current
# directory and checks that the sorted output and the exit code are the same.
# Output is sorted because threads finish files in any order.
#
# usage: scripts/compare.sh ARGS...
#   LRG: the lrg binary (default: target/release/lrg next to this script)
#   RG:  the ripgrep binary (default: rg)
set -u
here=$(cd "$(dirname "$0")/.." && pwd)
lrg=${LRG:-$here/target/release/lrg}
rg=${RG:-rg}
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT

# Both read stdin as a file when it is a pipe, so give them none.
"$rg" "$@" </dev/null | LC_ALL=C sort >"$tmp/rg"
rg_exit=${PIPESTATUS[0]}
"$lrg" "$@" </dev/null | LC_ALL=C sort >"$tmp/lrg"
lrg_exit=${PIPESTATUS[0]}

if cmp -s "$tmp/rg" "$tmp/lrg" && [ "$rg_exit" = "$lrg_exit" ]; then
  echo "OK   $* ($(wc -l <"$tmp/rg") lines, exit $rg_exit)"
else
  echo "DIFF $* (rg: $(wc -l <"$tmp/rg") lines, exit $rg_exit;" \
    "lrg: $(wc -l <"$tmp/lrg") lines, exit $lrg_exit)"
  diff "$tmp/rg" "$tmp/lrg" | head -10
  exit 1
fi
