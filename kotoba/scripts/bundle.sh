#!/bin/sh
# Concatenate mcap.kotoba with a guest file. Kotoba admits one ns per unit
# and has no require, so examples/tests are guests appended to the library.
set -eu
root=$(CDPATH= cd -- "$(dirname "$0")/.." && pwd)
out=${1:?"usage: bundle.sh <output.kotoba> <guest.kotoba>"}
guest=${2:?"usage: bundle.sh <output.kotoba> <guest.kotoba>"}
extra=$(grep -E '^\(defn ' "$guest" | sed -E 's/^\(defn ([^ ]+).*/\1/' | tr '\n' ' ' | sed 's/[[:space:]]*$//')
{
  sed -E "s/\\(:export \\[/(:export [${extra} /" "$root/src/mcap.kotoba"
  printf '\n;; ---- guest ----\n'
  cat "$guest"
} >"$out"
