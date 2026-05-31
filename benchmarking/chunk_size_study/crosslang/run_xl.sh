#!/usr/bin/env bash
# Cross-language read/write throughput correlation check.
# zstd is run for the languages with a native zstd codec (C++, Rust, Go,
# Python); TypeScript is run uncompressed because @mcap/core ships no zstd
# compressor. An uncompressed pass is run for all five so TypeScript can be
# compared on equal footing.
set -euo pipefail
cd "$(dirname "$0")"

OUT=${OUT:-results}
DATA=${DATA:-/tmp/xl}
CHUNK=${CHUNK:-4194304}
ITERS=${ITERS:-3}
PYTHONPATH_MCAP=${PYTHONPATH_MCAP:-/workspace/python/mcap}
mkdir -p "$OUT" "$DATA"

CPP=./cpp/xl
GO=./go/xl
RUST=./rust/target/release/xlbench
PY="python3 python/xl.py"
TS="npx --no-install tsx ts/xl.ts"

# workload: name num size
WORKLOADS=("small 200000 100" "large 2000 50000")

RAW="$OUT/xl_raw.tsv"
echo -e "iter\tworkload\tlang\top\tcomp\tnum\tpayload_bytes\tfile_size\twall" > "$RAW"

run_lang() { # lang_cmd file workload_name num size comp uses_pythonpath
  local cmd="$1" file="$2" wl="$3" num="$4" size="$5" comp="$6" pp="$7"
  for it in $(seq 1 "$ITERS"); do
    if [ "$pp" = "py" ]; then
      wline=$(PYTHONPATH="$PYTHONPATH_MCAP" $cmd write "$file" "$num" "$size" "$CHUNK" "$comp")
      rline=$(PYTHONPATH="$PYTHONPATH_MCAP" $cmd read  "$file" "$num" "$size" "$CHUNK" "$comp")
    else
      wline=$($cmd write "$file" "$num" "$size" "$CHUNK" "$comp")
      rline=$($cmd read  "$file" "$num" "$size" "$CHUNK" "$comp")
    fi
    echo -e "$it\t$wl\t$wline" >> "$RAW"
    echo -e "$it\t$wl\t$rline" >> "$RAW"
  done
  rm -f "$file"
}

for wl in "${WORKLOADS[@]}"; do
  set -- $wl; name=$1; num=$2; size=$3
  echo ">> workload=$name comp=zstd" >&2
  run_lang "$CPP"  "$DATA/c.mcap" "$name" "$num" "$size" zstd no
  run_lang "$RUST" "$DATA/r.mcap" "$name" "$num" "$size" zstd no
  run_lang "$GO"   "$DATA/g.mcap" "$name" "$num" "$size" zstd no
  run_lang "$PY"   "$DATA/p.mcap" "$name" "$num" "$size" zstd py
  echo ">> workload=$name comp=none" >&2
  run_lang "$CPP"  "$DATA/c.mcap" "$name" "$num" "$size" none no
  run_lang "$RUST" "$DATA/r.mcap" "$name" "$num" "$size" none no
  run_lang "$GO"   "$DATA/g.mcap" "$name" "$num" "$size" none no
  run_lang "$PY"   "$DATA/p.mcap" "$name" "$num" "$size" none py
  run_lang "$TS"   "$DATA/t.mcap" "$name" "$num" "$size" none no
done

echo "Done. $RAW" >&2
