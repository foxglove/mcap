#!/usr/bin/env bash
# Drives the chunk-size sweep across message-size classes, compression formats
# and read access patterns, emitting one TSV row per measurement.
set -euo pipefail

cd "$(dirname "$0")"

BENCH=${BENCH:-./bench}
OUTDIR=${OUTDIR:-results}
DATADIR=${DATADIR:-/tmp/chunk_size_study}
TARGET_BYTES=${TARGET_BYTES:-268435456}   # ~256 MiB of uncompressed payload per corpus
ITERS_W=${ITERS_W:-3}
ITERS_R=${ITERS_R:-5}
CLASSES=${CLASSES:-"small jpeg pointcloud mixed"}
COMP_LIST=${COMP_LIST:-"zstd lz4"}
# Chunk sizes (bytes): 256 KiB, 768 KiB, 1 MiB, 4 MiB, 8 MiB, 16 MiB, 32 MiB
CHUNKS=${CHUNKS:-"262144 786432 1048576 4194304 8388608 16777216 33554432"}

mkdir -p "$OUTDIR" "$DATADIR"
RAW="$OUTDIR/raw.tsv"
echo -e "iter\top\tclass\tchunk_bytes\tcomp\tpattern\tmsgs\tpayload_bytes\tchunks_touched\tchunk_fetched_bytes\tsummary_bytes\traw_fetched\traw_reads\tfile_size\twall\trss_kb" > "$RAW"

emit() { # iter + bench output line
  echo -e "$1\t$2"
}

for comp in $COMP_LIST; do
  for cls in $CLASSES; do
    if [ "$cls" = "mixed" ]; then
      PATTERNS="full point range streaming topic"
    else
      PATTERNS="full point range streaming"
    fi
    for chunk in $CHUNKS; do
      f="$DATADIR/${cls}_${comp}_${chunk}.mcap"
      echo ">> write class=$cls comp=$comp chunk=$chunk" >&2
      for i in $(seq 1 "$ITERS_W"); do
        line=$($BENCH write "$f" "$cls" "$chunk" "$TARGET_BYTES" "$comp")
        emit "$i" "$line" >> "$RAW"
      done
      for p in $PATTERNS; do
        for i in $(seq 1 "$ITERS_R"); do
          line=$($BENCH read "$f" "$cls" "$chunk" "$comp" "$p")
          emit "$i" "$line" >> "$RAW"
        done
      done
      rm -f "$f"
    done
  done
done

echo "Done. Raw results: $RAW" >&2
