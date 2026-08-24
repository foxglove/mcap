#!/usr/bin/env bash
set -euo pipefail

# Configuration via environment variables
NUM_MESSAGES="${NUM_MESSAGES:-1000000}"
PAYLOAD_SIZE="${PAYLOAD_SIZE:-100}"
BENCH_ITERS="${BENCH_ITERS:-5}"
BENCH_DIR="${BENCH_DIR:-/tmp}"
MODES="${MODES:-unchunked chunked zstd lz4}"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
RESULTS_FILE="${BENCH_DIR}/bench_results.tsv"
BLOB_FILE="${BLOB_FILE:-${BENCH_DIR}/bench_fill.bin}"

# Paths to binaries
RUST_WRITE="${SCRIPT_DIR}/rust_bench/target/release/bench_write"
RUST_READ="${SCRIPT_DIR}/rust_bench/target/release/bench_read"
GO_WRITE="${SCRIPT_DIR}/go_bench/bench_write"
GO_READ="${SCRIPT_DIR}/go_bench/bench_read"
CPP_WRITE="${SCRIPT_DIR}/cpp_bench/bench_write"
CPP_READ="${SCRIPT_DIR}/cpp_bench/bench_read"

# Python config
PYTHON="${PYTHON:-python3}"
INTEROP_PYPATH="${INTEROP_PYPATH:-${SCRIPT_DIR}/../python/mcap}"
PY_WRITE="${SCRIPT_DIR}/python_bench/bench_write.py"
PY_READ="${SCRIPT_DIR}/python_bench/bench_read.py"

# TypeScript config
TSX="${TSX:-npx tsx}"
TS_WRITE="${SCRIPT_DIR}/typescript_bench/bench_write.ts"
TS_READ="${SCRIPT_DIR}/typescript_bench/bench_read.ts"

# Verify binaries exist
for bin in "$RUST_WRITE" "$RUST_READ" "$GO_WRITE" "$GO_READ" "$CPP_WRITE" "$CPP_READ"; do
  if [ ! -x "$bin" ]; then
    echo "ERROR: Binary not found or not executable: $bin" >&2
    echo "Run 'make all' first." >&2
    exit 1
  fi
done

# Verify Python scripts exist
for script in "$PY_WRITE" "$PY_READ" "${SCRIPT_DIR}/gen_blob.py"; do
  if [ ! -f "$script" ]; then
    echo "ERROR: Python script not found: $script" >&2
    echo "Run 'make all' first." >&2
    exit 1
  fi
done

# Verify TypeScript scripts exist
for script in "$TS_WRITE" "$TS_READ"; do
  if [ ! -f "$script" ]; then
    echo "ERROR: TypeScript script not found: $script" >&2
    echo "Run 'make all' first." >&2
    exit 1
  fi
done

echo "=== MCAP Cross-Language Benchmark ==="
echo "Messages: ${NUM_MESSAGES}, Payload: ${PAYLOAD_SIZE} bytes, Iterations: ${BENCH_ITERS}"
echo "Output dir: ${BENCH_DIR}"
echo "Modes: ${MODES}"
echo ""

# Generate the shared payload blob (deterministic; reused if present)
if [ ! -f "$BLOB_FILE" ]; then
  echo "Generating payload blob ${BLOB_FILE}..."
  "$PYTHON" "${SCRIPT_DIR}/gen_blob.py" "$BLOB_FILE"
  echo ""
fi

# Verify that every language fed identical payload bytes to its writer:
# column 10 of each write row is a CRC-32 of the payload stream, which
# must match across languages (and iterations) for each mode.
verify_checksums() {
  local results_file="$1" modes="$2" bad=0 mode n
  for mode in $modes; do
    n=$(awk -F'\t' -v m="$mode" '$1=="write" && $3==m {print $10}' "$results_file" | sort -u | wc -l)
    if [ "$n" -gt 1 ]; then
      echo "ERROR: payload CRC mismatch across languages for mode ${mode}:" >&2
      awk -F'\t' -v m="$mode" '$1=="write" && $3==m {print "  "$2"\t"$10}' "$results_file" | sort -u >&2
      bad=1
    fi
  done
  if [ "$bad" -ne 0 ]; then
    echo "Aborting: implementations did not write identical payloads." >&2
    exit 1
  fi
}

# Verify that every read bench consumed the expected number of messages:
# column 10 of each read row is the message count reported by the bench. A
# reader that silently under-reads would otherwise post a fast time with no
# error.
verify_read_counts() {
  local results_file="$1" expected="$2" rows
  rows=$(awk -F'\t' -v e="$expected" \
    '$1=="read" && $10 != e {print "  "$2"/"$3": read "$10" messages, expected "e}' "$results_file")
  if [ -n "$rows" ]; then
    echo "ERROR: read benches did not consume the expected message count:" >&2
    echo "$rows" >&2
    echo "Aborting: incomplete reads invalidate the read timings." >&2
    exit 1
  fi
}

# Filtered reads: a zero count means a broken filter, so fail hard. Exact
# counts may legitimately differ between languages if their time-range
# boundary semantics differ, so cross-language disagreement is reported
# loudly but does not abort.
check_filter_counts() {
  local results_file="$1" zero mode n
  zero=$(awk -F'\t' '$1=="read" && $10+0 == 0 {print "  "$2"/"$3}' "$results_file")
  if [ -n "$zero" ]; then
    echo "ERROR: filtered read returned zero messages:" >&2
    echo "$zero" >&2
    echo "Aborting: an empty filtered read invalidates the filtered timings." >&2
    exit 1
  fi
  for mode in $(awk -F'\t' '$1=="read" {print $3}' "$results_file" | sort -u); do
    n=$(awk -F'\t' -v m="$mode" '$1=="read" && $3==m {print $10}' "$results_file" | sort -u | wc -l)
    if [ "$n" -gt 1 ]; then
      echo "WARNING: message count disagreement across languages for ${mode}:"
      awk -F'\t' -v m="$mode" '$1=="read" && $3==m {print "  "$2"\t"$10}' "$results_file" | sort -u
    fi
  done
}

# Clear results file
> "$RESULTS_FILE"

LANGS="rust go python cpp typescript"

# Set write_cmd and read_cmd for a given language.
set_lang_cmds() {
  local lang="$1"
  case "$lang" in
    rust)       write_cmd="$RUST_WRITE"; read_cmd="$RUST_READ" ;;
    go)         write_cmd="$GO_WRITE"; read_cmd="$GO_READ" ;;
    python)     write_cmd="PYTHONPATH=$INTEROP_PYPATH $PYTHON $PY_WRITE"; read_cmd="PYTHONPATH=$INTEROP_PYPATH $PYTHON $PY_READ" ;;
    cpp)        write_cmd="$CPP_WRITE"; read_cmd="$CPP_READ" ;;
    typescript) write_cmd="$TSX $TS_WRITE"; read_cmd="$TSX $TS_READ" ;;
    *)          echo "unknown lang: $lang" >&2; exit 1 ;;
  esac
}

for mode in $MODES; do
  for lang in $LANGS; do
    set_lang_cmds "$lang"

    # TypeScript doesn't support LZ4 compression for writes
    if [ "$lang" = "typescript" ] && [ "$mode" = "lz4" ]; then
      continue
    fi

    outfile="${BENCH_DIR}/bench_${lang}_${mode}.mcap"

    for iter in $(seq 1 "$BENCH_ITERS"); do
      echo -n "  ${lang}/${mode} write iter ${iter}/${BENCH_ITERS}..."
      result=$(eval "$write_cmd" "$outfile" "$mode" "$NUM_MESSAGES" "$PAYLOAD_SIZE" "$BLOB_FILE")
      echo "$result" >> "$RESULTS_FILE"
      echo " done"

      echo -n "  ${lang}/${mode} read  iter ${iter}/${BENCH_ITERS}..."
      result=$(eval "$read_cmd" "$outfile" "$mode" "$NUM_MESSAGES" "$PAYLOAD_SIZE")
      echo "$result" >> "$RESULTS_FILE"
      echo " done"
    done
  done
done

verify_checksums "$RESULTS_FILE" "$MODES"
verify_read_counts "$RESULTS_FILE" "$NUM_MESSAGES"

echo ""
echo "=== Raw results ==="
cat "$RESULTS_FILE"
echo ""

# --- Compute and display tables ---

# median helper: takes a list of numbers (one per line), outputs the median
median() {
  sort -n | awk '{a[NR]=$1} END {if (NR%2==1) print a[(NR+1)/2]; else print (a[NR/2]+a[NR/2+1])/2}'
}

# Collect unique file sizes per (lang, mode) for the write operations
echo "=== FILE SIZE COMPARISON ==="
echo ""
printf "%-6s  %-12s  %12s  %10s\n" "Lang" "Mode" "FileSize" "Ratio"
printf "%-6s  %-12s  %12s  %10s\n" "----" "--------" "--------" "-----"

# File size from the first write result for this (lang, mode). A plain lookup
# rather than an associative-array cache: declare -A needs bash >= 4, and stock
# macOS still ships bash 3.2.
file_size_for() {
  awk -F'\t' -v l="$1" -v m="$2" \
    '$1=="write" && $2==l && $3==m {print $6; exit}' "$RESULTS_FILE"
}

for lang in $LANGS; do
  base_size=$(file_size_for "$lang" unchunked)
  base_size="${base_size:-0}"
  for mode in $MODES; do
    fsize=$(file_size_for "$lang" "$mode")
    fsize="${fsize:-0}"
    if [ "$base_size" -gt 0 ] 2>/dev/null; then
      ratio=$(awk "BEGIN {printf \"%.2fx\", $fsize/$base_size}")
    else
      ratio="N/A"
    fi
    # Human-readable file size
    if [ "$fsize" -gt 1048576 ] 2>/dev/null; then
      hr_size=$(awk "BEGIN {printf \"%.1f MB\", $fsize/1048576}")
    elif [ "$fsize" -gt 1024 ] 2>/dev/null; then
      hr_size=$(awk "BEGIN {printf \"%.1f KB\", $fsize/1024}")
    else
      hr_size="${fsize} B"
    fi
    printf "%-6s  %-12s  %12s  %10s\n" "$lang" "$mode" "$hr_size" "$ratio"
  done
done

echo ""

# Display memory usage table
echo "=== MEMORY USAGE (median of ${BENCH_ITERS} iterations) ==="
echo ""
printf "%-6s  %-12s  %12s  %12s\n" "Lang" "Mode" "Write(MB)" "Read(MB)"
printf "%-6s  %-12s  %12s  %12s\n" "----" "--------" "---------" "--------"

for mode in $MODES; do
  for lang in $LANGS; do
    write_rss_values=$(awk -F'\t' -v l="$lang" -v m="$mode" \
      '$1=="write" && $2==l && $3==m {print $9}' "$RESULTS_FILE")
    read_rss_values=$(awk -F'\t' -v l="$lang" -v m="$mode" \
      '$1=="read" && $2==l && $3==m {print $9}' "$RESULTS_FILE")

    if [ -z "$write_rss_values" ] && [ -z "$read_rss_values" ]; then
      continue
    fi

    write_mb="N/A"
    read_mb="N/A"
    if [ -n "$write_rss_values" ]; then
      write_median_kb=$(echo "$write_rss_values" | median)
      write_mb=$(awk "BEGIN {printf \"%.1f\", $write_median_kb / 1024}")
    fi
    if [ -n "$read_rss_values" ]; then
      read_median_kb=$(echo "$read_rss_values" | median)
      read_mb=$(awk "BEGIN {printf \"%.1f\", $read_median_kb / 1024}")
    fi

    printf "%-6s  %-12s  %12s  %12s\n" "$lang" "$mode" "$write_mb" "$read_mb"
  done
done

echo ""

# Display write and read results
for op in write read; do
  echo "=== $(echo "$op" | tr '[:lower:]' '[:upper:]') RESULTS (median of ${BENCH_ITERS} iterations) ==="
  echo ""
  printf "%-6s  %-12s  %12s  %12s  %12s  %12s  %10s\n" \
    "Lang" "Mode" "Median(ms)" "Min(ms)" "Max(ms)" "Msg/sec" "MB/sec"
  printf "%-6s  %-12s  %12s  %12s  %12s  %12s  %10s\n" \
    "----" "--------" "----------" "--------" "--------" "--------" "------"

  for mode in $MODES; do
    for lang in $LANGS; do
      # Extract elapsed_ns values for this (op, lang, mode)
      ns_values=$(awk -F'\t' -v o="$op" -v l="$lang" -v m="$mode" \
        '$1==o && $2==l && $3==m {print $7}' "$RESULTS_FILE")

      if [ -z "$ns_values" ]; then
        continue
      fi

      median_ns=$(echo "$ns_values" | median)
      min_ns=$(echo "$ns_values" | sort -n | head -1)
      max_ns=$(echo "$ns_values" | sort -n | tail -1)

      # Compute derived metrics from median
      awk -v med="$median_ns" -v mn="$min_ns" -v mx="$max_ns" \
          -v nm="$NUM_MESSAGES" -v ps="$PAYLOAD_SIZE" \
          -v lang="$lang" -v mode="$mode" \
        'BEGIN {
          med_ms = med / 1e6
          min_ms = mn / 1e6
          max_ms = mx / 1e6
          med_sec = med / 1e9
          if (med_sec > 0) {
            msg_per_sec = nm / med_sec
            mb_per_sec = (nm * ps) / med_sec / 1048576
          } else {
            msg_per_sec = 0
            mb_per_sec = 0
          }
          printf "%-6s  %-12s  %12.1f  %12.1f  %12.1f  %12.0f  %10.1f\n", \
            lang, mode, med_ms, min_ms, max_ms, msg_per_sec, mb_per_sec
        }'
    done
  done
  echo ""
done

# --- Mixed-payload benchmarks ---

MIXED_MODES="${MIXED_MODES:-unchunked chunked zstd lz4}"
MIXED_RESULTS_FILE="${BENCH_DIR}/bench_mixed_results.tsv"
> "$MIXED_RESULTS_FILE"

echo "=== MCAP Mixed-Payload Benchmark (10s robot recording) ==="
echo "Channels: /imu(96B@200Hz) /odom(296B@50Hz) /tf(80-1600B@100Hz) /lidar(230KB@10Hz) /camera(512KB@15Hz)"
echo "Total: 3750 messages, ~102 MB"
echo "Iterations: ${BENCH_ITERS}"
echo "Modes: ${MIXED_MODES}"
echo ""

for mode in $MIXED_MODES; do
  for lang in $LANGS; do
    set_lang_cmds "$lang"

    # TypeScript doesn't support LZ4 compression for writes
    if [ "$lang" = "typescript" ] && [ "$mode" = "lz4" ]; then
      continue
    fi

    outfile="${BENCH_DIR}/bench_${lang}_mixed_${mode}.mcap"

    for iter in $(seq 1 "$BENCH_ITERS"); do
      echo -n "  ${lang}/mixed-${mode} write iter ${iter}/${BENCH_ITERS}..."
      result=$(eval "$write_cmd" "$outfile" "$mode" 0 mixed "$BLOB_FILE")
      echo "$result" >> "$MIXED_RESULTS_FILE"
      echo " done"

      echo -n "  ${lang}/mixed-${mode} read  iter ${iter}/${BENCH_ITERS}..."
      result=$(eval "$read_cmd" "$outfile" "$mode" 0 mixed)
      echo "$result" >> "$MIXED_RESULTS_FILE"
      echo " done"
    done
  done
done

verify_checksums "$MIXED_RESULTS_FILE" "$MIXED_MODES"
verify_read_counts "$MIXED_RESULTS_FILE" 3750

echo ""
echo "=== Mixed-payload raw results ==="
cat "$MIXED_RESULTS_FILE"
echo ""

# Mixed-payload results table
for op in write read; do
  echo "=== MIXED $(echo "$op" | tr '[:lower:]' '[:upper:]') RESULTS (median of ${BENCH_ITERS} iterations) ==="
  echo ""
  printf "%-12s  %-12s  %12s  %12s  %12s\n" \
    "Lang" "Mode" "Median(ms)" "Min(ms)" "Max(ms)"
  printf "%-12s  %-12s  %12s  %12s  %12s\n" \
    "----" "--------" "----------" "--------" "--------"

  for mode in $MIXED_MODES; do
    for lang in $LANGS; do
      ns_values=$(awk -F'\t' -v o="$op" -v l="$lang" -v m="$mode" \
        '$1==o && $2==l && $3==m {print $7}' "$MIXED_RESULTS_FILE")

      if [ -z "$ns_values" ]; then
        continue
      fi

      median_ns=$(echo "$ns_values" | median)
      min_ns=$(echo "$ns_values" | sort -n | head -1)
      max_ns=$(echo "$ns_values" | sort -n | tail -1)

      awk -v med="$median_ns" -v mn="$min_ns" -v mx="$max_ns" \
          -v lang="$lang" -v mode="$mode" \
        'BEGIN {
          med_ms = med / 1e6
          min_ms = mn / 1e6
          max_ms = mx / 1e6
          printf "%-12s  %-12s  %12.1f  %12.1f  %12.1f\n", \
            lang, mode, med_ms, min_ms, max_ms
        }'
    done
  done
  echo ""
done

# --- Filtered read benchmarks (using mixed-payload files) ---

FILTER_MODES="${FILTER_MODES:-topic timerange topic_timerange}"
FILTER_COMPRESSION="${FILTER_COMPRESSION:-chunked zstd}"
FILTER_RESULTS_FILE="${BENCH_DIR}/bench_filter_results.tsv"
> "$FILTER_RESULTS_FILE"

echo "=== MCAP Filtered Read Benchmark ==="
echo "Filters: topic(/imu) timerange(3-5s) topic_timerange(/lidar 4-6s)"
echo "Compression modes: ${FILTER_COMPRESSION}"
echo "Iterations: ${BENCH_ITERS}"
echo ""

for compression in $FILTER_COMPRESSION; do
  for lang in $LANGS; do
    set_lang_cmds "$lang"

    if [ "$lang" = "typescript" ] && [ "$compression" = "lz4" ]; then
      continue
    fi

    # Use the mixed-payload file written earlier
    outfile="${BENCH_DIR}/bench_${lang}_mixed_${compression}.mcap"
    if [ ! -f "$outfile" ]; then
      echo "  SKIP ${lang}/${compression}: mixed file not found (run mixed benchmarks first)"
      continue
    fi

    for filter in $FILTER_MODES; do
      for iter in $(seq 1 "$BENCH_ITERS"); do
        echo -n "  ${lang}/${compression}/${filter} read iter ${iter}/${BENCH_ITERS}..."
        result=$(eval "$read_cmd" "$outfile" "${compression}-${filter}" 0 mixed "$filter")
        echo "$result" >> "$FILTER_RESULTS_FILE"
        echo " done"
      done
    done
  done
done

check_filter_counts "$FILTER_RESULTS_FILE"

echo ""
echo "=== Filtered read raw results ==="
cat "$FILTER_RESULTS_FILE"
echo ""

echo "=== FILTERED READ RESULTS (median of ${BENCH_ITERS} iterations) ==="
echo ""
printf "%-12s  %-20s  %12s  %12s  %12s\n" \
  "Lang" "Compression/Filter" "Median(ms)" "Min(ms)" "Max(ms)"
printf "%-12s  %-20s  %12s  %12s  %12s\n" \
  "----" "------------------" "----------" "--------" "--------"

for compression in $FILTER_COMPRESSION; do
  for filter in $FILTER_MODES; do
    combined="${compression}-${filter}"
    for lang in $LANGS; do
      ns_values=$(awk -F'\t' -v l="$lang" -v m="$combined" \
        '$1=="read" && $2==l && $3==m {print $7}' "$FILTER_RESULTS_FILE")

      if [ -z "$ns_values" ]; then
        continue
      fi

      median_ns=$(echo "$ns_values" | median)
      min_ns=$(echo "$ns_values" | sort -n | head -1)
      max_ns=$(echo "$ns_values" | sort -n | tail -1)

      awk -v med="$median_ns" -v mn="$min_ns" -v mx="$max_ns" \
          -v lang="$lang" -v combined="$combined" \
        'BEGIN {
          med_ms = med / 1e6
          min_ms = mn / 1e6
          max_ms = mx / 1e6
          printf "%-12s  %-20s  %12.1f  %12.1f  %12.1f\n", \
            lang, combined, med_ms, min_ms, max_ms
        }'
    done
  done
done
echo ""

echo "=== Benchmark complete ==="
