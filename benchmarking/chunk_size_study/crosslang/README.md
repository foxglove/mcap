# Cross-language read/write correlation check

A small, self-contained benchmark that writes and reads the **same fixed-payload
MCAP corpus** with each language's library from this repo, to confirm the C++
read/write numbers in the main study are representative rather than an artifact
of one implementation. It is intentionally minimal — one workload shape, a
single reused payload generated outside the timed loop — so it measures
library/codec throughput, not payload generation.

Each program takes the same CLI and prints one TSV line:

```
<prog> <write|read> <file> <num_msgs> <payload_size> <chunk_bytes> <none|zstd>
# -> lang  op  comp  num  payload_bytes  file_size  wall_sec
```

- C++ (`cpp/main.cpp`), Rust (`rust/`), Go (`go/`), Python (`python/xl.py`),
  TypeScript (`ts/xl.ts`).
- zstd is run for C++/Rust/Go/Python (native zstd codec). TypeScript is run
  uncompressed because `@mcap/core` ships no zstd *compressor*; an uncompressed
  pass is run for all five for an equal-footing comparison.

## Building and running

```sh
# C++
g++ -O2 -std=c++17 -I../../../cpp/mcap/include cpp/main.cpp -o cpp/xl -llz4 -lzstd
# Go (uses a local go.work-free module with replace; pinned to repo deps)
cd go && GOTOOLCHAIN=local go build -o xl . && cd ..
# Rust
cd rust && cargo build --release && cd ..
# Python needs the repo mcap package on PYTHONPATH and `zstandard` installed
# TypeScript runs via tsx against @mcap/core source (root node_modules required)

./run_xl.sh            # -> results/xl_raw.tsv
python3 analyze_xl.py  # -> results/fig_crosslang_throughput.png, results/xl_summary.md
```

See the "Cross-language read/write correlation" section of `../REPORT.md` for
results and interpretation.
