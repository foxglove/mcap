# MCAP Cross-Language Benchmarks

Read and write benchmarks for the MCAP libraries across five languages:
C++, Rust, Go, Python, and TypeScript. Each language has its own
`*_bench/` subdirectory.

Three benchmark scenarios are included:

- **Fixed-payload** — 1M messages with a fixed 100-byte payload on a
  single channel, across all compression modes
- **Mixed-payload** — simulated 10-second robot recording with 5
  channels at realistic rates and sizes (3750 messages, ~102 MB)
- **Filtered reads** — topic filter, time range filter, and combined
  topic+time filter using the mixed-payload files

## Directory structure

```
benchmarking/
  cpp_bench/          C++ benchmarks (header-only mcap library)
  rust_bench/         Rust benchmarks (mcap crate)
  go_bench/           Go benchmarks (mcap module)
  python_bench/       Python benchmarks (mcap package)
  typescript_bench/   TypeScript benchmarks (@mcap/core)
  gen_blob.py         Generator for the shared payload blob
  Makefile            Build targets for all languages
  run_bench.sh        Unified benchmark runner with result tables
```

## Dependencies

### C++

- **g++** (or another C++17 compiler)
- **liblz4-dev** — LZ4 compression library
- **libzstd-dev** — Zstandard compression library

On Debian/Ubuntu:

```
sudo apt install g++ liblz4-dev libzstd-dev
```

### Rust

- **cargo** and a Rust toolchain (stable)

Install via [rustup](https://rustup.rs/):

```
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

### Go

- **go** 1.23 or later

Install from https://go.dev/dl/ or via your package manager.

### Python

- **python3**
- The **mcap** package from this repo (added to `PYTHONPATH` automatically by `run_bench.sh`)

No additional install is needed; the benchmark script imports from `../python/mcap`.

### TypeScript

- **Node.js** (v20.15 or later, for `crc32` in `node:zlib`)
- **npx** (included with Node.js)
- **tsx** (invoked via `npx tsx`; no global install required)
- Node modules must be installed at the repo root (`npm install` from the repo root)

## Building

From the `benchmarking/` directory:

```
make all
```

This will:

- Compile the C++ benchmarks (`cpp_bench/bench_write`, `cpp_bench/bench_read`)
- Build the Rust benchmarks in release mode (`rust_bench/target/release/`)
- Build the Go benchmarks (`go_bench/bench_write`, `go_bench/bench_read`)
- Verify the Python and TypeScript scripts exist (no compilation needed)

To build a single language:

```
make cpp_bench                                     # C++ only
make rust_bench                        # Rust only
make go_bench                          # Go only
```

## Running

### Full benchmark suite

```
make bench
```

This runs all languages across all compression modes (unchunked, chunked,
zstd, lz4), all three benchmark scenarios (fixed-payload, mixed-payload,
filtered reads), with 5 iterations each. Expect ~10-15 minutes on a
modern machine.

### Configuration

The benchmark runner accepts environment variables:

| Variable             | Default                           | Description                                                             |
| -------------------- | --------------------------------- | ----------------------------------------------------------------------- |
| `NUM_MESSAGES`       | `1000000`                         | Number of messages for fixed-payload benchmarks                         |
| `PAYLOAD_SIZE`       | `100`                             | Message payload size in bytes for fixed-payload benchmarks (max 524288) |
| `BENCH_ITERS`        | `5`                               | Number of iterations per (language, mode) pair                          |
| `BENCH_DIR`          | `/tmp`                            | Directory for temporary MCAP files and results                          |
| `BLOB_FILE`          | `$BENCH_DIR/bench_fill.bin`       | Path of the shared payload blob                                         |
| `MODES`              | `unchunked chunked zstd lz4`      | Compression modes for fixed-payload benchmarks                          |
| `MIXED_MODES`        | `unchunked chunked zstd lz4`      | Compression modes for mixed-payload benchmarks                          |
| `FILTER_COMPRESSION` | `chunked zstd`                    | Compression modes for filtered read benchmarks                          |
| `FILTER_MODES`       | `topic timerange topic_timerange` | Filter types to benchmark                                               |
| `LANGS`              | `rust go python cpp typescript`   | Languages to benchmark                                                  |
| `WRAPPER`            | (empty)                           | Command prefixed to every bench invocation (e.g. `perf record -g --`)   |

Example: run a quick benchmark with fewer messages and iterations:

```
NUM_MESSAGES=10000 BENCH_ITERS=2 ./run_bench.sh
```

### Running a single language

`LANGS` restricts the run to a subset of languages, and the preflight
check only requires the selected languages to be built:

```
make rust_bench
LANGS=rust ./run_bench.sh
```

Note that the cross-language payload-CRC and message-count checks
compare whatever languages ran, so a single-language run trivially
passes them — run the full suite before trusting cross-language
conclusions.

### Profiling

The bench programs are plain executables, so the easiest way to profile
is to invoke one directly under your profiler with the same arguments
`run_bench.sh` passes (see the `Usage` line each binary prints). The
native builds include debug line tables (`debug = "line-tables-only"`
for Rust, `-g -fno-omit-frame-pointer` for C++, and Go keeps symbols by
default), so perf output resolves to source lines at no runtime cost:

```
# Write a file to profile against, then profile a filtered read.
./rust_bench/target/release/bench_write /tmp/mixed.mcap zstd 0 mixed /tmp/bench_fill.bin
perf record -g -- ./rust_bench/target/release/bench_read /tmp/mixed.mcap zstd 0 mixed topic
perf report            # or open perf.data in hotspot
```

Alternatively, `WRAPPER` prefixes a command onto every bench invocation
the harness makes, which is handy for one-shot profiling without
retyping arguments (use `BENCH_ITERS=1` and a single language/mode so
the profile covers one process):

```
WRAPPER="perf record -g --" BENCH_ITERS=1 LANGS=rust MODES=zstd ./run_bench.sh
```

For the non-native languages, use a language-aware profiler instead of
perf: `py-spy record -- python3 python_bench/bench_read.py ...` for
Python, and `node --cpu-prof` (or `0x`) with `tsx` for TypeScript.

### Output

Results are written to TSV files in `$BENCH_DIR` and summarized in
tables printed to stdout:

**Fixed-payload benchmarks** (`bench_results.tsv`):

- File size comparison with compression ratios
- Peak memory usage (write and read)
- Write performance — median/min/max time, messages/sec, MB/sec
- Read performance — median/min/max time, messages/sec, MB/sec

**Mixed-payload benchmarks** (`bench_mixed_results.tsv`):

- Write performance — median/min/max time
- Read performance — median/min/max time

**Filtered read benchmarks** (`bench_filter_results.tsv`):

- Filtered read performance — median/min/max time per filter type

Each TSV row has the columns `op lang mode num_msgs payload_size
file_size elapsed_ns wall_sec peak_rss_kb`, plus a tenth column:
`payload_crc32` on write rows (see below) and `msg_count` on read rows.
`run_bench.sh` verifies the message counts: fixed and mixed reads must
equal the number of messages written, and a filtered read returning
zero messages aborts the run. Filtered counts are also compared across
languages; a disagreement is reported as a warning rather than an
error, since time-range boundary semantics may legitimately differ
between library APIs.

### Timing convention

The timed region for write benchmarks is: message loop + library
finish/close + flush of user-space buffers, i.e. it ends once all bytes have
been handed to the OS. The file-descriptor close falls outside the timed
region, except in C++ where the library owns the file and closes it inside
`writer.close()`; the extra close syscall is noise at benchmark timescales.

### Memory measurement convention

Each bench reports peak RSS in kilobytes, and any platform normalization
happens inside the bench, not in `run_bench.sh`. The native benches (C++,
Rust, Go, Python) read `ru_maxrss`, which is KB on Linux but bytes on macOS,
so they divide by 1024 on macOS. TypeScript's
`process.resourceUsage().maxRSS` is already normalized to KB on all
platforms by libuv. New benches must follow the same convention: emit KB,
normalize at the source. Every write bench holds the 16 MB payload blob
resident, so write RSS numbers include that constant equally across
languages. Read benches stream the file rather than buffering it
wholesale, so read RSS reflects the library, not the harness — filtered
results do not feed the memory table.

## Payload data

All write benchmarks draw their message payloads from a single shared
16 MiB blob, `$BENCH_DIR/bench_fill.bin`, generated once by
`gen_blob.py` (deterministic, fixed seed). This guarantees every
language feeds byte-identical data to its writer — the comparison
between implementations stays fair by construction, with no
per-language payload-generation code to keep in sync.

Message `i`'s payload is a window into the blob:

```
offset(i) = (i * 7919) % (16 MiB - 512 KiB)
```

where `i` is the global message index in write order (the loop index in
fixed-payload mode, the schedule index in mixed mode). The 7919-byte
stride means consecutive small messages get disjoint windows, while
large payloads (e.g. the 512 KiB camera messages) overlap between
messages — similar to the redundancy between consecutive frames in real
recordings. Since MCAP compresses per chunk, only overlap within a
chunk is visible to the compressor.

The blob itself is shaped to compress like real sensor data rather than
sitting at either extreme: it is a stream of little-endian 16-bit
"samples" — a slowly-varying triangle wave with noise added to roughly
1 in 4 samples — so exact repeats are common but interrupted, which zstd
compresses at about a 0.43 ratio. Tune `GATE_MASK` / `NOISE_BITS` /
`TRI_PERIOD` in `gen_blob.py` to adjust the ratio, and delete the blob
file to regenerate it.

To catch any divergence, each write bench computes a CRC-32 of the
exact payload byte stream it hands to the writer (outside the timed
region) and emits it as the tenth TSV column. `run_bench.sh` verifies
the CRC matches across all languages and iterations for each mode, and
aborts on mismatch.

## Mixed-payload scenario

The mixed-payload benchmark simulates a 10-second robot recording:

| Channel  | Topic                | Payload                 | Rate   | Messages |
| -------- | -------------------- | ----------------------- | ------ | -------- |
| IMU      | `/imu`               | 96 bytes                | 200 Hz | 2000     |
| Odometry | `/odom`              | 296 bytes               | 50 Hz  | 500      |
| TF       | `/tf`                | 80-1600 bytes (cycling) | 100 Hz | 1000     |
| LiDAR    | `/lidar`             | 230,400 bytes           | 10 Hz  | 100      |
| Camera   | `/camera/compressed` | 524,288 bytes           | 15 Hz  | 150      |

Total: 3750 messages, ~102 MB. Messages are interleaved by timestamp.

## Filtered read benchmarks

Filtered reads use the mixed-payload files and test three filter types:

- **topic** — read only `/imu` messages (2000 of 3750)
- **timerange** — read messages from seconds 3-5 (20% of the recording)
- **topic_timerange** — read `/lidar` messages from seconds 4-6 (~20 messages)

These benchmarks reveal whether each language's reader uses the MCAP
index to skip irrelevant chunks, or falls back to a linear scan.

## Notes

- TypeScript benchmarks skip LZ4 writes because `@foxglove/wasm-lz4`
  only provides decompression. TypeScript can still read LZ4-compressed
  files.
- The C++ benchmarks link against system lz4/zstd libraries. The Rust
  and Go benchmarks use their own compression implementations.
- Python and TypeScript benchmarks are interpreted/JIT and will be
  significantly slower than the compiled language benchmarks.

## Cleaning up

```
make clean
```

This removes compiled C++ binaries, Rust build artifacts, and Go binaries.
