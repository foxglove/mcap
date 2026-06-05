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

- **Node.js** (v18 or later)
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
zstd, lz4), both fill patterns (uniform and varied), all three benchmark
scenarios (fixed-payload, mixed-payload, filtered reads), with 5
iterations each. Expect ~20-25 minutes on a modern machine.

### Configuration

The benchmark runner accepts environment variables:

| Variable             | Default                           | Description                                                |
| -------------------- | --------------------------------- | ---------------------------------------------------------- |
| `NUM_MESSAGES`       | `1000000`                         | Number of messages for fixed-payload benchmarks            |
| `PAYLOAD_SIZE`       | `100`                             | Message payload size in bytes for fixed-payload benchmarks |
| `BENCH_ITERS`        | `5`                               | Number of iterations per (language, mode) pair             |
| `BENCH_DIR`          | `/tmp`                            | Directory for temporary MCAP files and results             |
| `MODES`              | `unchunked chunked zstd lz4`      | Compression modes for fixed-payload benchmarks             |
| `MIXED_MODES`        | `unchunked chunked zstd lz4`      | Compression modes for mixed-payload benchmarks             |
| `FILTER_COMPRESSION` | `chunked zstd`                    | Compression modes for filtered read benchmarks             |
| `FILTER_MODES`       | `topic timerange topic_timerange` | Filter types to benchmark                                  |

Example: run a quick benchmark with fewer messages and iterations:

```
NUM_MESSAGES=10000 BENCH_ITERS=2 ./run_bench.sh
```

Note: when invoking `run_bench.sh` directly, the `FILL` environment
variable controls the payload fill pattern (`uniform` or `varied`).
`make bench` runs both fill patterns automatically.

### Output

Results are written to TSV files in `$BENCH_DIR` and summarized in
tables printed to stdout:

**Fixed-payload benchmarks** (`bench_results_${FILL}.tsv`):

- File size comparison with compression ratios
- Peak memory usage (write and read)
- Write performance — median/min/max time, messages/sec, MB/sec
- Read performance — median/min/max time, messages/sec, MB/sec

**Mixed-payload benchmarks** (`bench_mixed_results_${FILL}.tsv`):

- Write performance — median/min/max time
- Read performance — median/min/max time

**Filtered read benchmarks** (`bench_filter_results_${FILL}.tsv`):

- Filtered read performance — median/min/max time per filter type

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
- The `uniform` fill (all 0x42 bytes) compresses unrealistically well.
  The `varied` fill uses a deterministic pattern `(i * 137 + 43) & 0xff`
  per byte position for more realistic compression ratios. Both patterns
  are reproducible across runs and languages.

## Cleaning up

```
make clean
```

This removes compiled C++ binaries, Rust build artifacts, and Go binaries.
