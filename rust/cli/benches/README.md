# MCAP CLI benchmarks

This directory contains benchmarks for the `mcap` CLI. The benchmarks generate synthetic
MCAP inputs at runtime and run the real `mcap` binary, so large benchmark fixtures do not need to be
checked into the repository.

## Running benchmarks

Run the full benchmark target:

```sh
cargo bench -p mcap-cli --bench commands
```

Cargo provides the bench-built `mcap` binary by default. Set `MCAP_CLI_BENCH_BIN` to compare a
different binary. Criterion writes reports under `target/criterion/`. Use Criterion's benchmark
filter to run a single command, input mode, or specific case:

```sh
cargo bench -p mcap-cli --bench commands -- merge
cargo bench -p mcap-cli --bench commands -- indexed
cargo bench -p mcap-cli --bench commands -- filter/linear
cargo bench -p mcap-cli --bench commands -- merge/indexed/100KiB
```

## Workload controls

The default workload is large enough to reduce fixed overhead noise. Override it with environment
variables for faster local iteration or larger comparison runs:

| Variable                          |                 Default | Description                                              |
| --------------------------------- | ----------------------: | -------------------------------------------------------- |
| `MCAP_CLI_BENCH_TOTAL_MIB`        |                   `256` | Total generated payload bytes per payload-size case.     |
| `MCAP_CLI_BENCH_INPUTS`           |                     `4` | Number of inputs for `merge` benchmarks.                 |
| `MCAP_CLI_BENCH_CHUNK_SIZE`       |               `4194304` | Generated output chunk size in bytes.                    |
| `MCAP_CLI_BENCH_DIR`              | `target/mcap-cli-bench` | Directory for generated inputs and per-run outputs.      |
| `MCAP_CLI_BENCH_BIN`              |      Cargo bench binary | CLI binary to execute.                                   |
| `MCAP_CLI_BENCH_SAMPLE_SIZE`      |                    `10` | Criterion sample size; values below 10 are raised to 10. |
| `MCAP_CLI_BENCH_WARMUP_MS`        |                   `250` | Criterion warmup duration.                               |
| `MCAP_CLI_BENCH_MEASUREMENT_SECS` |                     `2` | Criterion measurement duration.                          |

Example larger run:

```sh
MCAP_CLI_BENCH_TOTAL_MIB=1024 \
MCAP_CLI_BENCH_INPUTS=8 \
cargo bench -p mcap-cli --bench commands -- merge
```

## Generated inputs and cleanup

Each suite runs both `indexed` inputs (summary, chunk indexes, and message indexes present) and
`linear` inputs (summary omitted, forcing a scan fallback). It uses deterministic pseudo-random
message payloads at `100B`, `1KiB`, `10KiB`, `100KiB`, and `1MiB` sizes. The benchmark validates
each command output for basic MCAP correctness, expected message count, summary presence, and
log-time ordering where applicable.

Generated inputs are cached under `MCAP_CLI_BENCH_DIR` and reused when the benchmark parameters
match the filename. Delete that directory if a previous run was interrupted or after large runs to
free disk space.
