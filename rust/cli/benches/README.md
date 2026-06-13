# MCAP CLI command benchmarks

This directory contains Criterion benchmarks for the `mcap` CLI. The benchmarks generate synthetic
MCAP inputs at runtime and run the real `mcap` binary, so large benchmark fixtures do not need to be
checked into the repository.

## Quick run

Run the benchmark target:

```sh
cargo bench -p mcap-cli --bench cli_commands
```

Cargo provides the bench-built `mcap` binary by default. Set `MCAP_CLI_BENCH_BIN` to compare a
different binary. Criterion writes reports under `target/criterion/`. Use Criterion's benchmark
filter to run a single suite:

```sh
cargo bench -p mcap-cli --bench cli_commands -- merge
cargo bench -p mcap-cli --bench cli_commands -- filter
```

## Workload controls

The default workload is intentionally small enough for local iteration. Increase it with environment
variables when collecting comparison data:

| Variable                          |                 Default | Description                                              |
| --------------------------------- | ----------------------: | -------------------------------------------------------- |
| `MCAP_CLI_BENCH_TOTAL_MIB`        |                    `16` | Total generated payload bytes per payload-size case.     |
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
cargo bench -p mcap-cli --bench cli_commands -- merge
```

## Generated inputs and cleanup

Each suite uses deterministic pseudo-random message payloads at `1 KiB`, `10 KiB`, `100 KiB`, and
`1 MiB` sizes. The benchmark validates each command output for basic MCAP correctness, expected
message count, summary presence, and log-time ordering where applicable.

Generated inputs are cached under `MCAP_CLI_BENCH_DIR` and reused when the benchmark parameters
match the filename. Delete that directory if a previous run was interrupted or after large runs to
free disk space.
