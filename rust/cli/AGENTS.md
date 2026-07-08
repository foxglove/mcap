# MCAP CLI development guide

This crate (`mcap-cli`) builds the `mcap` command line tool. It is an internal binary crate (`publish = false`) that depends on the sibling `mcap` library crate. End-user usage and installation documentation lives in `../../website/docs/guides/cli.md` (published at https://mcap.dev/guides/cli).

## Common commands

- Run `cargo` commands from the repository root.
- Lint with `cargo clippy -p mcap-cli --all-targets -- --no-deps -D warnings`.

## Architecture

clap parses arguments, `dispatch` (in `commands.rs`) routes to a per-command handler under `commands/`, and the handler reads an input source and renders output. The `mcap` library crate handles the underlying read/write. Multi-level commands group their subcommands in a subdirectory (`add/`, `get/`, `list/`, `convert/`).

A few modules carry more than their name implies:

| Module       | Responsibility                                                                                                                                                         |
| ------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `cli.rs`     | clap `Args`/`Command` definitions, plus shared value parsers — reuse these for new args instead of rolling your own.                                                   |
| `source.rs`  | Input abstraction over local files (memory-mapped) and remote object stores. Owns summary/index range reads, remote materialization, and `--allow-remote-scan` gating. |
| `parse.rs`   | `ParsedMcap` plus summary-first / linear-scan parsing and the exact-record parsers used by remote range reads.                                                         |
| `context.rs` | `CommandContext`, the global options (verbosity, color, `allow_remote_scan`) threaded into every handler.                                                              |
| `build.rs`   | Resolves commit sha (git rev-parse or export-subst) into `GIT_SHORT_SHA` env var.                                                                                      |

## Conventions

### Exit codes

- `0` — success.
- `1` — hard failure (any handler returning `Err`).
- `2` — argument-parsing error (clap default).
- `3` — completed with warning-level data loss (e.g. `recover` discarded records or stopped on a truncated input). Represented by `CommandOutcome::Warnings`.

When adding a command that can complete despite losing data, return `CommandOutcome::Warnings` rather than printing a warning and exiting `0`.

### Remote inputs

Remote inputs (HTTP(S) and object-store URLs: `s3://`, `gs://`, and Azure `az://`/`abfs://`) are handled in `source.rs` via `object_store`. Bounded, indexed reads — a summary-section read, or a single attachment/metadata range read under the no-opt-in caps — are allowed without a flag. Any command that would scan or download an entire remote file requires the global `--allow-remote-scan` flag; gate new whole-file remote reads behind `SourceOptions::allow_remote_scan` accordingly.

### Bounded-memory reads

MCAP files may reach hundreds of GB, so no command may hold a whole file in memory (see the root `AGENTS.md` design principle).

- Drive the `mcap::sans_io` readers (`LinearReader`, `IndexedReader`, `SummaryReader`) from the input `File` (seek + read) or the remote range reader. This is the default — one code path that stays bounded for local files, stdin, and remote inputs.
- The `mcap::read` slice API (`MessageStream`, `LinearReader`, `read::attachment`/`metadata`, `Summary::stream_chunk`) needs the whole file as one `&[u8]`, so use it only when you already have that: a small input, or a seekable on-disk local file you have deliberately memory-mapped (`mmap` pages such a file on demand, so it stays bounded). Don't use it — or `mmap` — for stdin or remote inputs, which would force materializing the whole file first: a pipe can't be mapped, and a spool on a tmpfs `/tmp` just puts the bytes back in RAM.
- An operation that needs random access over a non-seekable or reordered stream (e.g. sorting a file into an order it isn't stored in) must spool to a temporary file. Put the spool on the output volume (or a configured temp dir), not `/tmp`, and read it back with seek + read, not `mmap`.

### Output and logging

Results go to stdout; diagnostics and warnings go to stderr. Use the `render` helpers for tabular output so column alignment and byte/time formatting stay consistent.

### Testing

Most tests live inline in each module under `#[cfg(test)]`. Argument-parsing behavior is covered in `cli.rs`/`main.rs`, and dispatch/handler behavior in the relevant command module. For MCAP inputs, build fixtures in-memory with `mcap::Writer` rather than committing files. Committed binary fixtures are used where the input can't be synthesized that way — notably the `convert` tests, which load real ROS bag/db3 files from `testdata/` (resolved via `CARGO_MANIFEST_DIR`).

End-to-end tests that run the built binary live in `tests/cli.rs` (spawned via `CARGO_BIN_EXE_mcap`). Keep them at the process boundary — only behavior the unit tests can't reach, such as real exit codes and reading a non-seekable stdin pipe — and cover command logic with unit tests instead. Tests group by name prefix (`exit_code_*`, `stdin_pipe_*`, `completion_*`).

### Performance benchmarks

CLI benchmarks live in `benches/commands.rs` and run with `cargo bench -p mcap-cli --bench commands`. They are for performance work only: run them when explicitly improving performance or when a change could significantly affect CLI throughput. Do not run them for routine CLI edits.

The full default matrix uses a 250 MB generated workload per case and took about 20 minutes on a Cursor Cloud VM. In most cases, run the narrowest relevant Criterion filter instead of the full suite, for example `cargo bench -p mcap-cli --bench commands -- merge/indexed/100KB` or `cargo bench -p mcap-cli --bench commands -- filter/linear`.

Benchmarks are named `cli/<command>/<mode>/<payload>`. The modes are `indexed` (summary, chunk indexes, and message indexes present) and `linear` (summary omitted to force scan fallback). Payload sizes are `100B`, `1KB`, `10KB`, `100KB`, and `1MB`.
