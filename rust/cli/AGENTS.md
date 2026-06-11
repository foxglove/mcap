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
| `build.rs`   | Embeds the git SHA into the binary via the `GIT_SHA` env var.                                                                                                          |

## Conventions

### Exit codes

- `0` — success.
- `1` — hard failure (any handler returning `Err`).
- `2` — argument-parsing error (clap default).
- `3` — completed with warning-level data loss (e.g. `recover` discarded records or stopped on a truncated input). Represented by `CommandOutcome::Warnings`.

When adding a command that can complete despite losing data, return `CommandOutcome::Warnings` rather than printing a warning and exiting `0`.

### Remote inputs

Remote inputs (HTTP(S) and object-store URLs: `s3://`, `gs://`, and Azure `az://`/`abfs://`) are handled in `source.rs` via `object_store`. Bounded, indexed reads — a summary-section read, or a single attachment/metadata range read under the no-opt-in caps — are allowed without a flag. Any command that would scan or download an entire remote file requires the global `--allow-remote-scan` flag; gate new whole-file remote reads behind `SourceOptions::allow_remote_scan` accordingly.

### Output and logging

Results go to stdout; diagnostics and warnings go to stderr. Use the `render` helpers for tabular output so column alignment and byte/time formatting stay consistent.

### Testing

Most tests live inline in each module under `#[cfg(test)]`. Argument-parsing behavior is covered in `cli.rs`/`main.rs`, and dispatch/handler behavior in the relevant command module. For MCAP inputs, build fixtures in-memory with `mcap::Writer` rather than committing files. Committed binary fixtures are used where the input can't be synthesized that way — notably the `convert` tests, which load real ROS bag/db3 files from `testdata/` (resolved via `CARGO_MANIFEST_DIR`).

End-to-end tests that run the built binary live in `tests/cli.rs` (spawned via `CARGO_BIN_EXE_mcap`). Keep them at the process boundary — only behavior the unit tests can't reach, such as real exit codes and reading a non-seekable stdin pipe — and cover command logic with unit tests instead. Tests group by name prefix (`exit_code_*`, `stdin_pipe_*`, `completion_*`).
