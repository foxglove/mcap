# Development Guide

## Overview

This is a **polyglot library monorepo** for the [MCAP](https://mcap.dev) log file format, with implementations in TypeScript, Python, Go, Rust, C++, and Swift. There are no running services — this is purely a library/SDK project. The core functionality is writing and reading MCAP files.

**MCAP** is a modular container file format for recording timestamped pub/sub messages with arbitrary serialization formats. It is designed to work well under various workloads, resource constraints, and durability requirements. The format specification lives in `website/docs/spec/index.md`, with the well-known registry in `website/docs/spec/registry.md`, and feature notes in `website/docs/spec/notes.md`.

## Design principles

### Bounded memory when reading

MCAP files can be arbitrarily large (many GB). **Reader code paths must never require buffering an entire MCAP file into memory at once.** Memory use should stay bounded relative to the file size — it should scale with the current record/chunk being processed and any explicit caches, not with the total file length.

Practically, this means:

- **Prefer memory-mapping (`mmap`) for local files** in languages that support it (e.g. the Rust CLI maps files via `memmap2`). An mmap keeps the resident working set bounded because the OS pages data in on demand.
- **Prefer seek + bounded range reads** for random-access/indexed reads, and **streaming** (incremental record-by-record parsing) for sequential reads, over reading the whole file into a heap buffer.
- **For non-seekable inputs (e.g. a stdin pipe), spool the stream to a temporary file and mmap that**, rather than reading it all into a `Vec`/`Buffer`/`bytes`/`Data`. See `rust/cli/src/source.rs` (`load_input`) for the reference implementation.
- Reading a single record, chunk, or attachment into memory is acceptable (these are bounded by the data being requested), but avoid materializing _all_ messages/records of a file at once unless the caller explicitly opts in (e.g. Python's `NonSeekingReader` with `log_time_order=True`, which documents the whole-file cost).

When adding or changing a reader path, keep it within these bounds; if a full-file or full-collection read is unavoidable, make it explicit and opt-in.

## General prerequisites

- **Git LFS** — test data under `tests/conformance/data/` and `rust/mcap/tests/data/` is stored in Git LFS. Tests will fail with `InvalidMagic` errors if LFS pointers haven't been pulled. Run `git lfs pull` before running tests.
- **Conformance tests** orchestrate cross-language testing via the TypeScript harness: `yarn test:conformance:generate-inputs && yarn test:conformance`. They require Git LFS data and pre-built binaries for each target language.
- **Releasing** — see [RELEASING.md](./RELEASING.md) for the release process for each language.

## Pull requests

PR titles should start with a lowercase keyword prefix followed by a colon, usually the package or subsystem being edited, such as `cli:`, `rust:`, `python:`, `go:`, `typescript:`, `cpp:`, `swift:`, `ci:`, or `docs:`.

## TypeScript

**Prerequisites:** Node.js ≥ 18.12 with `corepack enable` (activates the Yarn version pinned via `packageManager` in `package.json`). Yarn Classic (1.x) is incompatible — the repo requires Yarn 4.x.

```
corepack enable
yarn install
```

| Action    | Command                                                                                           |
| --------- | ------------------------------------------------------------------------------------------------- |
| Build     | `yarn typescript:build`                                                                           |
| Test      | `yarn typescript:test`                                                                            |
| Lint      | `yarn workspace @mcap/core lint:ci` (repeat for `@mcap/support`, `@mcap/nodejs`, `@mcap/browser`) |
| Format    | `yarn fmt:check` (check) / `yarn fmt` (fix)                                                       |
| Typecheck | `yarn tsc:all`                                                                                    |
| Validate  | `yarn workspace @foxglove/mcap-example-validate validate FILE`                                    |
| Bench     | `yarn workspace @foxglove/mcap-benchmarks bench`                                                  |

All scripts are defined in the root `package.json`. Each workspace (`@mcap/core`, `@mcap/support`, `@mcap/nodejs`, `@mcap/browser`) also has its own `lint:ci`, `build`, and `test` scripts.

## Python

**Prerequisites:** Python 3.10 + [uv](https://docs.astral.sh/uv/).

```
python3 -m pip install uv
cd python
uv sync --frozen
```

| Action | Command                   |
| ------ | ------------------------- |
| Build  | `cd python && make build` |
| Test   | `cd python && make test`  |
| Lint   | `cd python && make lint`  |

The Python directory contains four packages: `mcap`, `mcap-protobuf-support`, `mcap-ros1-support`, and `mcap-ros2-support`. The Makefile runs tests and lint across all four.

## Go

**Prerequisites:** Go (version from `go/go.work`) + `golangci-lint` (see `.github/workflows/ci.yml` for the version used in CI). `golangci-lint` installs to `~/go/bin` — ensure `$HOME/go/bin` is on PATH.

| Action | Command                   |
| ------ | ------------------------- |
| Build  | `cd go && go build ./...` |
| Test   | `cd go && make test`      |
| Lint   | `cd go && make lint`      |

The Go workspace (`go/go.work`) includes the core library (`go/mcap`), ROS tools (`go/ros`), and conformance tests.

## Rust

**Prerequisites:** Rust stable (`rustup default stable`; CI uses the `stable` toolchain). Older `rustc` versions may fail to compile some dependencies.

| Action | Command                                                       |
| ------ | ------------------------------------------------------------- |
| Build  | `cargo build -p mcap --all-features`                          |
| Test   | `cargo test -p mcap --all-features`                           |
| Lint   | `cargo clippy -p mcap --all-targets -- --no-deps -D warnings` |
| Format | `cargo fmt --all -- --check`                                  |

The Rust workspace is defined at the repo root and includes the `mcap` library crate under `rust/mcap` and the `mcap-cli` CLI crate under `rust/cli`.

## C++

**Prerequisites:** Docker (builds run inside containers), Conan 2, CMake.

| Action | Command                       |
| ------ | ----------------------------- |
| Build  | `cd cpp && make build`        |
| Test   | `cd cpp && make test`         |
| Format | `cd cpp && make format-check` |

## Swift

**Prerequisites:** Swift ≥ 5.5. `Package.swift` is at the **repo root** — run these commands from the root, not from `swift/`.

| Action | Command                                                              |
| ------ | -------------------------------------------------------------------- |
| Build  | `swift build`                                                        |
| Test   | `swift test`                                                         |
| Lint   | See SwiftLint and SwiftFormat commands in `.github/workflows/ci.yml` |
