# Development Guide

## Overview

This is a **polyglot library monorepo** for the [MCAP](https://mcap.dev) log file format, with implementations in TypeScript, Python, Go, Rust, C++, and Swift. There are no running services — this is purely a library/SDK project. The core functionality is writing and reading MCAP files.

**MCAP** is a modular container file format for recording timestamped pub/sub messages with arbitrary serialization formats. It is designed to work well under various workloads, resource constraints, and durability requirements. The format specification lives in `website/docs/spec/index.md`, with the well-known registry in `website/docs/spec/registry.md`, and feature notes in `website/docs/spec/notes.md`.

## Design principles

**Bounded memory when reading.** Neither the language libraries nor the CLI may read (or force a consumer to read) an entire MCAP file into memory — files are typically 1–10 GB but must be assumed to reach hundreds of GB, or TB+ in the worst case. Reader memory should scale with the record or chunk being processed, not with the file length: holding one record, chunk, or attachment at a time is fine, but buffering the whole file, or all of its messages, is an out-of-memory foot-gun.

- Memory-map (`mmap`) seekable local files where the language supports it, or have the API consumer supply the bytes (e.g. via their own mmap); use seek + bounded range reads for random access and streaming for sequential scans.
- Prefer explicit `read`/`seek` (I/O-agnostic "sans-io" readers) as the default access method and treat `mmap` as an optimization. What matters is never requiring the whole file to be resident; the access method is secondary. (`mmap` pages a real disk file on demand, but inflates resident memory and helps nothing when the bytes live on a tmpfs spool.)
- When input isn't seekable (e.g. a stdin pipe) or an operation needs random access over a stream (e.g. sorting), spool to a temporary file rather than buffering in memory. Put the spool on the output volume (or a configured temp dir), not `/tmp` — it is often a tmpfs (RAM/swap-backed) that defeats the spool.

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

When reading, prefer the I/O-agnostic `mcap::sans_io` readers (`LinearReader`, `IndexedReader`, `SummaryReader`) driven from a `File`'s `read`/`seek` for bounded, large-file, stdin, or remote paths. The `mcap::read` slice API (`MessageStream`, `LinearReader`, `Summary::read`/`stream_chunk`, `read::attachment`/`metadata`) is a convenience layer over `sans_io` that assumes the whole file is addressable as `&[u8]` (memory-mapped or buffered); use it only when that already holds, not as the path that scales.

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
