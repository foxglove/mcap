# Development Guide

## Overview

This is a **polyglot library monorepo** for the [MCAP](https://mcap.dev) log file format, with implementations in TypeScript, Python, Go, Rust, C++, and Swift. There are no running services — this is purely a library/SDK project. The core functionality is writing and reading MCAP files.

**MCAP** is a modular container file format for recording timestamped pub/sub messages with arbitrary serialization formats. It is designed to work well under various workloads, resource constraints, and durability requirements. The format specification lives in `website/docs/spec/index.md`, with the well-known registry in `website/docs/spec/registry.md`, and feature notes in `website/docs/spec/notes.md`.

## Design principles

- **Bounded memory when reading.** Never read (or force a consumer to read) a whole MCAP file into memory — files can reach hundreds of GB. Reader memory must scale with the working set (the current record or chunk), not the file length.
  - **Read through a caller-supplied byte source.** Take a small abstraction the caller implements, so the library fetches only the bytes it needs and stays transport-agnostic (local file, in-memory buffer, memory map, HTTP range requests). Convenience constructors that open a path or wrap a stream are fine as sugar over this abstraction, not as the only way in.
  - **Yield lazily; hold only the current working set.** Expose records and messages as the language's lazy iterator, generator, or stream. File- and sequential-order reads keep one decompressed chunk at a time; log-time order keeps only the chunks whose time ranges currently overlap.
  - **Keep resident state proportional to indexes, not payloads.** Reading the summary holds index metadata (chunk, attachment, and metadata indexes, plus channels, schemas, and statistics), bounded by record count rather than payload size. Never fake an index-only capability by buffering the whole file: log-time ordering across overlapping chunks requires the index (or an already-sorted file), so on unindexed input degrade to file order or fail explicitly rather than sorting every message in memory.

- **Match the reader to the source, not the file's layout.** Build the reading API on the source axis — a random-access (seekable) source versus a forward-only stream — and let indexed-versus-linear reading be an internal detail.
  - **Prefer the language's standard I/O interfaces.** Use what already exists (Go `io.ReaderAt` / `io.ReadSeeker`, Rust `Read` + `Seek`, a seekable Python file object, C++ `std::istream`); invent an abstraction (TypeScript `IReadable`, Swift `IRandomAccessReadable`) only where the language lacks one.
  - **Hide indexed-versus-unindexed from ordinary reads.** A caller asking to read the messages shouldn't have to know whether the file is indexed: over a seekable source, use the summary and indexes when present and fall back to a bounded forward scan when absent. A seekable source stays more capable than a stream even without an index — skip past payloads, peek at the footer, or build an index in one pass — so don't cripple it when a file is unindexed. A non-seekable source (pipe, stdin) can only read forward.
  - **Let performance-sensitive callers opt in (C++, Rust).** Expose enough to introspect the summary and, where it matters, require indexed access, so a silent full-file scan never surprises a caller who expected a seek.

- **Feel idiomatic, and layer the API by audience.** Each library should read like it belongs in its language — naming, iteration protocols, and error handling included — not like a port of another implementation. Layer the surface so depth matches the reader: a high-level "read the messages" API on top, record-level iteration beneath it, and low-level primitives (parse a record from a byte range at a given offset) at the bottom. Systems languages (C++, Rust) should expose the lower layers for the CLI and power users, clearly marked as advanced; most users should never need to reach past the top.

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
