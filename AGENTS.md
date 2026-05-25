# Development Guide

## Overview

This is a **polyglot library monorepo** for the [MCAP](https://mcap.dev) log file format, with implementations in TypeScript, Python, Go, Rust, C++, and Swift. There are no running services — this is purely a library/SDK project. The core functionality is writing and reading MCAP files.

## General prerequisites

- **Git LFS** — test data under `tests/conformance/data/` and `rust/mcap/tests/data/` is stored in Git LFS. Tests will fail with `InvalidMagic` errors if LFS pointers haven't been pulled. Run `git lfs pull` before running tests.
- **Conformance tests** orchestrate cross-language testing via the TypeScript harness: `yarn test:conformance:generate-inputs && yarn test:conformance`. They require Git LFS data and pre-built binaries for each target language.
- **Releasing** — see [RELEASING.md](./RELEASING.md) for the release process for each language.

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

The Go workspace (`go/go.work`) includes the core library (`go/mcap`), ROS tools (`go/ros`), CLI (`go/cli/mcap`), and conformance tests.

## Rust

**Prerequisites:** Rust stable (`rustup default stable`; CI uses the `stable` toolchain). Older `rustc` versions may fail to compile some dependencies.

| Action | Command                                                                  |
| ------ | ------------------------------------------------------------------------ |
| Build  | `cd rust && cargo build -p mcap --all-features`                          |
| Test   | `cd rust && cargo test -p mcap --all-features`                           |
| Lint   | `cd rust && cargo clippy -p mcap --all-targets -- --no-deps -D warnings` |
| Format | `cd rust && cargo fmt --all -- --check`                                  |

The Rust workspace includes the `mcap` library crate under `rust/mcap` and the `mcap-cli` CLI crate under `rust/cli`.

## C++

**Prerequisites:** Docker (builds run inside containers), Conan v1, CMake.

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

## Cursor Cloud specific instructions

This is a library-only monorepo — there are no services to start. Development involves building and testing the libraries in each language.

### Environment notes

- **PATH**: The update script ensures `$HOME/.local/bin` (for `uv`) and `$HOME/go/bin` (for `golangci-lint`) are on PATH via `~/.bashrc`. Ensure these are on PATH if running commands in a non-login shell.
- **Rust toolchain**: The default Rust stable toolchain must be current (run `rustup default stable`). The pre-installed rustc 1.83 is too old for some transitive dependencies (e.g., `time` crate requires `edition2024` support). The update script handles this.
- **Python version**: The `python/pyproject.toml` requires `>=3.10,<3.11`. Python 3.10 is installed from the deadsnakes PPA. Use `uv run --python python3.10` or the venv at `python/.venv` directly.
- **Prettier/fmt:check**: Running `yarn fmt:check` from the repo root will flag files inside `python/.venv/` because `.prettierignore` doesn't exclude it. This is expected in a dev environment; CI doesn't have the venv present. Lint individual TypeScript workspaces with `yarn workspace <name> lint:ci` instead.
- **Go build**: Run `go build ./...` from within a module directory (e.g., `go/mcap`), not from the `go/` workspace root directly.
- **C++ and Swift**: These require Docker and Swift compiler respectively, which are not installed by default in the Cloud VM. They are optional for most development work.
