# AGENTS.md

## Overview

This is a **polyglot library monorepo** for the [MCAP](https://mcap.dev) container file format, with implementations in TypeScript, Python, Go, Rust, C++, and Swift. There are no running services — this is purely a library/SDK project. The core functionality is writing and reading MCAP files.

## Quick reference

| Language | Build | Test | Lint | Notes |
|---|---|---|---|---|
| TypeScript | `yarn typescript:build` | `yarn typescript:test` | `yarn workspace @mcap/core lint:ci`, `yarn fmt:check` | Yarn 4 via corepack; root `package.json` has all scripts |
| Python | `cd python && make build` | `cd python && make test` | `cd python && make lint` | Requires Python 3.10 + pipenv |
| Go | `cd go && make build-conformance-binaries` | `cd go && make test` | `cd go && make lint` | Requires `golangci-lint` on PATH |
| Rust | `cd rust && cargo build -p mcap --all-features` | `cd rust && cargo test -p mcap --all-features` | `cd rust && cargo clippy -p mcap --all-targets -- --no-deps -D warnings` | Needs stable Rust ≥ 1.85 |
| C++ | `cd cpp && make build` | `cd cpp && make test` | `cd cpp && make ci-format-check` | Docker-based build; optional |
| Swift | `swift build` | `swift test` | N/A | Requires Swift ≥ 6.1; optional |

## Prerequisites

- **Node.js ≥ 16** with `corepack enable` (activates the pinned `yarn@4.12.0`)
- **Python 3.10** with `pipenv` (`pip install pipenv`)
- **Go** (version from `go/go.work`) with `golangci-lint` (`go install github.com/golangci/golangci-lint/cmd/golangci-lint@v1.59.1`)
- **Rust stable ≥ 1.85** (`rustup default stable`)
- **Git LFS** (`git lfs pull` to fetch test data)

## Non-obvious caveats

- **Yarn version**: The repo pins `yarn@4.12.0` in `package.json`. You must run `corepack enable` before `yarn install`. Yarn Classic (1.x) is incompatible.
- **Git LFS**: Test data under `tests/conformance/data/` and `rust/tests/data/` is stored in Git LFS. Tests will fail with `InvalidMagic` errors if LFS pointers haven't been pulled. Run `git lfs pull` before running tests.
- **Python Pipfile requires 3.10**: The `[requires]` section pins `python_version = "3.10"`. Use `pipenv install --dev --python python3.10` instead of `make pipenv` if the `--deploy` flag fails due to a patch version mismatch.
- **Go linter PATH**: `golangci-lint` installs to `~/go/bin` which may not be on PATH. Ensure `$HOME/go/bin` is on PATH before running `make lint` in the `go/` directory.
- **Rust edition 2024**: Some dev-dependency crates require Rust edition 2024, so `rustc` < 1.85 will fail to compile tests. Run `rustup default stable` to ensure a recent enough toolchain.
- **Conformance tests**: These orchestrate cross-language testing via the TypeScript harness. Run `yarn test:conformance:generate-inputs && yarn test:conformance`. They require Git LFS data and pre-built binaries for each target language.
- **No services**: This is a pure library project — no web servers, databases, or Docker Compose stacks to run.
- **Releasing**: See [RELEASING.md](./RELEASING.md) for the release process for each language.
