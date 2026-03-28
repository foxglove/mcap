# AGENTS.md

## Cursor Cloud specific instructions

This is a **polyglot library monorepo** (MCAP format) with TypeScript, Python, Go, Rust, C++, and Swift implementations. There are no running services — this is purely a library/SDK project. The "hello world" is writing and reading an MCAP file.

### Quick reference

| Language | Build/Test/Lint | Notes |
|---|---|---|
| TypeScript | `yarn typescript:build`, `yarn typescript:test`, `yarn workspace @mcap/core lint:ci`, `yarn fmt:check` | Yarn 4 via corepack; root `package.json` has all scripts |
| Python | `cd python && make test`, `cd python && make lint` | Requires Python 3.10 + pipenv; use `--python python3.10` (not `--deploy`) when the lockfile Python patch differs |
| Go | `cd go && make test`, `cd go && make lint` | Needs `golangci-lint` on PATH (`~/go/bin`) |
| Rust | `cd rust && cargo test -p mcap --all-features`, `cargo clippy -p mcap --all-targets -- --no-deps -D warnings` | Needs stable Rust ≥ 1.85 |
| C++ | `cd cpp && make build` (Docker-based) | Optional; requires Docker |
| Swift | `swift build && swift test` | Optional; requires Swift ≥ 6.1 |

### Non-obvious caveats

- **Yarn version**: The repo pins `yarn@4.12.0` in `package.json`. You must run `corepack enable` before `yarn install`. The system may have Yarn Classic (1.x) by default which is incompatible.
- **Git LFS**: Test data under `tests/conformance/data/` is stored in Git LFS. Some Python and conformance tests will fail with `InvalidMagic` errors if LFS pointers haven't been pulled. Run `git lfs pull` if tests fail on file parsing.
- **Python Pipfile requires 3.10**: The `[requires]` section pins `python_version = "3.10"`. Skip `--deploy` when using a slightly different patch version. Use `pipenv install --dev --python python3.10` instead of `make pipenv` if the `--deploy` flag fails.
- **Go PATH**: `golangci-lint` installs to `~/go/bin` which may not be on PATH by default. Ensure `$HOME/go/bin` is on PATH before running `make lint` in the `go/` directory.
- **Rust toolchain**: Dev dependencies pull crates requiring Rust edition 2024. The pre-installed Rust 1.83 is too old. Run `rustup default stable` to use a sufficiently recent toolchain (≥ 1.85).
- **Conformance tests** orchestrate cross-language testing via the TypeScript harness. To run them: `yarn test:conformance:generate-inputs && yarn test:conformance`. They require Git LFS data and built binaries for the target language(s).
- **No services to start**: This is a pure library project with no web servers, databases, or Docker Compose stacks.
