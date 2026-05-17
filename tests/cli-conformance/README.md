# MCAP CLI conformance

This directory contains temporary Go-vs-Rust CLI conformance tests. The goal is to
make the upcoming Rust `mcap` CLI behavior match the legacy Go `mcap` CLI before
Rust CLI v1.0.

The tests compare both CLIs on the same fixtures and require every intentional or
known behavior difference to be documented in the case manifest. Undocumented
differences are test failures.

## Lifecycle

This directory is intentionally self-contained because it is expected to be
deleted once the Go CLI is retired and the Rust CLI has reached the parity and
release goals.

Expected cleanup when that happens:

1. Delete `tests/cli-conformance/`.
2. Remove the `tests/cli-conformance` root workspace entry and
   `test:cli-conformance` script.
3. Remove the `cli-conformance` CI job.

## What is tested

The framework has two related goals:

- **CLI conformance**: a concrete test harness for CLI behavior.
- **CLI parity**: the desired Go-vs-Rust outcome.

Most cases run the same command against the Go and Rust binaries and compare
exit code, stdout/stderr, and any output files. MCAP-producing commands first
try byte-for-byte comparison when configured; if byte equality is not stable or
not required, cases can compare semantic MCAP records or just message streams.

Terminal table output may be normalized so alignment differences do not fail a
case when the underlying rows are the same.

## Running locally

Build both CLIs first:

```bash
make -C go/cli/mcap build
cd rust && cargo build -p mcap-cli
```

Ensure conformance fixtures exist:

```bash
yarn test:conformance:generate-inputs --verify
```

Run the CLI conformance suite from the repository root:

```bash
yarn test:cli-conformance \
  --go-bin "$(pwd)/go/cli/mcap/bin/mcap" \
  --rust-bin "$(pwd)/rust/target/debug/mcap"
```

Run a subset:

```bash
yarn workspace @foxglove/mcap-cli-conformance run-tests \
  --data-dir "$(pwd)/tests/conformance/data" \
  --go-bin "$(pwd)/go/cli/mcap/bin/mcap" \
  --rust-bin "$(pwd)/rust/target/debug/mcap" \
  --case-regex "cat|filter"
```

Keep temporary work directories for debugging:

```bash
yarn workspace @foxglove/mcap-cli-conformance run-tests \
  --data-dir "$(pwd)/tests/conformance/data" \
  --go-bin "$(pwd)/go/cli/mcap/bin/mcap" \
  --rust-bin "$(pwd)/rust/target/debug/mcap" \
  --keep-work-dir
```

## Performance checks

Performance checks are intentionally opt-in and report-only by default because
CI environments are noisy. They provide an early Rust/Go timing ratio for a
small set of representative commands.

Build release binaries before using performance checks:

```bash
make -C go/cli/mcap build
cd rust && cargo build -p mcap-cli --release
```

Run the performance checks:

```bash
yarn workspace @foxglove/mcap-cli-conformance perf \
  --data-dir "$(pwd)/tests/conformance/data" \
  --go-bin "$(pwd)/go/cli/mcap/bin/mcap" \
  --rust-bin "$(pwd)/rust/target/release/mcap"
```

Pass `--fail-on-regression` only when you intentionally want the command to exit
nonzero if Rust exceeds a case's configured margin.

## Adding cases

Add cases in `src/cases.ts`.

Use normal parity cases when both CLIs should behave the same:

- set `invocation.args` to the shared command arguments;
- add setup actions when commands mutate files or need temporary inputs;
- choose comparators for stdout, stderr, and output files.

Use `knownDifference` when behavior intentionally or temporarily differs. Known
differences are assertions, not skips: the case must document and verify the Go
behavior and the Rust behavior.

Every known difference must include:

- stable id;
- summary;
- reason;
- desired behavior;
- Go behavior expectation;
- Rust behavior expectation.

## Comparator guidance

- Use `bytes` for payloads that must match exactly, such as extracted
  attachments.
- Use `mcap` with `mode: "messages"` when writer metadata, chunking, compression,
  or summary offsets may differ but message content must match.
- Use `mcap` with `mode: "records"` only when all records are expected to be
  semantically equivalent.
- Use `json` for JSON stdout.
- Use `table` for aligned terminal tables where spacing may differ.
- Use `text` for command output where exact normalized text matters.

For MCAP output, prefer byte-for-byte parity when practical. When byte equality
is not practical, the test should still ensure the message stream is identical
unless the difference is explicitly documented as a known difference.
