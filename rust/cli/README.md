# MCAP CLI (WIP)

This directory contains a WIP port of the MCAP CLI from Go to Rust.

It is not ready for production use yet.

## Implementation status

Status legend: 🟢 implemented, 🟡 partial, 🔴 not implemented.

| Command      | Status | Notes                                                        |
| ------------ | ------ | ------------------------------------------------------------ |
| `add`        | 🟢     |                                                              |
| `cat`        | 🟢     |                                                              |
| `compress`   | 🟢     |                                                              |
| `convert`    | 🟢     |                                                              |
| `decompress` | 🟢     |                                                              |
| `doctor`     | 🟢     |                                                              |
| `du`         | 🟢     |                                                              |
| `filter`     | 🟢     |                                                              |
| `get`        | 🟢     |                                                              |
| `info`       | 🟢     |                                                              |
| `list`       | 🟢     |                                                              |
| `merge`      | 🟢     |                                                              |
| `recover`    | 🟡     | Go-parity gaps remain around raw chunk passthrough behavior. |
| `sort`       | 🟢     |                                                              |
| `version`    | 🟢     |                                                              |

## Pre-1.0 compatibility cleanup

The Rust CLI is currently prioritizing Go CLI parity. Before declaring a Rust CLI
1.0, revisit compatibility behaviors that are awkward enough to change while the
port is still pre-production:

1. Time range arguments:
   - Current Go-compatible behavior treats bare numeric `--start` / `--end`
     values as integer nanoseconds, while `--start-secs` / `--end-secs` select
     seconds and `--start-nsecs` / `--end-nsecs` select nanoseconds.
   - This matches MCAP's internal timestamp unit and preserves copy/paste from
     existing `mcap cat` output, but it is surprising for CLI users.
   - For Rust CLI 1.0, remove the split `--*-secs` / `--*-nsecs` variants and
     standardize all commands on `--start` / `--end` where applicable.
   - Parse `--start` / `--end` values as RFC3339 timestamps, exact decimal
     seconds strings (for example `1.23456789`), or explicit unit-suffixed
     durations/timestamps such as `1.5s`, `250ms`, and
     `1709146829659264519ns`.
   - Apply the same timestamp input syntax consistently across all commands
     that accept times, including `cat`, `filter`, `add attachment`, and any
     future time-filtered commands.
2. Topic filter arguments:
   - Current Go-compatible `cat --topics` accepts one comma-separated string.
   - Before Rust CLI 1.0, decide whether topic filtering should use the comma
     list, repeatable flags such as `--topic foo --topic bar`, or support both.
   - Apply the chosen shape consistently across commands that filter by topic so
     users do not have to learn separate `cat` and `filter` conventions.
3. Output overwrite behavior:
   - Current Go-compatible behavior overwrites existing output files for commands
     such as `convert`, `filter`, `merge`, `recover`, and `sort`.
   - Current output path syntax is inconsistent: `convert` uses a positional
     output path, while other output-producing commands generally use
     `-o` / `--output` or `--output-file`.
   - Before Rust CLI 1.0, decide whether output-producing commands should fail
     when the output path exists unless the user passes `--force` / `-f`.
   - Decide whether Rust CLI output paths should standardize on positional
     arguments or `-o` / `--output` flags, then apply that consistently.
   - Write output-producing commands through a temporary sibling file and rename
     it into place only after successful completion, so failures do not leave a
     partial output at the requested path.
   - Apply the policy consistently so users do not have to remember separate
     overwrite behavior per command.
4. Git LFS pointer detection:
   - `mcap convert` detects Git LFS pointer inputs and tells users to run
     `git lfs pull`.
   - Before Rust CLI 1.0, centralize that check so all commands that read local
     MCAP, bag, db3, or other LFS-backed fixture files produce the same
     actionable error instead of lower-level parse failures.
5. PX4 ULog conversion:
   - Before Rust CLI 1.0, decide whether to support converting PX4 ULog (`.ulg`)
     files to MCAP.
   - ULog is a self-describing binary log format with embedded message
     definitions, so it should fit the same extension-based `mcap convert`
     dispatch model used for ROS 1 bag and ROS 2 db3 inputs.
6. Convert output paths and multiple inputs:
   - Current Go-compatible `mcap convert` requires exactly one input path and one
     output path.
   - If `convert` keeps or adopts optional output paths, consider writing a
     sibling file next to each input by replacing the input extension with
     `.mcap` when no explicit output is provided.
   - This would also allow `mcap convert` to accept multiple input paths in one
     invocation, including wildcard-expanded paths from the user's shell.
7. Remote read policy:
   - Current Go-compatible behavior allows remote reads without an opt-in flag,
     including commands or inputs that require reading the entire remote file.
   - Before Rust CLI 1.0, decide whether to keep this behavior or reintroduce an
     explicit opt-in such as `--allow-remote-scan` for commands that cannot use
     indexed range reads.
   - Apply the chosen policy consistently across HTTP(S) and future object-store
     inputs such as S3, GCS, and Azure Blob Storage.
8. Remote indexed read performance:
   - Current HTTP(S) range reads use the generic seek/read interface, which can
     issue several small range requests while reading MCAP headers, footers, and
     summaries.
   - Before Rust CLI 1.0, optimize summary/index reading in the underlying MCAP
     reader APIs so HTTP(S), S3, GCS, and Azure Blob Storage backends can share
     coalesced tail/summary range reads instead of each transport adding its own
     read-ahead workaround.
9. Range-backed metadata and attachment reads:
   - Current metadata and attachment convenience helpers, such as
     `mcap::read::metadata` and `mcap::read::attachment`, require a full MCAP
     byte slice.
   - Before Rust CLI 1.0, add CLI or `mcap` crate helpers that can read
     metadata and attachment records from indexed byte ranges so `get metadata`,
     `list metadata`, `get attachment`, and metadata/attachment-preserving
     transforms do not need whole-file fallback for HTTP(S) and future
     object-store inputs.

## Intentional divergences from Go CLI

1. `mcap du` attachment accounting:
   - Rust CLI includes `attachment` record bytes in the top-level record stats table.
   - Go CLI currently skips attachment records in `du` record-kind accounting due to lexer behavior.
2. `mcap convert` ROS 2 db3 schema discovery:
   - Rust CLI converts self-contained db3 files using embedded message definitions, including non-`/msg/` topics such as service event topics. It fails the conversion if any topic is missing an embedded definition.
   - Go CLI ignores embedded db3 schemas and emulates ament resource lookup with `--ament-prefix-path`, which can miss schemas or silently use definitions from the wrong workspace.
