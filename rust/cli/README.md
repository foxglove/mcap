# MCAP CLI (WIP)

This directory contains a WIP port of the MCAP CLI from Go to Rust.

It is not ready for production use yet.

## Implementation status

Status legend: 🟢 implemented, 🟡 partial, 🔴 not implemented.

| Command      | Status | Notes                                                                                                                                                                                     |
| ------------ | ------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `add`        | 🟢     |                                                                                                                                                                                           |
| `cat`        | 🟡     | Local and stdin input are implemented, including `--topics`, time-range filters, and `--json`; remote URI input is not yet supported.                                                     |
| `compress`   | 🟢     |                                                                                                                                                                                           |
| `convert`    | 🟡     | ROS 1 bag → MCAP conversion is implemented (including `none`/`lz4`/`bz2` bag chunk decompression); ROS 2 db3 → MCAP conversion is implemented for bags with embedded message definitions. |
| `decompress` | 🟢     |                                                                                                                                                                                           |
| `doctor`     | 🟢     |                                                                                                                                                                                           |
| `du`         | 🟢     |                                                                                                                                                                                           |
| `filter`     | 🟢     |                                                                                                                                                                                           |
| `get`        | 🟢     |                                                                                                                                                                                           |
| `info`       | 🟢     |                                                                                                                                                                                           |
| `list`       | 🟢     |                                                                                                                                                                                           |
| `merge`      | 🟢     |                                                                                                                                                                                           |
| `recover`    | 🟡     | Best-effort recovery is implemented for messages, attachments, and metadata; Go-parity gaps remain around raw chunk passthrough behavior.                                                 |
| `sort`       | 🟢     |                                                                                                                                                                                           |
| `version`    | 🟢     |                                                                                                                                                                                           |

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
   - Before Rust CLI 1.0, decide whether output-producing commands should fail
     when the output path exists unless the user passes `--force` / `-f`.
   - Apply the policy consistently so users do not have to remember separate
     overwrite behavior per command.
4. Git LFS pointer detection:
   - `mcap convert` detects Git LFS pointer inputs and tells users to run
     `git lfs pull`.
   - Before Rust CLI 1.0, centralize that check so all commands that read local
     MCAP, bag, db3, or other LFS-backed fixture files produce the same
     actionable error instead of lower-level parse failures.
5. Converter input architecture:
   - Future `mcap convert` inputs may include directory-shaped formats such as
     rosbag2 directories and additional self-describing file formats such as PX4
     ULog (`.ulg`).
   - Keep converter dispatch path-based rather than reader-based: the top-level
     command should identify the broad input format from the path shape or
     extension, while the selected converter validates magic bytes and format
     details before creating the output.
   - Preserve clear ownership of unsupported-but-recognized inputs. For example,
     a ROS 2 bag directory without embedded schemas should fail from the ROS 2
     converter with an actionable schema error rather than falling through as an
     unidentified input.

## Intentional divergences from Go CLI

1. `mcap du` attachment accounting:
   - Rust CLI includes `attachment` record bytes in the top-level record stats table.
   - Go CLI currently skips attachment records in `du` record-kind accounting due to lexer behavior.
2. `mcap convert` ROS 2 db3 schema discovery:
   - Rust CLI converts self-contained db3 files using embedded message definitions, including non-`/msg/` topics such as service event topics. It fails the conversion if any topic is missing an embedded definition.
   - Go CLI ignores embedded db3 schemas and emulates ament resource lookup with `--ament-prefix-path`, which can miss schemas or silently use definitions from the wrong workspace.
