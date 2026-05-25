# MCAP CLI (WIP)

This directory contains a WIP port of the MCAP CLI from Go to Rust.

It is not ready for production use yet.

## Implementation status

Status legend: 🟢 implemented, 🟡 partial, 🔴 not implemented.

| Command      | Status | Notes                                                                                                                                     |
| ------------ | ------ | ----------------------------------------------------------------------------------------------------------------------------------------- |
| `add`        | 🟢     |                                                                                                                                           |
| `cat`        | 🟡     | Local and stdin input are implemented, including `--topics`, time-range filters, and `--json`; remote URI input is not yet supported.     |
| `compress`   | 🟢     |                                                                                                                                           |
| `convert`    | 🟡     | ROS1 bag → MCAP conversion is implemented (including `none`/`lz4`/`bz2` bag chunk decompression); ROS2 db3 input is not yet supported.    |
| `decompress` | 🟢     |                                                                                                                                           |
| `doctor`     | 🟢     |                                                                                                                                           |
| `du`         | 🟢     |                                                                                                                                           |
| `filter`     | 🟢     |                                                                                                                                           |
| `get`        | 🟢     |                                                                                                                                           |
| `info`       | 🟢     |                                                                                                                                           |
| `list`       | 🟢     |                                                                                                                                           |
| `merge`      | 🟢     |                                                                                                                                           |
| `recover`    | 🟡     | Best-effort recovery is implemented for messages, attachments, and metadata; Go-parity gaps remain around raw chunk passthrough behavior. |
| `sort`       | 🟢     |                                                                                                                                           |
| `version`    | 🟢     |                                                                                                                                           |

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

## Intentional divergences from Go CLI

1. `mcap du` attachment accounting:
   - Rust CLI includes `attachment` record bytes in the top-level record stats table.
   - Go CLI currently skips attachment records in `du` record-kind accounting due to lexer behavior.
