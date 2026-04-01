# MCAP CLI (WIP)

This directory contains a WIP port of the MCAP CLI from Go to Rust.

It is not ready for production use yet.

## Implementation status

Status legend: 🟢 implemented, 🟡 partial, 🔴 not implemented.

| Command      | Status | Notes                                                                                                                                  |
| ------------ | ------ | -------------------------------------------------------------------------------------------------------------------------------------- |
| `add`        | 🟢     |                                                                                                                                        |
| `cat`        | 🟡     | Core `cat` output is implemented; Go-parity gaps remain (`--topic`/time-range filters, `--json`, stdin input, remote URI input).       |
| `compress`   | 🔴     |                                                                                                                                        |
| `convert`    | 🟡     | ROS1 bag → MCAP conversion is implemented (including `none`/`lz4`/`bz2` bag chunk decompression); ROS2 db3 input is not yet supported. |
| `decompress` | 🔴     |                                                                                                                                        |
| `doctor`     | 🟢     |                                                                                                                                        |
| `du`         | 🟢     |                                                                                                                                        |
| `filter`     | 🔴     |                                                                                                                                        |
| `get`        | 🟢     |                                                                                                                                        |
| `info`       | 🟢     |                                                                                                                                        |
| `list`       | 🟢     |                                                                                                                                        |
| `merge`      | 🔴     |                                                                                                                                        |
| `recover`    | 🔴     |                                                                                                                                        |
| `sort`       | 🔴     |                                                                                                                                        |
| `version`    | 🟢     |                                                                                                                                        |

## Intentional divergences from Go CLI

1. `mcap du` attachment accounting:
   - Rust CLI includes `attachment` record bytes in the top-level record stats table.
   - Go CLI currently skips attachment records in `du` record-kind accounting due to lexer behavior.
