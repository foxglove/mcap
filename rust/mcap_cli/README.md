# WIP Rust CLI

This directory contains a WIP Rust implementation of the MCAP CLI.

It is not ready for production use yet.

## Implementation status

| Command | Status | Notes |
| --- | --- | --- |
| `add` | 🔴 Not implemented | `add attachment` and `add metadata` are currently stubs. |
| `cat` | 🔴 Not implemented | Stub command. |
| `compress` | 🔴 Not implemented | Stub command. |
| `convert` | 🔴 Not implemented | Stub command. |
| `decompress` | 🔴 Not implemented | Stub command. |
| `doctor` | 🔴 Not implemented | Stub command. |
| `du` | 🟢 Implemented | Supports exact mode and `--approximate` mode with fallback behavior. |
| `filter` | 🔴 Not implemented | Stub command. |
| `get` | 🔴 Not implemented | `get attachment` and `get metadata` are currently stubs. |
| `info` | 🟢 Implemented | Reports MCAP file statistics. |
| `list` | 🟢 Implemented | Supports `attachments`, `channels`, `chunks`, `metadata`, and `schemas`. |
| `merge` | 🔴 Not implemented | Stub command. |
| `recover` | 🔴 Not implemented | Stub command. |
| `sort` | 🔴 Not implemented | Stub command. |
| `version` | 🟢 Implemented | Supports CLI and library version output. |

## Intentional divergences from Go CLI

1. `mcap du` attachment accounting:
   - Rust CLI includes `attachment` record bytes in the top-level record stats table.
   - Go CLI currently skips attachment records in `du` record-kind accounting due to lexer behavior.
