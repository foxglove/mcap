# WIP Rust CLI

This directory contains a WIP Rust implementation of the MCAP CLI.

It is not ready for production use yet.

## Implementation status

Status legend: 🟢 implemented, 🟡 partial, 🔴 not implemented.

| Command | Status | Notes |
| --- | --- | --- |
| `add` | 🔴 | — |
| `cat` | 🔴 | — |
| `compress` | 🔴 | — |
| `convert` | 🔴 | — |
| `decompress` | 🔴 | — |
| `doctor` | 🔴 | — |
| `du` | 🟢 | Exact mode and `--approximate` mode are implemented (with approximate fallback behavior). |
| `filter` | 🔴 | — |
| `get` | 🔴 | — |
| `info` | 🟢 | Implemented. |
| `list` | 🟢 | Implemented for `attachments`, `channels`, `chunks`, `metadata`, and `schemas`. |
| `merge` | 🔴 | — |
| `recover` | 🔴 | — |
| `sort` | 🔴 | — |
| `version` | 🟢 | Implemented with both CLI and library version output modes. |

## Intentional divergences from Go CLI

1. `mcap du` attachment accounting:
   - Rust CLI includes `attachment` record bytes in the top-level record stats table.
   - Go CLI currently skips attachment records in `du` record-kind accounting due to lexer behavior.
