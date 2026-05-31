# MCAP CLI (WIP)

This directory contains a WIP port of the MCAP CLI from Go to Rust.

It is not ready for production use yet.

## Implementation status

Status legend: 🟢 implemented, 🟡 partial, 🔴 not implemented.

| Command      | Status | Notes |
| ------------ | ------ | ----- |
| `add`        | 🟢     |       |
| `cat`        | 🟢     |       |
| `compress`   | 🟢     |       |
| `convert`    | 🟢     |       |
| `decompress` | 🟢     |       |
| `doctor`     | 🟢     |       |
| `du`         | 🟢     |       |
| `filter`     | 🟢     |       |
| `get`        | 🟢     |       |
| `info`       | 🟢     |       |
| `list`       | 🟢     |       |
| `merge`      | 🟢     |       |
| `recover`    | 🟢     |       |
| `sort`       | 🟢     |       |
| `version`    | 🟢     |       |

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
7. Future object-store remote input policy:
   - Before Rust CLI 1.0, apply the remote scan opt-in policy consistently to
     future object-store inputs such as S3, GCS, and Azure Blob Storage.
   - Preserve the current HTTP(S) behavior for those backends: summary/index-only
     operations should not require opt-in, while full-object scans/downloads and
     message chunk payload reads should require `--allow-remote-scan`.
8. Shared remote range-read APIs:
   - Before Rust CLI 1.0, consider moving the CLI-local coalesced summary range
     reads into reusable `mcap` crate reader APIs so HTTP(S), S3, GCS, and Azure
     Blob Storage backends can share tail/summary range reads instead of each
     transport adding its own read-ahead workaround.
   - A future `mcap` crate API should also make range-backed parsing of exact
     indexed records ergonomic without requiring callers to duplicate record
     parsing logic in the CLI.
   - Avoid double-touching summaryless remote inputs when `--allow-remote-scan`
     is set. Today the CLI attempts indexed discovery with a range probe and
     footer read before falling back to full-file materialization; a future
     implementation could share probe/footer state with materialization or skip
     indexed discovery for commands that already know they must scan.
9. Range-backed metadata and attachment transforms:
   - The CLI can read exact indexed metadata and attachment records for direct
     `get` / `list` commands without whole-file fallback for HTTP(S) inputs.
     Single indexed attachment and metadata reads are allowed as bounded range
     reads without a full-scan opt-in; multi-record metadata reads have a
     conservative no-opt-in byte cap.
   - Before Rust CLI 1.0, extend this pattern to metadata/attachment-preserving
     transforms so those commands do not need whole-file fallback for HTTP(S) and
     future object-store inputs.
10. `recover` chunk recompression optimization (deferred for valid chunks):
   - `recover` currently decodes every chunk, validates its records, and re-writes
     all records through the writer (which rebuilds chunks, indexes, the summary
     section, and CRCs). This guarantees a valid, readable output, and is
     implemented entirely in the CLI with **no new `mcap` crate public API**.
   - The cost is that valid chunks are recompressed even when their compression
     already matches the requested output compression. Go's `recover` avoids this
     by copying compressed chunk bytes through verbatim (the fast passthrough path
     from Go PR #1372, https://github.com/foxglove/mcap/pull/1372).
   - We deliberately do NOT do blind passthrough: it was a performance
     optimization that never validated chunk contents, so a chunk with a corrupt
     compressed payload (or a bad `uncompressed_crc`) is copied through and can
     produce an unreadable file. Always-valid output is treated as a hard
     invariant.
   - Discovered while designing this: a safe "avoid recompression" optimization
     still has to decode + strictly scan every chunk (you can't prove a chunk
     decodes without decoding it; the CRC is over the uncompressed data), and may
     only reuse the original compressed bytes when (a) the chunk's records frame
     and parse cleanly and (b) its compression already matches the target. It
     would also recompute and fix the stored CRC rather than propagate a bad one.
   - Before Rust CLI 1.0, decide whether to add this optimization. It requires new
     **public `mcap` crate API** (a compatibility commitment), at minimum:
     - `Writer::write_chunk(header: &ChunkHeader, data: &[u8], indexes: &[MessageIndex])`
       to append an already-compressed chunk plus its message indexes, updating
       the chunk index and statistics;
     - summary-only schema/channel registration (e.g. `register_schema` /
       `register_channel`) so schemas/channels that live inside a copied chunk
       still appear in the summary section without writing duplicate loose records;
     - optionally a public standalone-chunk decoder (otherwise the caller decodes
       chunk bodies itself).
     Adding this later is purely additive, so we ship the always-re-encode version
     now and revisit if recompression cost matters in practice.

## Intentional divergences from Go CLI

1. `mcap du` attachment accounting:
   - Rust CLI includes `attachment` record bytes in the top-level record stats table.
   - Go CLI currently skips attachment records in `du` record-kind accounting due to lexer behavior.
2. `mcap convert` ROS 2 db3 schema discovery:
   - Rust CLI converts self-contained db3 files using embedded message definitions, including non-`/msg/` topics such as service event topics. It fails the conversion if any topic is missing an embedded definition.
   - Go CLI ignores embedded db3 schemas and emulates ament resource lookup with `--ament-prefix-path`, which can miss schemas or silently use definitions from the wrong workspace.
3. Remote read policy:
   - Rust CLI requires `--allow-remote-scan` for remote full-object downloads,
     linear fallbacks, remote `convert` inputs, and remote message chunk payload
     reads such as `cat` output. Commands with multiple remote inputs materialize
     each remote input independently, so peak temporary disk usage can approach
     the sum of remote input sizes.
   - Go-compatible behavior allowed those remote reads without an explicit
     opt-in.
4. `mcap recover` always produces a valid output:
   - Rust CLI `recover` decodes and validates every chunk and re-writes records
     through the writer, so the output is always a readable MCAP with correct
     CRCs and rebuilt indexes. Undecodable/corrupt chunk payloads stop the scan
     (keeping what was recovered) rather than being copied through.
   - Go `recover` copies chunk bytes through verbatim by default and can emit
     corrupt or bad-CRC chunks, producing files that strict readers reject.
5. `mcap recover` compression is opt-in (`--compression preserve` by default):
   - Rust CLI `recover` defaults to `--compression preserve`, keeping the input
     file's compression (uncompressed if the input is unchunked). Pass
     `--compression zstd|lz4|none` to choose explicitly.
   - Go `recover` defaults to `zstd` and compresses uncompressed/loose input by
     default.
   - The Rust CLI also has no `--always-decode-chunk` flag (Go does): chunks are
     always decoded, and `--compression` alone determines the output codec.
