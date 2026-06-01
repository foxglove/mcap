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

## Pre-1.0 improvements

The Rust CLI is currently prioritizing Go CLI parity. Once we reach parity, we will publish the first Rust-powered release of the MCAP CLI (likely tagged as v0.1.0), and remove the legacy Go CLI.

After the Rust CLI is in production, the following is a list of potential improvements discovered during the port that we may wish to address prior to CLI v1.0.0:

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
   - Decide whether stdin/stdout should use explicit `-` path arguments instead
     of Go-compatible implicit stdin/stdout behavior (for example, omitted
     output path plus redirected stdout).
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
    - `recover` currently decodes every chunk, validates its records, and
      re-writes all records through the writer (which rebuilds chunks, indexes,
      the summary section, and CRCs). This guarantees a valid, readable output
      and is implemented entirely in the CLI with no new `mcap` crate public API.
    - The cost is that valid chunks are recompressed even when their compression
      already matches the requested output compression. Go's `recover` avoids
      this by copying compressed chunk bytes through verbatim (the fast
      passthrough path from Go PR #1372,
      https://github.com/foxglove/mcap/pull/1372).
    - We deliberately do NOT do blind passthrough: it was a performance
      optimization that never validated chunk contents, so a chunk with a
      corrupt compressed payload (or a bad `uncompressed_crc`) is copied through
      and can produce an unreadable file. Always-valid output is a hard
      invariant.
    - A safe "avoid recompression" optimization still has to decode and strictly
      scan every chunk (you cannot prove a chunk decodes without decoding it; the
      CRC is over the uncompressed data), and may reuse the original compressed
      bytes only when the chunk's records frame and parse cleanly and its
      compression already matches the target. It would also recompute and fix the
      stored CRC rather than propagate a bad one.
    - Before Rust CLI 1.0, decide whether to add this optimization. It requires
      new public `mcap` crate API (a compatibility commitment): a
      `Writer::write_chunk(header, data, &[MessageIndex])` to append an
      already-compressed chunk plus its message indexes (updating the chunk index
      and statistics), summary-only schema/channel registration so
      schemas/channels inside a copied chunk still appear in the summary without
      duplicate loose records, and optionally a public standalone-chunk decoder.
      The same work could make `--compression preserve` preserve compression per
      chunk instead of normalizing output to the first recovered chunk's
      compression.
      Adding this later is purely additive, so we ship the always-re-encode
      version now and revisit if recompression cost matters in practice.
11. `recover` out-of-order schema/channel handling:
    - `recover` registers each channel with the writer as soon as it is read, so a
      channel that appears before the schema it references is dropped (along with
      every message on that channel) and the recovery is reported as lossy.
    - Well-formed MCAP always writes a schema before any channel that references
      it, so this is fine in practice, but `recover` exists for malformed inputs
      and the previous implementation buffered such channels until their schema
      arrived.
    - Before Rust CLI 1.0, decide whether `recover` should buffer channels with a
      not-yet-seen schema id and flush them once the schema is recovered, rather
      than discarding them on first sight.
12. Shared lenient-scan infrastructure for `recover` and `doctor`:
    - `recover` and `doctor` independently implement the same lenient, single-pass
      scan of the data section: same permissive `LinearReader` options
      (`emit_chunks`, record-length limit), the same chunk decode via
      `ChunkReader`, the same `BadChunkCrc`/truncation handling, the same
      stop-at-`DataEnd`/`Footer` boundary, and the same per-record-kind dispatch
      (`recover::recover_records`/`recover_chunk_records` vs
      `doctor::scan_top_level`/`examine_chunk`).
    - The two commands diverge in intent: `doctor` is a read-only validator that
      also cross-checks the summary section and treats a chunk CRC mismatch as an
      error, while `recover` rebuilds output, ignores the summary, and treats a
      CRC mismatch as benign (it recomputes one on re-encode). They currently make
      these choices independently, so the differences are accidental rather than
      explicit.
    - Before Rust CLI 1.0, decide whether to factor the lenient chunk-decoding
      scan into one place (a `commands::common` scanner that both commands drive
      with their own per-record visitors, or a lower-level primitive in
      `mcap::sans_io`), keeping summary validation and the write path
      command-specific. This would remove the most error-prone duplication and
      force the `doctor`/`recover` policy differences to be stated explicitly.
13. `recover` attachment-CRC tolerance:
    - `recover` tolerates a bad chunk CRC (`with_validate_chunk_crcs(false)`, plus
      explicit `BadChunkCrc` handling) and recomputes a correct CRC on re-encode,
      but it has no equivalent path for a loose attachment: `mcap::parse_record`
      validates the attachment's stored CRC and returns `BadAttachmentCrc` on
      mismatch, so `recover` currently discards the whole attachment (counted as
      lossy, exit 3) even when the payload is intact. The output stays valid; the
      attachment is simply dropped.
    - Closing this consistently needs an `mcap` crate read option, not a CLI hack:
      the CLI-only workaround is to copy the record body and zero its trailing
      4-byte CRC so the `crc != 0` guard is skipped, but that duplicates the whole
      (potentially large) attachment in memory just to bypass validation and leans
      on the body layout rather than a supported API.
    - Before Rust CLI 1.0, add a first-class read option (for example a
      `LinearReaderOptions::with_validate_attachment_crcs` toggle mirroring
      `with_validate_chunk_crcs`, or a `parse_record` variant that surfaces the bad
      CRC without erroring) and have `recover` salvage the payload through it,
      recomputing the CRC on write so attachments match the lenient chunk
      behavior.

## Intentional divergences from Go CLI

### General

1. Remote read opt-in:
   - The Rust CLI requires `--allow-remote-scan` whenever a command would scan or
     download a remote (HTTP/S3) file in full, rather than a bounded indexed read.
     This covers whole-file commands such as `merge`, `filter`, and `convert`
     (including remote `convert` inputs and full-object downloads), as well as scan
     fallbacks for otherwise-indexed commands (for example, a remote file with no
     summary section, a server without range-request support, or `cat` falling back
     to a linear scan or reading remote message chunk payloads). Bounded indexed
     reads — a summary-section read, or a single attachment/metadata range read
     under the no-opt-in caps — do not require the flag.
   - Go-compatible behavior allowed those remote reads without an explicit
     opt-in.
2. Multi-input commands remote materialization:
   - Commands with multiple remote inputs materialize each remote input
     independently, so peak temporary disk usage can approach the sum of remote
     input sizes.

### `mcap convert`

1. ROS 2 db3 schema discovery:
   - Rust CLI converts self-contained db3 files using embedded message
     definitions, including non-`/msg/` topics such as service event topics. It
     fails the conversion if any topic is missing an embedded definition.
   - Go CLI ignores embedded db3 schemas and emulates ament resource lookup with `--ament-prefix-path`, which can miss schemas or silently use definitions from the wrong workspace.

### `mcap du`

1. Attachment accounting:
   - Rust CLI includes `attachment` record bytes in the top-level record stats
     table.
   - Go CLI currently skips attachment records in `du` record-kind accounting due
     to lexer behavior.

### `mcap recover`

1. Always produces a valid output:
   - Rust CLI `recover` decodes and validates every chunk and re-writes records
     through the writer, so the output is always a readable MCAP with correct
     CRCs and rebuilt indexes. Undecodable/corrupt chunk payloads stop the scan
     (keeping what was recovered) rather than being copied through.
   - Go `recover` copies chunk bytes through verbatim by default and can emit
     corrupt or bad-CRC chunks, producing files that strict readers reject.
2. Compression is opt-in (`--compression preserve` by default):
   - Rust CLI `recover` defaults to `--compression preserve`. The input is read
     as a single stream (local files, stdin, and remote inputs are treated
     identically), and the output codec is chosen from the first record that
     determines it: a chunk reuses the chunk's compression; a message,
     attachment, or other non-structural record before any chunk means the output
     stays uncompressed. Schema/channel/metadata records before that point are
     small and buffered so they land in the output with the chosen codec.
     Attachments are never buffered (per the spec they never appear inside a
     chunk, so they carry no codec signal and may be large); an attachment before
     the first chunk is preserved byte-for-byte but leaves the output
     uncompressed. Pass `--compression zstd|lz4|none` to force a codec.
   - Go `recover` defaults to `zstd` and compresses uncompressed/loose input by
     default.
   - The Rust CLI also has no `--always-decode-chunk` flag (Go does): chunks are
     always decoded, and `--compression` alone determines the output codec.
3. Exit codes signal data loss:
   - Rust CLI `recover` exits `0` when all records were recovered (rebuilding
     indexes/CRCs does not count as loss), `3` when recovery was lossy (one or
     more messages/records were discarded, or the input was truncated mid-record),
     and `1` on hard failure (nothing recovered).
   - Go `recover` exits `0` once recovery starts, even for a truncated or
     partially recovered file; data loss is only visible in its stderr output.
