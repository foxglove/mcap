# MCAP CLI

A command line tool for inspecting, editing, and converting [MCAP](https://mcap.dev) files.

## Getting started

Download the [latest release from GitHub](https://github.com/foxglove/mcap/releases?q=mcap-cli), or install via Homebrew:

```sh
brew install mcap
```

Run `mcap --help` to list the available commands, or `mcap <command> --help` for the options of a specific command:

```sh
mcap info demo.mcap
```

For more installation options and full usage documentation, see https://mcap.dev/guides/cli.

## Development

The CLI is written in Rust using the [mcap crate](../mcap).

To build from source:

```sh
cargo build -p mcap-cli
```

The binary is written to `target/debug/mcap`.

For build, test, and architecture conventions, see [AGENTS.md](./AGENTS.md).

## Intentional divergences from Go CLI

### General

1. Remote read opt-in:
   - The Rust CLI requires `--allow-remote-scan` whenever a command would scan or
     download a remote file in full, rather than a bounded indexed read. Remote
     inputs include HTTP(S) URLs and object-store URLs (`s3://`, `gs://`, and
     Azure `az://`/`abfs://` and friends).
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
