# MCAP Kotoba codec (v1)

In-language reader/writer for a slice of the [MCAP](https://mcap.dev) container
format. This is not a wrap of the C++ or Rust libraries: Kotoba has no ambient
FFI, so the codec is implemented in `.kotoba` the same way the Python and
TypeScript libraries are implemented in-process.

## v1 scope

Supported:

- Leading and trailing magic (`0x89 M C A P 0x30 \\r \\n`)
- Header (opcode `0x01`)
- DataEnd (opcode `0x0F`)
- Footer (opcode `0x02`)
- Encode of `magic + Header + DataEnd + Footer + magic`

Not in v1: Schema, Channel, Message, Chunk, indexes, attachments, summary
records, compression, or CRC32 computation. CRC fields are parsed as integers.
This binding is not on the cross-language conformance matrix yet.

## Kotoba surface

Verified on [kotoba-lang/kotoba](https://github.com/kotoba-lang/kotoba) **v0.7.2**.
Language authority: [kotoba-lang/kotoba-lang](https://github.com/kotoba-lang/kotoba-lang).

Admitted pieces this library uses:

- typed `ns` / `defn`, documents, keywords, strings, `i64`
- `document`, `document-get`, `document-assoc`, `document-count`,
  `document-kind`, `document-vector-at`, `document-vector-conj`,
  `document-edn-read`, `document-edn-print`
- `string-concat` is unused; `string-from-i64`, `string-code-point-at`,
  `string-length`, `string=?`
- `+`, `-`, `*`, `quot`, `bit-and` (no bit shifts — they have no lowering on
  this CLI)

Constraints the codec does **not** paper over:

- There is no JSON or nonempty `bytes` builtin. `(document x)` is inert EDN,
  so runtime integers are boxed with `document-edn-read` / `string-from-i64`.
- Document vectors cap at 32 items. Wire bytes are a vector of chunks.
- Portable strings cap at 127 UTF-8 bytes. v1 Header strings are ASCII.
- The value profile is `i64`. This library does not use or claim floats.
- `require` is forbidden, so `mcap` is one compilation unit. Examples and
  fixtures are guests concatenated by `scripts/bundle.sh`.

`kotoba run` on this source hits the EDN-IR adapter, which does not implement
documents. Compile to wasm or restricted ESM (`--target web`) instead.

## Build

```sh
kotoba compile src/mcap.kotoba --target wasm --output mcap.wasm --json
```

Accept `kotoba.cli/ok?` true and `kotoba.cli/code` `emitted`. Fixture execution
uses `--target web` and `instantiateKotoba` (empty grant set).

```sh
# kotoba CLI v0.7.2 on PATH
make test
# or
scripts/ci.sh
```

Install the CLI from the v0.7.2 release tarball or
`brew tap kotoba-lang/kotoba && brew install kotoba`.

## Tests

`test/fixtures.kotoba` walks the vendored NoData golden (same 75 bytes as
`tests/conformance/data/NoData/NoData.mcap`) and checks encode/decode of Header,
DataEnd, and Footer. `encode-file` is the record encoders concatenated; a full
75-byte encode exceeds the web target's 512-fuel budget, so fixtures cover the
records and `blob-concat` separately. Vectors live under `testdata/`. CI does
not fetch GitHub or pull LFS.

## License

MIT, same as the rest of this repository.
