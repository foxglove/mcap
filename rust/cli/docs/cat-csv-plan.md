# Plan: `mcap cat --csv --topic <TOPIC>`

## Goal

Add CSV output to the `cat` command so a single MCAP topic can be exported as a
flat, tabular CSV stream on stdout. This follows the maintainer intent captured
in [foxglove/mcap#438](https://github.com/foxglove/mcap/issues/438): reuse the
existing JSON transcoding path and define a JSON→CSV translation, gated on a
single selected topic.

Primary motivating consumer: the [Nominal SDK](https://docs.nominal.io) tabular
ingest (`add_tabular_data` / `add_from_io`), which wants one CSV per topic with a
timestamp column plus scalar "channel" columns. MCAP `log_time` (int64 ns) maps
directly onto Nominal's `EPOCH_NANOSECONDS` timestamp type.

## Scope

In scope:
- `--csv` boolean flag on `cat`.
- `--topic <TOPIC>` singular flag selecting exactly one topic for CSV mode.
- Flatten decoded messages (dot notation) into CSV columns.
- Reuse existing decoders: `jsonschema`, `protobuf`, `ros1msg`, schemaless `json`.
- Local files, remote indexed files, and stdin streaming (same paths as `cat`).

Out of scope (document as limitations / future work):
- Multi-topic CSV in a single invocation (heterogeneous schemas → different
  columns). Users run once per topic.
- Non-scalar leaves beyond the array policy below (e.g. nested arrays of
  messages get JSON-stringified).
- Parquet output (Nominal accepts it, but keep this change CSV-only).

## CLI surface (`rust/cli/src/cli.rs`)

Extend `CatCommand`:

```rust
/// Print messages as JSON. Supported message encodings: ros1, protobuf, and json.
#[arg(long = "json", default_value_t = false, conflicts_with = "csv")]
pub json: bool,

/// Print a single topic's messages as CSV (requires --topic).
#[arg(long = "csv", default_value_t = false)]
pub csv: bool,

/// Single topic to export. Required by --csv; ignored otherwise.
#[arg(long = "topic")]
pub topic: Option<String>,
```

Validation (in `CatOptions::from_args`, returning `anyhow::Error`):
- `--csv` requires `--topic` to be set → otherwise bail
  (`"--csv requires --topic <TOPIC>"`).
- `--topic` and `--topics` are mutually exclusive at the semantic level; when
  `--csv` is set, fold `--topic` into the internal single-element `topics` vec so
  the existing topic-filtering machinery (indexed reader + per-message
  `include_topic`) is reused unchanged.
- `--csv` + `--json` already rejected by clap `conflicts_with`.

## Options model (`rust/cli/src/commands/cat.rs`)

Add an output-mode enum instead of two booleans:

```rust
enum OutputMode { Fields, Json, Csv }
```

`CatOptions` gains `mode: OutputMode` (replacing the `json: bool`; keep the field
change local). For CSV, `topics` holds exactly the one requested topic so all
three read paths (`cat_indexed`, `cat_remote_indexed`, `cat_streaming`/
`cat_linear`) already restrict to it via `include_topic`.

## JSON→CSV translation

Reuse `JsonTranscoders`, but add a value-returning entry point so we don't
re-parse JSON bytes:

```rust
impl JsonTranscoders {
    // existing: encode(...) -> Cow<[u8]>
    fn decode_value(&mut self, channel, data) -> Result<serde_json::Value>;
}
```

- For `jsonschema` / schemaless `json`: `serde_json::from_slice(data)`.
- For `protobuf`: serialize `DynamicMessage` into `serde_json::Value` (via
  `serialize_with_options` into a `Value` serializer) rather than bytes.
- For `ros1msg`: the current `Ros1MessageDef::transcode` returns JSON bytes;
  either (a) `serde_json::from_slice` on its output, or (b) add a
  `transcode_value` variant. Start with (a) to minimize surface; optimize later.

`encode()` can be refactored to call `decode_value()` then serialize, keeping a
single decode implementation. Keep this refactor behavior-preserving for the
existing `--json` output (guard with existing JSON tests).

### Flattening

New helper `flatten_value(prefix, &Value, &mut IndexMap<String, String>)`:
- Object → recurse with `"{prefix}.{key}"` (root prefix empty).
- Array → index suffix: `"{prefix}.{i}"` (array policy = flatten-by-index).
  Nested arrays / arrays of objects recurse the same way.
- Scalars → stringify: numbers as-is, bools `true`/`false`, strings raw,
  `null` → empty string.
- Use an insertion-ordered map (`indexmap`, already a common dep — verify in
  `Cargo.toml`, otherwise use `BTreeMap` for deterministic ordering).

## Column / header strategy (bounded memory)

CSV needs a header row, but MCAP is streamed and array lengths can vary between
messages. To respect the repo's bounded-memory principle (no buffering all
messages), use **first-message header derivation**:

1. Prepend fixed metadata columns: `log_time`, `publish_time`, `sequence`.
2. Decode + flatten the **first** matching message; its ordered flattened keys
   define the data columns. Write the header row immediately.
3. For each subsequent message, map its flattened map into the fixed column set:
   - Missing key → empty cell.
   - Extra key not in header (e.g. a longer variable-length array) → by default
     dropped, with a single deferred warning to stderr
     (`"row had columns not present in header (from first message); dropped: ..."`).
     This keeps rows aligned and memory bounded.

Rationale: a single topic almost always has one stable schema; the only
variability is array length. First-message derivation is O(1) memory and matches
the maintainer's "pipe JSON output through a JSON→CSV translation" suggestion.

Optional future enhancement (documented, not built now): a `--csv-columns a,b,c`
flag to pin the header explicitly, avoiding first-message dependence.

## CSV writing

- Add the `csv` crate (well-maintained, minimal) to `rust/cli/Cargo.toml`, or
  hand-roll RFC 4180 quoting (quote fields containing `,` `"` `\n` `\r`; double
  embedded quotes). Prefer the `csv` crate for correctness unless dependency
  budget forbids it — confirm during implementation.
- Write through the same `BufWriter<StdoutLock>` used by `cat`.
- Preserve `cat`'s broken-pipe handling: map `BrokenPipe` writes to a graceful
  stop (return `true` sentinel like `write_message`).

## Integration points

`write_message` dispatches on `OutputMode`:
- `Fields` → `write_message_fields` (unchanged).
- `Json` → `write_json_message` (unchanged).
- `Csv` → new `CsvWriter` state object threaded through the read loops.

Because the header depends on the first message, CSV needs per-invocation state
(header written yet? column list). Thread a `&mut CsvState` alongside
`&mut JsonTranscoders` through:
- `cat_mcap` / `cat_indexed`
- `cat_remote_indexed`
- `cat_linear` / `cat_streaming` / `handle_linear_record`

`CsvState { header: Option<Vec<String>> }`. On first message: build + write
header, then write the row. On later messages: write row against `header`.

Note: `cat` may fall through from indexed to linear read for the same file
(`cat_mcap` calls `cat_indexed` then `cat_linear`). CSV state must be shared
across that fallthrough so the header isn't written twice. Since `cat_indexed`
returns `Some(_)` for indexed files and short-circuits, the two paths are
mutually exclusive per file — but a single `cat` invocation over multiple files
should still emit **one** header total. Decision: header is per-invocation
(written once), rows continue across files. Document that multi-file CSV assumes
a consistent schema for the topic across files (same first-message columns).

## Testing (`rust/cli/src/commands/cat.rs` tests + `main.rs` parse tests)

Parsing (`main.rs`):
- `cat --csv --topic /foo` parses; `topic = Some("/foo")`, `csv = true`.
- `cat --csv` without `--topic` → error.
- `cat --csv --json` → clap conflict error.

Unit:
- `flatten_value`: nested object, arrays (index suffixes), null → empty, bool,
  string with comma/quote/newline gets quoted.
- Header derivation from first message; missing key → empty cell; extra key →
  dropped + warned.
- CSV RFC 4180 escaping.

Integration (build a small in-memory MCAP like existing tests):
- `jsonschema`/`json` topic → expected header + rows, `log_time` column present.
- `protobuf` topic → flattened proto fields (reuse existing proto test fixture).
- `ros1msg` topic → flattened fields.
- Topic filter: messages on other topics excluded.
- Time filters (`--start-nsecs` / `--end-nsecs`) still apply in CSV mode.
- Stdin streaming path produces identical CSV to the indexed path.

## Docs

- Update `website/docs/guides/cli.md` `cat` section with `--csv`/`--topic`.
- Add a short note: pairs with Nominal ingest via
  `timestamp_column="log_time", timestamp_type=EPOCH_NANOSECONDS`.

## Risks / open questions

1. **Array-length variability** → first-message header can drop later columns.
   Mitigated by warning; `--csv-columns` is the escape hatch (future).
2. **`ros1msg` double JSON round-trip** cost — acceptable initially; add
   `transcode_value` later if profiling shows it matters.
3. **`csv` crate dependency** — confirm it's acceptable for the CLI crate; else
   hand-roll quoting.
4. **Column ordering determinism** — rely on decoder field order (proto/ros1 are
   ordered; JSON object key order via `serde_json` `preserve_order` feature must
   be enabled, otherwise sort keys for determinism). Verify `serde_json`
   features in `Cargo.toml`.

## Implementation order

1. CLI flags + `CatOptions`/`OutputMode` + validation (+ parse tests).
2. `decode_value` refactor of `JsonTranscoders` (keep `--json` green).
3. `flatten_value` + CSV escaping + `CsvState` (unit tests).
4. Thread `CsvState` through the three read paths; `write_message` dispatch.
5. Integration tests across encodings + filters + stdin.
6. Docs.
