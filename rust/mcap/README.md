# Rust MCAP library

A library for reading and writing
[Foxglove MCAP](https://github.com/foxglove/mcap) files. See the
[crate documentation](https://docs.rs/mcap) for examples.

## Design goals

- **Simple APIs:** Users should be able to iterate over messages, with each
  automatically linked to its channel, and that channel linked to its schema.
  Users shouldn't have to manually track channel and schema IDs.

- **Bounded memory:** Writers shouldn't hold large buffers (e.g., the current
  chunk) in memory. Readers stream records through the sans-io APIs, so memory
  scales with the largest record or chunk, not the file; random access goes
  through the summary with bounded range reads.

- **Resilience:** Like MCAP itself, the library should let you recover every
  valid message from an incomplete file or chunk.

## Building

By default this package will build with zstd compression support enabled. To
build without the zstd dependency pass the `--no-default-features` flag:

```
cargo build -p mcap --no-default-features
```
