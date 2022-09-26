# mcap-rs

A library for reading and writing [Foxglove MCAP](https://github.com/foxglove/mcap) files.
See the [crate documentation](https://docs.rs/mcap) for examples.

## Design goals

- **Simple APIs:** Users should be able to iterate over messages, with each
  automatically linked to its channel, and that channel linked to its schema.
  Users shouldn't have to manually track channel and schema IDs.

- **Performance:** Writers shouldn't hold large buffers (e.g., the current chunk)
  in memory. Readers should support memory-mapped files to avoid needless copies
  and to let the OS do what it does best: loading and caching large files based
  on how you're actually reading them.

- **Resilience:** Like MCAP itself, the library should let you recover every
  valid message from an incomplete file or chunk.
