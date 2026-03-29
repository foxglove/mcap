# Experimental Rust CLI

This directory contains an experimental Rust implementation of the MCAP CLI.

It is not ready for production use yet.

## Implemented command slices

The Rust CLI currently includes implementations for:

- Read-only inspection: `version`, `info`, `list *`
- Record retrieval/mutation: `get *`, `add *`
- Transform commands: `cat`, `filter`, `compress`, `decompress`
- File rewrite/repair: `sort`, `merge`, `recover`
- Diagnostics: `du`, `doctor`
- Conversion (partial): `convert` for `.mcap` input

## Current limitations

- `convert` for ROS1 bag (`.bag`) and ROS2 db3 (`.db3`) inputs is not implemented yet.
- Remote URI support currently covers read-only access (`gs://...`, `s3://...`) by
  downloading objects over HTTPS.
- Output paths must be local files (remote output paths are rejected).
