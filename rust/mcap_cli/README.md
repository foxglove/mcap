# Experimental Rust CLI

This directory contains an experimental Rust implementation of the MCAP CLI.

It is not ready for production use yet.

## Intentional divergences from Go CLI

1. `mcap du` attachment accounting:
   - Rust CLI includes `attachment` record bytes in the top-level record stats table.
   - Go CLI currently skips attachment records in `du` record-kind accounting due to lexer behavior.
