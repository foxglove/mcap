# MCAP CLI (Rust)

A Rust implementation of the MCAP command-line tool for working with MCAP files.

## Development Status

🚧 **Under Development** - This is a port of the Go MCAP CLI to Rust.

Currently implemented commands:

- `version` - Show version information

## Building

From the `rust/cli` directory:

```bash
cargo build --release
```

The binary will be available at `target/release/mcap`.

## Running

```bash
# Show help
cargo run -- --help

# Show version
cargo run -- version
```

## Features

- **Type Safety**: Leverages Rust's type system for robust argument validation
- **Memory Safety**: Eliminates potential memory issues
- **Performance**: Zero-cost abstractions and efficient async I/O
- **Cloud Storage**: Optional support for Google Cloud Storage (with `cloud` feature)
- **Progress Tracking**: Beautiful progress bars for long-running operations

## Development

See [PLAN.md](PLAN.md) for the complete implementation roadmap.

## License

MIT
