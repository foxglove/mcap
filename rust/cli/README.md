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
cd rust
cargo build -p mcap-cli
```

The binary is written to `rust/target/debug/mcap`.

For build, test, and architecture conventions, see [AGENTS.md](./AGENTS.md).
