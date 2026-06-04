## MCAP CLI

A command line tool to work with MCAP files. See https://mcap.dev/guides/cli for documentation.

> **Note:** The released MCAP CLI is now built from the Rust implementation in
> [`rust/cli`](../../../rust/cli). This Go CLI is still built and tested in CI
> but is no longer published as part of the `releases/mcap-cli/vX.Y.Z` releases.

## Build from source

You can build the CLI tool from source by running `make build` in the same directory as this README.

```
$ make build
```

The binary will be built to a `bin` folder in the same directory.
