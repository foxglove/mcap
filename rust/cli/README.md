# MCAP CLI

A command line tool for working with MCAP files, written in Rust.

It is a work-in-progress port of the current CLI (see https://mcap.dev/guides/cli) that is written in Go.

## Getting Started

You can run the CLI by issuing the following command:

```sh
cargo run
```

## Tests

Tests can be run with the following command:

```sh
cargo test
```

## Profiling

There is support for emitting opentelemetry events to a local collector to profile

First, start by running a local collector like [jaeger](https://www.jaegertracing.io/).

You can start it with the following command:

```sh
docker run -d --rm --name jaeger \
  -p 16686:16686 \
  -p 4317:4317 \
  jaegertracing/all-in-one:latest
```

With the collector running, run the CLI with the `timings` feature enabled.

```sh
cargo run --features=timings -- <your args here>
```

Once the command completes, visit `http://localhost:16686` and inspect the profile.

The top level span for each CLI command is named `mcap_cli::run`.
