# Protobuf

## Read and write MCAP

If you're starting from scratch, you can write code that allows you to **write your Protobuf data to MCAP files** and subsequently **read your Protobuf data from your MCAP files**.

The [`mcap` GitHub repo](https://github.com/foxglove/mcap/tree/main) includes the [`mcap-protobuf-support` Python package](https://github.com/foxglove/mcap/tree/main/python/mcap-protobuf-support) to help you write an MCAP reader and writer for Protobuf.

### Guides

- [Python](../guides/python/reading-writing-protobuf.md)
- [C++](../guides/cpp/writing-protobuf.md)

### Examples

- [Python](https://github.com/foxglove/mcap/tree/main/python/examples/protobuf) - [reader](https://github.com/foxglove/mcap/tree/main/python/examples/protobuf/reader.py) and [writer](https://github.com/foxglove/mcap/tree/main/python/examples/protobuf/writer.py)
- [C++](https://github.com/foxglove/mcap/tree/main/cpp/examples/protobuf) - [dynamic reader](https://github.com/foxglove/mcap/tree/main/cpp/examples/protobuf/dynamic_reader.cpp), [static reader](https://github.com/foxglove/mcap/tree/main/cpp/examples/protobuf/static_reader.cpp), and [writer](https://github.com/foxglove/mcap/tree/main/cpp/examples/protobuf/writer.cpp)

## Inspect MCAP

Use the [`mcap` CLI tool](https://github.com/foxglove/mcap/tree/main/go/cli/mcap) to inspect MCAP files, validate their contents, and even echo their messages to `stdout`.

For an exhaustive list of ways to interact with your MCAP data, check out the [`mcap` GitHub repo](https://github.com/foxglove/mcap/tree/main/go/cli/mcap).

## Visualize MCAP

[Foxglove Studio](https://foxglove.dev/studio) supports playing back local and remote MCAP files containing Protobuf data.
