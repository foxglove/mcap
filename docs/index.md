# MCAP

MCAP (pronounced "em-cap") is a modular container file format for heterogeneous timestamped data. It is ideal for robotics applications, as it can record multiple streams of structured and unstructured data (e.g. ROS, Protobuf, JSON Schema, MessagePack, etc.) in a single file.

MCAP works well under various workloads, resource constraints, and durability requirements.

## Features

### Heterogenous data

- Store messages encoded in multiple serialization formats in a single file
- Include metadata and attachments

### Performant writing

- Append-only structure
- Recover partially-written files when data recording is interrupted

### Efficient seeking

- Extract data without scanning the entire file
- Fast access to indexed summary data

### Self-contained files

- Embed all message schemas in the file
- No extra dependencies needed for decoding

## Quick start

Install the [mcap CLI tool]() to accomplish any of the following tasks and more:

- Examine and get summary information about MCAP files
- Validate MCAP files
- Dump an MCAP file’s contents to stdout
- Convert a ROS 1 .bag or ROS 2 .db3 file into an MCAP file

You can also use the [MCAP libraries](api-docs.md) to read and write MCAP files in a variety of programming languages. Check out the [Guides](guides/getting-started/index.md) to get started.
