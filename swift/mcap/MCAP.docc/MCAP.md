# ``MCAP``

Read and write log files containing heterogeneous timestamped data.

## Overview

[MCAP](https://mcap.dev/) is a modular container file format for heterogeneous timestamped data. It is ideal for robotics applications, as it can record multiple streams of structured and unstructured data (e.g. ROS, Protobuf, JSON Schema, etc.) in a single file.

MCAP works well under various workloads, resource constraints, and durability requirements.

### Heterogeneous data

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

## Adding MCAP as a dependency

In `Package.swift`, add a dependency on this repo, and include the `"MCAP"` library as a dependency for your target:

```swift
Package(
  dependencies: [
    .package(url: "https://github.com/foxglove/mcap", from: "0.1.0"),
  ],
  targets: [
    .target(name: "<target>", dependencies: [
      .product(name: "MCAP", package: "mcap"),
    ]),
  ]
)
```

Import the library using `import MCAP`.
