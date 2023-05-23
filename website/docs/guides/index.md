---
sidebar_position: 1
---

# Introduction

MCAP (pronounced "em-cap") is a modular container file format for heterogeneous timestamped data. It is ideal for robotics applications, as it can record multiple streams of structured and unstructured data (e.g. ROS, Protobuf, JSON Schema, etc.) in a single file.

## Benefits

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

## History

<!-- note: not using markdown link here to avoid docusaurus asset pipeline -->

We evaluated <a target="_blank" href="/files/evaluation.pdf">many existing data storage formats in the industry</a> and identified a clear need for a general-purpose, open source data container format – specifically optimized for robotics use cases. Designing this format would solve an industry-wide problem and make it easier for teams to leverage third-party tools and share their own tooling.

### Before MCAP

Many robotics companies spend valuable in-house resources to develop custom file formats, only to create future work and complicate third-party tool integrations. We built MCAP to allow teams to focus on their core robotics problems and avoid wasting precious time making commodity tools.

Before MCAP, the format that robotics teams used to store their log data depended mainly on their framework. Those using ROS 1 defaulted to the [.bag format](http://wiki.ros.org/Bags/Format/2.0); those on ROS 2 defaulted to a SQLite-based format. Companies that don’t use ROS often employed a custom in-house binary format, such as length-delimited Protobuf, or stored their messages as opaque bytes inside existing file formats such as HDF5.

These existing storage options have several shortcomings. Custom in-house formats lack interoperability and require developing corresponding libraries in multiple languages to read and write files. The ROS 1 bag format is challenging to work with outside of the ROS ecosystem, while the ROS 2 SQLite format is [not fully self-contained](https://github.com/ros2/rosbag2/issues/782), making it difficult for third-party tools to read.

### After MCAP

As a container format, MCAP solves many of these issues. It is self-contained, can embed multiple data streams encoded with different serialization formats in a single file, and even supports metadata and attachments. MCAP files are optimized for both high-performance writing and efficient indexed reading, even over remote connections.

## Supported Formats

MCAP files can store multiple channels of timestamped, heterogeneous data of any format. In particular, it is commonly used to store the following data formats:

- [ROS 1](http://wiki.ros.org/)
- [ROS 2](https://docs.ros.org/)
- [JSON Schema](https://json-schema.org/)
- [Protobuf](https://developers.google.com/protocol-buffers)
- [FlatBuffers](https://google.github.io/flatbuffers/)
