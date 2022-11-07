# Comparison of `rosbag2` Storage Plugins

_Oct, 2022_  
_James Smith ([@james-rms](https://github.com/james-rms))_  
_[Foxglove](https://foxglove.dev)_

## Context

The [ROS 2 bag recording framework](https://github.com/ros2/rosbag2) supports pluggable storage layers, allowing users to choose different storage formats and recording libraries. When a user installs ROS 2 for the first time, they get a storage plugin by default which records bags with [SQLite](https://sqlite.org). This document compares write performance of the default plugin against the [MCAP storage plugin](https://github.com/ros-tooling/rosbag2_storage_mcap).

### Why compare write performance?

Performance is most critical when writing bag data rather than reading it back. If a bag recorder can't keep up with the stream of messages, either the incoming message queue grows unbounded, resulting in an OOM, or some messages are dropped, resulting in data loss. Both of these are serious problems when running a robotics application.

## Benchmark description

### Message drop measurement

[Robotec.ai](https://robotec.ai) previously contributed [a benchmark](https://github.com/ros2/rosbag2/tree/rolling/rosbag2_performance/rosbag2_performance_benchmarking) which measured ROS 2 storage performance. This benchmark publishes many messages on a local network and runs a recording node to capture them. The primary output metric for this benchmark is the number of messages which are dropped by the recorder - the fewer, the better. This is a general-purpose benchmarking suite which can be used to compare ROS 2 middlewares, storage plugins, or the rosbag2 recorder itself. For the purposes of this comparison, the middleware was kept constant and only the plugin configuration ,the rosbag2 cache size, and the distribution of message sizes were varied.

### Raw write throughput

A new benchmark was created for this comparison, designed to directly measure the storage plugin with no other moving parts. [Available here](https://github.com/james-rms/rosbag2/tree/jrms/plugin-comparison/rosbag2_performance/rosbag2_storage_plugin_comparison), this benchmark directly uses the storage plugin API to write a fixed number of random bytes as fast as possible. This measures the raw write throughput of each storage plugin configuration.

## Plugin Configurations

The following configurations of both plugins are tested:

- `sqlite_default`: The default configuration of the SQLite storage plugin, effectively measuring the performance that ROS 2 users get out-of-the-box. This is equivalent to running the following statements when opening a SQLite file:`PRAGMA journal_mode=MEMORY; PRAGMA synchronous=OFF;`. These settings produce the highest write performance available from SQLite at the risk of corruption in the case of an interruption or power loss.
- `sqlite_resilient`: The "resilient" preset profile of the SQLite storage plugin. This setting is equivalent to running the following statements when opening a SQLite file: `PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL`. This removes the risk of an entire `.db3` file being corrupted, at the cost of performance. By using a write-ahead log, only the messages in the last few transactions may be lost if the data from those transactions were not fully committed to disk. Therefore in this mode, SQLite3 offers similar guarantees against corruption as MCAP.
- `mcap_default`: The default recording configuration of the MCAP storage plugin. Produces un-compressed, chunked MCAPs with a message index for efficient read performance.
- `mcap_nochunking`: The highest-throughput configuration of the MCAP storage plugin. Does not write a message index to the MCAP, so bags recorded with this mode need to be reindexed later to be read efficiently.
- `mcap_uncompressed_crc`: Like `mcap_default`, but calculates CRCs for each chunk so that readers can identify if a chunk contains corrupted data. This feature does not have an equivalent in the SQLite3 storage plugin.
- `mcap_compressed_nocrc`: Like `mcap_default`, but compressed each chunk with `zstd` compression on default settings.

## Software Versions

- list all ROS 2 packages by version

## Message sizes tested

- 1MiB
- 10KiB
- 100B
- Mixed: Messages from all of the above sizes at the following ratios (by number of bytes, not messages)
  - 1MiB: 70%
  - 10KiB: 20%
  - 100B: 10%

When testing write throughput, 250MiB of messages are stored as quickly as possible. When testing message drops,

## Cache sizes tested

The `rosbag2::SequentialWriter` collects messages in an internal cache before calling the storage plugin `write()` call with all messages at once. The cache size determines the size of each "batch" that is written to a storage plugin at once. This matters in particular for the SQLite storage plugin, because each batch constitutes a SQL transaction, and it's generally better for write performance to write many messages in one transaction.

Cache sizes tested were:

- 1KiB
- 10MiB

## Benchmark hardware

These benchmarks were recorded on two hardware platforms:

| Attribute | M1 Mac | Intel NUC7i5BNH |
| --- | --- | --- |
| CPU | Apple M1 Pro | Core i5-7260U @ 2.20Ghz, 2 cores (4 threads) |
| RAM | 32GB LPDDR5-6400, estimated 200GB/s bandwidth | 8GiB DDR4-2133, estimated 34.1 GB/s bandwidth |
| Kernel | Linux 5.15.0-48-generic | Linux 5.15.0-52-generic |
| Distro | Ubuntu 22.04.1 Jammy aarch64 virtualized with Parallels 17 | Ubuntu 22.04.1 Jammy x86_64 |

Bags were recorded into a `tmpfs` mount, in order to eliminate variability in disk write speeds from the test results.

## Results

### Message drop performance

### Raw write throughput

Error bars represent a 95% confidence interval.

## Discussion

### Recommendations

### How do I replicate these results?
