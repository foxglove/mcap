# Comparison of `rosbag2` Storage Plugins

_Oct, 2022_  
_James Smith ([@james-rms](https://github.com/james-rms))_  
_[Foxglove](https://foxglove.dev)_

<!-- cspell:words nocrc nochunking pluggable robotec middlewares fastrtps LPDDR aarch -->

## Context

The [ROS 2 bag recording framework](https://github.com/ros2/rosbag2) supports pluggable storage layers, allowing users to choose different storage formats and recording libraries. When a user installs ROS 2 for the first time, they get a storage plugin by default which records bags with [SQLite](https://sqlite.org). This document compares write performance of the default plugin against the [MCAP storage plugin](https://github.com/ros-tooling/rosbag2_storage_mcap).

### Why compare write performance?

Performance is most critical when writing bag data rather than reading it back. If a bag recorder can't keep up with the stream of messages, either the incoming message queue grows unbounded, resulting in an OOM, or some messages are dropped, resulting in data loss. Both of these are serious problems when running a robotics application.

## Benchmark description

A new benchmark was created for this comparison, designed to directly measure the storage plugin with no other moving parts. [Available here](https://github.com/james-rms/rosbag2/tree/jrms/plugin-comparison/rosbag2_performance/rosbag2_storage_plugin_comparison), this benchmark directly uses the storage plugin API to write a fixed number of random bytes as fast as possible. This measures the raw write throughput of each storage plugin configuration.

## Plugin Configurations

The following configurations of both plugins are tested:

- `sqlite_default`: The default configuration of the SQLite storage plugin, effectively measuring the performance that ROS 2 users get out-of-the-box. This is equivalent to running the following statements when opening a SQLite file:`PRAGMA journal_mode=MEMORY; PRAGMA synchronous=OFF;`. These settings produce the highest write performance available from SQLite at the risk of corruption in the case of an interruption or power loss.
- `sqlite_resilient`: The "resilient" preset profile of the SQLite storage plugin. This setting is equivalent to running the following statements when opening a SQLite file: `PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL`. This removes the risk of an entire `.db3` file being corrupted, at the cost of performance. By using a write-ahead log, only the messages in the last few transactions may be lost if the data from those transactions were not fully committed to disk. Therefore in this mode, SQLite3 offers similar guarantees against corruption as MCAP.
- `mcap_default`: The default recording configuration of the MCAP storage plugin. Produces un-compressed, chunked MCAPs with a message index for efficient read performance. Chunk size is 768KiB, though chunks expand to fit if a message is larger than the chunk size.
- `mcap_nochunking`: The highest-throughput configuration of the MCAP storage plugin. Does not write a message index to the MCAP, so bags recorded with this mode need to be reindexed later to be read efficiently.
- `mcap_uncompressed_crc`: Like `mcap_default`, but calculates CRCs for each chunk so that readers can identify if a chunk contains corrupted data. This feature does not have an equivalent in the SQLite3 storage plugin.
- `mcap_compressed_nocrc`: Like `mcap_default`, but compressed each chunk with `zstd` compression on default settings.

## Software Versions

Some installed package versions were omitted for clarity.

| ROS 2 Package Name                | Version |
| --------------------------------- | ------- |
| mcap_vendor                       | 0.5.0   |
| rcl                               | 5.4.0   |
| rmw                               | 6.3.0   |
| rmw_dds_common                    | 1.7.0   |
| rmw_fastrtps_cpp                  | 6.3.0   |
| rmw_fastrtps_shared_cpp           | 6.3.0   |
| rmw_implementation                | 2.9.0   |
| rmw_implementation_cmake          | 6.3.0   |
| ros_core                          | 0.10.0  |
| ros_environment                   | 3.2.0   |
| ros_workspace                     | 1.0.2   |
| rosbag2_compression               | 0.17.0  |
| rosbag2_cpp                       | 0.17.0  |
| rosbag2_storage                   | 0.17.0  |
| rosbag2_storage_default_plugins   | 0.17.0  |
| rosbag2_storage_mcap              | 0.5.0   |
| rosbag2_storage_plugin_comparison | 0.1.0   |
| rosbag2_storage_sqlite3           | 0.17.0  |
| rosbag2_test_common               | 0.17.0  |
| shared_queues_vendor              | 0.17.0  |
| sqlite3_vendor                    | 0.17.0  |
| zstd_vendor                       | 0.17.0  |

## Message sizes tested

> Here the `MiB` suffix indicates that we are measuring [Mebibytes](https://en.wikipedia.org/wiki/Byte#Multiple-byte_units).

- 1MiB
- 10KiB
- 100B
- Mixed: Messages from all of the above sizes at the following ratios (by number of bytes, not messages)
  - 1MiB: 70%
  - 10KiB: 20%
  - 100B: 10%

When testing write throughput, 250MiB of messages are stored as quickly as possible.

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

All bags were written to a ramdisk, to eliminate the effect of disk I/O speed from the tests.

## Results

### Message drop performance

### Raw write throughput

Results in CSV form are available here:

<ul>
  <li><a href="throughput/m1/m1_throughput_results.csv" download>Apple M1 Pro results</a></li>
  <li><a href="throughput/nuc/nuc_throughput_results.csv" download>Intel NUC7i5BNH results</a></li>
</ul>

The throughput values are represented below as bar charts. Error bars represent a 95% confidence interval.

<div style="display: flex; flex-wrap: wrap; padding: 0 4px">
  <div style="flex: 50%; padding: 0 4px;">
    <img src="throughput/m1/1MiB_1KiB.png" title="M1 Throughput, 1MiB messages, 1KiB cache"/>
    <img src="throughput/m1/1MiB_10MiB.png" title="M1 Throughput, 1MiB messages, 10MiB cache"/>
    <img src="throughput/m1/10KiB_1KiB.png" title="M1 Throughput, 10KiB messages, 1KiB cache"/>
    <img src="throughput/m1/10KiB_10MiB.png" title="M1 Throughput, 10KiB messages, 10MiB cache"/>
    <img src="throughput/m1/100B_1KiB.png" title="M1 Throughput, 100B messages, 1KiB cache"/>
    <img src="throughput/m1/100B_10MiB.png" title="M1 Throughput, 100B messages, 10MiB cache"/>
    <img src="throughput/m1/mixed_1KiB.png" title="M1 Throughput, mixed messages, 1KiB cache"/>
    <img src="throughput/m1/mixed_10MiB.png" title="M1 Throughput, mixed messages, 10MiB cache"/>
  </div>
  <div style="flex: 50%; padding: 0 4px;">
    <img src="throughput/nuc/1MiB_1KiB.png" title="nuc throughput, 1MiB messages, 1KiB cache"/>
    <img src="throughput/nuc/1MiB_10MiB.png" title="nuc throughput, 1MiB messages, 10MiB cache"/>
    <img src="throughput/nuc/10KiB_1KiB.png" title="nuc throughput, 10KiB messages, 1KiB cache"/>
    <img src="throughput/nuc/10KiB_10MiB.png" title="nuc throughput, 10KiB messages, 10MiB cache"/>
    <img src="throughput/nuc/100B_1KiB.png" title="nuc throughput, 100B messages, 1KiB cache"/>
    <img src="throughput/nuc/100B_10MiB.png" title="nuc throughput, 100B messages, 10MiB cache"/>
    <img src="throughput/nuc/mixed_1KiB.png" title="nuc throughput, mixed messages, 1KiB cache"/>
    <img src="throughput/nuc/mixed_10MiB.png" title="nuc throughput, mixed messages, 10MiB cache"/>
  </div>
</div>

#### Key Takeaways

- CRC calculation currently incurs a large performance penalty. MCAP currently uses an architecture-independent CRC calculation strategy with a 16KiB precalculated lookup table. See [this issue](https://github.com/foxglove/mcap/issues/708) for discussion on improving this.
- When dealing with many small messages, there is a clear advantage to using MCAP in un-chunked mode. This removes the overhead of calculating and writing message the message index to the file.
- MCAP either performs on-par with or significantly better than the SQLite storage plugin in their default configurations. It's also worth noting that in their default configurations, MCAP can offer better corruption resilience than the SQLite storage plugin as well.

### Recommendations

Given the above results, we feel comfortable recommending MCAP as a replacement for SQLite as a general-purpose ROS 2 storage plugin.

### How do I replicate these results?

#### Message Drops

#### Raw Throughput
