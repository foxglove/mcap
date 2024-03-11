---
description: Read, write, and visualize MCAP files containing ROS 1 data.
---

# ROS 1

## Convert to MCAP

To convert your existing ROS 1 bag files into MCAP files, [install the mcap CLI tool](https://github.com/foxglove/mcap/tree/main/go/cli/mcap#installing) and run the following command:

```
$ mcap convert ../../testdata/bags/demo.bag demo.mcap
```

You can also use the `mcap` CLI tool to inspect MCAP files, validate them, and even echo their messages to `stdout`. For a full list of possible commands, check out the [mcap GitHub repo](https://github.com/foxglove/mcap/tree/main/go/cli/mcap).

## Read and write MCAP

If you're starting from scratch, you can write code that allows you to **write your ROS 1 data to MCAP files** and subsequently **read your ROS 1 data from your MCAP files**.

The [`mcap` GitHub repo](https://github.com/foxglove/mcap/tree/main) includes the [`mcap-ros1-support` Python package](https://github.com/foxglove/mcap/tree/main/python/mcap-ros1-support) to help you write an MCAP reader and writer for ROS 1.

### Guides

- [Python](../python/ros1.md)

### Examples

- [Python](https://github.com/foxglove/mcap/tree/main/python/examples/ros1) - [reader](https://github.com/foxglove/mcap/blob/main/python/examples/ros1/reader.py) and [writer](https://github.com/foxglove/mcap/blob/main/python/examples/ros1/writer.py)

## Inspect MCAP

Use the [`mcap` CLI tool](https://github.com/foxglove/mcap/tree/main/go/cli/mcap) to inspect MCAP files, validate their contents, and even echo their messages to `stdout`.

For an exhaustive list of ways to interact with your MCAP data, check out the [mcap GitHub repo](https://github.com/foxglove/mcap/tree/main/go/cli/mcap).

## Visualize MCAP

[Foxglove](https://foxglove.dev/product) supports playing back local and remote ROS 1 bag files, as well as local and remote MCAP files containing ROS 1 data.
