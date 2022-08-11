# ROS 2

## Convert to MCAP

To convert your existing ROS 2 db3 files into MCAP files, [install the mcap CLI tool](https://github.com/foxglove/mcap/tree/main/go/cli/mcap#installing) and run the following command:

```
$ mcap convert multiple_files_1.db3 demo.mcap
```

mcap will search the path stored in your $AMENT_PREFIX_PATH environment variable to locate the ROS message definitions on your hard drive.

Alternatively, you can specify a colon-separated list of directories for the CLI tool to search using the ament-prefix-path flag:

```
$ mcap convert ros1_input.bag ros1_output.mcap --ament-prefix-path=/your/first/directory;/your/second/directory
```

You can also use the mcap CLI tool to inspect MCAP files, validate them, and even echo their messages to `stdout`. For a full list of possible commands, check out the [mcap GitHub repo](https://github.com/foxglove/mcap/tree/main/go/cli/mcap).

## Read and write MCAP

If you're starting from scratch, you can write code that allows you to **write your ROS 2 data to MCAP files** and subsequently **read your ROS 2 data from your MCAP files**.

### Guides

- [Python](../guides/python/reading-writing-ros2.md)

### Examples

- [Python](https://github.com/foxglove/mcap/tree/main/python/examples/ros2) - [reader](https://github.com/foxglove/mcap/tree/main/python/examples/ros2/py_mcap_demo/py_mcap_demo/reader.py) and [writer](https://github.com/foxglove/mcap/tree/main/python/examples/ros2/py_mcap_demo/py_mcap_demo/writer.py)

## Inspect MCAP

Use the [`mcap` CLI tool](https://github.com/foxglove/mcap/tree/main/go/cli/mcap) to inspect MCAP files, validate their contents, and even echo their messages to `stdout`.

For an exhaustive list of ways to interact with your MCAP data, check out the [mcap GitHub repo](https://github.com/foxglove/mcap/tree/main/go/cli/mcap).

## Visualize MCAP

[Foxglove Studio](https://foxglove.dev/studio) supports playing back local and remote ROS 2 db3 files, as well as local and remote MCAP files containing ROS 2 data.

With that said, we recommend MCAP files over ROS 2 db3 files, as the latter are not completely self-contained.
