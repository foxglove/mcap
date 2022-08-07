---
title: "ROS 2"
slug: "ros-2"
hidden: false
createdAt: "2022-08-04T23:12:26.102Z"
updatedAt: "2022-08-04T23:29:48.006Z"
---
## Converting ROS 2 db3 files to MCAP

If you already have existing data that is not in the MCAP file format, you may want to **convert this non-MCAP data into MCAP files**.

[Install the mcap CLI tool](https://github.com/foxglove/mcap/tree/main/go/cli/mcap#installing) to start converting your non-MCAP data to MCAP files.

```
$ mcap convert multiple_files_1.db3 demo.mcap
```

mcap will search the path stored in your $AMENT_PREFIX_PATH environment variable to locate the ROS message definitions on your hard drive.

Alternatively, you can specify a colon-separated list of directories for the CLI tool to search using the ament-prefix-path flag:

```
$ mcap convert ros1_input.bag ros1_output.mcap --ament-prefix-path=/your/first/directory;/your/second/directory
```

You can also use the mcap CLI tool to inspect MCAP files, validate them, and even echo their messages to `stdout`. For a full list of possible commands, check out the [mcap GitHub repo](https://github.com/foxglove/mcap/tree/main/go/cli/mcap).

## Writing ROS 2 data to MCAP files

If you're starting from scratch, you may want to write code to **read and write your own MCAP data**. 

We provide MCAP readers and writers in the following languages:
- [Python](https://github.com/foxglove/mcap/tree/main/python)
- [C++](https://github.com/foxglove/mcap/tree/main/cpp)
- [Go](https://github.com/foxglove/mcap/tree/main/go)
- [Swift](https://github.com/foxglove/mcap/tree/main/swift)
- [TypeScript](https://github.com/foxglove/mcap/tree/main/typescript)

## Inspect your MCAP files

Use the [mcap CLI tool](https://github.com/foxglove/mcap/tree/main/go/cli/mcap) to inspect, validate, and otherwise interact with your MCAP files.

## Visualize your MCAP files

Foxglove Studio supports playing back local and remote ROS 2 db3 files, as well as MCAP files including ROS 2 data.

With that said, ROS 2