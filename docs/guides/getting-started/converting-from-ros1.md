# Converting ROS 1 bag files to MCAP

If you already have existing data that is not in the MCAP file format, you may want to **convert this non-MCAP data into MCAP files**.

[Install the mcap CLI tool](https://github.com/foxglove/mcap/tree/main/go/cli/mcap#installing) to start converting your non-MCAP data to MCAP files.

```
$ mcap convert ../../testdata/bags/demo.bag demo.mcap
```

You can also use the mcap CLI tool to inspect MCAP files, validate them, and even echo their messages to `stdout`. For a full list of possible commands, check out the [mcap GitHub repo](https://github.com/foxglove/mcap/tree/main/go/cli/mcap).

## Writing ROS 1 data to MCAP files

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

Foxglove Studio supports playing back local and remote ROS 1 bag files, as well as MCAP files including ROS 1 data.
