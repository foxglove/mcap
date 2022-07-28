# Converting To MCAP

If you already have existing data that is not in the MCAP file format, you may want to **convert this non-MCAP data into MCAP files**.

[Install the mcap CLI tool](https://github.com/foxglove/mcap/tree/main/go/cli/mcap#installing) to start converting your non-MCAP data to MCAP files.

### ROS 1

```
$ mcap convert ../../testdata/bags/demo.bag demo.mcap
```

### ROS 2

```
$ mcap convert multiple_files_1.db3 demo.mcap
```

mcap will search the path stored in your $AMENT_PREFIX_PATH environment variable to locate the ROS message definitions on your hard drive.

Alternatively, you can specify a colon-separated list of directories for the CLI tool to search using the ament-prefix-path flag:

```
$ mcap convert ros1_input.bag ros1_output.mcap --ament-prefix-path=/your/first/directory;/your/second/directory
```
