---
description: Read, write, and visualize MCAP files containing ROS 2 data.
---

# ROS 2

## Record with MCAP

ROS 2 supports recording directly to MCAP using the [rosbag2 MCAP storage plugin](https://github.com/ros-tooling/rosbag2_storage_mcap). To get started, install the plugin:

```
$ sudo apt-get install ros-$ROS_DISTRO-rosbag2-storage-mcap
```

Set your storage ID to `mcap` when recording:

```
$ ros2 bag record -s mcap --all
```

You can also customize [MCAP writer options](https://github.com/ros-tooling/rosbag2_storage_mcap#writer-configuration) such as compression and chunk size using storage options:

```
$ cat << EOF > my_storage_config.yaml
compression: "Lz4"
compressionLevel: "Fastest"
EOF
$ ros2 bag record -s mcap --all --storage-config-file my_storage_config.yaml
```

## Convert to MCAP

### Using `ros2 bag convert`

To convert your existing ROS 2 `.db3` files into MCAP files, you can use `ros2 bag convert` from within your ROS 2 workspace.

```bash
$ cat << EOF > convert.yaml
output_bags:
  - uri: ros2_output
    storage_id: mcap
    storage_config_uri: my_storage_config.yaml
    all: true
EOF
$ cat << EOF > my_storage_config.yaml
compression: Zstd
EOF
$ ros2 bag convert -i ros2_input.db3 -o convert.yaml
```

If you want to use compression when converting, specify a storage config file. Specifying `compression_mode` and `compression_format` directly in the `convert.yaml` file will result in an unplayable rosbag2 (https://github.com/ros2/rosbag2/issues/1920).

### Using the `mcap` CLI tool

The [`mcap` CLI tool](../cli.md#installation) also supports converting ROS 2 `.db3` files into MCAP files directly.

```
$ mcap convert multiple_files_1.db3 demo.mcap
```

The `mcap` CLI converts `.db3` files using the message definitions embedded in the file, so the input must be self-contained. ROS 2 db3 files recorded with Iron or newer embed their schemas; the conversion fails if any topic is missing an embedded definition. For older db3 files that don't embed definitions, use the `ros2 bag convert` method described above.

You can also use the mcap CLI tool to inspect MCAP files, validate them, and even echo their messages to `stdout`. For a full list of possible commands, check out the [mcap CLI documentation](../cli.md).

## Read and write MCAP

If you're starting from scratch, you can write code that allows you to **write your ROS 2 data to MCAP files** and subsequently **read your ROS 2 data from your MCAP files**.

### Guides

- [Python](../python/ros2.md)

### Examples

- [Python](https://github.com/foxglove/mcap/tree/main/python/examples/ros2) - [reader](https://github.com/foxglove/mcap/tree/main/python/examples/ros2/py_mcap_demo/py_mcap_demo/reader.py) and [writer](https://github.com/foxglove/mcap/tree/main/python/examples/ros2/py_mcap_demo/py_mcap_demo/writer.py)

## Inspect MCAP

Use the [`mcap` CLI tool](../cli.md) to inspect MCAP files, validate their contents, and even echo their messages to `stdout`.

For an exhaustive list of ways to interact with your MCAP data, check out the [mcap CLI documentation](../cli.md).

## Visualize MCAP

[Foxglove](https://foxglove.dev/) supports playing back local and remote ROS 2 db3 files, as well as local and remote MCAP files containing ROS 2 data.

With that said, we recommend MCAP files over ROS 2 db3 files, as the latter are not completely self-contained.
