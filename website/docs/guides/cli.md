---
sidebar_position: 3
---

# CLI

The MCAP command line tool is useful for working with MCAP files.

## Installation

### Release binaries

Download binaries for your platform from [the latest GitHub release](https://github.com/foxglove/mcap/releases?q=mcap-cli).

Then, mark it executable:

    $ chmod +x mcap

If required, move the binary onto your path.

### Homebrew

To install using [Homebrew](https://brew.sh) on macOS or Linux, run:

    $ brew install mcap

### From Source

1. Clone the [mcap repository](https://github.com/foxglove/mcap).
2. `$ cargo build -p mcap-cli --release`
3. The binary will be built at `target/release/mcap`.

## Usage

Run `mcap --help` for detailed usage information, or `mcap <command> --help` for the options of a specific command.

    $ mcap --help

    Usage: mcap [OPTIONS] <COMMAND>

    Commands:
      add         Add records to an existing MCAP file
      cat         Concatenate the messages in one or more MCAP files to stdout
      completion  Generate shell completion scripts
      compress    Create a compressed copy of an MCAP file
      convert     Convert supported input files to MCAP
      decompress  Create an uncompressed copy of an MCAP file
      doctor      Check an MCAP file structure
      du          Compute byte usage statistics for MCAP records
      filter      Copy filtered MCAP data to a new file
      get         Get a record from an MCAP file
      info        Report statistics about an MCAP file
      list        List records of an MCAP file
      merge       Merge MCAP files
      recover     Recover data from a potentially corrupt MCAP file
      sort        Read an MCAP file and write messages sorted by log time
      help        Print this message or the help of the given subcommand(s)

    Options:
      -c, --color <COLOR>      [default: auto] [possible values: auto, always, never]
          --allow-remote-scan  Allow commands to download/scan remote inputs
      -v, --verbose...         Verbosity (-v, -vv, -vvv, etc.)
      -h, --help               Print help
      -V, --version            Print version

### Shell completion

Generate a shell completion script with `mcap completion <shell>` (supports `bash`, `zsh`, `fish`, `elvish`, and `powershell`). For example, to enable completions in the current `bash` session:

    $ source <(mcap completion bash)

### Converting other formats to MCAP

Convert a ROS 1 bag file to mcap:

<!-- cspell: disable -->

    $ mcap convert demo.bag demo.mcap

<!-- cspell: enable -->

Convert a ROS 2 db3 file to mcap:

<!-- cspell: disable -->

    $ mcap convert demo.db3 demo.mcap

<!-- cspell: enable -->

Convert a PX4 ULog file to mcap:

<!-- cspell: disable -->

    $ mcap convert flight.ulg flight.mcap

<!-- cspell: enable -->

The `mcap` CLI dispatches on the input file extension (`.bag` for ROS 1, `.db3` for ROS 2, `.ulg`/`.ulog` for PX4 ULog) and reads each format using the message definitions embedded in the file.

ROS 2 Iron and later embed message definitions when recording, so these files convert without a sourced workspace. Bags recorded before ROS 2 Iron do not contain embedded message definitions and cannot be converted directly. Use the [`ros2 bag convert`](https://github.com/ros2/rosbag2#converting-bags-merge-split-etc-) utility instead (with the original ROS 2 workspace sourced) to convert between `.db3` and MCAP.

ULog files use the `px4` profile. Each uORB topic is converted to a protobuf message with a schema named `px4.<message_name>` on a `<message_name>/<multi_id>` topic. Logged strings (`PX4_INFO`/`PX4_WARN`/`PX4_ERR` output) are written to a `log_message` topic using the `px4.log_message` schema, parameters to a `parameters` topic using `px4.parameter`, and ULog info fields to an MCAP `info` metadata record. ULog timestamps are recorded relative to system boot.

### File summarization

Report summary statistics on an MCAP file:

<!-- cspell: disable -->

    $ mcap info demo.mcap
    library:     mcap-cli/0.2.0 mcap-rust/0.25.0
    profile:     ros1
    messages:    1606
    duration:    7.780758504s
    start:       2017-03-22T02:26:20.103843113Z (1490149580.103843113)
    end:         2017-03-22T02:26:27.884601617Z (1490149587.884601617)
    compression:
    	zstd: [14/14 chunks] [124.89 MB/61.46 MB (50.79%)] [7.90 MB/s]
    chunks:
    	max uncompressed size: 9.65 MB
    	max compressed size: 4.76 MB
    	overlaps: no
    channels:
    	(0) /diagnostics              52 msgs (6.6..6.7Hz)    : diagnostic_msgs/DiagnosticArray [ros1msg]
    	(1) /image_color/compressed  234 msgs (29.9..30.1Hz)  : sensor_msgs/CompressedImage [ros1msg]
    	(2) /tf                      774 msgs (99.3..99.5Hz)  : tf2_msgs/TFMessage [ros1msg]
    	(3) /radar/points            156 msgs (19.9..20.0Hz)  : sensor_msgs/PointCloud2 [ros1msg]
    	(4) /radar/range             156 msgs (19.9..20.0Hz)  : sensor_msgs/Range [ros1msg]
    	(5) /radar/tracks            156 msgs (19.9..20.0Hz)  : radar_driver/RadarTracks [ros1msg]
    	(6) /velodyne_points          78 msgs (9.9..10.0Hz)   : sensor_msgs/PointCloud2 [ros1msg]
    channels:    7
    attachments: 0
    metadata:    0

<!-- cspell: enable -->

### Indexed reading

Echo messages for a specific topic to stdout as JSON:

    $ mcap cat demo.mcap --topics /tf --json | head -n 10
    {"topic":"/tf","sequence":2,"log_time":1490149580.103843113,"publish_time":1490149580.103843113,"data":{"transforms":[{"header":{"seq":0,"stamp":1490149580.117017840,"frame_id":"base_link"},"child_frame_id":"radar","transform":{"translation":{"x":3.835,"y":0,"z":0},"rotation":{"x":0,"y":0,"z":0,"w":1}}}]}}
    {"topic":"/tf","sequence":3,"log_time":1490149580.113944947,"publish_time":1490149580.113944947,"data":{"transforms":[{"header":{"seq":0,"stamp":1490149580.127078895,"frame_id":"base_link"},"child_frame_id":"radar","transform":{"translation":{"x":3.835,"y":0,"z":0},"rotation":{"x":0,"y":0,"z":0,"w":1}}}]}}
    {"topic":"/tf","sequence":8,"log_time":1490149580.124028613,"publish_time":1490149580.124028613,"data":{"transforms":[{"header":{"seq":0,"stamp":1490149580.137141823,"frame_id":"base_link"},"child_frame_id":"radar","transform":{"translation":{"x":3.835,"y":0,"z":0},"rotation":{"x":0,"y":0,"z":0,"w":1}}}]}}
    {"topic":"/tf","sequence":10,"log_time":1490149580.134219155,"publish_time":1490149580.134219155,"data":{"transforms":[{"header":{"seq":0,"stamp":1490149580.147199242,"frame_id":"base_link"},"child_frame_id":"radar","transform":{"translation":{"x":3.835,"y":0,"z":0},"rotation":{"x":0,"y":0,"z":0,"w":1}}}]}}
    {"topic":"/tf","sequence":11,"log_time":1490149580.144292780,"publish_time":1490149580.144292780,"data":{"transforms":[{"header":{"seq":0,"stamp":1490149580.157286100,"frame_id":"base_link"},"child_frame_id":"radar","transform":{"translation":{"x":3.835,"y":0,"z":0},"rotation":{"x":0,"y":0,"z":0,"w":1}}}]}}
    {"topic":"/tf","sequence":12,"log_time":1490149580.154895238,"publish_time":1490149580.154895238,"data":{"transforms":[{"header":{"seq":0,"stamp":1490149580.167376974,"frame_id":"base_link"},"child_frame_id":"radar","transform":{"translation":{"x":3.835,"y":0,"z":0},"rotation":{"x":0,"y":0,"z":0,"w":1}}}]}}
    {"topic":"/tf","sequence":15,"log_time":1490149580.165152280,"publish_time":1490149580.165152280,"data":{"transforms":[{"header":{"seq":0,"stamp":1490149580.177463023,"frame_id":"base_link"},"child_frame_id":"radar","transform":{"translation":{"x":3.835,"y":0,"z":0},"rotation":{"x":0,"y":0,"z":0,"w":1}}}]}}
    {"topic":"/tf","sequence":20,"log_time":1490149580.175192697,"publish_time":1490149580.175192697,"data":{"transforms":[{"header":{"seq":0,"stamp":1490149580.187523449,"frame_id":"base_link"},"child_frame_id":"radar","transform":{"translation":{"x":3.835,"y":0,"z":0},"rotation":{"x":0,"y":0,"z":0,"w":1}}}]}}
    {"topic":"/tf","sequence":21,"log_time":1490149580.185428613,"publish_time":1490149580.185428613,"data":{"transforms":[{"header":{"seq":0,"stamp":1490149580.197612248,"frame_id":"base_link"},"child_frame_id":"radar","transform":{"translation":{"x":3.835,"y":0,"z":0},"rotation":{"x":0,"y":0,"z":0,"w":1}}}]}}
    {"topic":"/tf","sequence":22,"log_time":1490149580.196638030,"publish_time":1490149580.196638030,"data":{"transforms":[{"header":{"seq":0,"stamp":1490149580.207699065,"frame_id":"base_link"},"child_frame_id":"radar","transform":{"translation":{"x":3.835,"y":0,"z":0},"rotation":{"x":0,"y":0,"z":0,"w":1}}}]}}

### Remote file support

The `mcap` CLI can read files over **HTTP(S)** and from object stores: **Amazon S3** (`s3://`, `s3a://`), **Google Cloud Storage** (`gs://`), and **Azure Blob Storage** (`az://`, `azure://`, `adl://`, `abfs://`, `abfss://`):

<!-- cspell: disable -->

    $ mcap info gs://your-remote-bucket/demo.mcap
    library:     mcap-cli/0.2.0 mcap-rust/0.25.0
    profile:     ros1
    messages:    1606
    duration:    7.780758504s
    start:       2017-03-22T02:26:20.103843113Z (1490149580.103843113)
    end:         2017-03-22T02:26:27.884601617Z (1490149587.884601617)
    compression:
    	zstd: [14/14 chunks] [124.89 MB/61.46 MB (50.79%)] [7.90 MB/s]
    chunks:
    	max uncompressed size: 9.65 MB
    	max compressed size: 4.76 MB
    	overlaps: no
    channels:
    	(0) /diagnostics              52 msgs (6.6..6.7Hz)    : diagnostic_msgs/DiagnosticArray [ros1msg]
    	(1) /image_color/compressed  234 msgs (29.9..30.1Hz)  : sensor_msgs/CompressedImage [ros1msg]
    	(2) /tf                      774 msgs (99.3..99.5Hz)  : tf2_msgs/TFMessage [ros1msg]
    	(3) /radar/points            156 msgs (19.9..20.0Hz)  : sensor_msgs/PointCloud2 [ros1msg]
    	(4) /radar/range             156 msgs (19.9..20.0Hz)  : sensor_msgs/Range [ros1msg]
    	(5) /radar/tracks            156 msgs (19.9..20.0Hz)  : radar_driver/RadarTracks [ros1msg]
    	(6) /velodyne_points          78 msgs (9.9..10.0Hz)   : sensor_msgs/PointCloud2 [ros1msg]
    channels:    7
    attachments: 0
    metadata:    0

<!-- cspell: enable -->

Indexed reads use the summary index at the end of the file to fetch only the bytes they need, minimizing latency and data transfer. Commands that only need indexed data — such as `info`, `list`, and single-record `get` — work against remote files without any extra flags.

#### Allowing full remote scans

Some operations must read or download the entire remote file: commands that rewrite a file (`filter`, `merge`, `convert`, `recover`) and any command that falls back to a linear scan (for example a remote file with no summary section, a server that does not support range requests, or `cat` reading message payloads). These require the `--allow-remote-scan` flag to opt in to the larger transfer:

```bash
mcap filter --allow-remote-scan gs://your-remote-bucket/demo.mcap -o filtered.mcap -y /tf
```

#### Credentials

Credentials are read from the standard environment variables for each backend (`AWS_*` for S3, `GOOGLE_*` for GCS, and `AZURE_*` for Azure Blob Storage). When reading from S3 you must also specify the region of the bucket:

```bash
AWS_REGION=eu-north-1 mcap info s3://my-public-bucket/demo.mcap
```

### File Diagnostics

#### List chunks in a file

`mcap list chunks` prints the chunk index: each chunk's byte offset and length, message time range, compression, compressed/uncompressed sizes and ratio, and the size of its message index.

    $ mcap list chunks recording.mcap
    offset  length  start       end         compression  compressed size  uncompressed size  compression ratio  message index length
    60      312     1000000002  3000000004  zstd         259              436                0.594037           78

#### Recovering data from a corrupt file

`mcap recover` reads a potentially corrupt or truncated MCAP file and writes a valid, readable copy, rebuilding the chunk indexes and summary section.

    $ mcap recover damaged.mcap -o recovered.mcap

By default (`--compression preserve`) the output keeps the input's compression; pass `--compression zstd|lz4|none` to choose a codec. The exit code reports how recovery went: `0` if every record was recovered, `3` if recovery was lossy (records were discarded or the file was truncated mid-record), and `1` if nothing could be recovered.
