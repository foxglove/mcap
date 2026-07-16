---
sidebar_position: 3
---

# CLI

A command line tool for inspecting and manipulating MCAP files.

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

    A command line tool for inspecting and manipulating MCAP files.

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
      sort        Read an MCAP file and write it back with messages reordered (log_time, preserve, or topic)
      help        Print this message or the help of the given subcommand(s)

    Options:
      -c, --color <COLOR>                [default: auto] [possible values: auto, always, never]
          --allow-remote-scan            Allow commands to download/scan remote inputs
          --time-format <TIME_FORMAT>    [default: auto] [possible values: auto, rfc3339, seconds, nanoseconds]
      -v, --verbose...                   Verbosity (-v, -vv, -vvv, etc.)
      -h, --help                         Print help
      -V, --version                      Print version

    Learn more:
      Homepage       https://mcap.dev
      Specification  https://mcap.dev/spec

    MCAP is an open source project by Foxglove (https://foxglove.dev).

### Shell completion

Generate a shell completion script with `mcap completion <shell>` (supports `bash`, `zsh`, `fish`, `elvish`, and `powershell`). For example, to enable completions in the current `bash` session:

    $ source <(mcap completion bash)

### ROS Bag to MCAP conversion

Convert a ROS 1 bag file to mcap:

<!-- cspell: disable -->

    $ mcap convert demo.bag demo.mcap

<!-- cspell: enable -->

Convert a ROS 2 db3 file to mcap:

<!-- cspell: disable -->

    $ mcap convert demo.db3 demo.mcap

<!-- cspell: enable -->

The `mcap` CLI dispatches on the input file extension (`.bag` for ROS 1, `.db3` for ROS 2) and reads ROS 2 `.db3` files using the message definitions embedded in the file. ROS 2 Iron and later embed message definitions when recording, so these files convert without a sourced workspace.

Bags recorded before ROS 2 Iron do not contain embedded message definitions and cannot be converted directly. Use the [`ros2 bag convert`](https://github.com/ros2/rosbag2#converting-bags-merge-split-etc-) utility instead (with the original ROS 2 workspace sourced) to convert between `.db3` and MCAP.

### File summarization

Report summary statistics on an MCAP file:

<!-- cspell: disable -->

    $ mcap info demo.mcap
    library:     mcap-cli/0.3.0 mcap-rust/0.25.0
    profile:     ros1
    messages:    1606
    duration:    7.780758504s
    start:       2017-03-22T02:26:20.103843113Z
    end:         2017-03-22T02:26:27.884601617Z
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

Echo messages for a specific topic to stdout as newline-delimited JSON (one object per message):

    $ mcap cat demo.mcap --topics /tf --format=ndjson | head -n 10
    {"topic":"/tf","sequence":2,"log_time":"2017-03-22T02:26:20.103843113Z","publish_time":"2017-03-22T02:26:20.103843113Z","data":{"transforms":[{"header":{"seq":0,"stamp":1490149580.117017840,"frame_id":"base_link"},"child_frame_id":"radar","transform":{"translation":{"x":3.835,"y":0,"z":0},"rotation":{"x":0,"y":0,"z":0,"w":1}}}]}}
    {"topic":"/tf","sequence":3,"log_time":"2017-03-22T02:26:20.113944947Z","publish_time":"2017-03-22T02:26:20.113944947Z","data":{"transforms":[{"header":{"seq":0,"stamp":1490149580.127078895,"frame_id":"base_link"},"child_frame_id":"radar","transform":{"translation":{"x":3.835,"y":0,"z":0},"rotation":{"x":0,"y":0,"z":0,"w":1}}}]}}
    {"topic":"/tf","sequence":8,"log_time":"2017-03-22T02:26:20.124028613Z","publish_time":"2017-03-22T02:26:20.124028613Z","data":{"transforms":[{"header":{"seq":0,"stamp":1490149580.137141823,"frame_id":"base_link"},"child_frame_id":"radar","transform":{"translation":{"x":3.835,"y":0,"z":0},"rotation":{"x":0,"y":0,"z":0,"w":1}}}]}}
    {"topic":"/tf","sequence":10,"log_time":"2017-03-22T02:26:20.134219155Z","publish_time":"2017-03-22T02:26:20.134219155Z","data":{"transforms":[{"header":{"seq":0,"stamp":1490149580.147199242,"frame_id":"base_link"},"child_frame_id":"radar","transform":{"translation":{"x":3.835,"y":0,"z":0},"rotation":{"x":0,"y":0,"z":0,"w":1}}}]}}
    {"topic":"/tf","sequence":11,"log_time":"2017-03-22T02:26:20.144292780Z","publish_time":"2017-03-22T02:26:20.144292780Z","data":{"transforms":[{"header":{"seq":0,"stamp":1490149580.157286100,"frame_id":"base_link"},"child_frame_id":"radar","transform":{"translation":{"x":3.835,"y":0,"z":0},"rotation":{"x":0,"y":0,"z":0,"w":1}}}]}}
    {"topic":"/tf","sequence":12,"log_time":"2017-03-22T02:26:20.154895238Z","publish_time":"2017-03-22T02:26:20.154895238Z","data":{"transforms":[{"header":{"seq":0,"stamp":1490149580.167376974,"frame_id":"base_link"},"child_frame_id":"radar","transform":{"translation":{"x":3.835,"y":0,"z":0},"rotation":{"x":0,"y":0,"z":0,"w":1}}}]}}
    {"topic":"/tf","sequence":15,"log_time":"2017-03-22T02:26:20.165152280Z","publish_time":"2017-03-22T02:26:20.165152280Z","data":{"transforms":[{"header":{"seq":0,"stamp":1490149580.177463023,"frame_id":"base_link"},"child_frame_id":"radar","transform":{"translation":{"x":3.835,"y":0,"z":0},"rotation":{"x":0,"y":0,"z":0,"w":1}}}]}}
    {"topic":"/tf","sequence":20,"log_time":"2017-03-22T02:26:20.175192697Z","publish_time":"2017-03-22T02:26:20.175192697Z","data":{"transforms":[{"header":{"seq":0,"stamp":1490149580.187523449,"frame_id":"base_link"},"child_frame_id":"radar","transform":{"translation":{"x":3.835,"y":0,"z":0},"rotation":{"x":0,"y":0,"z":0,"w":1}}}]}}
    {"topic":"/tf","sequence":21,"log_time":"2017-03-22T02:26:20.185428613Z","publish_time":"2017-03-22T02:26:20.185428613Z","data":{"transforms":[{"header":{"seq":0,"stamp":1490149580.197612248,"frame_id":"base_link"},"child_frame_id":"radar","transform":{"translation":{"x":3.835,"y":0,"z":0},"rotation":{"x":0,"y":0,"z":0,"w":1}}}]}}
    {"topic":"/tf","sequence":22,"log_time":"2017-03-22T02:26:20.196638030Z","publish_time":"2017-03-22T02:26:20.196638030Z","data":{"transforms":[{"header":{"seq":0,"stamp":1490149580.207699065,"frame_id":"base_link"},"child_frame_id":"radar","transform":{"translation":{"x":3.835,"y":0,"z":0},"rotation":{"x":0,"y":0,"z":0,"w":1}}}]}}

### Timestamp formatting

The global `--time-format` flag controls how `cat`, `info`, `list chunks`, and `list attachments` render timestamps. It accepts:

| Value            | Aliases   | Output                                                                                                          |
| ---------------- | --------- | --------------------------------------------------------------------------------------------------------------- |
| `auto` (default) |           | RFC3339 UTC for real wall-clock times, decimal seconds otherwise; ndjson output always uses RFC3339 (see below) |
| `rfc3339`        | `iso8601` | RFC3339 UTC, e.g. `2017-03-22T02:26:20.103843113Z`                                                              |
| `seconds`        |           | decimal seconds, e.g. `1490149580.103843113`                                                                    |
| `nanoseconds`    |           | integer nanoseconds, e.g. `1490149580103843113`                                                                 |

    $ mcap cat demo.mcap --time-format=seconds
    $ mcap info demo.mcap --time-format=nanoseconds

Under `auto`, human-facing output (the default `cat` text, `info`, and `list` tables) renders timestamps at or after `2000-01-01T00:00:00Z` as RFC3339 dates, and smaller values (typical of relative or monotonic recordings that start near zero) as decimal seconds — so a real recording shows `2017-03-22T02:26:20.103843113Z` while a relative one shows `1.000000000` instead of a misleading `1970` date. This choice is made **once per command** from the recording's start time and applied to every timestamp, so a file whose clock jumps across the cutoff (for example when GPS time is acquired mid-recording) still renders uniformly. To force real dates regardless of the cutoff, use `--time-format=rfc3339`.

Machine-facing output (`cat --format=ndjson`) is different: under `auto` it **always** uses RFC3339, with no cutoff, so the field has a single predictable shape a parser can rely on. `log_time` and `publish_time` are always emitted as quoted JSON strings (never bare numbers) to avoid floating-point and large-integer precision loss, so a nanosecond-aware parser (pandas, Apache Arrow, DuckDB, …) can read them back without loss. Because there is no cutoff, a relative recording renders as `1970`-relative timestamps, which still round-trip exactly:

    $ mcap cat relative.mcap --format=ndjson | head -n 1
    {"topic":"/data","sequence":1,"log_time":"1970-01-01T00:00:01.000000000Z","publish_time":"1970-01-01T00:00:01.000000000Z","data":{"value":1}}

For a numeric column instead, use `--time-format=nanoseconds` (integer nanoseconds, parseable as a JavaScript `BigInt`) or `--time-format=seconds` (a fixed-point decimal string); both stay quoted strings, so full precision survives rather than being lost to JSON numbers. Explicit `--time-format` values are always honored as-is, in every command.

`--time-format` only changes how timestamps are **displayed**; it does not alter the nanosecond values stored in files, and the rewrite commands (`filter`, `compress`, `decompress`, `merge`, `convert`, `recover`, `sort`) ignore it.

### Remote file support

The `mcap` CLI can read files over **HTTP(S)** and from object stores: **Amazon S3** (`s3://`, `s3a://`), **Google Cloud Storage** (`gs://`), and **Azure Blob Storage** (`az://`, `azure://`, `adl://`, `abfs://`, `abfss://`):

<!-- cspell: disable -->

    $ mcap info gs://your-remote-bucket/demo.mcap
    library:     mcap-cli/0.3.0 mcap-rust/0.25.0
    profile:     ros1
    messages:    1606
    duration:    7.780758504s
    start:       2017-03-22T02:26:20.103843113Z
    end:         2017-03-22T02:26:27.884601617Z
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
    offset  length  start        end          compression  compressed size  uncompressed size  compression ratio  message index length
    60      312     2.534221109  4.712889376  zstd         259              436                0.594037           78

#### Recovering data from a corrupt file

`mcap recover` reads a potentially corrupt or truncated MCAP file and writes a valid, readable copy, rebuilding the chunk indexes and summary section.

    $ mcap recover damaged.mcap -o recovered.mcap

By default (`--compression preserve`) the output keeps the input's compression; pass `--compression zstd|lz4|none` to choose a codec. The exit code reports how recovery went: `0` if every record was recovered, `3` if recovery was lossy (records were discarded or the file was truncated mid-record), and `1` if nothing could be recovered.
