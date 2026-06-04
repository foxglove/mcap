---
sidebar_position: 3
---

# CLI

The `mcap` command line tool inspects, edits, and converts MCAP files. Its source lives in [`rust/cli`](https://github.com/foxglove/mcap/tree/main/rust/cli).

## Installation

### Release binaries

Download binaries for your platform from [the latest GitHub release](https://github.com/foxglove/mcap/releases?q=mcap-cli).

Then, mark it executable:

    $ chmod +x mcap

If required, move the binary onto your path.

### Homebrew

To install using [Homebrew](https://brew.sh) on macOS or Linux, run:

    $ brew install mcap

### From source

1. Clone the [mcap repository](https://github.com/foxglove/mcap).
2. `$ cd rust`
3. `$ cargo build -p mcap-cli --release`
4. The binary is built at `rust/target/release/mcap`.

## Usage

Run `mcap --help` to list the available commands, and `mcap <command> --help` for the detailed options of any command.

<!-- cspell: disable -->

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
      merge       Merge a selection of MCAP files by record timestamp
      recover     Recover data from a potentially corrupt MCAP file
      sort        Read an MCAP file and write messages sorted by log time
      help        Print this message or the help of the given subcommand(s)

<!-- cspell: enable -->

## Inspecting files

### Summarize a file

`mcap info` reports summary statistics, channels, and compression for a file:

<!-- cspell: disable -->

    $ mcap info demo.mcap
    library: mcap-rs-0.24.0
    profile:
    messages: 120
    duration: 990ms
    start: 2017-03-22T02:26:20Z (1490149580.000000000)
    end: 2017-03-22T02:26:20.99Z (1490149580.990000000)
    compression:
    	zstd: [1/1 chunks] [8.17 KiB/1.75 KiB (78.53%)] [1.77 KiB/sec]
    chunks:
    	max uncompressed size: 8.17 KiB
    	max compressed size: 1.75 KiB
    	overlaps: no
    channels:
    	(1) /pose       	100 msgs (100..101Hz)	 : geometry/Pose [jsonschema]
    	(2) /diagnostics	 20 msgs (19..20Hz)  	 : diagnostics/Status [jsonschema]
    channels: 2
    attachments: 0
    metadata: 0

<!-- cspell: enable -->

### List records

`mcap list` prints the `channels`, `schemas`, `chunks`, `attachments`, or `metadata` in a file:

<!-- cspell: disable -->

    $ mcap list channels demo.mcap
    id	schemaId	topic       	messageEncoding	metadata
    1 	1       	/pose       	json           	{}
    2 	2       	/diagnostics	json           	{}

<!-- cspell: enable -->

### Echo messages

`mcap cat` writes messages to stdout. Pass `--json` to decode messages to JSON (supported for the `ros1`, `protobuf`, and `json` message encodings), and `--topics` to select a comma-separated list of topics:

<!-- cspell: disable -->

    $ mcap cat demo.mcap --topics /pose --json | head -n 3
    {"topic":"/pose","sequence":0,"log_time":1490149580.000000000,"publish_time":1490149580.000000000,"data":{"x": 0.0, "y": 0.0, "z": 0.0}}
    {"topic":"/pose","sequence":1,"log_time":1490149580.010000000,"publish_time":1490149580.010000000,"data":{"x": 0.1, "y": 0.2, "z": 0.0}}
    {"topic":"/pose","sequence":2,"log_time":1490149580.020000000,"publish_time":1490149580.020000000,"data":{"x": 0.2, "y": 0.4, "z": 0.0}}

<!-- cspell: enable -->

### Check file health

`mcap doctor` validates a file's structure. It prints nothing and exits with status `0` when the file is well-formed:

    $ mcap doctor demo.mcap

## Converting other formats

`mcap convert` converts a ROS 1 bag (`.bag`) or ROS 2 SQLite (`.db3`) file to MCAP, choosing the input format from the file extension:

<!-- cspell: disable -->

    $ mcap convert demo.bag demo.mcap
    $ mcap convert demo.db3 demo.mcap

<!-- cspell: enable -->

In ROS 2 releases prior to Iron, db3 files did not contain message definitions (schemas). When converting such a file, first source the same ROS 2 workspace it was recorded with. If that is unavailable, point `--ament-prefix-path` at a directory containing the message definitions (for example `/opt/ros/humble`):

    $ mcap convert demo.db3 demo.mcap --ament-prefix-path /path/to/humble

## Editing and transforming files

Filter a file by topic into a new file. Use `-y`/`--include-topic-regex` to include topics and `-n`/`--exclude-topic-regex` to exclude them, and restrict the time range with `--start`/`--end` (nanoseconds or RFC3339) or `--start-secs`/`--end-secs`:

    $ mcap filter demo.mcap -o filtered.mcap -y /pose

Merge multiple files into one, ordered by log time:

    $ mcap merge a.mcap b.mcap -o merged.mcap

Sort a file's messages by log time:

    $ mcap sort demo.mcap -o sorted.mcap

Create a compressed or uncompressed copy of a file:

    $ mcap compress demo.mcap -o compressed.mcap
    $ mcap decompress demo.mcap -o decompressed.mcap

Recover readable data from a corrupt or truncated file:

    $ mcap recover demo.mcap -o recovered.mcap

## Remote files

Most commands can read files stored in Amazon S3 (`s3://`), Google Cloud Storage (`gs://`), Azure Blob Storage, or over HTTP(S). Indexed reads such as `mcap info` fetch only the file's index to minimize latency and data transfer:

    $ mcap info s3://your-bucket/demo.mcap

Commands that need to scan or download an entire remote file, such as `mcap cat` or `mcap convert`, require the `--allow-remote-scan` flag:

    $ mcap cat s3://your-bucket/demo.mcap --json --allow-remote-scan

When reading from S3, specify the bucket region with the `AWS_REGION` environment variable:

```bash
AWS_REGION=eu-north-1 mcap info s3://my-public-bucket/demo.mcap
```

## Shell completion

`mcap completion <shell>` prints a completion script for `bash`, `zsh`, `fish`, `powershell`, or `elvish`. To load completions in the current shell session:

<!-- cspell: disable -->

    $ source <(mcap completion bash)   # bash
    $ source <(mcap completion zsh)    # zsh
    $ mcap completion fish | source    # fish

<!-- cspell: enable -->

To make completions permanent, write the script to the location your shell loads completions from.
