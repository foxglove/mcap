## `mcap` CLI

A command line tool to demonstrate the MCAP file format. See `mcap -h` for
details.

### Installing:

Either install from [releases
binaries](https://github.com/foxglove/mcap/releases), from [Homebrew](https://formulae.brew.sh/formula/mcap#default), or by using go.

#### From release binaries

Download the executable for your platform and mark it executable (if on mac or
linux). For example,

    wget https://github.com/foxglove/mcap/releases/latest/download/mcap-linux-amd64 -O mcap
    chmod +x mcap

If desired, move the binary onto your path.

If on windows, download and run the appropriate .exe for your architecture from
the releases page.

#### From Homebrew

To install using [Homebrew](https://brew.sh) on macOS or Linux, run:

    brew install mcap

#### Using go

To install from the latest commit, use

    go install github.com/foxglove/mcap/go/cli/mcap@latest

### Examples:

#### Bag to mcap conversion

Convert a bag file to MCAP:

<!-- cspell: disable -->

    $ mcap convert demo.bag demo.mcap

<!-- cspell: enable -->

Convert a ros2 bag file to mcap:

<!-- cspell: disable -->

    $ mcap convert multiple_files_1.db3 demo.mcap

<!-- cspell: enable -->

Note that if the system the conversion is called on is not the original ros2
system, the command requires a search directory for packages. This can be found
by copying the relevant directory (e.g /opt/ros/galactic) from the original
system

    $ mcap convert multiple_files_1.db3 demo.mcap --ament-prefix-path ./galactic

#### File summarization

Report summary statistics on an MCAP file:

<!-- cspell: disable -->

    $ mcap info demo.mcap
    library: mcap go #(devel)
    profile: ros1
    messages: 1606
    duration: 7.780758504s
    start: 2017-03-21T19:26:20.103843113-07:00 (1490149580.103843113)
    end: 2017-03-21T19:26:27.884601617-07:00 (1490149587.884601617)
    compression:
    	zstd: [14/14 chunks] (50.73%)
    channels:
      	(0) /diagnostics              52 msgs (6.68 Hz)    : diagnostic_msgs/DiagnosticArray [ros1msg]
      	(1) /image_color/compressed  234 msgs (30.07 Hz)   : sensor_msgs/CompressedImage [ros1msg]
      	(2) /tf                      774 msgs (99.48 Hz)   : tf2_msgs/TFMessage [ros1msg]
      	(3) /radar/points            156 msgs (20.05 Hz)   : sensor_msgs/PointCloud2 [ros1msg]
      	(4) /radar/range             156 msgs (20.05 Hz)   : sensor_msgs/Range [ros1msg]
      	(5) /radar/tracks            156 msgs (20.05 Hz)   : radar_driver/RadarTracks [ros1msg]
      	(6) /velodyne_points          78 msgs (10.02 Hz)   : sensor_msgs/PointCloud2 [ros1msg]
    attachments: 0

<!-- cspell: enable -->

#### Indexed reading

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

#### Remote file support

All commands except `convert` support reading from remote files stored in GCS:

<!-- cspell: disable -->

    $ mcap info gs://your-remote-bucket/demo.mcap
    library: mcap go #(devel)
    profile: ros1
    messages: 1606
    duration: 7.780758504s
    start: 2017-03-21T19:26:20.103843113-07:00 (1490149580.103843113)
    end: 2017-03-21T19:26:27.884601617-07:00 (1490149587.884601617)
    compression:
    	zstd: [14/14 chunks] (50.73%)
    channels:
      	(0) /diagnostics              52 msgs (6.68 Hz)    : diagnostic_msgs/DiagnosticArray [ros1msg]
      	(1) /image_color/compressed  234 msgs (30.07 Hz)   : sensor_msgs/CompressedImage [ros1msg]
      	(2) /tf                      774 msgs (99.48 Hz)   : tf2_msgs/TFMessage [ros1msg]
      	(3) /radar/points            156 msgs (20.05 Hz)   : sensor_msgs/PointCloud2 [ros1msg]
      	(4) /radar/range             156 msgs (20.05 Hz)   : sensor_msgs/Range [ros1msg]
      	(5) /radar/tracks            156 msgs (20.05 Hz)   : radar_driver/RadarTracks [ros1msg]
      	(6) /velodyne_points          78 msgs (10.02 Hz)   : sensor_msgs/PointCloud2 [ros1msg]
    attachments: 0

<!-- cspell: enable -->

Remote reads will use the index at the end of the file to minimize latency and data transfer.

#### File Diagnostics

##### List chunks in a file

The `mcap list` command can be used with chunks or attachments:

    $ mcap list chunks ~/data/mcap/demo.mcap
    offset    length   start                end                  compression  compressed size  uncompressed size  compression ratio
    43        4529455  1490149580103843113  1490149580608392239  zstd         4529402          9400437            0.481829
    4531299   4751426  1490149580618484655  1490149581212757989  zstd         4751373          9621973            0.493804
    9284910   4726518  1490149581222848447  1490149581811286531  zstd         4726465          9617327            0.491453
    14013453  4734289  1490149581821378989  1490149582418243031  zstd         4734236          9624850            0.491876
    18749879  4742989  1490149582428402906  1490149583010292990  zstd         4742936          9646234            0.491688
    23494877  4712785  1490149583020377156  1490149583617657323  zstd         4712732          9619341            0.489923
    28209799  4662983  1490149583627720990  1490149584217852199  zstd         4662930          9533042            0.489133
    32874919  4643191  1490149584227924615  1490149584813214116  zstd         4643138          9499481            0.488778
    37520119  4726655  1490149584823300282  1490149585411567366  zstd         4726602          9591399            0.492796
    42248895  4748884  1490149585421596866  1490149586021460449  zstd         4748831          9621776            0.493550
    46999820  4746828  1490149586031607908  1490149586617282658  zstd         4746775          9632302            0.492798
    51748769  4759213  1490149586627453408  1490149587217501700  zstd         4759160          9634744            0.493958
    56510103  4750731  1490149587227624742  1490149587814043200  zstd         4750678          9622778            0.493691
    61262859  217330   1490149587824113700  1490149587884601617  zstd         217277           217255             1.000101

### Building

To ensure the resulting binary is statically linked, build with `make`:

    make build

### API Evolution Policy

This tool uses [semantic versioning](https://semver.org) to indicate the scope of an update to
users. In addition, we try to follow these guidelines when updating the functionality of the tool.

Here "option" refers to either a subcommand (such as `mcap convert`) or a command-line argument
(such as `mcap --help`).

- When a option needs to be removed, it is first marked as deprecated. Once deprecated,
  it will remain functional for at least two releases or one month,
  whichever is longer. Deprecated options will print a warning to stderr when used.
  After the deprecation period, it will be removed in the next major release.
- When an option needs to be renamed, we duplicate the functionality under the new name and
  deprecate the old name. The deprecation warning for the old name should indicate the new name
  for this functionality.
- If some option that you use is deprecated without a clear migration path, please
  file a [GitHub issue](https://github.com/foxglove/mcap/issues) explaining your use case.
