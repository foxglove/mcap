## `mcap` tool

> Note: this tool is experimental and will change without warning until finalization of the MCAP spec.

A basic command line tool to demonstrate the MCAP file format. See `mcap -h`
for details.

### Installing:

Either install from [releases
binaries](https://github.com/foxglove/mcap/releases) or by using go.

#### From release binaries

Download the executable for your platform and mark it executable (if on mac or
linux). For example,

    wget https://github.com/foxglove/mcap/releases/latest/download/mcap-linux-amd64 -O mcap
    chmod +x mcap

If desired, move the binary onto your path.

If on windows, download and run the appropriate .exe for your architecture from
the releases page.

#### Using go

To install from the latest commit, use

    go install github.com/foxglove/mcap/go/cli/mcap@latest

### Examples:

#### Bag to mcap conversion

Convert a bag file to MCAP:

<!-- cspell: disable -->

    [~/work/mcap/go/mcap] (task/mcap-client) $ mcap convert ../../testdata/bags/demo.bag demo.mcap

<!-- cspell: enable -->

#### File summarization

Report summary statistics on an MCAP file:

    [~/work/mcap/go/cli/mcap] (main) $ mcap info demo.mcap
    messages: 1606
    duration: 7.780758504s
    start: 2017-03-21T19:26:20.103843113-07:00
    end: 2017-03-21T19:26:27.884601617-07:00
    chunks:
            zstd: [14/14 chunks] (50.79%)
    channels:
            (0) /diagnostics              52 msgs   : diagnostic_msgs/DiagnosticArray [ros1msg]
            (1) /image_color/compressed  234 msgs   : sensor_msgs/CompressedImage [ros1msg]
            (2) /tf                      774 msgs   : tf2_msgs/TFMessage [ros1msg]
            (3) /radar/points            156 msgs   : sensor_msgs/PointCloud2 [ros1msg]
            (4) /radar/range             156 msgs   : sensor_msgs/Range [ros1msg]
            (5) /radar/tracks            156 msgs   : radar_driver/RadarTracks [ros1msg]
            (6) /velodyne_points          78 msgs   : sensor_msgs/PointCloud2 [ros1msg]
    attachments: 0

#### Indexed reading

Echo messages to stdout using the end of file index:

    [~/work/mcap/go/mcap] (task/mcap-client) $ mcap cat demo.mcap --topics /tf,/diagnostics | head -n 10
    1490149580103843113 /diagnostics [42 10 0 0 204 224 209 88 99 250]...
    1490149580103843113 /tf [1 0 0 0 0 0 0 0 204 224]...
    1490149580113944947 /tf [1 0 0 0 0 0 0 0 204 224]...
    1490149580124028613 /tf [1 0 0 0 0 0 0 0 204 224]...
    1490149580134219155 /tf [1 0 0 0 0 0 0 0 204 224]...
    1490149580144292780 /tf [1 0 0 0 0 0 0 0 204 224]...
    1490149580154895238 /tf [1 0 0 0 0 0 0 0 204 224]...
    1490149580165152280 /diagnostics [94 13 0 0 204 224 209 88 174 52]...
    1490149580165152280 /diagnostics [95 13 0 0 204 224 209 88 215 86]...
    1490149580165152280 /tf [1 0 0 0 0 0 0 0 204 224]...

Convert a ros2 bag file to mcap:

    [~/work/mcap/go/mcap] (task/mcap-client) $ mcap convert multiple_files_1.db3 demo.mcap

Note that if the system the conversion is called on is not the original ros2
system, the command requires a search directory for packages. This can be found
by copying the relevant directory (e.g /opt/ros/galactic) from the original
system

    [~/work/mcap/go/mcap] (task/mcap-client) $ mcap convert multiple_files_1.db3 demo.mcap --ament-prefix-path ./galactic

#### Remote file support

All commands except `convert` support reading from remote files stored in GCS:

    $ mcap info gs://foxglove-wyatt-dev-inbox/demo.mcap
    messages: 1606
    duration: 7.780758504s
    start: 2017-03-21T19:26:20.103843113-07:00
    end: 2017-03-21T19:26:27.884601617-07:00
    chunks:
            zstd: [14/14 chunks] (50.79%)
    channels:
            (0) /diagnostics              52 msgs   : diagnostic_msgs/DiagnosticArray [ros1msg]
            (1) /image_color/compressed  234 msgs   : sensor_msgs/CompressedImage [ros1msg]
            (2) /tf                      774 msgs   : tf2_msgs/TFMessage [ros1msg]
            (3) /radar/points            156 msgs   : sensor_msgs/PointCloud2 [ros1msg]
            (4) /radar/range             156 msgs   : sensor_msgs/Range [ros1msg]
            (5) /radar/tracks            156 msgs   : radar_driver/RadarTracks [ros1msg]
            (6) /velodyne_points          78 msgs   : sensor_msgs/PointCloud2 [ros1msg]
    attachments: 0

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
