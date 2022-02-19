## `mcap` tool

> Note: this tool is experimental and will change without warning until finalization of the MCAP spec.

A basic command line tool to demonstrate the MCAP file format. See `mcap -h`
for details:

    A small utility for demonstration of the mcap file format

    Usage:
      mcap [command]

    Available Commands:
      cat         Cat the messages in an mcap file to stdout
      completion  Generate the autocompletion script for the specified shell
      convert     Convert a bag file to an mcap file
      doctor      Check an mcap file structure
      help        Help about any command
      info        Report statistics about an mcap file

    Flags:
          --config string   config file (default is $HOME/.mcap.yaml)
      -h, --help            help for mcap
      -t, --toggle          Help message for toggle

    Use "mcap [command] --help" for more information about a command.

Examples:

Convert a bag file to MCAP:

<!-- cspell: disable -->

    [~/work/mcap/go/mcap] (task/mcap-client) $ mcap convert ../../testdata/bags/demo.bag demo.mcap

<!-- cspell: enable -->

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

    [~/work/mcap/go/mcap] (task/mcap-client) $ mcap convert multiple_files_1.db3 demo.mcap --directories ./galactic

### Building

To ensure the resulting binary is statically linked, build with `make`:

    make build
