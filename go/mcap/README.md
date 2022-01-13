## mcap tool

> Note: this tool is experimental and will change without warning until finalization of the MCAP spec.

A basic command line tool to demonstrate the mcap file format. See `mcap -h`
for details:

    A small utility for demonstration of the mcap file format

    Usage:
      mcap [command]
    
    Available Commands:
      cat         Cat the messages in an mcap file to stdout
      completion  Generate the autocompletion script for the specified shell
      convert     Convert a bag file to an mcap file
      help        Help about any command
      info        Report statistics about an mcap file
    
    Flags:
          --config string   config file (default is $HOME/.mcap.yaml)
      -h, --help            help for mcap
      -t, --toggle          Help message for toggle
    
    Use "mcap [command] --help" for more information about a command.



Examples:


Convert a bag file to mcap:

    [~/work/mcap/go/mcap] (task/mcap-client) $ mcap convert ../../testdata/bags/demo.bag demo.mcap

Report summary statistics on an mcap file:

    [~/work/mcap/go/mcap] (task/mcap-client) $ mcap info demo.mcap 
    duration: 7.780758504s
    start: 2017-03-21T19:26:20.103843113-07:00
    end: 2017-03-21T19:26:27.884601617-07:00
    messages: 1606
    chunks:
    	lz4: [27/27 chunks] (44.32%) 
    channels
    	(0) /diagnostics: 52 msgs
    	(1) /image_color/compressed: 234 msgs
    	(2) /tf: 774 msgs
    	(3) /radar/points: 156 msgs
    	(4) /radar/range: 156 msgs
    	(5) /radar/tracks: 156 msgs
    	(6) /velodyne_points: 78 msgs
    attachments: 0
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
