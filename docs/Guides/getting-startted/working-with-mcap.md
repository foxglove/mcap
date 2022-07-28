# Working With MCAP

Once you have MCAP data to work with, you may want to **inspect, visualize, and store your MCAP files**.

### CLI commands

[Install the mcap CLI tool](https://github.com/foxglove/mcap/tree/main/go/cli/mcap#installing) to start working with your MCAP files.

To validate an MCAP file:

```
$ mcap doctor demo.mcap
```

To report summary statistics on an MCAP file to get information like number of messages, start and end time, channels, etc.:

```
$ mcap info demo.mcap
```

You can even read from remote files stored in GCS:

```
$ mcap info gs://foxglove-wyatt-dev-inbox/demo.mcap
```

To echo messages for a topic to stdout as JSON:

```
$ mcap cat demo.mcap --topics /tf --json | head -n 10
```

For a full list of possible commands, check out the [GitHub repo](https://github.com/foxglove/mcap/tree/main/go/cli/mcap).

### Integrations

[Integrate](doc:visualizing-mcap-in-foxglove-studio) your MCAP data with a variety of third-party tools, including [Foxglove Studio](https://foxglove.dev/studio) and [Foxglove Data Platform](https://foxglove.dev/data-platform).
