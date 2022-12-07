---
description: MCAP can be used for a variety of scenarios throughout your robotics development workflows.
---

# Overview

You may be interested in using MCAP for a variety of scenarios throughout your robotics development workflows.

## Convert to MCAP

If you already have existing ROS data that is not in the MCAP file format, you may want to **convert it into MCAP files**. The easiest way to do this is to [install the MCAP CLI tool](https://github.com/foxglove/mcap/tree/main/go/cli/mcap#installing) and use it to [convert your existing bag files](https://github.com/foxglove/mcap/tree/main/go/cli/mcap#bag-to-mcap-conversion).

## Read and write MCAP

If you're starting from scratch, you may want to write code to **read and write your own MCAP data**.

We provide MCAP readers and writers in the following languages, for any of our supported [data serialization formats](../home/data-serialization-formats.md):

- [Python](https://github.com/foxglove/mcap/tree/main/python)
- [C++](https://github.com/foxglove/mcap/tree/main/cpp)
- [Go](https://github.com/foxglove/mcap/tree/main/go)
- [Swift](https://github.com/foxglove/mcap/tree/main/swift)
- [TypeScript](https://github.com/foxglove/mcap/tree/main/typescript)
- [Rust](https://github.com/foxglove/mcap/tree/main/rust)

## Inspect and visualize MCAP

Once you have MCAP data to work with, you may want to [**use the mcap CLI to inspect and interact with your MCAP files**](https://github.com/foxglove/mcap/tree/main/go/cli/mcap#examples).

You can also start visualizing your MCAP data with third-party tools like [Foxglove Studio](https://foxglove.dev/studio). To start leveraging all the visualizations Studio has to offer, you must write messages that adhere to a pre-defined set of Foxglove schemas.

The [@foxglove/schemas](https://github.com/foxglove/schemas) repo provides pre-defined schemas in the following data serialization formats:

- [ROS 1](https://github.com/foxglove/schemas/tree/main/schemas/ros1)
- [ROS 2](https://github.com/foxglove/schemas/tree/main/schemas/ros2)
- [JSON Schema](https://github.com/foxglove/schemas/tree/main/schemas/jsonschema)
- [Protobuf](https://github.com/foxglove/schemas/tree/main/schemas/proto/foxglove)
- [Flatbuffers](https://github.com/foxglove/schemas/tree/main/schemas/flatbuffer)
- [TypeScript](https://github.com/foxglove/schemas/tree/main/schemas/typescript)

Next, download Foxglove Studio as a [desktop app](https://foxglove.dev/download), or navigate to [studio.foxglove.dev](https://studio.foxglove.dev) in your web browser.

**For local MCAP files**, simply drag and drop the file into the Studio app to start playing back your data.

**For remote MCAP files**, select "Open file from URL" when you first load the Studio app, and specify the URL to your remote MCAP file. Open the connection to start playing back your data.

Add and configure different [panels](https://foxglove.dev/docs/studio/panels/introduction) to your [layout](https://foxglove.dev/docs/studio/layouts) to visualize different aspects of your data.

## Store MCAP

[Sign up for a Foxglove account ](https://console.foxglove.dev) to access the data management features of [Foxglove Data Platform](https://foxglove.dev/data-platform) .

Instead of having to pass around hard drives every time you want to share data, you can start importing your MCAP data into a central repository for all your teammates to access and collaborate on.

Instead of having to download enormous files to your local machine whenever you want to inspect some data, you can stream your team data directly to whatever environment you'd like for further analysis.
