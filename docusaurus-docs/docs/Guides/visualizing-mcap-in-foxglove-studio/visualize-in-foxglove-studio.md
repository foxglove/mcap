---
title: "Foxglove Studio"
slug: "visualize-in-foxglove-studio"
hidden: false
createdAt: "2022-07-26T19:06:30.461Z"
updatedAt: "2022-07-26T23:29:18.429Z"
---
## Using Foxglove schemas 

To start visualizing and debugging your MCAP data in [Foxglove Studio](https://foxglove.dev/studio) (and leveraging all the visualizations it has to offer), you must write messages that adhere to a pre-defined set of Foxglove schemas.

The [@foxglove/schemas](https://github.com/foxglove/schemas) repo provides pre-defined schemas in the following data serialization formats:

- [ROS 1](https://github.com/foxglove/schemas/tree/main/schemas/ros1)
- [ROS 2](https://github.com/foxglove/schemas/tree/main/schemas/ros2)
- [JSON Schema](https://github.com/foxglove/schemas/tree/main/schemas/jsonschema)
- [Protobuf](https://github.com/foxglove/schemas/tree/main/schemas/proto/foxglove)
- [TypeScript](https://github.com/foxglove/schemas/tree/main/schemas/typescript)

## Visualizing MCAP files

Download Foxglove Studio as a [desktop app](https://foxglove.dev/download), or navigate to [studio.foxglove.dev](https://studio.foxglove.dev) in your web browser. 

**For local MCAP files**, simply drag and drop the file into the Studio app to start playing back your data.

**For remote MCAP files**, select "Open file from URL" when you first load the Studio app, and specify the URL to your remote MCAP file. Open the connection to start playing back your data.

Add and configure different [panels](https://foxglove.dev/docs/studio/panels/introduction) to your [layout](https://foxglove.dev/docs/studio/layouts) to visualize different aspects of your data.