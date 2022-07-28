# History

## Before MCAP

Many robotics companies valuable in-house resources to develop custom file formats, only to create more future work and complicate third-party tool integrations. We built MCAP to allow teams to focus on their important robotics problems and avoid wasting precious time building commodity tools.

Before MCAP, the format that robotics teams used to store their log data depended largely on their framework. Those using ROS 1 defaulted to the “bag file” format; those on ROS 2 defaulted to a SQLite-based format. Companies that don’t use ROS at all often employ a custom in-house binary format such as length-delimited Protobuf, or store their messages as opaque bytes inside existing file formats such as HDF5.

These existing storage options have several shortcomings. The ROS 1 bag format is difficult to work with outside of the ROS ecosystem, while the ROS 2 SQLite format is [not fully self-contained](https://github.com/ros2/rosbag2/issues/782), making it difficult for third-party tools to read. Custom in-house formats lack interoperability and require developing corresponding libraries in multiple languages to read and write files.

## After MCAP

We evaluated [many existing data storage formats in the industry](https://github.com/foxglove/mcap/blob/main/docs/motivation/evaluation-of-robotics-data-recording-file-formats.md) and identified a clear need for a general-purpose, open source data container format – specifically optimized for robotics use cases. Designing this format would not only solve an industry-wide problem, but also make it easier for teams to leverage third-party tools and share their own tooling.

As a container format, MCAP solves many of these issues. MCAP files are self-contained, can embed multiple data streams encoded with different serialization formats in a single file, and even support metadata and attachments. It is optimized for both high-performance writing and efficient indexed reading, even over remote connections.
