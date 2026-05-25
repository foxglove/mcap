# ROS1 bag fixture generation

This directory contains tooling for regenerating small ROS1 bag fixtures with
official ROS Noetic tooling. The generated bags are intended for conversion
parity tests; they should be checked in only when intentionally updating
fixtures.

Run from the repository root:

```bash
tests/fixtures/ros1/generate_noetic_bags.sh
```

The script runs the official `ros:noetic-ros-base` Docker image, installs the
small message/compression packages needed by the generator, and writes bags to
`tests/fixtures/ros1/generated/`.

Parser edge cases that cannot be produced through the public `rosbag.Bag` writer
API, such as a connection record with no messages, should stay as small
programmatic fixtures in the relevant tests.
