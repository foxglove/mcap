# Bag test data

This directory contains shared bag fixtures used across language implementations
and CLI tests.

## ROS1 Noetic fixture generation

`generate_noetic_bags.sh` regenerates small ROS1 bags with official ROS Noetic
tooling. The generated bags are intended for conversion parity tests; check them
in only when intentionally updating fixtures.

Run from the repository root:

```bash
testdata/bags/generate_noetic_bags.sh
```

The script runs the official `ros:noetic-ros-base` Docker image, installs the
small message/compression packages needed by the generator, deletes existing
`noetic-*.bag` files in `testdata/bags/generated/`, and writes regenerated bags
there.

Parser edge cases that cannot be produced through the public `rosbag.Bag` writer
API, such as a connection record with no messages, should stay as small
programmatic fixtures in the relevant tests.
