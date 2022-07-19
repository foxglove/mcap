#!/bin/bash
set -e

source /opt/ros/humble/setup.bash
colcon build
colcon test

source install/local_setup.bash
set +e
exec "$@"
