#!/bin/bash
set -eo pipefail

source /opt/ros/humble/setup.bash
colcon build
colcon test
