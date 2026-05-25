#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "$script_dir/../.." && pwd)"
image="${ROS1_FIXTURE_IMAGE:-ros:noetic-ros-base}"
output_dir="${ROS1_FIXTURE_OUTPUT_DIR:-generated}"
host_uid="$(id -u)"
host_gid="$(id -g)"

docker run --rm \
  --volume "$repo_root:/workspace" \
  --workdir /workspace/testdata/bags \
  "$image" \
  bash -lc "
    set -euo pipefail
    apt-get update
    apt-get install -y --no-install-recommends ros-noetic-roslz4 ros-noetic-std-msgs
    rm -rf /var/lib/apt/lists/*
    source /opt/ros/noetic/setup.bash
    python3 generate_noetic_bags.py --output '$output_dir'
    chown -R '$host_uid:$host_gid' '$output_dir'
  "
