#!/usr/bin/env python3
"""Generate tiny ROS1 bag fixtures with official ROS Noetic tooling."""

from __future__ import annotations

import argparse
import hashlib
import shutil
from pathlib import Path

import rosbag
import rospy
from std_msgs.msg import String, UInt32


def write_multitopic_bag(path: Path, compression: str) -> None:
    with rosbag.Bag(
        str(path),
        mode="w",
        compression=compression,
        chunk_threshold=256,
    ) as bag:
        bag.write("/chatter", String(data="hello"), t=rospy.Time(1, 2))
        bag.write("/numbers", UInt32(data=42), t=rospy.Time(2, 3))
        bag.write("/chatter", String(data="world"), t=rospy.Time(3, 4))


def write_empty_bag(path: Path) -> None:
    with rosbag.Bag(str(path), mode="w", compression=rosbag.Compression.NONE):
        pass


def digest(path: Path) -> str:
    hasher = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("generated"),
        help="directory to recreate and write generated .bag fixtures into",
    )
    args = parser.parse_args()

    if args.output.exists():
        shutil.rmtree(args.output)
    args.output.mkdir(parents=True)

    fixtures = [
        ("noetic-multitopic-none.bag", rosbag.Compression.NONE, write_multitopic_bag),
        ("noetic-multitopic-bz2.bag", rosbag.Compression.BZ2, write_multitopic_bag),
        ("noetic-multitopic-lz4.bag", rosbag.Compression.LZ4, write_multitopic_bag),
    ]
    for filename, compression, writer in fixtures:
        path = args.output / filename
        writer(path, compression)
        print(f"{path}: sha256={digest(path)}")

    empty_path = args.output / "noetic-empty.bag"
    write_empty_bag(empty_path)
    print(f"{empty_path}: sha256={digest(empty_path)}")


if __name__ == "__main__":
    main()
