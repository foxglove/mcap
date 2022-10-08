"""A simple example of reading ROS2 messages from an MCAP file without a ROS2 environment."""

import sys

from mcap_ros2.reader import read_ros2_messages


def main():
    """Read all ROS2 messages from the given MCAP file and print them."""
    for msg in read_ros2_messages(sys.argv[1]):
        print(
            f"{msg.channel.topic} [{msg.log_time}] ({type(msg.ros_msg).__name__}): {msg.ros_msg}"
        )


if __name__ == "__main__":
    main()
