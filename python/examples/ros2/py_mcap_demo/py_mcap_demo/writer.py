"""script that writes ROS2 messages to MCAP using the rosbag2_py api."""
import argparse

from rclpy.serialization import serialize_message
from std_msgs.msg import String

import rosbag2_py

TOPIC_NAME = "/chatter"


def write_to(output_path: str):
    writer = rosbag2_py.SequentialWriter()
    writer.open(
        rosbag2_py.StorageOptions(uri=output_path, storage_id="mcap"),
        rosbag2_py.ConverterOptions(
            input_serialization_format="cdr", output_serialization_format="cdr"
        ),
    )

    writer.create_topic(
        rosbag2_py.TopicMetadata(
            name=TOPIC_NAME, type="std_msgs/msg/String", serialization_format="cdr"
        )
    )

    start_time = 0
    for i in range(10):
        msg = String()
        msg.data = f"Chatter #{i}"
        timestamp = start_time + (i * 100)
        writer.write(TOPIC_NAME, serialize_message(msg), timestamp)

    del writer


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", help="output directory to create and write to")

    args = parser.parse_args()
    write_to(args.output)


if __name__ == "__main__":
    main()
