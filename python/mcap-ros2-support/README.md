# Python MCAP ROS2 support

This package provides ROS2 support for the Python MCAP file format reader.

## Installation

You can install directly via pip.

```bash
pip install mcap-ros2-support
```

## Reading ROS2 Messages

```python
# Reading from a MCAP file
from mcap_ros2.reader import read_ros2_messages

for msg in read_ros2_messages("my_data.mcap"):
    print(f"{msg.topic}: f{msg.ros_msg}")
```

## Stay in touch

Join our [Slack channel](https://foxglove.dev/join-slack) to ask questions,
share feedback, and stay up to date on what our team is working on.
