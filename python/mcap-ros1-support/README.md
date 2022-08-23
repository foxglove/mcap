# Python MCAP Ros1 support

This package provides ROS1 support for the Python MCAP file format reader &amp;
writer.

## Installation

You can install directly via pip. This also requires the rosbag package from the
ROS package index.

```bash
pip install --index-url https://rospypi.github.io/simple genpy
pip install mcap-ros1-support
```

Or you can install via [Pipenv](https://pipenv.pypa.io/en/latest/) and a
Pipfile. This requires specifying the source for the rosbag package like this:

```
[[source]]
url = "https://pypi.org/simple"
verify_ssl = true
name = "pypi"

[[source]]
url = "https://rospypi.github.io/simple"
verify_ssl = true
name = "ros"

[packages]
mcap-ros1-support = "*"
genpy = "*"
```

## Reading ROS1 Messages

```python
# Reading from a MCAP file
from mcap_ros1.reader import read_ros1_messages

for msg in read_ros1_messages("my_data.mcap"):
    print(f"{msg.topic}: f{msg.ros_msg}")
```

## Writing ROS1 Messages

```python
from mcap_ros1.writer import Writer as Ros1Writer
from std_msgs.msg import String

output = open("example.mcap", "w+b")
ros_writer = Ros1Writer(output=output)
for i in range(0, 10):
    ros_writer.write_message("chatter", String(data=f"string message {i}"))
ros_writer.finish()
```

## Stay in touch

Join our [Slack channel](https://foxglove.dev/join-slack) to ask questions,
share feedback, and stay up to date on what our team is working on.
