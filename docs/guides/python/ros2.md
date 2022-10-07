---
description: Read and write MCAP files with ROS 2 message data in Python.
---

# Reading and writing ROS 2

To start writing Python code that reads ROS 2 data from MCAP, You will need a supported ROS2 distro installed, along with the `rosbag2` and `rosbag2_storage_mcap` packages. To get set up, make sure you've followed the [ROS2 installation guide](https://docs.ros.org/en/humble/Installation.html), then:

```bash
$ sudo apt-get install ros-$ROS_DISTRO-rosbag2 ros-$ROS_DISTRO-rosbag2-storage-mcap
```

## Reading

We’ll start with our imports:

```python
import argparse
from rclpy.serialization import deserialize_message
from rosidl_runtime_py.utilities import get_message
import rosbag2_py
```

Next, we'll write a function for reading our ROS 2 messages from an MCAP file:

```python
def read_messages(input_bag: str):
    reader = rosbag2_py.SequentialReader()
    reader.open(
        rosbag2_py.StorageOptions(uri=input_bag, storage_id="mcap"),
        rosbag2_py.ConverterOptions(
            input_serialization_format="cdr", output_serialization_format="cdr"
        ),
    )

    topic_types = reader.get_all_topics_and_types()

    def typename(topic_name):
        for topic_type in topic_types:
            if topic_type.name == topic_name:
                return topic_type.type
        raise ValueError(f"topic {topic_name} not in bag")

    while reader.has_next():
        topic, data, timestamp = reader.read_next()
        msg_type = get_message(typename(topic))
        msg = deserialize_message(data, msg_type)
        yield topic, msg, timestamp
    del reader
```

Finally, we'll print out each message with its topic, data type, timestamp, and contents:

```python
def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "input", help="input bag path (folder or filepath) to read from"
    )

    args = parser.parse_args()
    for topic, msg, timestamp in read_messages(args.input):
        print(f"{topic} ({type(msg).__name__}) [{timestamp}]: '{msg.data}'")

if __name__ == "__main__":
    main()
```

## Writing

Writing MCAP using the `rosbag2_py` API is simple, starting with some imports:

```python
from rclpy.serialization import serialize_message
from std_msgs.msg import String
import rosbag2_py
```

Then we'll open a new bag using the `mcap` storage plugin:

```python
writer = rosbag2_py.SequentialWriter()
writer.open(
    rosbag2_py.StorageOptions(uri="output.mcap", storage_id="mcap"),
    rosbag2_py.ConverterOptions(
        input_serialization_format="cdr", output_serialization_format="cdr"
    ),
)
```

Now we can create a topic and add some messages to it:

```python
writer.create_topic(
    rosbag2_py.TopicMetadata(
        name="/chatter", type="std_msgs/msg/String", serialization_format="cdr"
    )
)

start_time = 0
for i in range(10):
    msg = String()
    msg.data = f"Chatter #{i}"
    timestamp = start_time + (i * 100)
    writer.write("/chatter", serialize_message(msg), timestamp)
```

Finally, we delete the writer to close the MCAP:

```python
del writer
```

## Important links

- [`rosbag2` source repository](https://github.com/ros2/rosbag2)
- [MCAP Storage Plugin](https://github.com/ros-tooling/rosbag2_storage_mcap)
- [Example code](https://github.com/foxglove/mcap/tree/main/python/examples/ros2)
