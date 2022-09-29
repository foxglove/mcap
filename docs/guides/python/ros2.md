---
description: Read MCAP files with ROS 2 message data in Python.
---
# Reading ROS 2

To start writing Python code that reads ROS 2 data from MCAP, install the [`rosbag2_py` package](https://index.ros.org/p/rosbag2_py/).

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

## Important links

- [MCAP Python library](https://github.com/foxglove/mcap/tree/main/python/mcap)
- [Example code](https://github.com/foxglove/mcap/tree/main/python/examples/ros2)
