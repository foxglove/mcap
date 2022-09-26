# Reading and writing ROS 1

To start writing Python code that reads and writes ROS 1 data in MCAP, install the [`mcap-ros1-support` helper library](https://github.com/foxglove/mcap/tree/main/python/mcap-ros1-support).

## Reading

Read ROS1 messages from an MCAP file using the `mcap_ros1.reader` module:

```python
import sys

from mcap_ros1.reader import read_ros1_messages

for msg in read_ros1_messages(sys.argv[1]):
    print(f"{msg.topic}: {msg.ros_msg}")
```

## Writing

Import the necessary packages from Python and a `Writer` from [`mcap-ros1-support`](https://github.com/foxglove/mcap/tree/main/python/mcap-ros1-support):

```python
import sys
from std_msgs.msg import String
from mcap_ros1.writer import Writer
```

Open a file and create a writer:

```python
with open(sys.argv[1], "wb") as f:
    ros_writer = Writer(f)
```

Finally, specify the channel ("/chatter") and its schema (`String`) you will be using before publishing your messages ("string message 0", "string message 1", "string message 2", etc.):

```python
    for i in range(0, 10):
        ros_writer.write_message("/chatter", String(data=f"string message {i}"))
    ros_writer.finish()
```

## Important links

- [MCAP Python library](https://github.com/foxglove/mcap/tree/main/python/mcap)
- [`mcap-ros1-support` helper library](https://github.com/foxglove/mcap/tree/main/python/mcap-ros1-support)
- [Example code](https://github.com/foxglove/mcap/tree/main/python/examples/ros1)
