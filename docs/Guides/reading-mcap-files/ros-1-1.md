# ROS 1

To start writing Python code that reads and writes ROS 1 data in MCAP, install the [`mcap-ros1-support` helper library](https://github.com/foxglove/mcap/tree/main/python/mcap-ros1-support).

## Reading ROS 1 from MCAP

### As a stream

Import a `StreamReader` from the [MCAP Python library](https://github.com/foxglove/mcap/tree/main/python/mcap) and a `Decoder` from [`mcap-ros1-support`](https://github.com/foxglove/mcap/tree/main/python/mcap-ros1-support):

```python
from mcap.mcap0.stream_reader import StreamReader
from mcap_ros1.decoder import Decoder
```

Create a reader for your MCAP file (`my_data.mcap`), and decode its data stream:

```python
reader = StreamReader("my_data.mcap")
data = Decoder(reader)
```

Finally, print out each message:

```
for topic, record, message in data.messages:
    print(message)
```

### As raw data

Import the `Decoder` from [`mcap-ros1-support`](https://github.com/foxglove/mcap/tree/main/python/mcap-ros1-support):

```python
from mcap_ros1.decoder import Decoder
```

Read the contents of your MCAP file (`my_data.mcap`):

```python
data = open("my_data.mcap", "rb").read()
```

Finally, iterate through the messages and print out each one's datatype and contents:

```python
for topic, record, message in Decoder(data).messages:
    print(f"{topic}: ({type(message).__name__}): {message.data}")
```

## Writing ROS 1 to MCAP

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

### Important links

- [MCAP Python library](https://github.com/foxglove/mcap/tree/main/python/mcap)
- [`mcap-ros1-support` helper library](https://github.com/foxglove/mcap/tree/main/python/mcap-ros1-support)
- [Example code](https://github.com/foxglove/mcap/tree/main/python/examples/ros1)
