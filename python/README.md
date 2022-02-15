# MCAP Python Library

This library provides classes for reading and writing the MCAP file format.

## Reader Example

```
from mcap.mcap0.stream_reader import StreamReader

stream = open("example.mcap", "rb")
reader = StreamReader(stream)
for record in reader.records:
    print(record)
```

## Writer Example

```
from time import time_ns
from mcap.mcap0.writer import Writer

stream = open("example.mcap", "wb")
writer = Writer(stream)
writer.start("ros1", "example")
schema_id = writer.register_schema(
    "example", "text/plain", data="example schema".encode()
)
channel_id = writer.register_channel(
    schema_id=schema_id,
    topic="example_topic",
    message_encoding="text/plain",
    metadata={"first": "a"},
)
writer.add_message(
    channel_id=channel_id,
    log_time=time_ns(),
    data="example message".encode(),
    publish_time=time_ns(),
)
writer.finish()
stream.close()
```
