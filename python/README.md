# MCAP Python Library

This library provides classes for reading and writing the MCAP file format.

## Reader Example

```python
from mcap.mcap0.reader import make_reader

with open("example.mcap", "rb") as f:
    reader = make_reader(f)
    for schema, channel, message in reader.iter_messages(topics=["/diagnostics"]):
        print(f"{channel.topic} ({schema.name}): {message.data}")
```

## Writer Example

```python
from time import time_ns
import json
from mcap.mcap0.writer import Writer

stream = open("example.mcap", "wb")
writer = Writer(stream)

# The library argument helps identify what tool wrote the file.
writer.start(profile="x-custom", library="my-writer-v1")

schema_id = writer.register_schema(
    name="sample",
    encoding="jsonschema",
    data=json.dumps({
        "type": "object",
        "properties": {
            "sample": {
                "type": "string",
            }
        }
    }).encode()
)

channel_id = writer.register_channel(
    schema_id=schema_id,
    topic="sample_topic",
    message_encoding="json",
)

writer.add_message(
    channel_id=channel_id,
    log_time=time_ns(),
    data=json.dumps({"sample": "test"}).encode('utf-8'),
    publish_time=time_ns(),
)

writer.finish()
stream.close()
```
