---
description: Read and write MCAP files with Protobuf-encoded messages in Python.
---

# Reading and writing Protobuf

To start writing Python code that reads and writes Protobuf data in MCAP, install the [`mcap-protobuf-support` helper library](https://github.com/foxglove/mcap/tree/main/python/mcap-protobuf-support).

## Reading

To read in Protobuf data from an MCAP file (`my_data.mcap`), use the high-level `reader` interface.

```python
import sys

from mcap_protobuf.reader import read_protobuf_messages

def main():
    for msg in read_protobuf_messages("my_data.mcap"):
        print(f"{msg.topic}: {msg.proto_msg}")

if __name__ == "__main__":
    main()
```

## Writing

```python
import sys
from mcap_protobuf.writer import Writer

from complex_message_pb2 import ComplexMessage
from simple_message_pb2 import SimpleMessage


def main():
    with open(sys.argv[1], "wb") as f, Writer(f) as mcap_writer:
        for i in range(1, 11):
            mcap_writer.write_message(
                topic="/simple_messages",
                message=SimpleMessage(data=f"Hello MCAP protobuf world #{i}!"),
                log_time=i * 1000,
                publish_time=i * 1000,
            )
            complex_message = ComplexMessage(
                fieldA=f"Field A {i}", fieldB=f"Field B {i}"
            )
            mcap_writer.write_message(
                topic="/complex_messages",
                message=complex_message,
                log_time=i * 1000,
                publish_time=i * 1000,
            )


if __name__ == "__main__":
    main()
```

## Important links

- [MCAP Python library](https://github.com/foxglove/mcap/tree/main/python/mcap)
- [`mcap-protobuf-support` helper library](https://github.com/foxglove/mcap/tree/main/python/mcap-protobuf-support)
- [Example code](https://github.com/foxglove/mcap/tree/main/python/examples/protobuf)
