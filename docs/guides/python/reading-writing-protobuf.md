---
title: "Protobuf"
slug: "protobuf-1"
hidden: false
createdAt: "2022-07-21T19:43:35.144Z"
updatedAt: "2022-07-26T23:54:57.543Z"
---
To start writing Python code that reads and writes Protobuf data in MCAP, install the [`mcap-protobuf-support` helper library](https://github.com/foxglove/mcap/tree/main/python/mcap-protobuf-support).

## Reading Protobuf from MCAP

To read in Protobuf data from an MCAP file (`my_data.mcap`) as a **stream**:

```python
import sys

from mcap.mcap0.stream_reader import StreamReader
from mcap_protobuf.decoder import Decoder

def main():
    reader = StreamReader(sys.argv[1])
    decoder = Decoder(reader)
    for topic, message in decoder.messages:
        print(f"{topic}: {message}")


if __name__ == "__main__":
    main()
```

## Writing Protobuf to MCAP

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

### Important links
- [MCAP Python library](https://github.com/foxglove/mcap/tree/main/python/mcap)
- [`mcap-protobuf-support` helper library](https://github.com/foxglove/mcap/tree/main/python/mcap-protobuf-support) 
- [Example code](https://github.com/foxglove/mcap/tree/main/python/examples/protobuf)