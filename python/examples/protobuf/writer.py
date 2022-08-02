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
