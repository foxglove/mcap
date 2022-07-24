from typing import IO, Any

from mcap_protobuf.writer import Writer

from .complex_message_pb2 import ComplexMessage
from .simple_message_pb2 import SimpleMessage


def generate_sample_data(output: IO[Any]):
    with Writer(output) as writer:
        for i in range(1, 11):
            simple_message = SimpleMessage(data=f"Hello MCAP protobuf world #{i}!")
            writer.write_message(
                topic="/simple_message",
                message=simple_message,
                log_time=i * 1000,
                publish_time=i * 1000,
            )
            complex_message = ComplexMessage(
                fieldA=f"Field A {i}", fieldB=f"Field B {i}"
            )
            writer.write_message(
                topic="/complex_message",
                message=complex_message,
                log_time=i * 1000,
                publish_time=i * 1000,
            )
    output.seek(0)
