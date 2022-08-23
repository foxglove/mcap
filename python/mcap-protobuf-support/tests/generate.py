import os
import sys

sys.path.append(os.path.dirname(__file__))  # for test_proto imports

from typing import IO, Any  # noqa: #402

from mcap_protobuf.writer import Writer  # noqa: #402

from test_proto.complex_message_pb2 import ComplexMessage  # noqa: #402
from test_proto.intermediate_message_1_pb2 import IntermediateMessage1  # noqa: #402
from test_proto.intermediate_message_2_pb2 import IntermediateMessage2  # noqa: #402
from test_proto.simple_message_pb2 import SimpleMessage  # noqa: #402


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
                intermediate1=IntermediateMessage1(
                    simple=SimpleMessage(data=f"Field A {i}")
                ),
                intermediate2=IntermediateMessage2(
                    simple=SimpleMessage(data=f"Field B {i}")
                ),
            )
            writer.write_message(
                topic="/complex_message",
                message=complex_message,
                log_time=i * 1000,
                publish_time=i * 1000,
            )
    output.seek(0)
