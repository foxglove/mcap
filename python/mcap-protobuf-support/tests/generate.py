import os
import sys

from google.protobuf.descriptor_pb2 import FileDescriptorSet

from mcap.writer import Writer as McapWriter

sys.path.append(os.path.dirname(__file__))  # for test_proto imports

from typing import IO, Any, Set  # noqa: #402

from mcap_protobuf.writer import Writer  # noqa: #402
from test_proto.complex_message_pb2 import ComplexMessage  # noqa: #402
from test_proto.intermediate_message_1_pb2 import IntermediateMessage1  # noqa: #402
from test_proto.intermediate_message_2_pb2 import IntermediateMessage2  # noqa: #402
from test_proto.simple_message_pb2 import SimpleMessage  # noqa: #402


def register_schema(writer: McapWriter, message_class: Any):
    file_descriptor_set = build_file_descriptor_set(message_class=message_class)

    return writer.register_schema(
        name=message_class.DESCRIPTOR.full_name,
        encoding="protobuf",
        data=file_descriptor_set.SerializeToString(),
    )


def build_file_descriptor_set(message_class: Any) -> FileDescriptorSet:
    file_descriptor_set = FileDescriptorSet()
    seen_dependencies: Set[str] = set()
    toplevel = message_class.DESCRIPTOR.file
    to_add = {toplevel.name: toplevel}
    while to_add:
        fd = to_add.popitem()[1]
        seen_dependencies.add(fd.name)
        fd.CopyToProto(file_descriptor_set.file.add())
        for dep in fd.dependencies:
            if dep.name not in seen_dependencies:
                to_add[dep.name] = dep

    return file_descriptor_set


def generate_sample_data_with_disordered_proto_fds(output: IO[Any]):
    writer = McapWriter(output)
    writer.start()
    schema_id = register_schema(writer, ComplexMessage)
    channel_id = writer.register_channel("/complex_msgs", "protobuf", schema_id)
    writer.add_message(
        channel_id,
        0,
        ComplexMessage(
            intermediate1=IntermediateMessage1(simple=SimpleMessage(data="a")),
            intermediate2=IntermediateMessage2(simple=SimpleMessage(data="b")),
        ).SerializeToString(),
        0,
        0,
    )
    writer.finish()
    output.seek(0)


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
