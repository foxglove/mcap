from typing import IO, Any

from mcap.mcap0.writer import Writer as McapWriter
from mcap_protobuf.schema import register_schema

from .complex_message_pb2 import ComplexMessage
from .simple_message_pb2 import SimpleMessage


def generate_sample_data(output: IO[Any]):
    mcap_writer = McapWriter(output)
    mcap_writer.start(profile="protobuf", library="test")

    simple_schema_id = register_schema(writer=mcap_writer, message_class=SimpleMessage)

    complex_schema_id = register_schema(
        writer=mcap_writer, message_class=ComplexMessage
    )

    simple_channel_id = mcap_writer.register_channel(
        topic="/simple_message",
        message_encoding="protobuf",
        schema_id=simple_schema_id,
    )

    complex_channel_id = mcap_writer.register_channel(
        topic="/complex_message",
        message_encoding="protobuf",
        schema_id=complex_schema_id,
    )

    for i in range(1, 11):
        simple_message = SimpleMessage(data=f"Hello MCAP protobuf world #{i}!")
        mcap_writer.add_message(
            channel_id=simple_channel_id,
            log_time=i * 1000,
            data=simple_message.SerializeToString(),  # type: ignore
            publish_time=i * 1000,
        )
        complex_message = ComplexMessage(fieldA=f"Field A {i}", fieldB="Field B {i}")
        mcap_writer.add_message(
            channel_id=complex_channel_id,
            log_time=i * 1000,
            data=complex_message.SerializeToString(),  # type: ignore
            publish_time=i * 1000,
        )

    mcap_writer.finish()
    output.seek(0)
