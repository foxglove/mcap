from array import array
from io import BytesIO

import pytest
from mcap_ros2.decoder import DecoderFactory
from mcap_ros2.writer import Writer as Ros2Writer

from mcap.reader import NonSeekingReader, make_reader
from mcap.writer import CompressionType


def read_ros2_messages(stream: BytesIO):
    reader = make_reader(stream, decoder_factories=[DecoderFactory()])
    return reader.iter_decoded_messages()


def test_write_messages():
    output = BytesIO()
    ros_writer = Ros2Writer(output=output)
    schema = ros_writer.register_msgdef("test_msgs/TestData", "string a\nint32 b")
    for i in range(0, 10):
        ros_writer.write_message(
            topic="/test",
            schema=schema,
            message={"a": f"string message {i}", "b": i},
            log_time=i,
            publish_time=i,
            sequence=i,
        )
    ros_writer.finish()

    output.seek(0)
    for index, msg in enumerate(read_ros2_messages(output)):
        assert msg.channel.topic == "/test"
        assert msg.schema.name == "test_msgs/TestData"
        assert msg.decoded_message.a == f"string message {index}"
        assert msg.decoded_message.b == index
        assert msg.message.log_time == index
        assert msg.message.publish_time == index
        assert msg.message.sequence == index


def test_write_std_msgs_empty_messages():
    output = BytesIO()
    ros_writer = Ros2Writer(output=output)
    schema = ros_writer.register_msgdef("std_msgs/msg/Empty", "")
    for i in range(0, 10):
        ros_writer.write_message(
            topic="/test",
            schema=schema,
            message={},
            log_time=i,
            publish_time=i,
            sequence=i,
        )
    ros_writer.finish()

    output.seek(0)
    for index, msg in enumerate(read_ros2_messages(output)):
        assert msg.channel.topic == "/test"
        assert msg.schema.name == "std_msgs/msg/Empty"
        assert msg.message.log_time == index
        assert msg.message.publish_time == index
        assert msg.message.sequence == index


def test_write_uint8_array_with_py_array():
    output = BytesIO()
    ros_writer = Ros2Writer(output=output)
    schema = ros_writer.register_msgdef("test_msgs/ByteArray", "uint8[] data")

    for i in range(10):
        byte_array = array("B", [i] * 5)
        ros_writer.write_message(
            topic="/image",
            schema=schema,
            message={"data": byte_array},
            log_time=i,
            publish_time=i,
            sequence=i,
        )

    ros_writer.finish()

    output.seek(0)
    for i, msg in enumerate(read_ros2_messages(output)):
        assert msg.channel.topic == "/image"
        assert msg.schema.name == "test_msgs/ByteArray"
        assert list(msg.decoded_message.data) == [i] * 5
        assert msg.message.log_time == i
        assert msg.message.publish_time == i
        assert msg.message.sequence == i


def test_decode_nested_array_reuses_message_class():
    nested_msgdef = (
        "Point[] points\n"
        "================================================================================\n"
        "MSG: test_msgs/Point\n"
        "float64 x\n"
        "float64 y"
    )

    output = BytesIO()
    ros_writer = Ros2Writer(output=output)
    schema = ros_writer.register_msgdef("test_msgs/PointCloud", nested_msgdef)
    offsets = [0, 1, 0]
    for i, offset in enumerate(offsets):
        ros_writer.write_message(
            topic="/points",
            schema=schema,
            message={
                "points": [{"x": float(j), "y": float(j + offset)} for j in range(4)]
            },
            log_time=i,
            publish_time=i,
            sequence=i,
        )
    ros_writer.finish()

    output.seek(0)
    decoded = [msg.decoded_message for msg in read_ros2_messages(output)]
    assert len(decoded) == 3

    for offset, msg in zip(offsets, decoded):
        assert [(p.x, p.y) for p in msg.points] == [
            (float(j), float(j + offset)) for j in range(4)
        ]

    # Classes are reused within arrays and across messages on the same channel.
    first = decoded[0]
    assert all(type(p) is type(first.points[0]) for p in first.points)
    assert type(decoded[1].points[0]) is type(first.points[0])
    assert type(decoded[2]) is type(first)
    # Shared classes enable structural equality.
    assert decoded[0] != decoded[1]
    assert decoded[0] == decoded[2]


def test_write_metadata():
    output = BytesIO()
    ros_writer = Ros2Writer(output=output)
    ros_writer.add_metadata("test_metadata", {"key": "value"})
    ros_writer.finish()

    output.seek(0)
    reader = make_reader(output, decoder_factories=[DecoderFactory()])
    metadata = list(reader.iter_metadata())
    assert len(metadata) == 1
    assert metadata[0].name == "test_metadata"
    assert metadata[0].metadata == {"key": "value"}


def test_write_attachment():
    output = BytesIO()
    ros_writer = Ros2Writer(output=output)
    ros_writer.add_attachment(10, 10, "test_attachment", "text/plain", b"test_data")
    ros_writer.finish()

    output.seek(0)
    reader = make_reader(output, decoder_factories=[DecoderFactory()])
    attachments = list(reader.iter_attachments())
    assert len(attachments) == 1
    assert attachments[0].name == "test_attachment"
    assert attachments[0].media_type == "text/plain"
    assert attachments[0].data == b"test_data"


def test_write_array_field_named_values():
    output = BytesIO()
    ros_writer = Ros2Writer(output=output)
    schema = ros_writer.register_msgdef("test_msgs/Pts", "float64[] values")
    ros_writer.write_message(
        topic="/test",
        schema=schema,
        message={"values": [1.0, 2.0, 3.0]},
        log_time=0,
        publish_time=0,
        sequence=0,
    )
    ros_writer.finish()

    output.seek(0)
    for msg in read_ros2_messages(output):
        assert list(msg.decoded_message.values) == [1.0, 2.0, 3.0]


def test_write_array_field_named_items():
    output = BytesIO()
    ros_writer = Ros2Writer(output=output)
    schema = ros_writer.register_msgdef("test_msgs/Pts", "int32[] items")
    ros_writer.write_message(
        topic="/test",
        schema=schema,
        message={"items": [10, 20, 30]},
        log_time=0,
        publish_time=0,
        sequence=0,
    )
    ros_writer.finish()

    output.seek(0)
    for msg in read_ros2_messages(output):
        assert list(msg.decoded_message.items) == [10, 20, 30]


@pytest.mark.parametrize("compression", [CompressionType.NONE, CompressionType.ZSTD])
@pytest.mark.parametrize(
    "second_name,second_definition,second_value",
    [
        ("test_msgs/OtherData", "string data", "new message type"),
        ("test_msgs/TestData", "string data", "new definition"),
        ("test_msgs/TestData", "int32 data", 20),
    ],
)
def test_write_multiple_schemas_on_same_topic(
    compression, second_name, second_definition, second_value
):
    output = BytesIO()
    ros_writer = Ros2Writer(output, chunk_size=1, compression=compression)
    first_schema = ros_writer.register_msgdef("test_msgs/TestData", "int32 data")
    ros_writer.write_message("/test", first_schema, {"data": 10}, log_time=0)
    second_schema = ros_writer.register_msgdef(second_name, second_definition)
    ros_writer.write_message("/test", second_schema, {"data": second_value}, log_time=1)
    ros_writer.write_message("/test", first_schema, {"data": 30}, log_time=2)
    ros_writer.finish()

    expected_schemas = [first_schema, second_schema, first_schema]
    expected_values = [10, second_value, 30]
    for reader in (
        make_reader(BytesIO(output.getvalue()), decoder_factories=[DecoderFactory()]),
        NonSeekingReader(
            BytesIO(output.getvalue()), decoder_factories=[DecoderFactory()]
        ),
    ):
        messages = list(reader.iter_decoded_messages())
        assert len(messages) == 3
        for index, (message, schema, value) in enumerate(
            zip(messages, expected_schemas, expected_values)
        ):
            assert message.channel.topic == "/test"
            assert message.channel.schema_id == schema.id
            assert message.schema == schema
            assert message.decoded_message.data == value
            assert message.message.log_time == message.message.publish_time == index
        assert messages[0].channel.id == messages[2].channel.id
        assert (messages[0].channel.id == messages[1].channel.id) == (
            first_schema.id == second_schema.id
        )

    summary = make_reader(BytesIO(output.getvalue())).get_summary()
    assert summary is not None
    assert len(summary.channels) == len({first_schema.id, second_schema.id})


def test_write_same_schema_on_multiple_topics_reuses_each_channel():
    output = BytesIO()
    ros_writer = Ros2Writer(output)
    schema = ros_writer.register_msgdef("test_msgs/TestData", "int32 data")
    topics = ["/first", "/second", "/first", "/second"]
    for index, topic in enumerate(topics):
        ros_writer.write_message(topic, schema, {"data": index}, log_time=index)
    ros_writer.finish()

    output.seek(0)
    messages = list(read_ros2_messages(output))
    assert [message.channel.topic for message in messages] == topics
    assert [message.decoded_message.data for message in messages] == [0, 1, 2, 3]
    assert all(message.schema == schema for message in messages)
    assert messages[0].channel.id == messages[2].channel.id
    assert messages[1].channel.id == messages[3].channel.id
    assert messages[0].channel.id != messages[1].channel.id
