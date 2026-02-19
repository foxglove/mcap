from array import array
from io import BytesIO

from mcap_ros2.decoder import DecoderFactory
from mcap_ros2.writer import Writer as Ros2Writer

from mcap.reader import make_reader


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
