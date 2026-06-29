from array import array
from collections.abc import Sequence
from io import BytesIO

import pytest
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


class FloatList(Sequence):
    """A minimal Sequence implementation for testing array-like support."""

    def __init__(self, data):
        self._data = list(data)

    def __getitem__(self, index):
        return self._data[index]

    def __len__(self):
        return len(self._data)


def _write_and_read_float64_array(data, msgdef="float64[] data"):
    output = BytesIO()
    ros_writer = Ros2Writer(output=output)
    schema = ros_writer.register_msgdef("test_msgs/Floats", msgdef)
    ros_writer.write_message(
        topic="/test",
        schema=schema,
        message={"data": data},
        log_time=0,
        publish_time=0,
        sequence=0,
    )
    ros_writer.finish()
    output.seek(0)
    for msg in read_ros2_messages(output):
        return msg.decoded_message.data


def test_write_float64_array_with_sequence():
    values = FloatList([1.0, 2.0, 3.0])
    result = _write_and_read_float64_array(values)
    assert list(result) == [1.0, 2.0, 3.0]


def test_write_float64_array_with_list():
    result = _write_and_read_float64_array([1.0, 2.0, 3.0])
    assert list(result) == [1.0, 2.0, 3.0]


def test_write_float64_array_with_tuple():
    result = _write_and_read_float64_array((1.0, 2.0, 3.0))
    assert list(result) == [1.0, 2.0, 3.0]


def test_write_float64_array_sequence_matches_list():
    list_result = _write_and_read_float64_array([1.0, 2.0, 3.0])
    seq_result = _write_and_read_float64_array(FloatList([1.0, 2.0, 3.0]))
    assert list(list_result) == list(seq_result)


def test_write_float64_array_rejects_string():
    with pytest.raises(ValueError, match="is not an array"):
        _write_and_read_float64_array("not an array")


def test_write_float64_array_rejects_int():
    with pytest.raises(ValueError, match="is not an array"):
        _write_and_read_float64_array(42)


def test_write_float64_array_rejects_dict():
    with pytest.raises(ValueError, match="is not an array"):
        _write_and_read_float64_array({"a": 1.0})


def test_write_fixed_size_float64_array_with_sequence():
    values = FloatList([1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0])
    result = _write_and_read_float64_array(values, msgdef="float64[9] data")
    assert list(result) == [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0]


def test_write_numpy_float64_array():
    np = pytest.importorskip("numpy")
    values = np.array([1.0, 2.0, 3.0], dtype=np.float64)
    result = _write_and_read_float64_array(values)
    assert list(result) == [1.0, 2.0, 3.0]


def test_write_numpy_fixed_size_float64_array():
    np = pytest.importorskip("numpy")
    values = np.array([1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0], dtype=np.float64)
    result = _write_and_read_float64_array(values, msgdef="float64[9] data")
    assert list(result) == [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0]


def test_write_numpy_float64_array_matches_list():
    np = pytest.importorskip("numpy")
    list_result = _write_and_read_float64_array([1.0, 2.0, 3.0])
    np_result = _write_and_read_float64_array(
        np.array([1.0, 2.0, 3.0], dtype=np.float64)
    )
    assert list(list_result) == list(np_result)
