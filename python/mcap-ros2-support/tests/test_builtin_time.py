import struct
from io import BytesIO

import pytest
from mcap_ros2._dynamic import generate_dynamic, serialize_dynamic
from mcap_ros2.decoder import DecoderFactory
from mcap_ros2.writer import Writer as Ros2Writer

from mcap.reader import make_reader
from mcap.writer import CompressionType

SCHEMA_NAME = "test_msgs/Stamped"
TIME_VALUES = [
    (-(2**31), 0),
    (-2, 300_000_000),
    (-1, 999_999_999),
    (0, 0),
    (1, 700_000_000),
    (2**31 - 1, 999_999_999),
]


@pytest.mark.parametrize("builtin", ["Time", "Duration"])
@pytest.mark.parametrize("sec,nanosec", TIME_VALUES)
@pytest.mark.parametrize("little_endian", [True, False])
def test_decode_builtin_signed_seconds(builtin, sec, nanosec, little_endian):
    # ROS 2 defines both builtin sec fields as int32, and nanosec as uint32.
    header = b"\x00\x01\x00\x00" if little_endian else b"\x00\x00\x00\x00"
    payload = header + struct.pack("<iI" if little_endian else ">iI", sec, nanosec)
    decoder = generate_dynamic(SCHEMA_NAME, f"builtin_interfaces/{builtin} stamp")[
        SCHEMA_NAME
    ]
    msg = decoder(payload)
    assert (msg.stamp.sec, msg.stamp.nanosec) == (sec, nanosec)


@pytest.mark.parametrize("builtin", ["Time", "Duration"])
@pytest.mark.parametrize("sec,nanosec", TIME_VALUES)
def test_encode_builtin_signed_seconds(builtin, sec, nanosec):
    encoder = serialize_dynamic(SCHEMA_NAME, f"builtin_interfaces/{builtin} stamp")[
        SCHEMA_NAME
    ]
    payload = encoder({"stamp": {"sec": sec, "nanosec": nanosec}})
    assert payload == b"\x00\x01\x00\x00" + struct.pack("<iI", sec, nanosec)


@pytest.mark.parametrize("builtin", ["Time", "Duration"])
@pytest.mark.parametrize("sec,nanosec", [(-2, 300_000_000), (1, 700_000_000)])
@pytest.mark.parametrize(
    "compression", [CompressionType.NONE, CompressionType.LZ4, CompressionType.ZSTD]
)
def test_builtin_signed_seconds_mcap_roundtrip(builtin, sec, nanosec, compression):
    output = BytesIO()
    writer = Ros2Writer(output, compression=compression)
    schema = writer.register_msgdef(SCHEMA_NAME, f"builtin_interfaces/{builtin} stamp")
    writer.write_message(
        topic="/timed",
        schema=schema,
        message={"stamp": {"sec": sec, "nanosec": nanosec}},
        log_time=10,
        publish_time=10,
    )
    writer.finish()
    output.seek(0)
    reader = make_reader(output, decoder_factories=[DecoderFactory()])
    messages = list(reader.iter_decoded_messages())
    assert len(messages) == 1
    _, channel, message, decoded = messages[0]
    assert channel.topic == "/timed"
    assert message.data == b"\x00\x01\x00\x00" + struct.pack("<iI", sec, nanosec)
    assert (decoded.stamp.sec, decoded.stamp.nanosec) == (sec, nanosec)


@pytest.mark.parametrize("builtin", ["Time", "Duration"])
def test_explicit_builtin_definition(builtin):
    # Explicit concatenated definitions should retain their existing behavior.
    schema = (
        f"builtin_interfaces/{builtin} stamp\n===\n"
        f"MSG: builtin_interfaces/{builtin}\nint32 sec\nuint32 nanosec"
    )
    payload = b"\x00\x01\x00\x00" + struct.pack("<iI", -2, 300_000_000)
    decoder = generate_dynamic(SCHEMA_NAME, schema)[SCHEMA_NAME]
    encoder = serialize_dynamic(SCHEMA_NAME, schema)[SCHEMA_NAME]
    assert encoder({"stamp": {"sec": -2, "nanosec": 300_000_000}}) == payload
    decoded = decoder(payload)
    assert (decoded.stamp.sec, decoded.stamp.nanosec) == (-2, 300_000_000)
