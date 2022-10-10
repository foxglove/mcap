from io import BytesIO

from mcap_ros2.cdr import CdrReader, CdrWriter


tf2_msg__TFMessage = (
    "0001000001000000cce0d158f08cf9060a000000626173655f6c696e6b00000006000000"
    "7261646172000000ae47e17a14ae0e400000000000000000000000000000000000000000"
    "0000000000000000000000000000000000000000000000000000f03f"
)


def test_serialize():
    buffer = BytesIO()
    writer = CdrWriter(buffer)
    write_example_message(writer)
    assert writer.offset == 100
    assert buffer.getvalue() == bytes.fromhex(tf2_msg__TFMessage)


def test_alignment():
    buffer = BytesIO()
    writer = CdrWriter(buffer)
    assert bytes.hex(buffer.getvalue()) == "00010000"
    writer.write_uint64(1)
    assert bytes.hex(buffer.getvalue()) == "00010000" "0100000000000000"
    writer.write_uint8(2)
    assert bytes.hex(buffer.getvalue()) == "00010000" "0100000000000000" "02"
    writer.write_uint16(3)
    assert (
        bytes.hex(buffer.getvalue()) == "00010000" "0100000000000000" "02" "00" "0300"
    )
    writer.write_uint32(4)
    assert (
        bytes.hex(buffer.getvalue()) == "00010000"
        "0100000000000000"
        "02"
        "00"
        "0300"
        "04000000"
    )
    writer.write_uint8(5)
    writer.write_uint64(6)
    assert (
        bytes.hex(buffer.getvalue()) == "00010000"
        "0100000000000000"
        "02"
        "00"
        "0300"
        "04000000"
        "05"
        "00000000000000"
        "0600000000000000"
    )


def test_roundtrip():
    buffer = BytesIO()
    writer = CdrWriter(buffer)
    writer.write_int8(-1)
    writer.write_uint8(2)
    writer.write_int16(-300)
    writer.write_uint16(400)
    writer.write_int32(-500_000)
    writer.write_uint32(600_000)
    writer.write_int64(-7_000_000_001)
    writer.write_uint64(8_000_000_003)
    writer.write_uint16BE(0x1234)
    writer.write_uint32BE(0x12345678)
    writer.write_uint64BE(0x123456789ABCDEF0)
    writer.write_float32(-9.140000343322754)
    writer.write_float64(1.7976931348623158e100)
    writer.write_string("abc")
    writer.write_uint32(42)
    data = buffer.getvalue()
    assert len(data) == 80

    reader = CdrReader(data)
    assert reader.int8() == -1
    assert reader.uint8() == 2
    assert reader.int16() == -300
    assert reader.uint16() == 400
    assert reader.int32() == -500_000
    assert reader.uint32() == 600_000
    assert reader.int64() == -7_000_000_001
    assert reader.uint64() == 8_000_000_003
    assert reader.uint16BE() == 0x1234
    assert reader.uint32BE() == 0x12345678
    assert reader.uint64BE() == 0x123456789ABCDEF0
    assert reader.float32() == -9.140000343322754
    assert reader.float64() == 1.7976931348623158e100
    assert reader.string() == "abc"
    assert reader.uint32() == 42


def write_example_message(writer: CdrWriter):
    # geometry_msgs/TransformStamped[] transforms
    writer.write_uint32(1)
    # std_msgs/Header header
    writer.write_uint32(1490149580)  # uint32 sec
    writer.write_uint32(117017840)  # uint32 nsec
    writer.write_string("base_link")  # string frame_id
    writer.write_string("radar")  # string child_frame_id
    # geometry_msgs/Transform transform
    writer.write_float64(3.835)  # float64 x
    writer.write_float64(0)  # float64 y
    writer.write_float64(0)  # float64 z
    writer.write_float64(0)  # float64 x
    writer.write_float64(0)  # float64 y
    writer.write_float64(0)  # float64 z
    writer.write_float64(1)  # float64 w
