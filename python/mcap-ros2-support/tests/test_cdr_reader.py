from io import BytesIO
from math import inf

from mcap_ros2.cdr import CdrReader, CdrWriter, EncapsulationKind

tf2_msg__TFMessage = (
    "0001000001000000cce0d158f08cf9060a000000626173655f6c696e6b00000006000000"
    "7261646172000000ae47e17a14ae0e400000000000000000000000000000000000000000"
    "0000000000000000000000000000000000000000000000000000f03f"
)
rcl_interfaces__ParameterEvent = (
    "00010000a9b71561a570ea01110000002f5f726f7332636c695f33373833363300000000"
    "010000000d0000007573655f73696d5f74696d6500010000000000000000000000000000"
    "000000000100000000000000000000000000000000000000000000000000000000000000"
    "00000000"
)


def test_parse_tfmessage():
    data = bytes.fromhex(tf2_msg__TFMessage)
    reader = CdrReader(data)
    assert reader.byte_length() == len(data)
    assert reader.decoded_bytes() == 4
    assert reader.kind() == EncapsulationKind.CDR_LE

    # geometry_msgs/TransformStamped[] transforms
    assert reader.sequence_length() == 1
    # std_msgs/Header header
    assert reader.uint32() == 1490149580  # uint32 sec
    assert reader.int32() == 117017840  # int32 nanosec
    assert reader.string() == "base_link"  # frame_id
    assert reader.string() == "radar"  # child_frame_id
    # geometry_msgs/Transform transform
    assert reader.float64() == 3.835  # float64 x
    assert reader.float64() == 0.0  # float64 y
    assert reader.float64() == 0.0  # float64 z
    assert reader.float64() == 0.0  # float64 x
    assert reader.float64() == 0.0  # float64 y
    assert reader.float64() == 0.0  # float64 z
    assert reader.float64() == 1.0  # float64 w

    assert reader.offset == len(data)
    assert reader.decoded_bytes() == len(data)


def test_parse_parameterevent():
    data = bytes.fromhex(rcl_interfaces__ParameterEvent)
    reader = CdrReader(data)

    # builtin_interfaces/Time stamp
    assert reader.uint32() == 1628813225  # uint32 sec
    assert reader.int32() == 32141477  # int32 nanosec
    # string node
    assert reader.string() == "/_ros2cli_378363"
    # Parameter[] new_parameters
    assert reader.sequence_length() == 1
    assert reader.string() == "use_sim_time"  # string name
    # ParameterValue value
    assert reader.uint8() == 1  # uint8 type
    assert reader.int8() == 0  # bool bool_value
    assert reader.int64() == 0  # int64 integer_value
    assert reader.float64() == 0  # float64 double_value
    assert reader.string() == ""  # string string_value

    assert reader.sequence_length() == 0
    assert reader.int8_array(0) == []  # byte[] byte_array_value
    assert reader.sequence_length() == 0
    assert reader.uint8_array(0) == bytes()  # bool[] bool_array_value
    assert reader.sequence_length() == 0
    assert reader.int64_array(0) == []  # int64[] integer_array_value
    assert reader.sequence_length() == 0
    assert reader.float64_array(0) == []  # float64[] double_array_value
    assert reader.sequence_length() == 0
    assert reader.string_array(0) == []  # string[] string_array_value

    # Parameter[] changed_parameters
    assert reader.sequence_length() == 0
    # Parameter[] deleted_parameters
    assert reader.sequence_length() == 0

    assert reader.decoded_bytes() == len(data)


def test_read_big_endian():
    data = bytes.fromhex("000100001234000056789abcdef0000000000000")
    reader = CdrReader(data)
    assert reader.uint16BE() == 0x1234
    assert reader.uint32BE() == 0x56789ABC
    assert reader.uint64BE() == 0xDEF0000000000000


def test_seeking():
    data = bytes.fromhex(tf2_msg__TFMessage)
    reader = CdrReader(data)

    reader.seek_to(4 + 4 + 4 + 4 + 4 + 10 + 4 + 6)
    assert reader.float64() == 3.835

    # This works due to aligned reads
    reader.seek_to(4 + 4 + 4 + 4 + 4 + 10 + 4 + 3)
    assert reader.float64() == 3.835

    reader.seek(-8)
    assert reader.float64() == 3.835
    assert reader.float64() == 0.0


def test_arrays():
    INT64_MAX = 9223372036854775807
    INT64_MIN = -9223372036854775808
    UINT64_MAX = 0xFFFFFFFFFFFFFFFF
    FLOAT1 = -123.19999694824219
    FLOAT2 = 123.56999969482422

    for [write_fn, read_fn, array, output] in (
        ["write_boolean_array", "boolean_array", [True, False, True], None],
        ["write_int8_array", "int8_array", [-128, -1, 0, 1, 127], None],
        ["write_uint8_array", "uint8_array", [0, 128, 255], b"\x00\x80\xff"],
        ["write_int16_array", "int16_array", [-32767, -1, 0, 1, 32767], None],
        ["write_uint16_array", "uint16_array", [0, 0xFF, 0xFFFF], None],
        ["write_int32_array", "int32_array", [-2147483648, -1, 0, 1, 2147483647], None],
        ["write_uint32_array", "uint32_array", [0, 0xFF, 0xFFFF, 0xFFFFFFFF], None],
        ["write_int64_array", "int64_array", [INT64_MIN, -1, 0, 1, INT64_MAX], None],
        ["write_uint64_array", "uint64_array", [0, UINT64_MAX], None],
        [
            "write_float32_array",
            "float32_array",
            [FLOAT1, -1.0, 0.0, 1.0, FLOAT2, inf, -inf, 42.0],
            None,
        ],
        [
            "write_float64_array",
            "float64_array",
            [FLOAT1, -1.0, 0.0, 1.0, FLOAT2, inf, -inf, 42.0],
            None,
        ],
        ["write_string_array", "string_array", ["", "a", "ab", "Test!", "↑↓"], None],
    ):
        assert isinstance(write_fn, str)
        assert isinstance(read_fn, str)
        assert isinstance(array, list)

        buffer = BytesIO()
        writer = CdrWriter(buffer)
        writer.write_uint32(len(array))
        getattr(writer, write_fn)(array)

        data = buffer.getvalue()
        reader = CdrReader(data)
        output = output or array
        assert reader.sequence_length() == len(array)
        assert getattr(reader, read_fn)(len(array)) == output
        assert reader.decoded_bytes() == len(data)
