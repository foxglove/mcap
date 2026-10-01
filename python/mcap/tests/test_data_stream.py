import zlib
from io import BytesIO

import pytest

from mcap.data_stream import ReadDataStream
from mcap.exceptions import EndOfFile


@pytest.mark.parametrize("data", [b"", b"x"])
def test_read_rejects_short_data(data: bytes):
    stream = ReadDataStream(BytesIO(data), calculate_crc=True)
    with pytest.raises(EndOfFile):
        stream.read(2)
    assert stream.count == len(data)
    assert stream.checksum() == zlib.crc32(data)


def test_read_rejects_negative_length():
    stream = ReadDataStream(BytesIO(b"data"))
    with pytest.raises(ValueError, match="non-negative"):
        stream.read(-1)
    assert stream.count == 0
    assert stream.read(4) == b"data"


def test_read_exact_and_zero_lengths():
    stream = ReadDataStream(BytesIO(b"data"), calculate_crc=True)
    assert stream.read(0) == b""
    assert stream.count == 0
    assert stream.checksum() == 0
    assert stream.read(4) == b"data"
    assert stream.count == 4
    assert stream.checksum() == zlib.crc32(b"data")
    assert stream.read(0) == b""
