import struct
import zlib
from typing import IO, Optional, Union

from .exceptions import EndOfFile
from .opcode import Opcode

_U8 = struct.Struct("<B")
_U16 = struct.Struct("<H")
_U32 = struct.Struct("<I")
_U64 = struct.Struct("<Q")
_RECORD_PREFIX = struct.Struct("<BQ")


class ReadDataStream:
    def __init__(self, stream: IO[bytes], calculate_crc: bool = False):
        self._count = 0
        self._stream = stream
        self._crc: Optional[int] = None
        if calculate_crc:
            self._crc = 0

    @property
    def count(self) -> int:
        return self._count

    def read(self, length: int) -> bytes:
        if length == 0:
            return b""

        data = self._stream.read(length)
        self._count += len(data)
        if self._crc is not None:
            self._crc = zlib.crc32(data, self._crc)
        if data == b"":
            raise EndOfFile()
        return data

    def checksum(self) -> int:
        if self._crc is not None:
            return self._crc
        else:
            raise RuntimeError("requested checksum where calculate_crc == false")

    def read1(self) -> int:
        return _U8.unpack(self.read(1))[0]

    def read2(self) -> int:
        return _U16.unpack(self.read(2))[0]

    def read4(self) -> int:
        return _U32.unpack(self.read(4))[0]

    def read8(self) -> int:
        return _U64.unpack(self.read(8))[0]

    def read_prefixed_string(self) -> str:
        length = self.read4()
        return str(self.read(length), "utf-8")


class RecordBuilder:
    """Accumulates serialized records in a bytearray: appends are amortized O(1) and
    a record's length is patched in with a slice assignment, with no seek/tell.
    """

    def __init__(self) -> None:
        self.buffer = bytearray()
        self._record_start_offset = 0

    @property
    def count(self) -> int:
        return len(self.buffer)

    def start_record(self, opcode: Opcode):
        self._record_start_offset = len(self.buffer)
        self.buffer += _RECORD_PREFIX.pack(opcode, 0)  # placeholder size

    def finish_record(self):
        start = self._record_start_offset
        length = len(self.buffer) - start - 9
        self.buffer[start + 1 : start + 9] = _U64.pack(length)

    def end(self) -> bytearray:
        """Returns the buffer itself, not a copy, and starts a new one."""
        buf = self.buffer
        self.buffer = bytearray()
        return buf

    def write(self, data: Union[bytes, bytearray, memoryview]):
        self.buffer += data

    def write_prefixed_string(self, value: str):
        bytes = value.encode()
        self.write4(len(bytes))
        self.buffer += bytes

    def write1(self, value: int):
        self.buffer += _U8.pack(value)

    def write2(self, value: int):
        self.buffer += _U16.pack(value)

    def write4(self, value: int):
        self.buffer += _U32.pack(value)

    def write8(self, value: int):
        self.buffer += _U64.pack(value)
