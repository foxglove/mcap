import struct
import zlib
from io import BytesIO
from typing import IO, Optional

from .exceptions import EndOfFile
from .opcode import Opcode


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
        [value] = struct.unpack("<B", self.read(1))
        return value

    def read2(self) -> int:
        [value] = struct.unpack("<H", self.read(2))
        return value

    def read4(self) -> int:
        [value] = struct.unpack("<I", self.read(4))
        return value

    def read8(self) -> int:
        [value] = struct.unpack("<Q", self.read(8))
        return value

    def read_prefixed_string(self) -> str:
        length = self.read4()
        return str(self.read(length), "utf-8")


class RecordBuilder:
    def __init__(self) -> None:
        self._buffer = BytesIO()

    @property
    def count(self) -> int:
        return self._buffer.tell()

    def start_record(self, opcode: Opcode):
        self._record_start_offset = self._buffer.tell()
        self._buffer.write(struct.pack("<BQ", opcode, 0))  # placeholder size

    def finish_record(self):
        pos = self._buffer.tell()
        length = pos - self._record_start_offset - 9
        self._buffer.seek(self._record_start_offset + 1)
        self._buffer.write(struct.pack("<Q", length))
        self._buffer.seek(pos)

    def end(self):
        buf = self._buffer.getvalue()
        self._buffer.close()
        self._buffer = BytesIO()
        return buf

    def write(self, data: bytes):
        self._buffer.write(data)

    def write_prefixed_string(self, value: str):
        bytes = value.encode()
        self.write4(len(bytes))
        self.write(bytes)

    def write1(self, value: int):
        self.write(struct.pack("<B", value))

    def write2(self, value: int):
        self.write(struct.pack("<H", value))

    def write4(self, value: int):
        self.write(struct.pack("<I", value))

    def write8(self, value: int):
        self.write(struct.pack("<Q", value))
