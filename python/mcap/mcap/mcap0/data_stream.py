import struct
from io import BytesIO
from typing import IO

from .exceptions import EndOfFile
from .opcode import Opcode


class ReadDataStream:
    def __init__(self, stream: IO[bytes]):
        self.__count = 0
        self.__stream = stream

    @property
    def count(self) -> int:
        return self.__count

    def read(self, length: int) -> bytes:
        if length == 0:
            return b""

        data = self.__stream.read(length)
        self.__count += len(data)
        if data == b"":
            raise EndOfFile()
        return data

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
    def __init__(self):
        self.__buffer = BytesIO()

    @property
    def count(self) -> int:
        return self.__buffer.tell()

    def start_record(self, opcode: Opcode):
        self.__record_start_offset = self.__buffer.tell()
        self.__buffer.write(struct.pack("<BQ", opcode, 0))  # placeholder size

    def finish_record(self):
        pos = self.__buffer.tell()
        length = pos - self.__record_start_offset - 9
        self.__buffer.seek(self.__record_start_offset + 1)
        self.__buffer.write(struct.pack("<Q", length))
        self.__buffer.seek(pos)

    def end(self):
        buf = self.__buffer.getvalue()
        self.__buffer.close()
        self.__buffer = BytesIO()
        return buf

    def write(self, data: bytes):
        self.__buffer.write(data)

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
