import struct
from io import BufferedIOBase, BytesIO

from .exceptions import EndOfFile
from .opcode import Opcode


class ReadDataStream:
    def __init__(self, stream: BufferedIOBase):
        self.__count = 0
        self.__stream = stream

    def __del__(self):
        self.__stream.close()

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


class WriteDataStream:
    def __init__(self, output: BufferedIOBase):
        self.__stream = output
        self.__count = 0
        self.__buffer = BytesIO()
        self.__current_opcode = 0

    def __del__(self):
        self.__stream.close()

    @property
    def count(self) -> int:
        return self.__count

    def start_record(self, opcode: Opcode):
        self.__current_opcode = opcode
        self.__buffer = BytesIO()

    def finish_record(self, include_padding: bool = True) -> int:
        start_count = self.__count
        data = self.__buffer.getvalue()
        length = len(data)
        self.__stream.write(struct.pack("<B", self.__current_opcode))
        self.__stream.write(struct.pack("<Q", length))
        self.__stream.write(data)
        self.__count += 9  # For opcode + length
        return self.__count - start_count

    def write(self, data: bytes):
        self.__buffer.write(data)
        self.__count += len(data)

    def write_magic(self):
        bytes = struct.pack("<8B", 137, 77, 67, 65, 80, 48, 13, 10)
        self.__stream.write(bytes)
        self.__count += len(bytes)

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
