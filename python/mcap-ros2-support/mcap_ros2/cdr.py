"""Decoding of CDR (Common Data Representation) data."""

import struct
from enum import Enum
from typing import List, Optional


class EncapsulationKind(Enum):
    """Represents the kind of encapsulation used in a CDR stream."""

    CDR_BE = 0  # Big-endian
    CDR_LE = 1  # Little-endian
    PL_CDR_BE = 2  # Parameter list in big-endian
    PL_CDR_LE = 3  # Parameter list in little-endian


class CdrReader:
    """Parses values from CDR data."""

    def __init__(self, data: bytearray):
        kind = struct.unpack_from("B", data, 0)[0]
        self.data = data
        self.offset = 0
        self.littleEndian = (
            kind == EncapsulationKind.CDR_LE or kind == EncapsulationKind.PL_CDR_LE
        )

    def kind(self) -> EncapsulationKind:
        return struct.unpack_from("B", self.data, 0)[0]

    def decoded_bytes(self) -> int:
        return self.offset

    def byte_length(self) -> int:
        return len(self.data)

    def int8(self) -> int:
        value = struct.unpack_from("b", self.data, self.offset)[0]
        self.offset += 1
        return value

    def uint8(self) -> int:
        value = struct.unpack_from("B", self.data, self.offset)[0]
        self.offset += 1
        return value

    def int16(self) -> int:
        self.__align(2)
        fmt = "<h" if self.littleEndian else ">h"
        value = struct.unpack_from(fmt, self.data, self.offset)[0]
        self.offset += 2
        return value

    def uint16(self) -> int:
        self.__align(2)
        fmt = "<H" if self.littleEndian else ">H"
        value = struct.unpack_from(fmt, self.data, self.offset)[0]
        self.offset += 2
        return value

    def int32(self) -> int:
        self.__align(4)
        fmt = "<i" if self.littleEndian else ">i"
        value = struct.unpack_from(fmt, self.data, self.offset)[0]
        self.offset += 4
        return value

    def uint32(self) -> int:
        self.__align(4)
        fmt = "<I" if self.littleEndian else ">I"
        value = struct.unpack_from(fmt, self.data, self.offset)[0]
        self.offset += 4
        return value

    def int64(self) -> int:
        self.__align(8)
        fmt = "<q" if self.littleEndian else ">q"
        value = struct.unpack_from(fmt, self.data, self.offset)[0]
        self.offset += 8
        return value

    def uint64(self) -> int:
        self.__align(8)
        fmt = "<Q" if self.littleEndian else ">Q"
        value = struct.unpack_from(fmt, self.data, self.offset)[0]
        self.offset += 8
        return value

    def uint16BE(self) -> int:
        self.__align(2)
        value = struct.unpack_from(">H", self.data, self.offset)[0]
        self.offset += 2
        return value

    def uint32BE(self) -> int:
        self.__align(4)
        value = struct.unpack_from(">I", self.data, self.offset)[0]
        self.offset += 4
        return value

    def uint64BE(self) -> int:
        self.__align(8)
        value = struct.unpack_from(">Q", self.data, self.offset)[0]
        self.offset += 8
        return value

    def float32(self) -> float:
        self.__align(4)
        fmt = "<f" if self.littleEndian else ">f"
        value = struct.unpack_from(fmt, self.data, self.offset)[0]
        self.offset += 4
        return value

    def float64(self) -> float:
        self.__align(8)
        fmt = "<d" if self.littleEndian else ">d"
        value = struct.unpack_from(fmt, self.data, self.offset)[0]
        self.offset += 8
        return value

    def string(self) -> str:
        length = self.uint32()
        if length <= 1:
            self.offset += length
            return ""
        data = self.data[self.offset : self.offset + length - 1]
        value = data.decode("utf-8")
        self.offset += length
        return value

    def sequence_length(self) -> int:
        return self.uint32()

    def int8_array(self, count: Optional[int] = None) -> List[int]:
        count = count or self.sequence_length()
        return [self.int8() for _ in range(count)]

    def uint8_array(self, count: Optional[int] = None) -> List[int]:
        count = count or self.sequence_length()
        return [self.uint8() for _ in range(count)]

    def seek(self, relative_offset: int):
        new_offset = self.offset + relative_offset
        if new_offset < 4 or new_offset >= len(self.data):
            raise Exception(
                f"seek({relative_offset}) failed, {new_offset} is outside the data range"
            )
        self.offset = new_offset

    def seek_to(self, offset: int):
        if offset < 4 or offset >= len(self.data):
            raise Exception(
                f"seek_to({offset}) failed, value is outside the data range"
            )
        self.offset = offset

    def __align(self, size: int):
        alignment = (self.offset - 4) % size
        if alignment > 0:
            self.offset += size - alignment
