"""Decoding of CDR (Common Data Representation) data."""

import struct
from enum import IntEnum


class EncapsulationKind(IntEnum):
    """Represents the kind of encapsulation used in a CDR stream."""

    CDR_BE = 0  # Big-endian
    CDR_LE = 1  # Little-endian
    PL_CDR_BE = 2  # Parameter list in big-endian
    PL_CDR_LE = 3  # Parameter list in little-endian


class CdrReader:
    """Parses values from CDR data."""

    __slots__ = ("data", "offset", "little_endian")

    def __init__(self, data: bytes):
        if len(data) < 4:
            raise ValueError(
                f"Invalid CDR data size {len(data)}, must contain at least a 4-byte header"
            )
        kind = struct.unpack_from("B", data, 1)[0]
        self.data = data
        self.offset = 4
        self.little_endian = kind & 1 == 1

    def kind(self) -> EncapsulationKind:
        return struct.unpack_from("B", self.data, 1)[0]

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
        fmt = "<h" if self.little_endian else ">h"
        value = struct.unpack_from(fmt, self.data, self.offset)[0]
        self.offset += 2
        return value

    def uint16(self) -> int:
        self.__align(2)
        fmt = "<H" if self.little_endian else ">H"
        value = struct.unpack_from(fmt, self.data, self.offset)[0]
        self.offset += 2
        return value

    def int32(self) -> int:
        self.__align(4)
        fmt = "<i" if self.little_endian else ">i"
        value = struct.unpack_from(fmt, self.data, self.offset)[0]
        self.offset += 4
        return value

    def uint32(self) -> int:
        self.__align(4)
        fmt = "<I" if self.little_endian else ">I"
        value = struct.unpack_from(fmt, self.data, self.offset)[0]
        self.offset += 4
        return value

    def int64(self) -> int:
        self.__align(8)
        fmt = "<q" if self.little_endian else ">q"
        value = struct.unpack_from(fmt, self.data, self.offset)[0]
        self.offset += 8
        return value

    def uint64(self) -> int:
        self.__align(8)
        fmt = "<Q" if self.little_endian else ">Q"
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
        fmt = "<f" if self.little_endian else ">f"
        value = struct.unpack_from(fmt, self.data, self.offset)[0]
        self.offset += 4
        return value

    def float64(self) -> float:
        self.__align(8)
        fmt = "<d" if self.little_endian else ">d"
        value = struct.unpack_from(fmt, self.data, self.offset)[0]
        self.offset += 8
        return value

    def string(self) -> str:
        length = self.uint32()
        if length <= 1:
            self.offset += length
            return ""
        return self.string_raw(length - 1)

    def string_raw(self, length: int) -> str:
        data = self.uint8_array(length)
        value = data.decode("utf-8")
        return value

    def sequence_length(self) -> int:
        return self.uint32()

    def uint8_array(self, length: int) -> bytes:
        data = self.data[self.offset : self.offset + length]
        self.offset += length
        return data

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
