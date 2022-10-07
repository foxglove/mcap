"""Decoding of CDR (Common Data Representation) data."""

import struct
from enum import IntEnum
from typing import List


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
        """Create a CdrReader wrapping a byte array."""
        if len(data) < 4:
            raise ValueError(
                f"Invalid CDR data size {len(data)}, must contain at least a 4-byte header"
            )
        kind = struct.unpack_from("B", data, 1)[0]
        self.data = data
        self.offset = 4
        self.little_endian = kind & 1 == 1

    def kind(self) -> EncapsulationKind:
        """Return the encapsulation kind of the CDR data."""
        return struct.unpack_from("B", self.data, 1)[0]

    def decoded_bytes(self) -> int:
        """Return the number of bytes that have been decoded."""
        return self.offset

    def byte_length(self) -> int:
        """Return the number of bytes in the CDR data."""
        return len(self.data)

    def boolean(self) -> bool:
        """Read an 8-bit value and interpret it as a boolean."""
        return self.uint8() != 0

    def int8(self) -> int:
        """Read a signed 8-bit integer."""
        return self._unpack("b", size=1)

    def uint8(self) -> int:
        """Read an unsigned 8-bit integer."""
        return self._unpack("B", size=1)

    def int16(self) -> int:
        """Read a signed 16-bit integer."""
        return self._unpack("<h" if self.little_endian else ">h", size=2)

    def uint16(self) -> int:
        """Read an unsigned 16-bit integer."""
        return self._unpack("<H" if self.little_endian else ">H", size=2)

    def int32(self) -> int:
        """Read a signed 32-bit integer."""
        return self._unpack("<i" if self.little_endian else ">i", size=4)

    def uint32(self) -> int:
        """Read an unsigned 32-bit integer."""
        return self._unpack("<I" if self.little_endian else ">I", size=4)

    def int64(self) -> int:
        """Read a signed 64-bit integer."""
        return self._unpack("<q" if self.little_endian else ">q", size=8)

    def uint64(self) -> int:
        """Read an unsigned 64-bit integer."""
        return self._unpack("<Q" if self.little_endian else ">Q", size=8)

    def uint16BE(self) -> int:
        """Read an unsigned big-endian 16-bit integer."""
        return self._unpack(">H", size=2)

    def uint32BE(self) -> int:
        """Read an unsigned big-endian 32-bit integer."""
        return self._unpack(">I", size=4)

    def uint64BE(self) -> int:
        """Read an unsigned big-endian 64-bit integer."""
        return self._unpack(">Q", size=8)

    def float32(self) -> float:
        """Read a 32-bit floating point number."""
        return self._unpack("<f" if self.little_endian else ">f", size=4)

    def float64(self) -> float:
        """Read a 64-bit floating point number."""
        return self._unpack("<d" if self.little_endian else ">d", size=8)

    def string(self) -> str:
        """Read a string prefixed with its 32-bit length."""
        length = self.uint32()
        if length <= 1:
            # CDR strings are null-terminated, but serializers differ on whether
            # empty strings are length 0 or 1
            self.offset += length
            return ""
        return self.string_raw(length - 1)

    def string_raw(self, length: int) -> str:
        """Read a string of the given length."""
        data = self.uint8_array(length)
        value = data.decode("utf-8")
        return value

    def sequence_length(self) -> int:
        """Read a 32-bit unsigned integer."""
        return self.uint32()

    def boolean_array(self, length: int) -> List[bool]:
        """Read an array of booleans of the given length."""
        return [self.uint8() != 0 for _ in range(length)]

    def int8_array(self, length: int) -> List[int]:
        """Read an array of signed 8-bit integers of the given length."""
        return [self.int8() for _ in range(length)]

    def uint8_array(self, length: int) -> bytes:
        """Read a byte array of the given length."""
        data = self.data[self.offset : self.offset + length]
        self.offset += length
        return data

    def int16_array(self, length: int) -> List[int]:
        """Read an array of signed 16-bit integers of the given length."""
        return [self.int16() for _ in range(length)]

    def uint16_array(self, length: int) -> List[int]:
        """Read an array of unsigned 16-bit integers of the given length."""
        return [self.uint16() for _ in range(length)]

    def int32_array(self, length: int) -> List[int]:
        """Read an array of signed 32-bit integers of the given length."""
        return [self.int32() for _ in range(length)]

    def uint32_array(self, length: int) -> List[int]:
        """Read an array of unsigned 32-bit integers of the given length."""
        return [self.uint32() for _ in range(length)]

    def int64_array(self, length: int) -> List[int]:
        """Read an array of signed 64-bit integers of the given length."""
        return [self.int64() for _ in range(length)]

    def uint64_array(self, length: int) -> List[int]:
        """Read an array of unsigned 64-bit integers of the given length."""
        return [self.uint64() for _ in range(length)]

    def float32_array(self, length: int) -> List[float]:
        """Read an array of 32-bit floating point numbers of the given length."""
        return [self.float32() for _ in range(length)]

    def float64_array(self, length: int) -> List[float]:
        """Read an array of 64-bit floating point numbers of the given length."""
        return [self.float64() for _ in range(length)]

    def string_array(self, length: int) -> List[str]:
        """Read an array of strings of the given length."""
        return [self.string() for _ in range(length)]

    def seek(self, relative_offset: int):
        """Seek to a relative offset from the current position."""
        new_offset = self.offset + relative_offset
        if new_offset < 4 or new_offset >= len(self.data):
            raise RuntimeError(
                f"seek({relative_offset}) failed, {new_offset} is outside the data range"
            )
        self.offset = new_offset

    def seek_to(self, offset: int):
        """Seek to an absolute offset."""
        if offset < 4 or offset >= len(self.data):
            raise RuntimeError(
                f"seek_to({offset}) failed, value is outside the data range"
            )
        self.offset = offset

    def _align(self, size: int):
        alignment = (self.offset - 4) % size
        if alignment > 0:
            self.offset += size - alignment

    def _unpack(self, fmt: str, size: int):
        if size > 1:
            self._align(size)
        value = struct.unpack_from(fmt, self.data, self.offset)[0]
        self.offset += size
        return value
