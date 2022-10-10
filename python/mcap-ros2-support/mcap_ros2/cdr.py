"""Decoding of CDR (Common Data Representation) data."""

import struct
from enum import IntEnum
from io import BufferedWriter
from typing import Any, BinaryIO, Callable, List, Tuple, Union


class EncapsulationKind(IntEnum):
    """Represents the kind of encapsulation used in a CDR stream."""

    CDR_BE = 0  # Big-endian
    CDR_LE = 1  # Little-endian
    PL_CDR_BE = 2  # Parameter list in big-endian
    PL_CDR_LE = 3  # Parameter list in little-endian


# Avoid the overhead of parsing struct strings every time we need to pack or unpack

unpack_int8 = struct.Struct("b").unpack_from
unpack_uint8 = struct.Struct("B").unpack_from
unpack_int16be = struct.Struct(">h").unpack_from
unpack_int16le = struct.Struct("<h").unpack_from
unpack_uint16be = struct.Struct(">H").unpack_from
unpack_uint16le = struct.Struct("<H").unpack_from
unpack_int32be = struct.Struct(">i").unpack_from
unpack_int32le = struct.Struct("<i").unpack_from
unpack_uint32be = struct.Struct(">I").unpack_from
unpack_uint32le = struct.Struct("<I").unpack_from
unpack_int64be = struct.Struct(">q").unpack_from
unpack_int64le = struct.Struct("<q").unpack_from
unpack_uint64be = struct.Struct(">Q").unpack_from
unpack_uint64le = struct.Struct("<Q").unpack_from
unpack_float32be = struct.Struct(">f").unpack_from
unpack_float32le = struct.Struct("<f").unpack_from
unpack_float64be = struct.Struct(">d").unpack_from
unpack_float64le = struct.Struct("<d").unpack_from

pack_int8 = struct.Struct("b").pack
pack_uint8 = struct.Struct("B").pack
pack_int16be = struct.Struct(">h").pack
pack_int16le = struct.Struct("<h").pack
pack_uint16be = struct.Struct(">H").pack
pack_uint16le = struct.Struct("<H").pack
pack_int32be = struct.Struct(">i").pack
pack_int32le = struct.Struct("<i").pack
pack_uint32be = struct.Struct(">I").pack
pack_uint32le = struct.Struct("<I").pack
pack_int64be = struct.Struct(">q").pack
pack_int64le = struct.Struct("<q").pack
pack_uint64be = struct.Struct(">Q").pack
pack_uint64le = struct.Struct("<Q").pack
pack_float32be = struct.Struct(">f").pack
pack_float32le = struct.Struct("<f").pack
pack_float64be = struct.Struct(">d").pack
pack_float64le = struct.Struct("<d").pack


class CdrReader:
    """Parses values from CDR data."""

    __slots__ = ("data", "offset", "little_endian")

    def __init__(self, data: bytes):
        """Create a CdrReader wrapping a byte array."""
        if len(data) < 4:
            raise ValueError(
                f"Invalid CDR data size {len(data)}, must contain at least a 4-byte header"
            )
        kind = unpack_uint8(data, 1)[0]
        self.data = data
        self.offset = 4
        self.little_endian = kind & 1 == 1

    def kind(self) -> EncapsulationKind:
        """Return the encapsulation kind of the CDR data."""
        return unpack_uint8(self.data, 1)[0]

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
        return self._unpack(unpack_int8, size=1)

    def uint8(self) -> int:
        """Read an unsigned 8-bit integer."""
        return self._unpack(unpack_uint8, size=1)

    def int16(self) -> int:
        """Read a signed 16-bit integer."""
        return self._unpack(
            unpack_int16le if self.little_endian else unpack_int16be, size=2
        )

    def uint16(self) -> int:
        """Read an unsigned 16-bit integer."""
        return self._unpack(
            unpack_uint16le if self.little_endian else unpack_uint16be, size=2
        )

    def int32(self) -> int:
        """Read a signed 32-bit integer."""
        return self._unpack(
            unpack_int32le if self.little_endian else unpack_int32be, size=4
        )

    def uint32(self) -> int:
        """Read an unsigned 32-bit integer."""
        return self._unpack(
            unpack_uint32le if self.little_endian else unpack_uint32be, size=4
        )

    def int64(self) -> int:
        """Read a signed 64-bit integer."""
        return self._unpack(
            unpack_int64le if self.little_endian else unpack_int64be, size=8
        )

    def uint64(self) -> int:
        """Read an unsigned 64-bit integer."""
        return self._unpack(
            unpack_uint64le if self.little_endian else unpack_uint64be, size=8
        )

    def uint16BE(self) -> int:
        """Read an unsigned big-endian 16-bit integer."""
        return self._unpack(unpack_uint16be, size=2)

    def uint32BE(self) -> int:
        """Read an unsigned big-endian 32-bit integer."""
        return self._unpack(unpack_uint32be, size=4)

    def uint64BE(self) -> int:
        """Read an unsigned big-endian 64-bit integer."""
        return self._unpack(unpack_uint64be, size=8)

    def float32(self) -> float:
        """Read a 32-bit floating point number."""
        return self._unpack(
            unpack_float32le if self.little_endian else unpack_float32be, size=4
        )

    def float64(self) -> float:
        """Read a 64-bit floating point number."""
        return self._unpack(
            unpack_float64le if self.little_endian else unpack_float64be, size=8
        )

    def string(self) -> str:
        """Read a string prefixed with its 32-bit length."""
        length = self.uint32()
        if length <= 1:
            # CDR strings are null-terminated, but serializers differ on whether
            # empty strings are length 0 or 1
            self.offset += length
            return ""
        value = self.string_raw(length - 1)
        self.offset += 1  # Skip null terminator
        return value

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

    def _unpack(self, fn: Callable[[bytes, int], Tuple[Any]], size: int):
        if size > 1:
            self._align(size)
        value = fn(self.data, self.offset)[0]
        self.offset += size
        return value


class CdrWriter:
    """Serialize CDR data."""

    __slots__ = ("little_endian", "output", "offset")

    little_endian: bool
    output: Union[BinaryIO, BufferedWriter]
    offset: int

    def __init__(
        self,
        output: Union[BinaryIO, BufferedWriter],
        kind: EncapsulationKind = EncapsulationKind.CDR_LE,
    ):
        """Initialize a CdrWriter wrapping a writable output and write the CDR header."""
        self.little_endian = kind & 1 == 1
        self.output = output
        self.offset = 0
        # Write the CDR header
        self.write_uint16BE(kind)
        self.write_uint16BE(0)

    def write_boolean(self, value: bool):
        """Write a boolean."""
        self.write_uint8(1 if value else 0)

    def write_int8(self, value: int):
        """Write a signed 8-bit integer."""
        self._pack(pack_int8, value, size=1)

    def write_uint8(self, value: int):
        """Write an unsigned 8-bit integer."""
        self._pack(pack_uint8, value, size=1)

    def write_int16(self, value: int):
        """Write a signed 16-bit integer."""
        self._pack(pack_int16le if self.little_endian else pack_int16be, value, size=2)

    def write_uint16(self, value: int):
        """Write an unsigned 16-bit integer."""
        self._pack(
            pack_uint16le if self.little_endian else pack_uint16be, value, size=2
        )

    def write_int32(self, value: int):
        """Write a signed 32-bit integer."""
        self._pack(pack_int32le if self.little_endian else pack_int32be, value, size=4)

    def write_uint32(self, value: int):
        """Write an unsigned 32-bit integer."""
        self._pack(
            pack_uint32le if self.little_endian else pack_uint32be, value, size=4
        )

    def write_int64(self, value: int):
        """Write a signed 64-bit integer."""
        self._pack(pack_int64le if self.little_endian else pack_int64be, value, size=8)

    def write_uint64(self, value: int):
        """Write an unsigned 64-bit integer."""
        self._pack(
            pack_uint64le if self.little_endian else pack_uint64be, value, size=8
        )

    def write_uint16BE(self, value: int):
        """Write an unsigned 16-bit integer in big endian."""
        self._pack(pack_uint16be, value, size=2)

    def write_uint32BE(self, value: int):
        """Write an unsigned 32-bit integer in big endian."""
        self._pack(pack_uint32be, value, size=4)

    def write_uint64BE(self, value: int):
        """Write an unsigned 64-bit integer in big endian."""
        self._pack(pack_uint64be, value, size=8)

    def write_float32(self, value: float):
        """Write a 32-bit floating point number."""
        self._pack(
            pack_float32le if self.little_endian else pack_float32be, value, size=4
        )

    def write_float64(self, value: float):
        """Write a 64-bit floating point number."""
        self._pack(
            pack_float64le if self.little_endian else pack_float64be, value, size=8
        )

    def write_string(self, value: str):
        """Write a string prefixed with its 32-bit length."""
        data = value.encode("utf-8")
        self.write_uint32(len(data) + 1)
        self.write_bytes(data)
        self.output.write(b"\0")  # Null terminator
        self.offset += 1

    def write_bytes(self, value: bytes):
        """Write a byte array."""
        self.output.write(value)
        self.offset += len(value)

    def write_boolean_array(self, values: List[bool]):
        """Write an array of booleans."""
        for value in values:
            self.write_boolean(value)

    def write_int8_array(self, values: List[int]):
        """Write an array of signed 8-bit integers."""
        for value in values:
            self.write_int8(value)

    def write_uint8_array(self, values: List[int]):
        """Write an array of unsigned 8-bit integers."""
        for value in values:
            self.write_uint8(value)

    def write_int16_array(self, values: List[int]):
        """Write an array of signed 16-bit integers."""
        for value in values:
            self.write_int16(value)

    def write_uint16_array(self, values: List[int]):
        """Write an array of unsigned 16-bit integers."""
        for value in values:
            self.write_uint16(value)

    def write_int32_array(self, values: List[int]):
        """Write an array of signed 32-bit integers."""
        for value in values:
            self.write_int32(value)

    def write_uint32_array(self, values: List[int]):
        """Write an array of unsigned 32-bit integers."""
        for value in values:
            self.write_uint32(value)

    def write_int64_array(self, values: List[int]):
        """Write an array of signed 64-bit integers."""
        for value in values:
            self.write_int64(value)

    def write_uint64_array(self, values: List[int]):
        """Write an array of unsigned 64-bit integers."""
        for value in values:
            self.write_uint64(value)

    def write_float32_array(self, values: List[float]):
        """Write an array of 32-bit floating point numbers."""
        for value in values:
            self.write_float32(value)

    def write_float64_array(self, values: List[float]):
        """Write an array of 64-bit floating point numbers."""
        for value in values:
            self.write_float64(value)

    def write_string_array(self, values: List[str]):
        """Write an array of strings, each prefixed with their 32-bit length."""
        for value in values:
            self.write_string(value)

    def _pack(self, fn: Callable[[Any], bytes], value: Any, size: int):
        if size > 1:
            self._align(size)
        self.output.write(fn(value))
        self.offset += size

    def _align(self, size: int):
        # The four byte header is not considered for alignment
        alignment = (self.offset - 4) % size
        padding = size - alignment if alignment > 0 else 0
        if padding > 0:
            self.output.write(b"\0" * padding)
            self.offset += padding
