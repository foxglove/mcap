import binascii
import struct
from io import BufferedReader, BytesIO, RawIOBase
from typing import Iterator, List, Optional, Tuple, Union, IO
import zstandard
import lz4.frame  # type: ignore

from .data_stream import ReadDataStream
from .exceptions import InvalidMagic
from .opcode import Opcode
from .records import (
    Attachment,
    AttachmentIndex,
    Channel,
    Chunk,
    ChunkIndex,
    DataEnd,
    Footer,
    Header,
    McapRecord,
    Message,
    MessageIndex,
    Metadata,
    MetadataIndex,
    Schema,
    Statistics,
    SummaryOffset,
)

MAGIC_SIZE = 8


class CRCValidationError(ValueError):
    def __init__(self, expected: int, actual: int):
        self._expected = expected
        self._actual = actual

    def __str__(self):
        return f"crc validation failed, expected {self._expected}, calculated: {self._actual}"


def breakup_chunk(chunk: Chunk, validate_crc: bool = False) -> List[McapRecord]:
    stream, stream_length = get_chunk_data_stream(chunk, calculate_crc=validate_crc)
    records: List[McapRecord] = []
    while stream.count < stream_length:
        opcode = stream.read1()
        length = stream.read8()
        if opcode == Opcode.CHANNEL:
            channel = Channel.read(stream)
            records.append(channel)
        elif opcode == Opcode.MESSAGE:
            message = Message.read(stream, length)
            records.append(message)
        elif opcode == Opcode.SCHEMA:
            schema = Schema.read(stream)
            records.append(schema)
        else:
            # Unknown chunk record type
            stream.read(length)
    if (
        validate_crc
        and chunk.uncompressed_crc != 0
        and chunk.uncompressed_crc != stream.checksum()
    ):
        raise CRCValidationError(
            expected=chunk.uncompressed_crc, actual=stream.checksum()
        )

    return records


def get_chunk_data_stream(
    chunk: Chunk, calculate_crc: bool = False
) -> Tuple[ReadDataStream, int]:
    if chunk.compression == "zstd":
        data: bytes = zstandard.decompress(chunk.data, chunk.uncompressed_size)
        return ReadDataStream(BytesIO(data), calculate_crc=calculate_crc), len(data)
    elif chunk.compression == "lz4":
        data: bytes = lz4.frame.decompress(chunk.data)  # type: ignore
        return ReadDataStream(BytesIO(data), calculate_crc=calculate_crc), len(data)
    else:
        return ReadDataStream(BytesIO(chunk.data), calculate_crc=calculate_crc), len(
            chunk.data
        )


def read_magic(stream: ReadDataStream) -> bool:
    magic = struct.unpack("<8B", stream.read(MAGIC_SIZE))
    if magic != (137, 77, 67, 65, 80, 48, 13, 10):
        raise InvalidMagic()
    return True


class StreamReader:
    """
    Reads MCAP data sequentially from an input stream.
    """

    @property
    def records(self) -> Iterator[McapRecord]:
        """
        Returns records encountered in the MCAP in order.
        """
        if not self._magic:
            self._magic = read_magic(self._stream)

        while self._footer is None:
            opcode = self._stream.read1()
            length = self._stream.read8()
            count = self._stream.count
            record = self._read_record(opcode, length)
            padding = length - (self._stream.count - count)
            if padding > 0:
                self._stream.read(padding)
            if isinstance(record, Chunk) and not self._emit_chunks:
                chunk_records = breakup_chunk(record, validate_crc=self._validate_crcs)
                for chunk_record in chunk_records:
                    yield chunk_record
            elif record:
                yield record
            if isinstance(record, Footer):
                self._footer = record
                read_magic(self._stream)

    def __init__(
        self,
        input: Union[str, BytesIO, RawIOBase, BufferedReader, IO[bytes]],
        skip_magic: bool = False,
        emit_chunks: bool = False,
        validate_crcs: bool = False,
    ):
        """
        input: The input stream from which to read records.
        """
        if isinstance(input, str):
            self._stream = ReadDataStream(
                open(input, "rb"), calculate_crc=validate_crcs
            )
        elif isinstance(input, RawIOBase):
            self._stream = ReadDataStream(
                BufferedReader(input), calculate_crc=validate_crcs
            )
        else:
            self._stream = ReadDataStream(input, calculate_crc=validate_crcs)
        self._footer: Optional[Footer] = None
        self._magic: bool = skip_magic
        self._emit_chunks: bool = emit_chunks
        self._validate_crcs: bool = validate_crcs

    def _read_record(self, opcode: int, length: int) -> Optional[McapRecord]:
        if opcode == Opcode.ATTACHMENT:
            return Attachment.read(self._stream)
        if opcode == Opcode.ATTACHMENT_INDEX:
            return AttachmentIndex.read(self._stream)
        if opcode == Opcode.CHANNEL:
            return Channel.read(self._stream)
        if opcode == Opcode.CHUNK:
            return Chunk.read(self._stream)
        if opcode == Opcode.CHUNK_INDEX:
            return ChunkIndex.read(self._stream)
        if opcode == Opcode.DATA_END:
            # We can only expect the data end CRC to be valid if we've read the start magic.
            if self._validate_crcs and self._magic:
                data_section_checksum = self._stream.checksum()
                data_end = DataEnd.read(self._stream)
                if (
                    data_end.data_section_crc != 0
                    and data_end.data_section_crc != data_section_checksum
                ):
                    raise CRCValidationError(
                        expected=data_end.data_section_crc, actual=data_section_checksum
                    )
            else:
                data_end = DataEnd.read(self._stream)
            return data_end
        if opcode == Opcode.FOOTER:
            return Footer.read(self._stream)
        if opcode == Opcode.HEADER:
            return Header.read(self._stream)
        if opcode == Opcode.MESSAGE:
            return Message.read(self._stream, length)
        if opcode == Opcode.MESSAGE_INDEX:
            return MessageIndex.read(self._stream)
        if opcode == Opcode.METADATA:
            return Metadata.read(self._stream)
        if opcode == Opcode.METADATA_INDEX:
            return MetadataIndex.read(self._stream)
        if opcode == Opcode.SCHEMA:
            return Schema.read(self._stream)
        if opcode == Opcode.STATISTICS:
            return Statistics.read(self._stream)
        if opcode == Opcode.SUMMARY_OFFSET:
            return SummaryOffset.read(self._stream)

        # Skip unknown record types
        self._stream.read(length - 9)


__all__ = ["StreamReader"]
