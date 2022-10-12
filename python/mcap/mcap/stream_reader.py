import struct
from io import BufferedReader, BytesIO, RawIOBase
from typing import Iterator, List, Optional, Tuple, Union, IO
import zstandard
import zlib
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
    def __init__(self, expected: int, actual: int, record: McapRecord):
        self.expected = expected
        self.actual = actual
        self.record = record

    def __str__(self):
        return (
            f"crc validation failed in {type(self.record).__name__}, "
            f"expected: {self.expected}, calculated: {self.actual}"
        )


def breakup_chunk(chunk: Chunk, validate_crc: bool = False) -> List[McapRecord]:
    stream, stream_length = get_chunk_data_stream(chunk, validate_crc=validate_crc)
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

    return records


def get_chunk_data_stream(
    chunk: Chunk, validate_crc: bool = False
) -> Tuple[ReadDataStream, int]:
    if chunk.compression == "zstd":
        data: bytes = zstandard.decompress(chunk.data, chunk.uncompressed_size)
    elif chunk.compression == "lz4":
        data: bytes = lz4.frame.decompress(chunk.data)  # type: ignore
    else:
        data = chunk.data

    if validate_crc and chunk.uncompressed_crc != 0:
        calculated_crc = zlib.crc32(data)
        if calculated_crc != chunk.uncompressed_crc:
            raise CRCValidationError(
                expected=chunk.uncompressed_crc,
                actual=calculated_crc,
                record=chunk,
            )

    return ReadDataStream(BytesIO(data)), len(data)


def read_magic(stream: ReadDataStream) -> bool:
    magic = struct.unpack("<8B", stream.read(MAGIC_SIZE))
    if magic != (137, 77, 67, 65, 80, 48, 13, 10):
        raise InvalidMagic(magic)
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
        if not self._skip_magic:
            read_magic(self._stream)

        checksum_before_read: int = 0

        while self._footer is None:
            # Can't validate the data_end crc if we skip magic.
            if self._validate_crcs and not self._skip_magic:
                checksum_before_read = self._stream.checksum()
            opcode = self._stream.read1()
            length = self._stream.read8()
            count = self._stream.count
            record = self._read_record(opcode, length)
            if (
                self._validate_crcs
                and not self._skip_magic
                and isinstance(record, DataEnd)
                and record.data_section_crc != 0
                and record.data_section_crc != checksum_before_read
            ):
                raise CRCValidationError(
                    expected=record.data_section_crc,
                    actual=checksum_before_read,
                    record=record,
                )
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
        self._skip_magic: bool = skip_magic
        self._emit_chunks: bool = emit_chunks
        self._validate_crcs: bool = validate_crcs
        self._calculated_data_section_crc = None

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
            return DataEnd.read(self._stream)
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
