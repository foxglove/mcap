import struct
import zlib
from io import BufferedReader, BytesIO, RawIOBase
from typing import IO, Iterator, List, Optional, Tuple, Union

try:
    import lz4.frame as lz4
except ImportError:
    lz4 = None

try:
    import zstandard
except ImportError:
    zstandard = None

from .data_stream import ReadDataStream
from .exceptions import (
    EndOfFile,
    InvalidMagic,
    RecordLengthLimitExceeded,
    UnsupportedCompressionError,
)
from .opcode import Opcode
from .records import (
    MESSAGE_FIELDS,
    MESSAGE_FIELDS_SIZE,
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

# Opcode and record length. Every record starts with this prefix.
_RECORD_PREFIX = struct.Struct("<BQ")
_RECORD_PREFIX_SIZE = _RECORD_PREFIX.size
# Plain ints: comparing against an IntEnum member goes through Enum.__eq__ on every record.
_OPCODE_CHANNEL = int(Opcode.CHANNEL)
_OPCODE_MESSAGE = int(Opcode.MESSAGE)
_OPCODE_SCHEMA = int(Opcode.SCHEMA)


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
    """Parses the records inside a chunk."""
    return _parse_chunk_records(_get_chunk_data(chunk, validate_crc=validate_crc))


def _parse_chunk_records(data: bytes) -> List[McapRecord]:
    """Parses a chunk's decompressed contents. This is the hot loop of every reader, so
    messages are unpacked in place rather than through ``Message.read``."""
    records: List[McapRecord] = []
    append = records.append
    prefix_unpack = _RECORD_PREFIX.unpack_from
    fields_unpack = MESSAGE_FIELDS.unpack_from
    pos = 0
    end = len(data)
    while pos < end:
        if end - pos < _RECORD_PREFIX_SIZE:
            raise EndOfFile()
        opcode, length = prefix_unpack(data, pos)
        pos += _RECORD_PREFIX_SIZE
        record_end = pos + length
        if record_end > end:
            raise EndOfFile()
        if opcode == _OPCODE_MESSAGE:
            channel_id, sequence, log_time, publish_time = fields_unpack(data, pos)
            append(
                Message(
                    channel_id,
                    log_time,
                    data[pos + MESSAGE_FIELDS_SIZE : record_end],
                    publish_time,
                    sequence,
                )
            )
        elif opcode == _OPCODE_CHANNEL:
            append(Channel.read(ReadDataStream(BytesIO(data[pos:record_end]))))
        elif opcode == _OPCODE_SCHEMA:
            append(Schema.read(ReadDataStream(BytesIO(data[pos:record_end]))))
        # Unknown chunk record types are skipped.
        pos = record_end

    return records


def _get_chunk_data(chunk: Chunk, validate_crc: bool = False) -> bytes:
    """Returns the decompressed contents of a chunk, validating the CRC if requested."""
    if chunk.compression == "zstd":
        if zstandard is None:
            raise UnsupportedCompressionError("zstandard")
        data: bytes = zstandard.decompress(chunk.data, chunk.uncompressed_size)
    elif chunk.compression == "lz4":
        if lz4 is None:
            raise UnsupportedCompressionError("lz4")
        data: bytes = lz4.decompress(chunk.data)  # type: ignore
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
    return data


def get_chunk_data_stream(
    chunk: Chunk, validate_crc: bool = False
) -> Tuple[ReadDataStream, int]:
    data = _get_chunk_data(chunk, validate_crc=validate_crc)
    return ReadDataStream(BytesIO(data)), len(data)


def read_magic(stream: ReadDataStream) -> bool:
    magic = struct.unpack("<8B", stream.read(MAGIC_SIZE))
    if magic != (137, 77, 67, 65, 80, 48, 13, 10):
        raise InvalidMagic(magic)
    return True


class StreamReader:
    """
    Reads MCAP data sequentially from an input stream.

    :param input: a file-like object for reading the source data from.
    :param skip_magic: if ``True``, will not expect MCAP magic at start or end of stream.
    :param emit_chunks: if ``True``, will return Chunk records directly and do not parse out the
        records inside.
    :param validate_crcs: if ``True``, will validate chunk and data section CRC values.
    :param record_size_limit: An upper bound to the size of MCAP records that this reader will
        attempt to load in bytes, defaulting to 4 GiB. If this reader encounters a record with a
        greater length, it will throw an :py:class:`~mcap.exceptions.RecordLengthLimitExceeded`
        error.  Setting to ``None`` removes the limit, but can allow corrupted MCAP files to trigger
        a `MemoryError` exception.
    """

    def __init__(
        self,
        input: Union[str, BytesIO, RawIOBase, BufferedReader, IO[bytes]],
        skip_magic: bool = False,
        emit_chunks: bool = False,
        validate_crcs: bool = False,
        record_size_limit: Optional[int] = (4 * 2**30),  # 4 Gib
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
        self._record_size_limit = record_size_limit

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
            opcode, length = _RECORD_PREFIX.unpack(
                self._stream.read(_RECORD_PREFIX_SIZE)
            )
            if self._record_size_limit is not None and length > self._record_size_limit:
                raise RecordLengthLimitExceeded(opcode, length, self._record_size_limit)
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

    def _read_record(self, opcode: int, length: int) -> Optional[McapRecord]:
        # Messages first: in an unchunked file they are almost every record.
        if opcode == _OPCODE_MESSAGE:
            body = self._stream.read(length)
            if len(body) < MESSAGE_FIELDS_SIZE:
                raise EndOfFile()
            channel_id, sequence, log_time, publish_time = MESSAGE_FIELDS.unpack_from(
                body
            )
            return Message(
                channel_id, log_time, body[MESSAGE_FIELDS_SIZE:], publish_time, sequence
            )
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
        self._stream.read(length)


__all__ = ["StreamReader"]
