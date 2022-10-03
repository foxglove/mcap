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


def breakup_chunk(chunk: Chunk) -> List[McapRecord]:
    stream, stream_length = get_chunk_data_stream(chunk)
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


def get_chunk_data_stream(chunk: Chunk) -> Tuple[ReadDataStream, int]:
    if chunk.compression == "zstd":
        data: bytes = zstandard.decompress(chunk.data, chunk.uncompressed_size)
        return ReadDataStream(BytesIO(data)), len(data)
    elif chunk.compression == "lz4":
        data: bytes = lz4.frame.decompress(chunk.data)  # type: ignore
        return ReadDataStream(BytesIO(data)), len(data)
    else:
        return ReadDataStream(BytesIO(chunk.data)), len(chunk.data)


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
        if not self.__magic:
            self.__magic = read_magic(self.__stream)

        while self.__footer is None:
            opcode = self.__stream.read1()
            length = self.__stream.read8()
            count = self.__stream.count
            record = self.__read_record(opcode, length)
            padding = length - (self.__stream.count - count)
            if padding > 0:
                self.__stream.read(padding)
            if isinstance(record, Chunk) and not self.__emit_chunks:
                chunk_records = breakup_chunk(record)
                for chunk_record in chunk_records:
                    yield chunk_record
            elif record:
                yield record
            if isinstance(record, Footer):
                self.__footer = record
                read_magic(self.__stream)

    def __init__(
        self,
        input: Union[str, BytesIO, RawIOBase, BufferedReader, IO[bytes]],
        skip_magic: bool = False,
        emit_chunks: bool = False,
    ):
        """
        input: The input stream from which to read records.
        """
        if isinstance(input, str):
            self.__stream = ReadDataStream(open(input, "rb"))
        elif isinstance(input, RawIOBase):
            self.__stream = ReadDataStream(BufferedReader(input))
        else:
            self.__stream = ReadDataStream(input)
        self.__footer: Optional[Footer] = None
        self.__magic: bool = skip_magic
        self.__emit_chunks: bool = emit_chunks

    def __read_record(self, opcode: int, length: int) -> Optional[McapRecord]:
        if opcode == Opcode.ATTACHMENT:
            return Attachment.read(self.__stream)
        if opcode == Opcode.ATTACHMENT_INDEX:
            return AttachmentIndex.read(self.__stream)
        if opcode == Opcode.CHANNEL:
            return Channel.read(self.__stream)
        if opcode == Opcode.CHUNK:
            return Chunk.read(self.__stream)
        if opcode == Opcode.CHUNK_INDEX:
            return ChunkIndex.read(self.__stream)
        if opcode == Opcode.DATA_END:
            return DataEnd.read(self.__stream)
        if opcode == Opcode.FOOTER:
            return Footer.read(self.__stream)
        if opcode == Opcode.HEADER:
            return Header.read(self.__stream)
        if opcode == Opcode.MESSAGE:
            return Message.read(self.__stream, length)
        if opcode == Opcode.MESSAGE_INDEX:
            return MessageIndex.read(self.__stream)
        if opcode == Opcode.METADATA:
            return Metadata.read(self.__stream)
        if opcode == Opcode.METADATA_INDEX:
            return MetadataIndex.read(self.__stream)
        if opcode == Opcode.SCHEMA:
            return Schema.read(self.__stream)
        if opcode == Opcode.STATISTICS:
            return Statistics.read(self.__stream)
        if opcode == Opcode.SUMMARY_OFFSET:
            return SummaryOffset.read(self.__stream)

        # Skip unknown record types
        self.__stream.read(length - 9)


__all__ = ["StreamReader"]
