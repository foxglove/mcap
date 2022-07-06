""" High-level classes for reading content out of MCAP data sources. """
from abc import ABC, abstractmethod
from typing import Iterable, Tuple, Iterator, Dict, Optional, List
import io
from .data_stream import ReadDataStream
from .exceptions import McapError
from .records import (
    Attachment,
    McapRecord,
    Schema,
    Channel,
    Message,
    Metadata,
    ChunkIndex,
    Statistics,
    Chunk,
    Footer,
    MetadataIndex,
    AttachmentIndex,
)
from .stream_reader import StreamReader, breakup_chunk, MAGIC_SIZE
from .data_stream import RecordBuilder
from .summary import Summary


def _get_record_size(record: McapRecord):
    """utility for counting the number of bytes a given record occupies in an MCAP."""
    rb = RecordBuilder()
    record.write(rb)
    return rb.count


FOOTER_SIZE = _get_record_size(Footer(0, 0, 0))


def _read_summary_from_stream_reader(stream_reader: StreamReader) -> Optional[Summary]:
    """read summary records from an MCAP stream reader, collecting them into a Summary."""
    summary = Summary()
    for record in stream_reader.records:
        if isinstance(record, Statistics):
            summary.statistics = record
        elif isinstance(record, Schema):
            summary.schemas[record.id] = record
        elif isinstance(record, Channel):
            summary.channels[record.id] = record
        elif isinstance(record, AttachmentIndex):
            summary.attachment_indexes.append(record)
        elif isinstance(record, ChunkIndex):
            summary.chunk_indexes.append(record)
        elif isinstance(record, MetadataIndex):
            summary.metadata_indexes.append(record)
        elif isinstance(record, Footer):
            # There is no summary!
            if record.summary_offset_start == 0:
                return None
            else:
                return summary
    return summary


def _chunks_matching_topics(
    summary: Summary,
    topics: Optional[Iterable[str]],
    start_time: Optional[float],
    end_time: Optional[float],
) -> List[ChunkIndex]:
    """returns a list of ChunkIndex records that include one or more messages of the given topics.
    :param summary: the summary of this MCAP.
    :param topics: topics to match. If None, all chunk indices in the summary are returned.
    :param start_time: if not None, messages from before this unix timestamp are not included.
    :param end_time: if not None, messages from after this unix timestamp are not included.
    """
    out: List[ChunkIndex] = []
    for chunk_index in summary.chunk_indexes:
        if start_time is not None and chunk_index.message_end_time < start_time:
            continue
        if end_time is not None and chunk_index.message_start_time >= end_time:
            continue
        for channel_id in chunk_index.message_index_offsets.keys():
            if topics is None or summary.channels[channel_id].topic in topics:
                print(
                    f"start: {chunk_index.message_start_time}, end: {chunk_index.message_end_time}"
                )
                out.append(chunk_index)
                break
    return out


class MCAPReader(ABC):
    @abstractmethod
    def iter_messages(
        self,
        topics: Optional[Iterable[str]] = None,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
    ) -> Iterator[Tuple[Schema, Channel, Message]]:
        raise NotImplementedError()

    @abstractmethod
    def get_summary(self) -> Optional[Summary]:
        raise NotImplementedError()

    @abstractmethod
    def iter_attachments(self) -> Iterator[Attachment]:
        raise NotImplementedError()

    @abstractmethod
    def iter_metadata(self) -> Iterator[Metadata]:
        raise NotImplementedError()


class SeekingReader(MCAPReader):
    """an MCAPReader for reading out of non-seekable data sources."""

    def __init__(self, stream: io.IOBase):
        self._stream = stream
        self._summary: Optional[Summary] = None

    def iter_messages(
        self,
        topics: Optional[Iterable[str]] = None,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
    ) -> Iterator[Tuple[Schema, Channel, Message]]:
        """iterates through the messages in an MCAP.
        :param topics: if not None, only messages from these topics will be returned.
        :param start_time: an integer nanosecond timestamp. if provided, messages logged before
            this timestamp are not included.
        :param end_time: an integer nanosecond timestamp. if provided, messages logged after
            this timestamp are not included.
        """
        summary = self.get_summary()
        assert summary is not None
        if summary is None:
            # no index available, use a non-seeking reader to read linearly through the stream.
            return NonSeekingReader(self._stream).iter_messages(
                topics, start_time, end_time
            )
        for chunk_index in _chunks_matching_topics(
            summary, topics, start_time, end_time
        ):
            self._stream.seek(chunk_index.chunk_start_offset + 1 + 8, io.SEEK_SET)
            chunk = Chunk.read(ReadDataStream(self._stream))
            for record in breakup_chunk(chunk):
                if isinstance(record, Message):
                    channel = summary.channels[record.channel_id]
                    if topics is not None and channel.topic not in topics:
                        continue
                    if start_time is not None and record.log_time < start_time:
                        continue
                    if end_time is not None and record.log_time >= end_time:
                        continue
                    schema = summary.schemas[channel.schema_id]
                    yield (schema, channel, record)

    def get_summary(self) -> Optional[Summary]:
        """returns a Summary object containing records from the (optional) summary section."""
        if self._summary is not None:
            return self._summary
        pos = self._stream.tell()
        try:
            self._stream.seek(-(FOOTER_SIZE + MAGIC_SIZE), io.SEEK_END)
            footer = next(StreamReader(self._stream, skip_magic=True).records)
            if not isinstance(footer, Footer):
                raise McapError(
                    f"expected footer at end of mcap file, found {type(footer)}"
                )
            if footer.summary_offset_start == 0:
                return None
            self._stream.seek(footer.summary_start, io.SEEK_SET)
            self._summary = _read_summary_from_stream_reader(
                StreamReader(self._stream, skip_magic=True)
            )
            return self._summary
        finally:
            self._stream.seek(pos)

    def iter_attachments(self) -> Iterator[Attachment]:
        """iterates through attachment records in the MCAP."""
        summary = self.get_summary()
        if summary is None:
            # no index available, use a non-seeking reader to read linearly through the stream.
            return NonSeekingReader(self._stream).iter_attachments()
        pos = self._stream.tell()
        try:
            for attachment_index in summary.attachment_indexes:
                self._stream.seek(attachment_index.offset)
                record = next(StreamReader(self._stream, skip_magic=True).records)
                if isinstance(record, Attachment):
                    yield record
                else:
                    raise McapError(f"expected attachment record, got {type(record)}")
        finally:
            self._stream.seek(pos)

    def iter_metadata(self) -> Iterator[Metadata]:
        """iterates through metadata records in the MCAP."""
        summary = self.get_summary()
        if summary is None:
            # fall back to a non-seeking reader
            return NonSeekingReader(self._stream).iter_metadata()
        pos = self._stream.tell()
        try:
            for metadata_index in summary.metadata_indexes:
                self._stream.seek(metadata_index.offset)
                record = next(StreamReader(self._stream, skip_magic=True).records)
                if isinstance(record, Metadata):
                    yield record
                else:
                    raise McapError(f"expected attachment record, got {type(record)}")
        finally:
            self._stream.seek(pos)


class NonSeekingReader(MCAPReader):
    def __init__(self, stream: io.IOBase):
        self._stream = stream
        self._schemas: Dict[int, Schema] = {}
        self._channels: Dict[int, Channel] = {}
        self._spent = False

    def _check_spent(self):
        if self._spent:
            raise RuntimeError(
                "cannot use more than one query against a non-seeking data source"
            )
        self._spent = True

    def iter_messages(
        self,
        topics: Optional[Iterable[str]] = None,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
    ) -> Iterator[Tuple[Schema, Channel, Message]]:
        """iterates through the messages in an MCAP.
        :param topics: if not None, only messages from these topics will be returned.
        :param start_time: an integer nanosecond timestamp. if provided, messages logged before
            this timestamp are not included.
        :param end_time: an integer nanosecond timestamp. if provided, messages logged after
            this timestamp are not included.
        """
        self._check_spent()
        for record in StreamReader(self._stream).records:
            if isinstance(record, Schema):
                self._schemas[record.id] = record
            if isinstance(record, Channel):
                if record.schema_id not in self._schemas:
                    raise McapError(
                        f"no schema record found with id {record.schema_id}"
                    )
                self._channels[record.id] = record
            if isinstance(record, Message):
                if record.channel_id not in self._channels:
                    raise McapError(
                        f"no channel record found with id {record.channel_id}"
                    )
                channel = self._channels[record.channel_id]
                if topics is not None and channel.topic not in topics:
                    continue
                if start_time is not None and record.log_time < start_time:
                    continue
                if end_time is not None and record.log_time >= end_time:
                    continue
                schema = self._schemas[channel.schema_id]
                yield (schema, channel, record)

    def get_summary(self) -> Optional[Summary]:
        """returns a Summary object containing records from the (optional) summary section."""
        self._check_spent()
        return _read_summary_from_stream_reader(StreamReader(self._stream))

    def iter_attachments(self) -> Iterator[Attachment]:
        """iterates through attachment records in the MCAP."""
        self._check_spent()
        for record in StreamReader(self._stream).records:
            if isinstance(record, Attachment):
                yield record

    def iter_metadata(self) -> Iterator[Metadata]:
        """iterates through metadata records in the MCAP."""
        self._check_spent()
        for record in StreamReader(self._stream).records:
            if isinstance(record, Metadata):
                yield record


def make_reader(stream: io.IOBase) -> MCAPReader:
    if stream.seekable():
        return SeekingReader(stream)
    return NonSeekingReader(stream)
