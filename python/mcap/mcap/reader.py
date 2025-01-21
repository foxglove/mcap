""" High-level classes for reading content out of MCAP data sources.
"""
import io
from abc import ABC, abstractmethod
from typing import (
    IO,
    Any,
    Callable,
    Dict,
    Iterable,
    Iterator,
    List,
    NamedTuple,
    Optional,
    Tuple,
)

from ._message_queue import make_message_queue
from .data_stream import ReadDataStream, RecordBuilder
from .decoder import DecoderFactory
from .exceptions import DecoderNotFoundError, McapError
from .records import (
    Attachment,
    AttachmentIndex,
    Channel,
    Chunk,
    ChunkIndex,
    Footer,
    Header,
    McapRecord,
    Message,
    Metadata,
    MetadataIndex,
    Schema,
    Statistics,
)
from .stream_reader import MAGIC_SIZE, StreamReader, breakup_chunk, read_magic
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
            if record.summary_start == 0:
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
    :param end_time: if not None, messages at or after this unix timestamp are not included.
    """
    out: List[ChunkIndex] = []
    for chunk_index in summary.chunk_indexes:
        if start_time is not None and chunk_index.message_end_time < start_time:
            continue
        if end_time is not None and chunk_index.message_start_time >= end_time:
            continue
        for channel_id in chunk_index.message_index_offsets.keys():
            if topics is None or summary.channels[channel_id].topic in topics:
                out.append(chunk_index)
                break
    return out


class DecodedMessageTuple(NamedTuple):
    """Yielded from every iteration of :py:meth:`~mcap.reader.McapReader.iter_decoded_messages`."""

    schema: Optional[Schema]
    channel: Channel
    message: Message
    decoded_message: Any


class McapReader(ABC):
    """Reads data out of an MCAP file, using the summary section where available to efficiently
    read only the parts of the file that are needed.

    :param decoder_factories: An iterable of :py:class:`~mcap.decoder.DecoderFactory`
        instances which can provide decoding functionality to
        :py:meth:`~mcap.reader.McapReader.iter_decoded_messages`.
    """

    def __init__(
        self,
        decoder_factories: Iterable[DecoderFactory] = (),
    ):
        self._decoder_factories = decoder_factories
        self._decoders: dict[int, Callable[[bytes], Any]] = {}

    @abstractmethod
    def iter_messages(
        self,
        topics: Optional[Iterable[str]] = None,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
        log_time_order: bool = True,
        reverse: bool = False,
    ) -> Iterator[Tuple[Optional[Schema], Channel, Message]]:
        """iterates through the messages in an MCAP.

        :param topics: if not None, only messages from these topics will be returned.
        :param start_time: an integer nanosecond timestamp. if provided, messages logged before this
            timestamp are not included.
        :param end_time: an integer nanosecond timestamp. if provided, messages logged at or after
            this timestamp are not included.
        :param log_time_order: if True, messages will be yielded in ascending log time order. If
            False, messages will be yielded in the order they appear in the MCAP file.
        :param reverse: if both ``log_time_order`` and ``reverse`` are True, messages will be
            yielded in descending log time order.
        """
        raise NotImplementedError()

    def iter_decoded_messages(
        self,
        topics: Optional[Iterable[str]] = None,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
        log_time_order: bool = True,
        reverse: bool = False,
    ) -> Iterator[DecodedMessageTuple]:
        """iterates through messages in an MCAP, decoding their contents.

        :param topics: if not None, only messages from these topics will be returned.
        :param start_time: an integer nanosecond timestamp. if provided, messages logged before this
            timestamp are not included.
        :param end_time: an integer nanosecond timestamp. if provided, messages logged at or after
            this timestamp are not included.
        :param log_time_order: if True, messages will be yielded in ascending log time order. If
            False, messages will be yielded in the order they appear in the MCAP file.
        :param reverse: if both ``log_time_order`` and ``reverse`` are True, messages will be
            yielded in descending log time order.
        """
        message_iterator = self.iter_messages(
            topics, start_time, end_time, log_time_order, reverse
        )

        def decoded_message(
            schema: Optional[Schema], channel: Channel, message: Message
        ) -> Any:
            decoder = self._decoders.get(message.channel_id)
            if decoder is not None:
                return decoder(message.data)
            for factory in self._decoder_factories:
                decoder = factory.decoder_for(channel.message_encoding, schema)
                if decoder is not None:
                    self._decoders[message.channel_id] = decoder
                    return decoder(message.data)

            raise DecoderNotFoundError(
                f"no decoder factory supplied for message encoding {channel.message_encoding}, "
                f"schema {schema}"
            )

        for schema, channel, message in message_iterator:
            yield DecodedMessageTuple(
                schema, channel, message, decoded_message(schema, channel, message)
            )

    @abstractmethod
    def get_header(self) -> Header:
        """Reads the Header records from the beginning of the MCAP file."""
        raise NotImplementedError()

    @abstractmethod
    def get_summary(self) -> Optional[Summary]:
        """Reads the (optional) summary section from the MCAP file."""
        raise NotImplementedError()

    @abstractmethod
    def iter_attachments(self) -> Iterator[Attachment]:
        """Iterates through attachment records in the MCAP."""
        raise NotImplementedError()

    @abstractmethod
    def iter_metadata(self) -> Iterator[Metadata]:
        """Iterates through metadata records in the MCAP."""
        raise NotImplementedError()


def make_reader(
    stream: IO[bytes],
    validate_crcs: bool = False,
    decoder_factories: Iterable[DecoderFactory] = (),
) -> McapReader:
    """constructs the appropriate McapReader implementation for this data source."""
    if stream.seekable():
        return SeekingReader(
            stream, validate_crcs=validate_crcs, decoder_factories=decoder_factories
        )
    return NonSeekingReader(
        stream, validate_crcs=validate_crcs, decoder_factories=decoder_factories
    )


class SeekingReader(McapReader):
    """an McapReader for reading out of seekable data sources.

    :param stream: a file-like object for reading the source data from.
    :param validate_crcs: if ``True``, will validate Chunk CRCs for any chunks read. This class
        does not validate the data section CRC in the DataEnd record because it is designed not to
        read the entire data section when reading messages. To read messages while validating the
        data section CRC, use :py:class:`NonSeekingReader`.
    :param decoder_factories: An iterable of :py:class:`~mcap.decoder.DecoderFactory`
        instances which can provide decoding functionality to
        :py:meth:`~mcap.reader.McapReader.iter_decoded_messages`.
    :param record_size_limit: An upper bound to the size of MCAP records that this reader will
        attempt to load in bytes, defaulting to 4 GiB. If this reader encounters a record with a
        greater length, it will throw an :py:class:`~mcap.exceptions.RecordLengthLimitExceeded`
        error.  Setting to ``None`` removes the limit, but can allow corrupted MCAP files to trigger
        a `MemoryError` exception.
    """

    def __init__(
        self,
        stream: IO[bytes],
        validate_crcs: bool = False,
        decoder_factories: Iterable[DecoderFactory] = (),
        record_size_limit: Optional[int] = 4 * 2**30,
    ):
        super().__init__(decoder_factories=decoder_factories)
        read_magic(ReadDataStream(stream, calculate_crc=False))
        self._stream = stream
        self._validate_crcs = validate_crcs
        self._summary: Optional[Summary] = None
        self._record_size_limit = record_size_limit

    def iter_messages(
        self,
        topics: Optional[Iterable[str]] = None,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
        log_time_order: bool = True,
        reverse: bool = False,
    ) -> Iterator[Tuple[Optional[Schema], Channel, Message]]:
        """iterates through the messages in an MCAP.

        :param topics: if not None, only messages from these topics will be returned.
        :param start_time: an integer nanosecond timestamp. if provided, messages logged before this
            timestamp are not included.
        :param end_time: an integer nanosecond timestamp. if provided, messages logged at or after
            this timestamp are not included.
        :param log_time_order: if True, messages will be yielded in ascending log time order. If
            False, messages will be yielded in the order they appear in the MCAP file.
        :param reverse: if both ``log_time_order`` and ``reverse`` are True, messages will be
            yielded in descending log time order.

        """
        summary = self.get_summary()
        if summary is None or len(summary.chunk_indexes) == 0:
            # No chunk indices available, so there is no index to search for messages.
            # use a non-seeking reader to read linearly through the stream.
            self._stream.seek(0, io.SEEK_SET)
            yield from NonSeekingReader(self._stream).iter_messages(
                topics, start_time, end_time, log_time_order
            )
            return

        message_queue = make_message_queue(
            log_time_order=log_time_order, reverse=reverse
        )
        for chunk_index in _chunks_matching_topics(
            summary, topics, start_time, end_time
        ):
            message_queue.push(chunk_index)
        while message_queue:
            next_item = message_queue.pop()
            if isinstance(next_item, ChunkIndex):
                self._stream.seek(next_item.chunk_start_offset + 1 + 8, io.SEEK_SET)
                chunk = Chunk.read(ReadDataStream(self._stream))
                for index, record in enumerate(
                    breakup_chunk(chunk, validate_crc=self._validate_crcs)
                ):
                    if isinstance(record, Message):
                        channel = summary.channels[record.channel_id]
                        if topics is not None and channel.topic not in topics:
                            continue
                        if start_time is not None and record.log_time < start_time:
                            continue
                        if end_time is not None and record.log_time >= end_time:
                            continue
                        if channel.schema_id == 0:
                            schema = None
                        else:
                            schema = summary.schemas[channel.schema_id]
                        message_queue.push(
                            (
                                (schema, channel, record),
                                next_item.chunk_start_offset,
                                index,
                            )
                        )
            else:
                yield next_item[0]

    def get_header(self) -> Header:
        """Reads the Header record from the beginning of the MCAP file."""
        self._stream.seek(0)
        header = next(
            StreamReader(
                self._stream,
                skip_magic=False,
                record_size_limit=self._record_size_limit,
            ).records
        )
        if not isinstance(header, Header):
            raise McapError(
                f"expected header at beginning of MCAP file, found {type(header)}"
            )
        return header

    def get_summary(self) -> Optional[Summary]:
        """Reads the (optional) summary section from the MCAP file."""
        if self._summary is not None:
            return self._summary
        self._stream.seek(-(FOOTER_SIZE + MAGIC_SIZE), io.SEEK_END)
        footer = next(
            StreamReader(
                self._stream,
                skip_magic=True,
                record_size_limit=self._record_size_limit,
            ).records
        )
        if not isinstance(footer, Footer):
            raise McapError(
                f"expected footer at end of MCAP file, found {type(footer)}"
            )
        if footer.summary_start == 0:
            return None
        self._stream.seek(footer.summary_start, io.SEEK_SET)
        self._summary = _read_summary_from_stream_reader(
            StreamReader(
                self._stream, skip_magic=True, record_size_limit=self._record_size_limit
            )
        )
        return self._summary

    def iter_attachments(self) -> Iterator[Attachment]:
        """Iterates through attachment records in the MCAP."""
        summary = self.get_summary()
        if summary is None:
            # no index available, use a non-seeking reader to read linearly through the stream.
            self._stream.seek(0, io.SEEK_SET)
            yield from NonSeekingReader(self._stream).iter_attachments()
            return
        for attachment_index in summary.attachment_indexes:
            self._stream.seek(attachment_index.offset)
            record = next(
                StreamReader(
                    self._stream,
                    skip_magic=True,
                    record_size_limit=self._record_size_limit,
                ).records
            )
            if isinstance(record, Attachment):
                yield record
            else:
                raise McapError(f"expected attachment record, got {type(record)}")

    def iter_metadata(self) -> Iterator[Metadata]:
        """Iterates through metadata records in the MCAP."""
        summary = self.get_summary()
        if summary is None:
            # fall back to a non-seeking reader
            self._stream.seek(0, io.SEEK_SET)
            yield from NonSeekingReader(self._stream).iter_metadata()
            return
        for metadata_index in summary.metadata_indexes:
            self._stream.seek(metadata_index.offset)
            record = next(
                StreamReader(
                    self._stream,
                    skip_magic=True,
                    record_size_limit=self._record_size_limit,
                ).records
            )
            if isinstance(record, Metadata):
                yield record
            else:
                raise McapError(f"expected attachment record, got {type(record)}")


class NonSeekingReader(McapReader):
    """an McapReader for reading out of non-seekable data sources, such as a pipe or socket.

    :param stream: a file-like object for reading the source data from.
    :param validate_crcs: if ``True``, will validate chunk and data section CRC values.
    :param decoder_factories: An iterable of :py:class:`~mcap.decoder.DecoderFactory`
        instances which can provide decoding functionality to
        :py:meth:`~mcap.reader.McapReader.iter_decoded_messages`.
    :param record_size_limit: An upper bound to the size of MCAP records that this reader will
        attempt to load in bytes, defaulting to 4 GiB. If this reader encounters a record with a
        greater length, it will throw an :py:class:`~mcap.exceptions.RecordLengthLimitExceeded`
        error.  Setting to ``None`` removes the limit, but can allow corrupted MCAP files to trigger
        a `MemoryError` exception.
    """

    def __init__(
        self,
        stream: IO[bytes],
        validate_crcs: bool = False,
        decoder_factories: Iterable[DecoderFactory] = (),
        record_size_limit: Optional[int] = 4 * 2**30,
    ):
        super().__init__(decoder_factories=decoder_factories)
        self._stream_reader = StreamReader(
            stream,
            validate_crcs=validate_crcs,
            record_size_limit=record_size_limit,
        )
        self._schemas: Dict[int, Schema] = {}
        self._channels: Dict[int, Channel] = {}
        self._spent: bool = False

    def _check_spent(self):
        if self._spent:
            raise RuntimeError(
                "cannot use more than one query against a non-seeking data source"
            )
        self._spent = True

    def get_header(self) -> Header:
        """Reads the Header record from the beginning of the MCAP file."""
        self._check_spent()
        header = next(self._stream_reader.records)
        if not isinstance(header, Header):
            raise McapError(
                f"expected header at beginning of MCAP file, found {type(header)}"
            )
        return header

    def iter_messages(
        self,
        topics: Optional[Iterable[str]] = None,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
        log_time_order: bool = True,
        reverse: bool = False,
    ) -> Iterator[Tuple[Optional[Schema], Channel, Message]]:
        """Iterates through the messages in an MCAP.

        :param topics: if not None, only messages from these topics will be returned.
        :param start_time: an integer nanosecond timestamp. if provided, messages logged before this
            timestamp are not included.
        :param end_time: an integer nanosecond timestamp. if provided, messages logged at or after
            this timestamp are not included.
        :param log_time_order: if True, messages will be yielded in ascending log time order. If
            False, messages will be yielded in the order they appear in the MCAP file.
        :param reverse: if both ``log_time_order`` and ``reverse`` are True, messages will be
            yielded in descending log time order.

        .. warning::
            setting log_time_order to True on a non-seekable stream will cause the entire content
            of the MCAP to be loaded into memory.
        """
        if not log_time_order:
            for t in self._iter_messages_internal(topics, start_time, end_time):
                yield t
        else:
            for t in sorted(
                self._iter_messages_internal(topics, start_time, end_time),
                key=lambda tup: tup[2].log_time,
                reverse=reverse,
            ):
                yield t

    def _iter_messages_internal(
        self,
        topics: Optional[Iterable[str]] = None,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
    ) -> Iterator[Tuple[Optional[Schema], Channel, Message]]:
        self._check_spent()
        for record in self._stream_reader.records:
            if isinstance(record, Schema):
                self._schemas[record.id] = record
            if isinstance(record, Channel):
                if record.schema_id != 0 and record.schema_id not in self._schemas:
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
                if channel.schema_id == 0:
                    schema = None
                else:
                    schema = self._schemas[channel.schema_id]
                yield (schema, channel, record)

    def get_summary(self) -> Optional[Summary]:
        """Returns a Summary object containing records from the (optional) summary section."""
        self._check_spent()
        return _read_summary_from_stream_reader(self._stream_reader)

    def iter_attachments(self) -> Iterator[Attachment]:
        """Iterates through attachment records in the MCAP."""
        self._check_spent()
        for record in self._stream_reader.records:
            if isinstance(record, Attachment):
                yield record

    def iter_metadata(self) -> Iterator[Metadata]:
        """Iterates through metadata records in the MCAP."""
        self._check_spent()
        for record in self._stream_reader.records:
            if isinstance(record, Metadata):
                yield record
