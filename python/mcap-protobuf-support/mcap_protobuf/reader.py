"""the :py:mod:`mcap_protobuf.reader` module provides high-level protobuf message reading
capability. For more low-level control, consider using the underlying
:py:class:`mcap_protobuf.decoder.Decoder` and :py:class:`mcap.reader.McapReader` objects
directly.
"""
from typing import IO, Union, Any, Dict, Iterator, Optional, Iterable
from datetime import datetime
from os import PathLike

from .decoder import Decoder

from mcap.records import Message, Channel
from mcap.reader import McapReader, make_reader


def read_protobuf_messages(
    source: Union[str, bytes, PathLike, McapReader, IO[bytes]],
    topics: Optional[Iterable[str]] = None,
    start_time: Optional[Union[int, datetime]] = None,
    end_time: Optional[Union[int, datetime]] = None,
    log_time_order: bool = True,
    reverse: bool = False,
) -> Iterator["McapProtobufMessage"]:
    """High-level generator that reads protobuf messages out of an MCAP.

    :param source: some source of MCAP file data. Supply a stream of bytes, an McapReader instance,
        or the path to a valid MCAP file in the filesystem.
    :param topics: an optional list of topics to read from the MCAP file.
    :param start_time: if not None, messages logged before this time will not be included.
    :param end_time: if not None, messages logged at this timestamp or after will not be included.
    :param log_time_order: if True, messages will be yielded in ascending log time order. If
        False, messages will be yielded in the order they appear in the MCAP file.
    :param reverse: if both ``log_time_order`` and ``reverse`` are True, messages will be
        yielded in descending log time order.
    :yields: an McapProtobufMessage instance for each protobuf message in the MCAP file.

    .. note::
        this generator assumes the source MCAP contains only Protobuf-encoded messages.
        If you need to decode protobuf messages from a file containing other encodings, use
        the :py:func:`mcap.reader.McapReader.iter_messages()` function to iterate through
        Message records in your MCAP, and decode the protobuf messages with
        the :py:class:`mcap_protobuf.decoder.Decoder` class.
    """

    if start_time is not None and isinstance(start_time, datetime):
        start_time = int(start_time.timestamp() * 1e9)
    if end_time is not None and isinstance(end_time, datetime):
        end_time = int(end_time.timestamp() * 1e9)

    fd = None
    if (
        isinstance(source, PathLike)
        or isinstance(source, str)
        or isinstance(source, bytes)
    ):
        fd = open(source, "rb")
        reader = make_reader(fd)
    elif isinstance(source, McapReader):
        reader = source
    else:
        reader = make_reader(source)

    try:
        decoder = Decoder()
        for schema, channel, message in reader.iter_messages(
            topics, start_time, end_time, log_time_order, reverse
        ):
            yield McapProtobufMessage(
                proto_msg=decoder.decode(schema=schema, message=message),
                message=message,
                channel=channel,
            )
    finally:
        if fd is not None:
            fd.close()


class McapProtobufMessage:
    """Contains a single protobuf message and associated metadata from an MCAP file.

    :ivar proto_msg: the decoded protobuf message.
    :ivar sequence_count: the message sequence count if included in the MCAP, or 0 otherwise.
    :ivar topic: the topic that the message was published on.
    :ivar channel_metadata: the metadata associated with this protobuf topic, if any.
    :ivar log_time_ns: the time this message was logged by the recorder, as a POSIX nanosecond
        timestamp.
    :ivar log_time_ns: the time this message was published, as a POSIX nanosecond
        timestamp.
    """

    def __init__(self, proto_msg: Any, message: Message, channel: Channel):
        self.proto_msg = proto_msg
        self.sequence_count: int = message.sequence
        self.topic: str = channel.topic
        self.channel_metadata: Dict[str, str] = channel.metadata

        self.log_time_ns: int = message.log_time
        self.publish_time_ns: int = message.publish_time

    @property
    def log_time(self) -> datetime:
        """The timestamp representing when this message was logged by the recorder."""
        return datetime.fromtimestamp(float(self.log_time_ns) / 1e9)

    @property
    def publish_time(self) -> datetime:
        """The timestamp representing when this message was published."""
        return datetime.fromtimestamp(float(self.publish_time_ns) / 1e9)
