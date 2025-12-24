""".. deprecated:: 0.7.0
    For similar functionality, instantiate :py:class:`mcap.reader.McapReader` with a
    :py:class:`mcap_ros1.decoder.DecoderFactory` instance, eg:

>>> from mcap.reader import make_reader
>>> from mcap_ros1.decoder import DecoderFactory
>>> reader = make_reader(open("ros1.mcap", "rb"), decoder_factories=[DecoderFactory()])
>>> for schema_, channel_, message_, ros1_msg in reader.iter_decoded_messages():
>>>     print(ros1_msg)
String(data="hello")
String(data="goodbye")
"""
import warnings
from datetime import datetime
from os import PathLike
from typing import IO, Any, Dict, Iterable, Iterator, Optional, Union

from mcap.reader import McapReader, make_reader
from mcap.records import Channel, Message, Schema

from .decoder import DecoderFactory

warnings.warn(__doc__, DeprecationWarning)


def read_ros1_messages(
    source: Union[str, bytes, "PathLike[str]", McapReader, IO[bytes]],
    topics: Union[Iterable[str], str, None] = None,
    start_time: Optional[Union[int, datetime]] = None,
    end_time: Optional[Union[int, datetime]] = None,
    log_time_order: bool = True,
    reverse: bool = False,
) -> Iterator["McapROS1Message"]:
    """
    High-level generator that reads ROS1 messages from an MCAP file.

    .. deprecated:: 0.7.0
      Use :py:class:`mcap_ros1.decoder.DecoderFactory` with :py:class:`mcap.reader.McapReader`
      instead.

    :param source: some source of MCAP file data. Supply a stream of bytes, an McapReader instance,
        or the path to a valid MCAP file in the filesystem.
    :param topics: an optional list of topics to read from the MCAP file.
    :param start_time: if not None, messages logged before this time will not be included.
    :param end_time: if not None, messages logged at this timestamp or after will not be included.
    :param log_time_order: if True, messages will be yielded in ascending log time order. If
        False, messages will be yielded in the order they appear in the MCAP file.
    :param reverse: if both ``log_time_order`` and ``reverse`` are True, messages will be
        yielded in descending log time order.
    :yields: an McapROS1Message instance for each ROS1 message in the MCAP file.

    .. note::
        this generator assumes the source MCAP conforms to the `ros1` MCAP profile.
        If you need to decode ROS1 messages from a file containing other encodings, use
        the :py:func:`mcap.reader.McapReader.iter_messages()` function to iterate through
        Message records in your MCAP, and decode the ROS1 messages with
        the :py:class:`mcap_ros1.decoder.Decoder` class.
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
        reader = make_reader(fd, decoder_factories=[DecoderFactory()])
    elif isinstance(source, McapReader):
        reader = source
    else:
        reader = make_reader(source, decoder_factories=[DecoderFactory()])

    try:
        for schema, channel, message, ros_msg in reader.iter_decoded_messages(
            topics, start_time, end_time, log_time_order, reverse
        ):
            assert schema is not None
            yield McapROS1Message(
                message=message, channel=channel, schema=schema, ros_msg=ros_msg
            )
    finally:
        if fd is not None:
            fd.close()


class McapROS1Message:
    """
    Contains a single ROS message and associated metadata.

    .. deprecated:: 0.7.0
      use the tuple yielded from :py:class:`mcap.reader.McapReader.iter_decoded_messages` instead.

    :ivar ros_msg: the decoded ROS1 message.
    :ivar sequence_count: the message sequence count if included in the MCAP, or 0 otherwise.
    :ivar topic: the topic that the message was published on.
    :ivar channel_metadata: the metadata associated with this ROS1 topic, if any.
    :ivar log_time_ns: the time this message was logged by the recorder, as a POSIX nanosecond
        timestamp.
    :ivar log_time_ns: the time this message was published, as a POSIX nanosecond
        timestamp.
    """

    __slots__ = (
        "ros_msg",
        "sequence_count",
        "topic",
        "channel_metadata",
        "log_time_ns",
        "publish_time_ns",
        "channel",
        "schema",
    )

    def __init__(
        self, ros_msg: Any, message: Message, channel: Channel, schema: Schema
    ):
        self.ros_msg: Any = ros_msg
        self.sequence_count: int = message.sequence
        self.topic: str = channel.topic
        self.channel_metadata: Dict[str, str] = channel.metadata
        self.log_time_ns: int = message.log_time
        self.publish_time_ns: int = message.publish_time
        self.channel: Channel = channel
        self.schema: Schema = schema

    @property
    def log_time(self) -> datetime:
        """The timestamp representing when this message was logged by the recorder."""
        return datetime.fromtimestamp(float(self.log_time_ns) / 1e9)

    @property
    def publish_time(self) -> datetime:
        """The timestamp representing when this message was published."""
        return datetime.fromtimestamp(float(self.publish_time_ns) / 1e9)
