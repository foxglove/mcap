"""the :py:mod:`mcap_ros1.reader` module is deprecated. For similar functionality,
instantiate the :py:class:`mcap.reader.McapReader` with a
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
from typing import IO, Any, Iterable, Iterator, Optional, Union

from mcap.reader import McapReader, make_reader
from mcap.records import Channel, Message, Schema

from .decoder import DecoderFactory

warnings.warn(__doc__, DeprecationWarning)


def read_ros2_messages(
    source: Union[str, bytes, "PathLike[str]", McapReader, IO[bytes]],
    topics: Optional[Iterable[str]] = None,
    start_time: Optional[Union[int, datetime]] = None,
    end_time: Optional[Union[int, datetime]] = None,
    log_time_order: bool = True,
    reverse: bool = False,
) -> Iterator["McapROS2Message"]:
    """High-level generator that reads ROS2 messages from an MCAP file.

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
        this generator assumes the source MCAP conforms to the `ros2` MCAP profile.
        If you need to decode ROS2 messages from a file containing other encodings, use
        the :py:func:`mcap.reader.McapReader.iter_messages()` function to iterate through
        Message records in your MCAP, and decode the ROS2 messages with
        the :py:class:`mcap_ros2.decoder.Decoder` class.
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
        for schema, channel, message, ros2_msg in reader.iter_decoded_messages(
            topics, start_time, end_time, log_time_order, reverse
        ):
            assert schema is not None
            yield McapROS2Message(
                ros_msg=ros2_msg,
                message=message,
                channel=channel,
                schema=schema,
            )
    finally:
        if fd is not None:
            fd.close()


class McapROS2Message:
    """Contains a single ROS2 message and associated metadata.

    :ivar ros_msg: the decoded ROS2 message.
    :ivar sequence_count: the message sequence count if included in the MCAP, or 0 otherwise.
    :ivar log_time_ns: the time this message was logged by the recorder, as a POSIX nanosecond
        timestamp.
    :ivar log_time_ns: the time this message was published, as a POSIX nanosecond
        timestamp.
    :ivar channel: the MCAP Channel record referenced by the Message record
    :ivar schema: the MCAP Schema record referenced by the Channel record
    """

    __slots__ = (
        "ros_msg",
        "sequence_count",
        "log_time_ns",
        "publish_time_ns",
        "channel",
        "schema",
    )

    def __init__(
        self, ros_msg: Any, message: Message, channel: Channel, schema: Schema
    ):
        """
        Construct a new McapROS2Message instance.

        :param ros_msg: the decoded ROS2 message.
        :param message: the MCAP Message record that contains the ROS2 message.
        :param channel: the MCAP Channel record referenced by the Message record.
        """
        self.ros_msg = ros_msg
        self.sequence_count: int = message.sequence
        self.log_time_ns: int = message.log_time
        self.publish_time_ns: int = message.publish_time
        self.channel = channel
        self.schema = schema

    @property
    def log_time(self) -> datetime:
        """Return the timestamp representing when this message was logged by the recorder."""
        return datetime.fromtimestamp(float(self.log_time_ns) / 1e9)

    @property
    def publish_time(self) -> datetime:
        """Return the timestamp representing when this message was published."""
        return datetime.fromtimestamp(float(self.publish_time_ns) / 1e9)
