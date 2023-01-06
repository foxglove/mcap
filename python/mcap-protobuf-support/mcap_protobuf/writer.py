import time
from typing import Optional, Dict, Any, Tuple

from mcap.writer import Writer as McapWriter
from mcap.well_known import MessageEncoding
import mcap

from .schema import register_schema
from . import __version__


def _library_identifier():
    """the default value written into MCAP headers by this library."""
    mcap_version = getattr(mcap, "__version__", "<=0.0.10")
    return f"python mcap-protobuf-support {__version__}; mcap {mcap_version}"


class Writer:
    """Writer provides a higher-level abstraction for writing Protobuf messages to an
    MCAP file.
    """

    def __init__(self, output):
        self._writer = McapWriter(output)
        self._schemas: Dict[str, Tuple[int, str]] = {}
        self._channels: Dict[str, int] = {}
        self._finished = False
        self._writer.start(library=_library_identifier())

    def write_message(
        self,
        topic: str,
        message: Any,
        log_time: Optional[int] = None,
        publish_time: Optional[int] = None,
        sequence: int = 0,
    ):
        """Writes a message to an MCAP file.

        :param topic: the topic that this message was originally published on.
        :param message: a Protobuf object to write into the MCAP.
        :param log_time: unix nanosecond timestamp of when this message was written to the MCAP.
        :param publish_time: unix nanosecond timestamp of when this message was originally
            published.
        :param sequence: an optional sequence count for messages on this topic.
        """
        msg_typename = type(message).DESCRIPTOR.full_name
        if topic in self._channels:
            channel_id = self._channels[topic]
            schema_id, schema_name = self._schemas[topic]
            if msg_typename != schema_name:
                raise ValueError(
                    f"topic '{topic}' has type {schema_name}, cannot write a {msg_typename}"
                )
        else:
            schema_id = register_schema(self._writer, type(message))
            self._schemas[topic] = (schema_id, msg_typename)
            channel_id = self._writer.register_channel(
                topic=topic,
                message_encoding=MessageEncoding.Protobuf,
                schema_id=schema_id,
            )
            self._channels[topic] = channel_id
        if log_time is None:
            log_time = time.time_ns()
        if publish_time is None:
            publish_time = time.time_ns()
        self._writer.add_message(
            channel_id=channel_id,
            log_time=log_time,
            data=message.SerializeToString(),  # type: ignore
            publish_time=publish_time,
            sequence=sequence,
        )

    def finish(self):
        """Writes the index and footer to the MCAP file."""
        if not self._finished:
            self._writer.finish()
        self._finished = True

    def __enter__(self):
        return self

    def __exit__(self, exc_, exc_type_, tb_):
        self.finish()
