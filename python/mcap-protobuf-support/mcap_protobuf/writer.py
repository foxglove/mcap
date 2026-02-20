import time
from io import BufferedWriter
from typing import IO, Any, Dict, Optional, Tuple, Union

import mcap
from mcap.well_known import MessageEncoding
from mcap.writer import CompressionType
from mcap.writer import Writer as McapWriter

from . import __version__
from .schema import register_schema


def _library_identifier():
    """the default value written into MCAP headers by this library."""
    mcap_version = getattr(mcap, "__version__", "<=0.0.10")
    return f"python mcap-protobuf-support {__version__}; mcap {mcap_version}"


class Writer:
    """Writer provides a higher-level abstraction for writing Protobuf messages to an
    MCAP file.
    """

    def __init__(
        self,
        output: Union[str, IO[Any], BufferedWriter],
        chunk_size: int = 1024 * 1024,
        compression: CompressionType = CompressionType.ZSTD,
        enable_crcs: bool = True,
    ):
        self._writer = McapWriter(
            output,
            chunk_size=chunk_size,
            compression=compression,
            enable_crcs=enable_crcs,
        )
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
        msg_typename: str = type(message).DESCRIPTOR.full_name
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

    def add_metadata(self, name: str, data: Dict[str, str]):
        """Writes metadata to an MCAP file.

        :param name: the name of the metadata.
        :param data: a dictionary of metadata key-value pairs.
        """
        self._writer.add_metadata(name, data)

    def add_attachment(
        self, create_time: int, log_time: int, name: str, media_type: str, data: bytes
    ):
        """Writes an attachment to an MCAP file.

        :param log_time: Time at which the attachment was recorded.
        :param create_time: Time at which the attachment was created. If not available,
            must be set to zero.
        :param name: Name of the attachment, e.g "scene1.jpg".
        :param media_type: Media Type (e.g "text/plain").
        :param data: Attachment data.
        """
        self._writer.add_attachment(create_time, log_time, name, media_type, data)

    def finish(self):
        """Writes the index and footer to the MCAP file."""
        if not self._finished:
            self._writer.finish()
        self._finished = True

    def __enter__(self):
        return self

    def __exit__(self, exc_: Any, exc_type_: Any, tb_: Any):
        self.finish()
