import time
from io import BufferedWriter
from typing import IO, Any, Dict, Optional, Union

import mcap
from mcap.exceptions import McapError
from mcap.records import Schema
from mcap.well_known import SchemaEncoding
from mcap.writer import CompressionType
from mcap.writer import Writer as McapWriter

from . import __version__
from ._dynamic import EncoderFunction, serialize_dynamic


class McapROS2WriteError(McapError):
    """Raised if a ROS2 message cannot be encoded to CDR with a given schema."""

    pass


def _library_identifier():
    mcap_version = getattr(mcap, "__version__", "<=0.0.10")
    return f"mcap-ros2-support {__version__}; mcap {mcap_version}"


class Writer:
    def __init__(
        self,
        output: Union[str, IO[Any], BufferedWriter],
        chunk_size: int = 1024 * 1024,
        compression: CompressionType = CompressionType.ZSTD,
        enable_crcs: bool = True,
    ):
        self._writer = McapWriter(
            output=output,
            chunk_size=chunk_size,
            compression=compression,
            enable_crcs=enable_crcs,
        )
        self._encoders: Dict[int, EncoderFunction] = {}
        self._channel_ids: Dict[str, int] = {}
        self._writer.start(profile="ros2", library=_library_identifier())
        self._finished = False

    def finish(self):
        """Finishes writing to the MCAP stream. This must be called before the stream is closed."""
        if not self._finished:
            self._writer.finish()
            self._finished = True

    def register_msgdef(self, datatype: str, msgdef_text: str) -> Schema:
        """Write a Schema record for a ROS2 message definition."""
        msgdef_data = msgdef_text.encode()
        schema_id = self._writer.register_schema(
            datatype, SchemaEncoding.ROS2, msgdef_data
        )
        return Schema(
            id=schema_id, name=datatype, encoding=SchemaEncoding.ROS2, data=msgdef_data
        )

    def write_message(
        self,
        topic: str,
        schema: Schema,
        message: Any,
        log_time: Optional[int] = None,
        publish_time: Optional[int] = None,
        sequence: int = 0,
    ):
        """
        Write a ROS2 Message record, automatically registering a channel as needed.

        :param topic: The topic of the message.
        :param message: The message to write.
        :param log_time: The time at which the message was logged as a nanosecond UNIX timestamp.
            Will default to the current time if not specified.
        :param publish_time: The time at which the message was published as a nanosecond UNIX
            timestamp. Will default to ``log_time`` if not specified.
        :param sequence: An optional sequence number.
        """
        encoder = self._encoders.get(schema.id)
        if encoder is None:
            if schema.encoding != SchemaEncoding.ROS2:
                raise McapROS2WriteError(
                    f'can\'t parse schema with encoding "{schema.encoding}"'
                )
            type_dict = serialize_dynamic(  # type: ignore
                schema.name, schema.data.decode()
            )
            # Check if schema.name is in type_dict
            if schema.name not in type_dict:
                raise McapROS2WriteError(f'schema parsing failed for "{schema.name}"')
            encoder = type_dict[schema.name]
            self._encoders[schema.id] = encoder

        if topic not in self._channel_ids:
            channel_id = self._writer.register_channel(
                topic=topic,
                message_encoding="cdr",
                schema_id=schema.id,
            )
            self._channel_ids[topic] = channel_id
        channel_id = self._channel_ids[topic]

        data = encoder(message)

        if log_time is None:
            log_time = time.time_ns()
        if publish_time is None:
            publish_time = log_time
        self._writer.add_message(
            channel_id=channel_id,
            log_time=log_time,
            publish_time=publish_time,
            sequence=sequence,
            data=data,
        )

    def __enter__(self):
        """Context manager support."""
        return self

    def __exit__(self, exc_: Any, exc_type_: Any, tb_: Any):
        """Call finish() on exit."""
        self.finish()
