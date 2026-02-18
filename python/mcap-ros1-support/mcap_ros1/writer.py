import time
from io import BufferedWriter, BytesIO
from typing import IO, Any, Dict, Optional, Union

import mcap
from mcap.writer import CompressionType
from mcap.writer import Writer as McapWriter

from . import __version__


def _library_identifier():
    mcap_version = getattr(mcap, "__version__", "<=0.0.10")
    return f"mcap-ros1-support {__version__}; mcap {mcap_version}"


class Writer:
    def __init__(
        self,
        output: Union[str, IO[Any], BufferedWriter],
        chunk_size: int = 1024 * 1024,
        compression: CompressionType = CompressionType.ZSTD,
        enable_crcs: bool = True,
    ):
        self.__writer = McapWriter(
            output=output,
            chunk_size=chunk_size,
            compression=compression,
            enable_crcs=enable_crcs,
        )
        self.__schema_ids: Dict[str, int] = {}
        self.__channel_ids: Dict[str, int] = {}
        self.__writer.start(profile="ros1", library=_library_identifier())
        self.__finished = False

    def finish(self):
        """
        Finishes writing to the MCAP stream. This must be called before the stream is closed.
        """
        if not self.__finished:
            self.__writer.finish()
            self.__finished = True

    def write_message(
        self,
        topic: str,
        message: Any,
        log_time: Optional[int] = None,
        publish_time: Optional[int] = None,
        sequence: int = 0,
    ):
        """
        Writes a message to the MCAP stream, automatically registering schemas and channels as
        needed.

        :param topic: The topic of the message.
        :param message: The message to write.
        :param log_time: The time at which the message was logged as a nanosecond UNIX timestamp.
            Will default to the current time if not specified.
        :param publish_time: The time at which the message was published as a nanosecond UNIX
            timestamp. Will default to ``log_time`` if not specified.
        :param sequence: An optional sequence number.
        """
        if message._type not in self.__schema_ids:
            schema_id = self.__writer.register_schema(
                name=message._type,
                data=message.__class__._full_text.encode(),
                encoding="ros1msg",
            )
            self.__schema_ids[message._type] = schema_id
        schema_id = self.__schema_ids[message._type]

        if topic not in self.__channel_ids:
            channel_id = self.__writer.register_channel(
                topic=topic,
                message_encoding="ros1",
                schema_id=schema_id,
            )
            self.__channel_ids[topic] = channel_id
        channel_id = self.__channel_ids[topic]

        buffer = BytesIO()
        message.serialize(buffer)
        if log_time is None:
            log_time = time.time_ns()
        if publish_time is None:
            publish_time = log_time
        self.__writer.add_message(
            channel_id=channel_id,
            log_time=log_time,
            publish_time=publish_time,
            sequence=sequence,
            data=buffer.getvalue(),
        )

    def add_metadata(self, name: str, metadata: Dict[str, str]):
        """Writes metadata to an MCAP file.

        :param name: the name of the metadata.
        :param metadata: a dictionary of metadata key-value pairs.
        """
        self.__writer.add_metadata(name, metadata)

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
        self.__writer.add_attachment(create_time, log_time, name, media_type, data)

    def __enter__(self):
        return self

    def __exit__(self, exc_: Any, exc_type_: Any, tb_: Any):
        self.finish()
