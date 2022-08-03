from io import BufferedWriter, BytesIO
from typing import IO, Any, Dict, Optional, Union
from mcap.mcap0.writer import Writer as McapWriter
import mcap
import time

from . import __version__


def _library_identifier():
    mcap_version = getattr(mcap, "__version__", "<=0.0.10")
    return f"mcap-ros1-support {__version__}; mcap {mcap_version}"


class Writer:
    def __init__(self, output: Union[str, IO[Any], BufferedWriter]):
        self.__writer = McapWriter(output=output)
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
        :param log_time: The time at which the message was logged.
            Will default to the current time if not specified.
        :param publish_time: The time at which the message was published.
            Will default to the current time if not specified.
        :param sequence: An optional sequence number.
        """
        if message._type not in self.__schema_ids.keys():
            schema_id = self.__writer.register_schema(
                name=message._type,
                data=message.__class__._full_text.encode(),
                encoding="ros1msg",
            )
            self.__schema_ids[message._type] = schema_id
        schema_id = self.__schema_ids[message._type]

        if topic not in self.__channel_ids.keys():
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
