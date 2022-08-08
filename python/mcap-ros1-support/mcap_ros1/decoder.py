from typing import Any, Dict, Tuple, Iterator

from genpy import dynamic  # type: ignore
from mcap.mcap0.exceptions import McapError
from mcap.mcap0.records import Channel, Message, Schema
from mcap.mcap0.well_known import SchemaEncoding


def decode_ros1_messages(
    message_iterator: Iterator[Tuple[Schema, Channel, Message]],
    ignore_non_ros1_messages: bool = False,
) -> Iterator[Tuple[str, Any, int]]:
    """takes a stream of messages from a McapReader, and automatically parses the ROS 1
    messages using the definitions in the MCAP.

    :param message_iterator: an iterator of Schema, Channel, and Message records.
        `McapReader.iter_messages()` is a convenient way to get this parameter.
    :param ignore_non_ros1_messages: if True, ignores non-ros1 messages in the MCAP rather
        than raising an exception.
    :returns: an iterator of (topic, ros1_message, log_time) tuples. Timestamps are provided
        as a nanosecond unix timestamp.
    """
    generated: Dict[str, Any] = {}
    for schema, channel, record in message_iterator:
        if schema.encoding != SchemaEncoding.ROS1:
            if ignore_non_ros1_messages:
                continue
            raise McapError(f"can't decode schema with encoding {schema.encoding}")
        generated_type = generated.get(schema.name)
        if generated_type is None:
            type_dict = dynamic.generate_dynamic(  # type: ignore
                schema.name, schema.data.decode()
            )
            generated_type = type_dict[schema.name]
            generated[schema.name] = generated_type

        message = generated_type()
        message.deserialize(record.data)
        yield channel.topic, message, record.log_time
