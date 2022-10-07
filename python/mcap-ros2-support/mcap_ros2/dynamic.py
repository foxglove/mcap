"""ROS2 message definition parsing and message deserialization."""

import re
from types import SimpleNamespace
from typing import Any, Callable, Dict, List

from .cdr import CdrReader
from .vendor.rosidl_adapter.parser import (
    Field,
    MessageSpecification,
    Type,
    parse_message_string,
)

# cSpell:words wstring msgdefs

Message = SimpleNamespace
DecoderFunction = Callable[[bytes], Message]


def _parseWstring(reader: CdrReader) -> str:
    raise NotImplementedError("wstring parsing is not implemented")


def _parseWstringArray(reader: CdrReader, array_length: int) -> List[str]:
    raise NotImplementedError("wstring[] parsing is not implemented")


FIELD_PARSERS = {
    "bool": CdrReader.boolean,
    "byte": CdrReader.uint8,
    "char": CdrReader.int8,
    "float32": CdrReader.float32,
    "float64": CdrReader.float64,
    "int8": CdrReader.int8,
    "uint8": CdrReader.uint8,
    "int16": CdrReader.int16,
    "uint16": CdrReader.uint16,
    "int32": CdrReader.int32,
    "uint32": CdrReader.uint32,
    "int64": CdrReader.int64,
    "uint64": CdrReader.uint64,
    "string": CdrReader.string,
    "wstring": _parseWstring,
}

ARRAY_PARSERS = {
    "bool": CdrReader.boolean_array,
    "byte": CdrReader.uint8_array,
    "char": CdrReader.int8_array,
    "float32": CdrReader.float32_array,
    "float64": CdrReader.float64_array,
    "int8": CdrReader.int8_array,
    "uint8": CdrReader.uint8_array,
    "int16": CdrReader.int16_array,
    "uint16": CdrReader.uint16_array,
    "int32": CdrReader.int32_array,
    "uint32": CdrReader.uint32_array,
    "int64": CdrReader.int64_array,
    "uint64": CdrReader.uint64_array,
    "string": CdrReader.string_array,
    "wstring": _parseWstringArray,
}

TimeDefinition = MessageSpecification(
    "builtin_interfaces",
    "Time",
    [Field(Type("uint32"), "seconds"), Field(Type("uint32"), "nanoseconds")],
    [],
)


def generate_dynamic(schema_name: str, schema_text: str) -> Dict[str, DecoderFunction]:
    """Convert a ROS2 concatenated message definition into a dictionary of message parsers.

    Modeled after the `generate_dynamic` function in ROS1 `genpy.dynamic`.

    :param schema_name: The name of the schema defined in `schema_text`.
    :param schema_text: The schema text to use for deserializing the message payload.
    :return: A dictionary containing the generated message.
    """
    msgdefs: Dict[str, MessageSpecification] = {
        "builtin_interfaces/Time": TimeDefinition,
        "builtin_interfaces/Duration": TimeDefinition,
    }
    generators: Dict[str, DecoderFunction] = {}
    cur_schema_name = schema_name

    # Split schema_text by separator lines containing at least 3 = characters
    # (e.g. "===") using a regular expression
    for cur_schema_text in re.split(r"^={3,}$", schema_text, flags=re.MULTILINE):
        cur_schema_text = cur_schema_text.strip()
        if not cur_schema_text:
            continue
        # Check for a "MSG: pkg_name/msg_name" line
        match = re.match(r"^MSG:\s+(\S+)$", cur_schema_text, flags=re.MULTILINE)
        if match:
            cur_schema_name = match.group(1)
            # Remove this line from the message definition
            cur_schema_text = re.sub(
                r"^MSG:\s+(\S+)$", "", cur_schema_text, flags=re.MULTILINE
            )

        # Parse the package and message names from the schema name
        # (e.g. "std_msgs/msg/String" -> "std_msgs")
        pkg_name = cur_schema_name.split("/")[0]
        msg_name = cur_schema_name.split("/")[-1]
        short_name = pkg_name + "/" + msg_name
        msgdef = parse_message_string(pkg_name, msg_name, cur_schema_text)

        # Add the message definition to the dictionary
        msgdefs[cur_schema_name] = msgdef
        msgdefs[short_name] = msgdef

        # Add the message generator to the dictionary
        generator: DecoderFunction = _make_read_message(cur_schema_name, msgdefs)
        generators[cur_schema_name] = generator
        generators[short_name] = generator

    return generators


def read_message(
    schema_name: str, msgdefs: Dict[str, MessageSpecification], data: bytes
) -> Message:
    """Deserialize a ROS2 message from bytes.

    :param schema_name: The name of the schema to use for deserializing the message payload. This
        key must exist in the `msgdefs` dictionary
    :param msgdefs: A dictionary containing the message definitions for the top-level message and
        any nested messages.
    :param data: The message payload to deserialize.
    :return: The deserialized message.
    """
    msgdef = msgdefs[schema_name]
    if msgdef is None:
        raise ValueError(f'Message definition not found for "{schema_name}"')
    reader = CdrReader(data)
    return _read_complex_type(msgdef.msg_name, msgdef.fields, msgdefs, reader)


def _make_read_message(
    schema_name: str, msgdefs: Dict[str, MessageSpecification]
) -> DecoderFunction:
    return lambda data: read_message(schema_name, msgdefs, data)


def _read_complex_type(
    msg_name: str,
    fields: List[Field],
    msgdefs: Dict[str, MessageSpecification],
    reader: CdrReader,
) -> Message:
    Msg = type(
        msg_name,
        (SimpleNamespace,),
        {
            "__name__": msg_name,
            "__slots__": [field.name for field in fields],
            "__repr__": __repr__,
            "__str__": __repr__,
        },
    )
    msg = Msg()

    for field in fields:
        if not field.type.is_primitive_type():
            # Complex type
            nested_definition = msgdefs[f"{field.type.pkg_name}/{field.type.type}"]
            if nested_definition is None:
                raise ValueError(
                    f'Message definition not found for field "{field.name}" with '
                    'type "{field.type.type}"'
                )

            if field.type.is_array:
                # For dynamic length arrays we need to read a uint32 prefix
                array_length = field.type.array_size or reader.uint32()
                array = [
                    _read_complex_type(
                        nested_definition.msg_name,
                        nested_definition.fields,
                        msgdefs,
                        reader,
                    )
                    for _ in range(array_length)
                ]
                setattr(msg, field.name, array)
            else:
                value = _read_complex_type(
                    nested_definition.msg_name,
                    nested_definition.fields,
                    msgdefs,
                    reader,
                )
                setattr(msg, field.name, value)
        else:
            # Primitive type
            if field.type.is_array:
                parser_fn = ARRAY_PARSERS[field.type.type]
                if parser_fn is None:
                    raise NotImplementedError(
                        f"Parsing for type {field.type.type}[] is not implemented"
                    )
                # For dynamic length arrays we need to read a uint32 prefix
                array_length = field.type.array_size or reader.sequence_length()
                value = parser_fn(reader, array_length)
                setattr(msg, field.name, value)
            else:
                parser_fn = FIELD_PARSERS[field.type.type]
                if parser_fn is None:
                    raise NotImplementedError(
                        f"Parsing for type {field.type.type} is not implemented"
                    )
                value = parser_fn(reader)
                setattr(msg, field.name, value)

    return msg


def __repr__(self: Any) -> str:
    fields = ", ".join(f"{field}={getattr(self, field)}" for field in self.__slots__)
    return f"{self.__name__}({fields})"
