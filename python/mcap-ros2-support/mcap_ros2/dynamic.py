import re
from .vendor.rosidl_adapter.parser import (
    Field,
    MessageSpecification,
    Type,
    parse_message_string,
)
from typing import Any, Callable, Dict, List
from types import SimpleNamespace

from .cdr import CdrReader

# cSpell:words wstring msgdefs

Message = SimpleNamespace
DecoderFunction = Callable[[bytes], Message]


class Time(object):
    __slots__ = ("seconds", "nanoseconds")

    def __init__(self, seconds: int, nanoseconds: int) -> None:
        self.seconds = seconds
        self.nanoseconds = nanoseconds


def parseBool(reader: CdrReader) -> bool:
    return reader.uint8() != 0


def parseFloat32(reader: CdrReader) -> float:
    return reader.float32()


def parseFloat64(reader: CdrReader) -> float:
    return reader.float64()


def parseInt8(reader: CdrReader) -> int:
    return reader.int8()


def parseUint8(reader: CdrReader) -> int:
    return reader.uint8()


def parseInt16(reader: CdrReader) -> int:
    return reader.int16()


def parseUint16(reader: CdrReader) -> int:
    return reader.uint16()


def parseInt32(reader: CdrReader) -> int:
    return reader.int32()


def parseUint32(parser: CdrReader) -> int:
    return parser.uint32()


def parseInt64(reader: CdrReader) -> int:
    return reader.int64()


def parseUint64(reader: CdrReader) -> int:
    return reader.uint64()


def parseString(reader: CdrReader) -> str:
    return reader.string()


def parseWstring(reader: CdrReader) -> str:
    raise NotImplementedError("wstring parsing is not implemented")


def parseTime(reader: CdrReader) -> Time:
    seconds = reader.int32()
    nanoseconds = reader.uint32()
    return Time(seconds=seconds, nanoseconds=nanoseconds)


def parseBoolArray(reader: CdrReader, array_length: int) -> List[bool]:
    return [parseBool(reader) for _ in range(array_length)]


def parseInt8Array(reader: CdrReader, array_length: int) -> List[int]:
    return [reader.int8() for _ in range(array_length)]


def parseUint8Array(reader: CdrReader, array_length: int) -> bytes:
    return reader.uint8_array(array_length)


def parseInt16Array(reader: CdrReader, array_length: int) -> List[int]:
    return [reader.int16() for _ in range(array_length)]


def parseUint16Array(reader: CdrReader, array_length: int) -> List[int]:
    return [reader.uint16() for _ in range(array_length)]


def parseInt32Array(reader: CdrReader, array_length: int) -> List[int]:
    return [reader.int32() for _ in range(array_length)]


def parseUint32Array(reader: CdrReader, array_length: int) -> List[int]:
    return [reader.uint32() for _ in range(array_length)]


def parseInt64Array(reader: CdrReader, array_length: int) -> List[int]:
    return [reader.int64() for _ in range(array_length)]


def parseUint64Array(reader: CdrReader, array_length: int) -> List[int]:
    return [reader.uint64() for _ in range(array_length)]


def parseFloat32Array(reader: CdrReader, array_length: int) -> List[float]:
    return [reader.float32() for _ in range(array_length)]


def parseFloat64Array(reader: CdrReader, array_length: int) -> List[float]:
    return [reader.float64() for _ in range(array_length)]


def parseStringArray(reader: CdrReader, array_length: int) -> List[str]:
    return [reader.string() for _ in range(array_length)]


def parseWstringArray(reader: CdrReader, array_length: int) -> List[str]:
    raise NotImplementedError("wstring[] parsing is not implemented")


def parseTimeArray(reader: CdrReader, array_length: int) -> List[Time]:
    return [parseTime(reader) for _ in range(array_length)]


FIELD_PARSERS = {
    "bool": parseBool,
    "byte": parseUint8,
    "char": parseInt8,
    "float32": parseFloat32,
    "float64": parseFloat64,
    "int8": parseInt8,
    "uint8": parseUint8,
    "int16": parseInt16,
    "uint16": parseUint16,
    "int32": parseInt32,
    "uint32": parseUint32,
    "int64": parseInt64,
    "uint64": parseUint64,
    "string": parseString,
    "wstring": parseWstring,
    "duration": parseTime,
    "time": parseTime,
}

ARRAY_PARSERS = {
    "bool": parseBoolArray,
    "byte": parseUint8Array,
    "char": parseInt8Array,
    "float32": parseFloat32Array,
    "float64": parseFloat64Array,
    "int8": parseInt8Array,
    "uint8": parseUint8Array,
    "int16": parseInt16Array,
    "uint16": parseUint16Array,
    "int32": parseInt32Array,
    "uint32": parseUint32Array,
    "int64": parseInt64Array,
    "uint64": parseUint64Array,
    "string": parseStringArray,
    "wstring": parseWstringArray,
    "duration": parseTimeArray,
    "time": parseTimeArray,
}

TimeDefinition = MessageSpecification(
    "builtin_interfaces",
    "Time",
    [
        Field(Type("uint32"), "seconds"),
        Field(Type("uint32"), "nanoseconds"),
    ],
    [],
)


def make_read_message(
    schema_name: str, msgdefs: Dict[str, MessageSpecification]
) -> DecoderFunction:
    return lambda data: read_message(schema_name, msgdefs, data)


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
    return read_complex_type(msgdef.msg_name, msgdef.fields, msgdefs, reader)


def read_complex_type(
    msg_name: str,
    fields: list[Field],
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
            "__getstate__": __getstate__,
            "__setstate__": __setstate__,
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
                    read_complex_type(
                        nested_definition.msg_name,
                        nested_definition.fields,
                        msgdefs,
                        reader,
                    )
                    for _ in range(array_length)
                ]
                setattr(msg, field.name, array)
            else:
                value = read_complex_type(
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


def generate_dynamic(schema_name: str, schema_text: str) -> Dict[str, DecoderFunction]:
    """Generate a dynamic ROS2 message type from a schema text.

    :param schema_name: The name of the schema defined in `schema_text`.
    :param schema_text: The schema text to use for deserializing the message payload.
    :return: A dictionary containing the generated message.
    """
    # Split schema_text by separator lines containing at least 3 = characters
    # (e.g. "===") using a regular expression
    msgdefs: Dict[str, MessageSpecification] = {
        "builtin_interfaces/Time": TimeDefinition,
        "builtin_interfaces/Duration": TimeDefinition,
    }
    generators: Dict[str, DecoderFunction] = {}
    cur_schema_name = schema_name
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
        short_name = (
            pkg_name + "/" + msg_name
        )  # Lookup by short name (e.g. "std_msg/String")
        msgdef = parse_message_string(pkg_name, msg_name, cur_schema_text)

        # Add the message definition to the dictionary
        msgdefs[cur_schema_name] = msgdef
        msgdefs[short_name] = msgdef

        # Add the message generator to the dictionary
        generator: DecoderFunction = make_read_message(cur_schema_name, msgdefs)
        generators[cur_schema_name] = generator
        generators[short_name] = generator

    return generators


def __repr__(self: Any) -> str:
    fields = ", ".join(f"{field}={getattr(self, field)}" for field in self.__slots__)
    return f"{self.__name__}({fields})"


def __getstate__(self: Any):
    """Support for Python pickling."""
    return [getattr(self, x) for x in self.__slots__]


def __setstate__(self: Any, state: Any):
    """Support for Python pickling."""
    for x, val in zip(self.__slots__, state):
        setattr(self, x, val)
