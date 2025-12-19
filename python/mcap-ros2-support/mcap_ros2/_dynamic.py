"""ROS2 message definition parsing and message deserialization."""

import array as py_array
import re
from io import BytesIO
from types import SimpleNamespace
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

from ._cdr import CdrReader, CdrWriter
from ._vendor.rosidl_adapter.parser import (
    Field,
    MessageSpecification,
    Type,
    parse_message_string,
)

# cSpell:words ftype wstring msgdefs typecode tobytes

DecodedMessage = SimpleNamespace
DecoderFunction = Callable[[bytes], DecodedMessage]
EncoderFunction = Callable[[Any], bytes]
PrimitiveValue = Union[bool, int, float, str]
DefaultValue = Union[PrimitiveValue, List[PrimitiveValue]]


def _parseWstring(reader: CdrReader) -> str:
    raise NotImplementedError("wstring parsing is not implemented")


def _parseWstringArray(reader: CdrReader, array_length: int) -> List[str]:
    raise NotImplementedError("wstring[] parsing is not implemented")


def _writeWstring(writer: CdrWriter, value: str):
    raise NotImplementedError("wstring writing is not implemented")


def _writeWstringArray(writer: CdrWriter, value: List[str]):
    raise NotImplementedError("wstring[] writing is not implemented")


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

FIELD_WRITERS = {
    "bool": CdrWriter.write_boolean,
    "byte": CdrWriter.write_uint8,
    "char": CdrWriter.write_int8,
    "float32": CdrWriter.write_float32,
    "float64": CdrWriter.write_float64,
    "int8": CdrWriter.write_int8,
    "uint8": CdrWriter.write_uint8,
    "int16": CdrWriter.write_int16,
    "uint16": CdrWriter.write_uint16,
    "int32": CdrWriter.write_int32,
    "uint32": CdrWriter.write_uint32,
    "int64": CdrWriter.write_int64,
    "uint64": CdrWriter.write_uint64,
    "string": CdrWriter.write_string,
    "wstring": _writeWstring,
}

ARRAY_WRITERS = {
    "bool": CdrWriter.write_boolean_array,
    "byte": CdrWriter.write_uint8_array,
    "char": CdrWriter.write_int8_array,
    "float32": CdrWriter.write_float32_array,
    "float64": CdrWriter.write_float64_array,
    "int8": CdrWriter.write_int8_array,
    "uint8": CdrWriter.write_uint8_array,
    "int16": CdrWriter.write_int16_array,
    "uint16": CdrWriter.write_uint16_array,
    "int32": CdrWriter.write_int32_array,
    "uint32": CdrWriter.write_uint32_array,
    "int64": CdrWriter.write_int64_array,
    "uint64": CdrWriter.write_uint64_array,
    "string": CdrWriter.write_string_array,
    "wstring": _writeWstringArray,
}

STRING_TYPES = ("string", "wstring")
FLOAT_TYPES = ("float32", "float64")
INT_TYPES = (
    "byte",
    "char",
    "int8",
    "uint8",
    "int16",
    "uint16",
    "int32",
    "uint32",
    "int64",
    "uint64",
)

TimeDefinition = MessageSpecification(
    "builtin_interfaces",
    "Time",
    [Field(Type("uint32"), "sec"), Field(Type("uint32"), "nanosec")],
    [],
)


def generate_dynamic(schema_name: str, schema_text: str) -> Dict[str, DecoderFunction]:
    """Convert a ROS2 concatenated message definition into a dictionary of message parsers.

    Modeled after the `generate_dynamic` function in ROS1 `genpy.dynamic`.

    :param schema_name: The name of the schema defined in `schema_text`.
    :param schema_text: The schema text to use for deserializing the message payload.
    :return: A dictionary mapping schema names to message parser.
    """
    msgdefs: Dict[str, MessageSpecification] = {
        "builtin_interfaces/Time": TimeDefinition,
        "builtin_interfaces/Duration": TimeDefinition,
    }
    decoders: Dict[str, DecoderFunction] = {}

    def handle_msgdef(
        cur_schema_name: str, short_name: str, msgdef: MessageSpecification
    ):
        # Add the message definition to the dictionary
        msgdefs[cur_schema_name] = msgdef
        msgdefs[short_name] = msgdef

        # Add the message decoder to the dictionary
        decoder: DecoderFunction = _make_read_message(cur_schema_name, msgdefs)
        decoders[cur_schema_name] = decoder
        decoders[short_name] = decoder

    _for_each_msgdef(schema_name, schema_text, handle_msgdef)
    return decoders


def serialize_dynamic(schema_name: str, schema_text: str) -> Dict[str, EncoderFunction]:
    """Convert a ROS2 concatenated message definition into a dictionary of message encoders.

    :param schema_name: The name of the schema defined in `schema_text`.
    :param schema_text: The schema text to use for serializing message payloads.
    :return: A dictionary mapping schema names to message encoders.
    """
    msgdefs: Dict[str, MessageSpecification] = {
        "builtin_interfaces/Time": TimeDefinition,
        "builtin_interfaces/Duration": TimeDefinition,
    }
    encoders: Dict[str, EncoderFunction] = {}

    def handle_msgdef(
        cur_schema_name: str, short_name: str, msgdef: MessageSpecification
    ):
        # Add the message definition to the dictionary
        msgdefs[cur_schema_name] = msgdef
        msgdefs[short_name] = msgdef

        # Add the message encoder to the dictionary
        encoder: EncoderFunction = _make_encode_message(cur_schema_name, msgdefs)
        encoders[cur_schema_name] = encoder
        encoders[short_name] = encoder

    _for_each_msgdef(schema_name, schema_text, handle_msgdef)
    return encoders


def read_message(
    schema_name: str, msgdefs: Dict[str, MessageSpecification], data: bytes
) -> DecodedMessage:
    """Deserialize a ROS2 message from bytes.

    :param schema_name: The name of the schema to use for deserializing the message payload. This
        key must exist in the `msgdefs` dictionary
    :param msgdefs: A dictionary containing the message definitions for the top-level message and
        any nested messages.
    :param data: The message payload to deserialize.
    :return: The deserialized message.
    """
    msgdef = msgdefs.get(schema_name)
    if msgdef is None:
        raise ValueError(f'Message definition not found for "{schema_name}"')
    reader = CdrReader(data)
    return _read_complex_type(msgdef, msgdefs, reader)


def encode_message(
    schema_name: str, msgdefs: Dict[str, MessageSpecification], ros2_msg: Any
) -> bytes:
    """Serialize a ROS2 message to bytes.

    :param schema_name: The name of the schema to use for deserializing the message payload. This
        key must exist in the `msgdefs` dictionary
    :param msgdefs: A dictionary containing the message definitions for the top-level message and
        any nested messages.
    :param ros2_msg: The message to serialize.
    :return: The serialized message.
    """
    msgdef = msgdefs.get(schema_name)
    if msgdef is None:
        raise ValueError(f'Message definition not found for "{schema_name}"')
    output = BytesIO()
    writer = CdrWriter(output)
    _write_complex_type(msgdef.msg_name, msgdef.fields, msgdefs, ros2_msg, writer)
    return output.getvalue()


def _make_read_message(
    schema_name: str, msgdefs: Dict[str, MessageSpecification]
) -> DecoderFunction:
    return lambda data: read_message(schema_name, msgdefs, data)


def _make_encode_message(
    schema_name: str, msgdefs: Dict[str, MessageSpecification]
) -> EncoderFunction:
    return lambda msg: encode_message(schema_name, msgdefs, msg)


def _read_complex_type(
    msgdef: MessageSpecification,
    msgdefs: Dict[str, MessageSpecification],
    reader: CdrReader,
) -> DecodedMessage:
    Msg = type(
        msgdef.msg_name,
        (SimpleNamespace,),
        {
            "__name__": msgdef.msg_name,
            "__slots__": [field.name for field in msgdef.fields],
            "__repr__": __repr__,
            "__str__": __repr__,
            "__eq__": __eq__,
            "__ne__": __ne__,
            "_type": str(msgdef.base_type),
            "_full_text": str(msgdef),
        },
    )
    msg = Msg()

    if len(msgdef.fields) == 0:
        # In case a message definition definition is empty, ROS 2 adds a
        # `uint8 structure_needs_at_least_one_member` field when converting to IDL,
        # to satisfy the requirement from IDL of not being empty.
        # See also https://design.ros2.org/articles/legacy_interface_definition.html
        reader.uint8()

    for field in msgdef.fields:
        ftype = field.type
        if not ftype.is_primitive_type():
            # Complex type
            nested_definition = msgdefs.get(f"{ftype.pkg_name}/{ftype.type}")
            if nested_definition is None:
                raise ValueError(
                    f'Message definition not found for field "{field.name}" with '
                    'type "{ftype.type}"'
                )

            if ftype.is_array:
                # For dynamic length arrays we need to read a uint32 prefix
                array_length = (
                    ftype.array_size
                    if ftype.is_fixed_size_array() and ftype.array_size is not None
                    else reader.uint32()
                )
                array = [
                    _read_complex_type(
                        nested_definition,
                        msgdefs,
                        reader,
                    )
                    for _ in range(array_length)
                ]
                setattr(msg, field.name, array)
            else:
                value = _read_complex_type(
                    nested_definition,
                    msgdefs,
                    reader,
                )
                setattr(msg, field.name, value)
        else:
            # Primitive type
            if ftype.is_array:
                array_parser_fn = ARRAY_PARSERS.get(ftype.type)
                if array_parser_fn is None:
                    raise NotImplementedError(
                        f"Parsing for type {ftype.type}[] is not implemented"
                    )
                # For dynamic length arrays we need to read a uint32 prefix
                array_length = (
                    ftype.array_size
                    if ftype.is_fixed_size_array() and ftype.array_size is not None
                    else reader.sequence_length()
                )
                value = array_parser_fn(reader, array_length)
                setattr(msg, field.name, value)
            else:
                parser_fn = FIELD_PARSERS.get(ftype.type)
                if parser_fn is None:
                    raise NotImplementedError(
                        f"Parsing for type {ftype.type} is not implemented"
                    )
                value = parser_fn(reader)
                setattr(msg, field.name, value)

    return msg


def _write_complex_type(
    msg_name: str,
    fields: List[Field],
    msgdefs: Dict[str, MessageSpecification],
    ros2_msg: Any,
    writer: CdrWriter,
) -> None:
    if len(fields) == 0:
        # In case a message definition definition is empty, ROS 2 adds a
        # `uint8 structure_needs_at_least_one_member` field when converting to IDL,
        # to satisfy the requirement from IDL of not being empty.
        # See also https://design.ros2.org/articles/legacy_interface_definition.html
        writer.write_uint8(0x00)

    for field in fields:
        ftype = field.type
        if not ftype.is_primitive_type():
            # Complex type
            nested_definition = msgdefs.get(f"{ftype.pkg_name}/{ftype.type}")
            if nested_definition is None:
                raise ValueError(
                    f'Message definition not found for field "{field.name}" with '
                    'type "{ftype.type}"'
                )

            if ftype.is_array:
                array: Union[List[Any], Tuple[Any], Any] = _get_property(
                    ros2_msg, field.name
                )
                if array is None:
                    array = []
                if not isinstance(array, list):
                    raise ValueError(
                        f'Field "{field.name}" is not an array but has array type '
                        f'"{ftype.type}[]"'
                    )

                if ftype.is_fixed_size_array() and ftype.array_size is not None:
                    # Fixed length array, ensure the input array is the correct length
                    while len(array) < ftype.array_size:
                        array.append({})
                    if len(array) > ftype.array_size:
                        array = array[: ftype.array_size]

                    for item in array:
                        _write_complex_type(
                            nested_definition.msg_name,
                            nested_definition.fields,
                            msgdefs,
                            item,
                            writer,
                        )
                else:
                    # Limit the array to the upper bound length, if present
                    if (
                        ftype.is_upper_bound
                        and ftype.array_size is not None
                        and len(array) > ftype.array_size
                    ):
                        array = array[: ftype.array_size]

                    # Dynamic length array, write a uint32 prefix
                    writer.write_uint32(len(array))
                    # Write the array values
                    for item in array:
                        _write_complex_type(
                            nested_definition.msg_name,
                            nested_definition.fields,
                            msgdefs,
                            item,
                            writer,
                        )
            else:
                _write_complex_type(
                    nested_definition.msg_name,
                    nested_definition.fields,
                    msgdefs,
                    _get_property(ros2_msg, field.name) or {},
                    writer,
                )
        else:
            # Primitive type
            if ftype.is_array:
                array: Union[List[Any], Tuple[Any], Any] = _get_property(
                    ros2_msg, field.name
                )
                if array is None:
                    array = []
                if (
                    not isinstance(array, list)
                    and not isinstance(array, tuple)
                    and not isinstance(array, bytes)
                    and not isinstance(array, py_array.array)
                ):
                    raise ValueError(
                        f'Field "{field.name}" is not an array ({type(array)}) but has array type '
                        f'"{ftype.type}[]"'
                    )

                # Special handling for bytes
                if isinstance(array, bytes) or (
                    isinstance(array, py_array.array) and array.typecode == "B"
                ):
                    byte_array: bytes = (
                        array if isinstance(array, bytes) else array.tobytes()
                    )
                    if ftype.type != "uint8" and ftype.type != "byte":
                        raise ValueError(
                            f'Field "{field.name}" has type "uint8[]" but has type "{ftype.type}[]"'
                        )

                    if ftype.is_fixed_size_array() and ftype.array_size is not None:
                        # Fixed length byte array, ensure the input array is the correct length
                        while len(byte_array) < ftype.array_size:
                            byte_array += b"\0"
                        if len(byte_array) > ftype.array_size:
                            byte_array = byte_array[: ftype.array_size]

                        writer.write_bytes(byte_array)
                    else:
                        # Limit the byte array to the upper bound length, if present
                        if (
                            ftype.is_upper_bound
                            and ftype.array_size is not None
                            and len(array) > ftype.array_size
                        ):
                            byte_array = byte_array[: ftype.array_size]

                        # Dynamic length byte array, write a uint32 prefix
                        writer.write_uint32(len(byte_array))
                        # Write the byte array values
                        writer.write_bytes(byte_array)
                else:
                    array_writer_fn = ARRAY_WRITERS.get(ftype.type)
                    if array_writer_fn is None:
                        raise NotImplementedError(
                            f"Writing for type {ftype.type}[] is not implemented"
                        )

                    if ftype.is_fixed_size_array() and ftype.array_size is not None:
                        # Convert tuples to lists
                        list_array = (
                            list(array)
                            if isinstance(array, (tuple, py_array.array))
                            else array
                        )
                        # Fixed length array, ensure the input array is the correct length
                        while len(list_array) < ftype.array_size:
                            list_array.append(None)
                        if len(list_array) > ftype.array_size:
                            list_array = list_array[: ftype.array_size]

                        list_array: List[Any] = _coerce_values(
                            list_array, ftype.type, field.default_value
                        )
                        array_writer_fn(writer, list_array)
                    else:
                        # Limit the array to the upper bound length, if present
                        if (
                            ftype.is_upper_bound
                            and ftype.array_size is not None
                            and len(array) > ftype.array_size
                        ):
                            array = array[: ftype.array_size]

                        array = (
                            list(array) if isinstance(array, py_array.array) else array
                        )
                        array = _coerce_values(array, ftype.type, field.default_value)

                        # Dynamic length array, write a uint32 prefix
                        writer.write_uint32(len(array))
                        # Write the array values
                        array_writer_fn(writer, array)
            else:
                writer_fn = FIELD_WRITERS.get(ftype.type)
                if writer_fn is None:
                    raise NotImplementedError(
                        f"Writing for type {ftype.type} is not implemented"
                    )

                value = _get_property(ros2_msg, field.name)
                value: Any = _coerce_value(value, ftype.type, field.default_value)
                writer_fn(writer, value)


def _for_each_msgdef(
    schema_name: str,
    schema_text: str,
    fn: Callable[[str, str, MessageSpecification], None],
) -> None:
    cur_schema_name = schema_name

    # Remove empty lines
    schema_text = "\n".join([s for s in schema_text.splitlines() if s.strip()])

    # Split schema_text by separator lines containing at least 3 = characters
    # (e.g. "===") using a regular expression
    for cur_schema_text in re.split(r"^={3,}$", schema_text, flags=re.MULTILINE):
        cur_schema_text = cur_schema_text.strip()

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

        fn(cur_schema_name, short_name, msgdef)


def _get_property(obj: Any, name: str) -> Any:
    if hasattr(obj, name):
        return getattr(obj, name)
    try:
        return obj[name]
    except (KeyError, TypeError):
        return None


def _coerce_value(
    value: Any, type_name: str, default_value: Optional[DefaultValue]
) -> PrimitiveValue:
    if isinstance(default_value, list):
        raise ValueError("Default value for primitive types cannot be an array")

    if type_name in STRING_TYPES:
        return (
            str(value)
            if value is not None
            else default_value
            if default_value is not None
            else ""
        )
    elif type_name in FLOAT_TYPES:
        return (
            float(value)
            if value is not None
            else default_value
            if default_value is not None
            else 0.0
        )
    elif type_name in INT_TYPES:
        return (
            int(value)
            if value is not None
            else default_value
            if default_value is not None
            else 0
        )
    elif type_name == "bool":
        return (
            bool(value)
            if value is not None
            else default_value
            if default_value is not None
            else False
        )
    else:
        raise NotImplementedError(f'coercion for type "{type_name}" is not implemented')


def _coerce_values(
    values: Union[List[Any], Tuple[Any]],
    type_name: str,
    default_value: Optional[DefaultValue],
) -> List[PrimitiveValue]:
    return [_coerce_value(value, type_name, default_value) for value in values]


def __repr__(self: Any) -> str:
    fields = ", ".join(f"{field}={getattr(self, field)}" for field in self.__slots__)
    return f"{self.__name__}({fields})"


def __eq__(self: Any, other: Any) -> bool:
    if not isinstance(other, type(self)):
        return False

    if (
        not hasattr(self, "__slots__")
        or not hasattr(other, "__slots__")
        or len(self.__slots__) != len(other.__slots__)
    ):
        return False

    for attr in self.__slots__:
        if getattr(self, attr) != getattr(other, attr):
            return False

    return True


def __ne__(self: Any, other: Any) -> bool:
    return not __eq__(self, other)
