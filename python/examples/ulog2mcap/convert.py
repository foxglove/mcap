import argparse
import itertools
import json
import typing
from contextlib import contextmanager
from datetime import datetime, timezone
from io import BufferedWriter

import numpy as np
import protos.Log_pb2 as log_pb2
from google.protobuf.descriptor_pb2 import (
    DescriptorProto,
    FieldDescriptorProto,
    FileDescriptorProto,
)
from google.protobuf.descriptor_pool import DescriptorPool
from google.protobuf.message_factory import GetMessageClass, GetMessageClassesForFiles
from mcap_protobuf.schema import register_schema
from pyulog import ULog

from mcap.well_known import MessageEncoding
from mcap.writer import Writer

ULOG_FD_NAME = "ulog_messages.proto"
ULOG_PACKAGE = "ulog"


def ulog_level_to_fox_log_level(level: str) -> int:
    if level == "EMERGENCY":
        return 5  # FATAL
    elif level in ["ALERT", "CRITICAL", "ERROR"]:
        return 4  # ERROR
    elif level in ["WARNING", "NOTICE"]:
        return 3  # WARNING
    elif level == "INFO":
        return 2  # INFO
    elif level == "DEBUG":
        return 1  # DEBUG
    else:
        return 0  # UNKNOWN


def _ulog_primitive_to_protobuf(ulog_type: str) -> typing.Optional[int]:
    """Map ulog primitive type string to FieldDescriptorProto type.
    Returns None for nested types."""
    mapping = {
        "int8_t": FieldDescriptorProto.TYPE_INT32,
        "uint8_t": FieldDescriptorProto.TYPE_UINT32,
        "int16_t": FieldDescriptorProto.TYPE_INT32,
        "uint16_t": FieldDescriptorProto.TYPE_UINT32,
        "int32_t": FieldDescriptorProto.TYPE_INT32,
        "uint32_t": FieldDescriptorProto.TYPE_UINT32,
        "int64_t": FieldDescriptorProto.TYPE_INT64,
        "uint64_t": FieldDescriptorProto.TYPE_UINT64,
        "float": FieldDescriptorProto.TYPE_FLOAT,
        "double": FieldDescriptorProto.TYPE_DOUBLE,
        "bool": FieldDescriptorProto.TYPE_BOOL,
        "char": FieldDescriptorProto.TYPE_STRING,
    }
    return mapping.get(ulog_type)


def _build_descriptor_proto(
    name: str,
    fields: typing.List[
        typing.Tuple[str, int | str, bool]
    ],  # field name, field type, is_repeated
) -> DescriptorProto:
    desc = DescriptorProto()
    desc.name = name
    field_number = 1
    for field_name, field_type, is_repeated in fields:
        field = FieldDescriptorProto()
        field.name = field_name
        field.number = field_number
        if isinstance(field_type, int):
            setattr(field, "type", field_type)
        else:
            setattr(field, "type", FieldDescriptorProto.TYPE_MESSAGE)
            field.type_name = field_type
        field.label = (
            FieldDescriptorProto.LABEL_REPEATED
            if is_repeated
            else FieldDescriptorProto.LABEL_OPTIONAL
        )
        field_number += 1
        desc.field.append(field)
    return desc


def _build_message_descriptor_proto(
    message_format: ULog.MessageFormat,
) -> DescriptorProto:
    fields: typing.List[typing.Tuple[str, int | str, bool]] = []
    for type_str, array_length, name in message_format.fields:
        if name == "timestamp" or name.startswith("_padding"):
            continue
        primitive = _ulog_primitive_to_protobuf(type_str)
        field_type = primitive if primitive is not None else type_str
        is_repeated = array_length > 0 and type_str != "char"
        fields.append((name, field_type, is_repeated))
    return _build_descriptor_proto(message_format.name, fields)


def _build_descriptor_pool(
    name: str,
    descriptors: typing.List[DescriptorProto],
    package: typing.Optional[str] = None,
) -> DescriptorPool:
    fd = FileDescriptorProto()
    fd.name = name
    if package:
        fd.package = package
    fd.syntax = "proto3"
    for desc in descriptors:
        fd.message_type.append(desc)

    pool = DescriptorPool()
    pool.Add(fd)  # type: ignore
    return pool


def _get_ulog_message_classes(
    pool: DescriptorPool,
) -> typing.Dict[str, type]:
    """Add fd to a new pool and return a dictionary of message classes by full name."""
    messages = GetMessageClassesForFiles([ULOG_FD_NAME], pool)
    nested: typing.Dict[str, type] = {}
    for message_class in messages.values():
        for nested_desc in message_class.DESCRIPTOR.nested_types_by_name.values():
            nested[nested_desc.full_name] = GetMessageClass(nested_desc)
    messages.update(nested)
    return messages


def _set_proto_field_from_ulog(
    msg: typing.Any,
    data: ULog.Data,
    idx: int,
    message_name: str,
    ulog: ULog,
    field_path: str,
    message_classes: typing.Dict[str, type],
) -> None:
    """Recursively set one message's fields from ulog data at row idx."""
    message_formats = ulog.message_formats
    format_desc = message_formats[message_name]
    for type_str, array_length, name in format_desc.fields:
        if name == "timestamp" or name.startswith("_padding"):
            continue
        key = f"{field_path}.{name}" if field_path else name
        prim = _ulog_primitive_to_protobuf(type_str)
        if prim is not None:
            if array_length > 0:
                if type_str == "char":
                    s = ""
                    for i in range(array_length):
                        s += chr(data.data[f"{key}[{i}]"][idx])
                    setattr(msg, name, s)
                else:
                    arr = getattr(msg, name)
                    for i in range(array_length):
                        val = data.data[f"{key}[{i}]"][idx]
                        arr.append(val)
            else:
                val = data.data[key][idx]
                setattr(msg, name, val)
        else:
            # Nested message type
            nested_class = message_classes.get(f"{ULOG_PACKAGE}.{type_str}")
            if nested_class is None:
                continue
            if array_length > 0:
                for i in range(array_length):
                    sub = getattr(msg, name).add()
                    _set_proto_field_from_ulog(
                        sub, data, idx, type_str, ulog, f"{key}[{i}]", message_classes
                    )
            else:
                sub = getattr(msg, name)
                _set_proto_field_from_ulog(
                    sub, data, idx, type_str, ulog, key, message_classes
                )


def convert_ulog(
    ulog: ULog,
    mcap: Writer,
    start_time: typing.Optional[datetime] = None,
    metadata: typing.Optional[
        typing.List[typing.Tuple[str, typing.Dict[str, str]]]
    ] = None,
) -> None:
    """Convert a ULog file to an MCAP file.

    :param ulog: The ULog file to convert.
    :param mcap: The MCAP writer to write to.
    :param start_time: The start time to use for message timestamps,
        useful since ulog timestamps are stored in time-since-startup
        (use either timestamp in microseconds or ISO 8601 format).
    :param metadata: Additional file-level metadata.
    """
    for name, metadata_dict in metadata or []:
        mcap.add_metadata(name, metadata_dict)

    if start_time is not None:
        # ULog timestamps are stored in terms of microseconds from device startup
        # This offset, when added to the message timestamp,
        # produces an absolute timestamp in microseconds
        time_offset_us = int(start_time.timestamp() * 1_000000) - ulog.start_timestamp
    else:
        time_offset_us = 0
    convert_timestamp: typing.Callable[[int], int] = (
        lambda ts: (ts + time_offset_us) * 1000
    )  # microseconds to nanoseconds

    # Write Data Messages
    if ulog.data_list:
        descriptors = list(
            map(_build_message_descriptor_proto, ulog.message_formats.values())
        )
        ulog_pool = _build_descriptor_pool(
            name=ULOG_FD_NAME, package=ULOG_PACKAGE, descriptors=descriptors
        )
        ulog_message_classes = _get_ulog_message_classes(ulog_pool)

        for data in ulog.data_list:
            topic_name = (
                f"/{data.name}/{data.multi_id}" if data.multi_id else f"/{data.name}"
            )
            message_class = ulog_message_classes[f"{ULOG_PACKAGE}.{data.name}"]

            schema_id = register_schema(mcap, message_class)
            channel_id = mcap.register_channel(
                topic=topic_name,
                message_encoding=MessageEncoding.Protobuf,
                schema_id=schema_id,
            )
            num_messages = len(data.data["timestamp"])
            for idx in range(num_messages):
                timestamp = convert_timestamp(data.data["timestamp"][idx])
                msg = message_class()
                _set_proto_field_from_ulog(
                    msg, data, idx, data.name, ulog, "", ulog_message_classes
                )
                mcap.add_message(
                    channel_id=channel_id,
                    data=msg.SerializeToString(),  # type: ignore
                    log_time=timestamp,
                    publish_time=timestamp,
                )

    # Write Log Messages
    if ulog.logged_messages or ulog.logged_messages_tagged:
        log_schema_id = register_schema(mcap, log_pb2.Log)  # type: ignore
        log_channel_id = mcap.register_channel(
            topic="/log_message",
            message_encoding=MessageEncoding.Protobuf,
            schema_id=log_schema_id,
        )
        for log_msg in itertools.chain(
            ulog.logged_messages, *ulog.logged_messages_tagged.values()
        ):
            timestamp = convert_timestamp(log_msg.timestamp)
            proto_msg = log_pb2.Log()  # type: ignore
            proto_msg.timestamp.sec = int(timestamp // 1_000_000_000)  # type: ignore
            proto_msg.timestamp.nsec = int(timestamp % 1_000_000_000)  # type: ignore
            proto_msg.level = ulog_level_to_fox_log_level(log_msg.log_level_str())
            proto_msg.message = log_msg.message
            mcap.add_message(
                channel_id=log_channel_id,
                data=proto_msg.SerializeToString(),  # type: ignore
                log_time=timestamp,
                publish_time=timestamp,
            )

    # Write Parameter Messages as JSON on a single /parameters channel
    if ulog.initial_parameters:
        initial_timestamp = convert_timestamp(ulog.start_timestamp)

        # Build JSON schema with a property per parameter
        json_properties: typing.Dict[str, typing.Any] = {}
        for param_name, param_value in ulog.initial_parameters.items():
            if isinstance(param_value, str):
                json_properties[param_name] = {"type": "string"}
            elif np.issubdtype(type(param_value), np.floating):
                json_properties[param_name] = {"type": "number"}
            elif np.issubdtype(type(param_value), np.integer):
                json_properties[param_name] = {"type": "integer"}
            else:
                raise ValueError(f"Unsupported parameter type: {type(param_value)}")

        json_schema: typing.Dict[str, typing.Any] = {
            "type": "object",
            "properties": json_properties,
        }

        # Register schema and channel via the internal writer
        parameter_schema_id = mcap.register_schema(
            name="Parameters",
            encoding="jsonschema",
            data=json.dumps(json_schema).encode("utf-8"),
        )
        parameter_channel_id = mcap.register_channel(
            topic="/parameters",
            message_encoding=MessageEncoding.JSON,
            schema_id=parameter_schema_id,
        )

        # Write initial message with all parameters
        initial_params: typing.Dict[str, typing.Any] = {}
        for param_name, param_value in ulog.initial_parameters.items():
            if np.issubdtype(type(param_value), np.floating):
                initial_params[param_name] = float(param_value)
            elif np.issubdtype(type(param_value), np.integer):
                initial_params[param_name] = int(param_value)
            else:
                initial_params[param_name] = param_value

        mcap.add_message(
            channel_id=parameter_channel_id,
            log_time=initial_timestamp,
            publish_time=initial_timestamp,
            data=json.dumps(initial_params).encode("utf-8"),
        )

        # Write partial updates containing only the changed parameter
        for timestamp, param_name, param_value in ulog.changed_parameters:
            timestamp_ns = convert_timestamp(timestamp)
            if np.issubdtype(type(param_value), np.floating):
                param_value = float(param_value)
            elif np.issubdtype(type(param_value), np.integer):
                param_value = int(param_value)

            mcap.add_message(
                channel_id=parameter_channel_id,
                log_time=timestamp_ns,
                publish_time=timestamp_ns,
                data=json.dumps({param_name: param_value}).encode("utf-8"),
            )


def parse_microseconds_date(date: str) -> datetime:
    if date.isdigit():
        return datetime.fromtimestamp(int(date) / 1_000000, tz=timezone.utc)
    else:
        return datetime.fromisoformat(date).astimezone(timezone.utc)


@contextmanager
def mcap_writer(stream: BufferedWriter):
    mcap = Writer(stream)
    mcap.start()
    try:
        yield mcap
    finally:
        mcap.finish()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Convert PX4 ULog file to MCAP file")
    parser.add_argument("input_file", type=str, help="Path to the input Ulog file.")
    parser.add_argument("output_file", type=str, help="Path to the output MCAP file.")
    parser.add_argument(
        "-d",
        "--start-date",
        type=str,
        help="""Adjusted start time for message timestamps,
        useful since ulog timestamps are stored in time-since-startup
        (use either timestamp in microseconds or ISO 8601 format)""",
    )
    parser.add_argument(
        "-m",
        "--metadata",
        metavar="key=value",
        action="append",
        type=str,
        help="Additional file-level metadata",
    )
    parser.add_argument(
        "-n",
        "--metadata-name",
        type=str,
        help="Name for metadata group, if adding metadata",
        default="ulog-metadata",
    )
    args = parser.parse_args()

    ulog = ULog(args.input_file)
    metadata = {}
    if args.metadata:
        for item in args.metadata:
            key, value = item.split("=", 1)
            metadata[key] = value

    start_time = None
    if args.start_date:
        start_time = parse_microseconds_date(args.start_date)

    with open(args.output_file, "wb") as stream, mcap_writer(stream) as mcap:
        convert_ulog(
            ulog, mcap, start_time=start_time, metadata=[(args.metadata_name, metadata)]
        )
