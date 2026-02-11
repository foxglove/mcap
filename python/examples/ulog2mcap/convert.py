import argparse
import itertools
import typing
from datetime import datetime, timezone

import protos.Log_pb2 as log_pb2
from google.protobuf.descriptor_pb2 import (
    DescriptorProto,
    FieldDescriptorProto,
    FileDescriptorProto,
)
from google.protobuf.descriptor_pool import DescriptorPool
from google.protobuf.message_factory import GetMessageClass, GetMessageClassesForFiles
from mcap_protobuf.writer import Writer
from pyulog import ULog

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


def _build_message_descriptor_proto(
    message_format: ULog.MessageFormat,
) -> DescriptorProto:
    desc = DescriptorProto()
    desc.name = message_format.name
    field_number = 1
    for type_str, array_length, name in message_format.fields:
        if name == "timestamp" or name.startswith("_padding"):
            continue
        field = FieldDescriptorProto()
        field.name = name
        field.number = field_number
        field_number += 1
        prim = _ulog_primitive_to_protobuf(type_str)
        if prim is not None:
            setattr(field, "type", prim)
            # char[N] is a single string; other primitives with length are repeated
            if type_str == "char" and array_length > 0:
                field.label = FieldDescriptorProto.LABEL_OPTIONAL
            else:
                field.label = (
                    FieldDescriptorProto.LABEL_REPEATED
                    if array_length > 0
                    else FieldDescriptorProto.LABEL_OPTIONAL
                )
        else:
            setattr(field, "type", FieldDescriptorProto.TYPE_MESSAGE)
            field.type_name = f".{ULOG_PACKAGE}.{type_str}"
            field.label = (
                FieldDescriptorProto.LABEL_REPEATED
                if array_length > 0
                else FieldDescriptorProto.LABEL_OPTIONAL
            )
        desc.field.append(field)
    return desc


def _build_ulog_file_descriptor(ulog: ULog) -> FileDescriptorProto:
    """Build a FileDescriptorProto containing all ulog message types."""
    fd = FileDescriptorProto()
    fd.name = "ulog_messages.proto"
    fd.package = ULOG_PACKAGE
    fd.syntax = "proto3"

    message_formats = ulog.message_formats
    for message_name in message_formats:
        desc = _build_message_descriptor_proto(message_formats[message_name])
        fd.message_type.append(desc)
    return fd


def _get_ulog_message_classes(
    fd: FileDescriptorProto,
) -> typing.Dict[str, type]:
    """Add fd to a new pool and return a dictionary of message classes by full name."""
    pool = DescriptorPool()
    pool.Add(fd)
    messages = GetMessageClassesForFiles([fd.name], pool)
    nested = {}
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
    :param mcap: The MCAP protobuf writer to write to.
    :param start_time: The start time to use for message timestamps,
        useful since ulog timestamps are stored in time-since-startup
        (use either timestamp in microseconds or ISO 8601 format).
    :param metadata: Additional file-level metadata.
    """
    for name, metadata_dict in metadata or []:
        mcap.add_metadata(name, metadata_dict)

    if start_time is not None:
        time_offset_us = int(start_time.timestamp() * 1_000000) - ulog.start_timestamp
    else:
        time_offset_us = 0

    if ulog.data_list:
        ulog_fd = _build_ulog_file_descriptor(ulog)
        ulog_message_classes = _get_ulog_message_classes(ulog_fd)

        for data in ulog.data_list:
            topic_name = (
                f"/{data.name}/{data.multi_id}" if data.multi_id else f"/{data.name}"
            )
            message_class = ulog_message_classes[f"{ULOG_PACKAGE}.{data.name}"]
            num_messages = len(data.data["timestamp"])
            for idx in range(num_messages):
                timestamp = (
                    data.data["timestamp"][idx] + time_offset_us
                ) * 1000  # microseconds to nanoseconds
                msg = message_class()
                _set_proto_field_from_ulog(
                    msg, data, idx, data.name, ulog, "", ulog_message_classes
                )
                mcap.write_message(
                    topic=topic_name,
                    message=msg,
                    log_time=timestamp,
                    publish_time=timestamp,
                )

    for log_msg in itertools.chain(
        ulog.logged_messages, ulog.logged_messages_tagged.values()
    ):
        timestamp = (log_msg.timestamp + time_offset_us) * 1000
        proto_msg = log_pb2.Log()  # type: ignore
        proto_msg.timestamp.sec = int(timestamp / 1_000_000_000)
        proto_msg.timestamp.nsec = int(timestamp % 1_000_000_000)
        proto_msg.level = ulog_level_to_fox_log_level(log_msg.log_level_str())
        proto_msg.message = log_msg.message
        mcap.write_message(
            topic="/log_message",
            message=proto_msg,
            log_time=timestamp,
            publish_time=timestamp,
        )


def parse_microseconds_date(date: str) -> datetime:
    if date.isdigit():
        return datetime.fromtimestamp(int(date) / 1_000000, tz=timezone.utc)
    else:
        return datetime.fromisoformat(date)


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

    with open(args.output_file, "wb") as stream, Writer(stream) as mcap:
        convert_ulog(
            ulog, mcap, start_time=start_time, metadata=[(args.metadata_name, metadata)]
        )
        mcap.finish()
