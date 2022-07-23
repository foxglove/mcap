from typing import Any
import google.protobuf.descriptor_pb2
from mcap.mcap0.writer import Writer as McapWriter


def register_schema(writer: McapWriter, message_class: Any):
    file_descriptor_set = build_file_descriptor_set(message_class=message_class)

    return writer.register_schema(
        name=message_class.DESCRIPTOR.name,
        encoding="protobuf",
        data=file_descriptor_set.SerializeToString(),
    )


def build_file_descriptor_set(message_class: Any):
    file_descriptor_set = google.protobuf.descriptor_pb2.FileDescriptorSet()

    def append_file_descriptor(fileDescriptor: Any):
        proto = google.protobuf.descriptor_pb2.FileDescriptorProto()
        proto.ParseFromString(fileDescriptor.serialized_pb)
        file_descriptor_set.file.append(proto)

        for dep in fileDescriptor.dependencies:
            append_file_descriptor(dep)

    append_file_descriptor(message_class.DESCRIPTOR.file)
    return file_descriptor_set
