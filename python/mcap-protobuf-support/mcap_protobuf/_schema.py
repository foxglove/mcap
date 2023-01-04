from typing import Any, Set
from google.protobuf.descriptor import FileDescriptor
from google.protobuf.descriptor_pb2 import FileDescriptorSet
from mcap.writer import Writer as McapWriter


def register_schema(writer: McapWriter, message_class: Any):
    file_descriptor_set = build_file_descriptor_set(message_class=message_class)

    return writer.register_schema(
        name=message_class.DESCRIPTOR.full_name,
        encoding="protobuf",
        data=file_descriptor_set.SerializeToString(),
    )


def build_file_descriptor_set(message_class: Any) -> FileDescriptorSet:
    file_descriptor_set = FileDescriptorSet()
    seen_dependencies: Set[str] = set()

    def append_file_descriptor(file_descriptor: FileDescriptor):
        for dep in file_descriptor.dependencies:
            if dep.name not in seen_dependencies:
                seen_dependencies.add(dep.name)
                append_file_descriptor(dep)
        file_descriptor.CopyToProto(file_descriptor_set.file.add())

    append_file_descriptor(message_class.DESCRIPTOR.file)
    return file_descriptor_set
