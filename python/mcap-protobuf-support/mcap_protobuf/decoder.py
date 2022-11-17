from collections import Counter
from typing import Dict, Any, Type, Iterable

from google.protobuf.descriptor_pb2 import FileDescriptorSet, FileDescriptorProto
from google.protobuf.message_factory import MessageFactory
from mcap.exceptions import McapError
from mcap.records import Message, Schema
from mcap.well_known import SchemaEncoding


class McapProtobufDecodeError(McapError):
    """Raised when a Message record cannot be decoded as a Protobuf message."""

    pass


class Decoder:
    def __init__(self):
        """Decodes Protobuf messages from MCAP message records."""
        self._types: Dict[int, Type[Any]] = {}
        self._factory = MessageFactory()

    def _get_message_classes(self, file_descriptors: Iterable[FileDescriptorProto]):
        """Adds file descriptors to the message factory pool in topological order, then returns
        the message classes for all file descriptors.

        Modified from the original at:
        https://github.com/protocolbuffers/protobuf/blob/main/python/google/protobuf/message_factory.py

        Protocol Buffers - Google's data interchange format
        Copyright 2008 Google Inc.  All rights reserved.
        https://developers.google.com/protocol-buffers/

        Redistribution and use in source and binary forms, with or without
        modification, are permitted provided that the following conditions are
        met:

            * Redistributions of source code must retain the above copyright
        notice, this list of conditions and the following disclaimer.
            * Redistributions in binary form must reproduce the above
        copyright notice, this list of conditions and the following disclaimer
        in the documentation and/or other materials provided with the
        distribution.
            * Neither the name of Google Inc. nor the names of its
        contributors may be used to endorse or promote products derived from
        this software without specific prior written permission.

        THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
        "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
        LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
        A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
        OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
        SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
        LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
        DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
        THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
        (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
        OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
        """
        descriptor_by_name = {
            file_descriptor.name: file_descriptor
            for file_descriptor in file_descriptors
        }

        def _add_file(file_descriptor: FileDescriptorProto):
            for dependency in file_descriptor.dependency:
                if dependency in descriptor_by_name:
                    # Remove from elements to be visited, in order to cut cycles.
                    _add_file(descriptor_by_name.pop(dependency))
            self._factory.pool.Add(file_descriptor)

        while descriptor_by_name:
            _add_file(descriptor_by_name.popitem()[1])

        return self._factory.GetMessages(
            [file_descriptor.name for file_descriptor in file_descriptors]
        )

    def decode(self, schema: Schema, message: Message) -> Any:
        """Takes a Message record from an MCAP along with its associated Schema,
        and returns the decoded protobuf message from within.

        :param schema: The message schema record from the MCAP.
        :type schema: mcap.records.Schema
        :param message: The message record containing content to be decoded.
        :type message: mcap.records.Message
        :raises McapProtobufDecodeError: if the content could not be decoded as a protobuf message
            with the given schema.
        :return: The decoded message content.
        """
        if schema.encoding != SchemaEncoding.Protobuf:
            raise McapProtobufDecodeError(
                f"can't decode schema with encoding {schema.encoding}"
            )
        generated = self._types.get(schema.id)
        if generated is None:
            fds = FileDescriptorSet.FromString(schema.data)
            for name, count in Counter(fd.name for fd in fds.file).most_common(1):
                if count > 1:
                    raise McapError(
                        f"FileDescriptorSet contains {count} file descriptors for {name}"
                    )
            messages = self._get_message_classes(fds.file)
            for name, klass in messages.items():
                if name == schema.name:
                    self._types[schema.id] = klass
                    generated = klass
        if generated is None:
            raise McapError(
                f"FileDescriptorSet for type {schema.name} is missing that schema"
            )
        proto_msg = generated()
        proto_msg.ParseFromString(message.data)
        return proto_msg
