import warnings
from collections import Counter
from typing import Any, Callable, Dict, Iterable, Optional, Type

from google.protobuf.descriptor_pb2 import FileDescriptorProto, FileDescriptorSet
from google.protobuf.message_factory import MessageFactory

from mcap.decoder import DecoderFactory as McapDecoderFactory
from mcap.exceptions import McapError
from mcap.records import Message, Schema
from mcap.well_known import MessageEncoding, SchemaEncoding


class McapProtobufDecodeError(McapError):
    """Raised when a Message record cannot be decoded as a Protobuf message."""

    pass


class DecoderFactory(McapDecoderFactory):
    """Provides functionality to an :py:class:`~mcap.reader.McapReader` to decode protobuf
    messages. Requires valid `protobuf` schemas to decode messages.
    """

    def __init__(self):
        self._types: Dict[int, Type[Any]] = {}

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
        factory = MessageFactory()

        def _add_file(file_descriptor: FileDescriptorProto):
            for dependency in file_descriptor.dependency:
                if dependency in descriptor_by_name:
                    # Remove from elements to be visited, in order to cut cycles.
                    _add_file(descriptor_by_name.pop(dependency))
            factory.pool.Add(file_descriptor)

        while descriptor_by_name:
            _add_file(descriptor_by_name.popitem()[1])

        return factory.GetMessages(
            [file_descriptor.name for file_descriptor in file_descriptors]
        )

    def decoder_for(
        self, message_encoding: str, schema: Optional[Schema]
    ) -> Optional[Callable[[bytes], Any]]:
        if (
            message_encoding != MessageEncoding.Protobuf
            or schema is None
            or schema.encoding != SchemaEncoding.Protobuf
        ):
            return None

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

        def decoder(data: bytes) -> Any:
            proto_msg = generated()
            proto_msg.ParseFromString(data)
            return proto_msg

        return decoder


class Decoder:
    """Decodes Protobuf messages.

    .. deprecated:: 0.3.0
      Use :py:class:`~mcap_protobuf.decoder.DecoderFactory` with :py:class:`~mcap.reader.McapReader`
      instead.
    """

    def __init__(self):
        warnings.warn(
            """The `mcap_protobuf.decoder.Decoder` class is deprecated.
For similar functionality, instantiate the `mcap.reader.McapReader` with a
`mcap_protobuf.decoder.DecoderFactory` instance.""",
            DeprecationWarning,
        )
        self._decoder_factory = DecoderFactory()

    def decode(self, schema: Schema, message: Message) -> Any:
        decoder = self._decoder_factory.decoder_for(MessageEncoding.Protobuf, schema)
        assert decoder is not None, "failed to construct a Protobuf decoder"
        return decoder(message.data)
