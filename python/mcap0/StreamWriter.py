from io import BufferedWriter, BytesIO, RawIOBase
from typing import Union

from .DataStream import WriteDataStream
from .Records import ChannelInfo, Footer, Header, McapRecord


class StreamWriter:
    def __init__(self, output: Union[str, BytesIO, BufferedWriter]):
        self.__next_channel_id = 0
        if isinstance(output, str):
            self.__stream = WriteDataStream(open(output, "wb"))
        elif isinstance(output, RawIOBase):
            self.__stream = WriteDataStream(BufferedWriter(output))
        else:
            self.__stream = WriteDataStream(output)

    def add_record(self, record: McapRecord):
        record.write(self.__stream)

    def finish(self):
        Footer(summary_start=0, summary_offset_start=0, summary_crc=0).write(
            self.__stream
        )
        self.__stream.write_magic()

    def register_channel(
        self,
        topic: str,
        message_encoding: str,
        metadata: dict[str, str],
    ) -> int:
        ChannelInfo(
            id=self.__next_channel_id,
            topic=topic,
            message_encoding=message_encoding,
            schema_id=1,
            metadata=metadata,
        ).write(self.__stream)
        self.__next_channel_id += 1
        return self.__next_channel_id

    def start(self, profile: str, library: str):
        self.__stream.write_magic()
        Header(profile, library).write(self.__stream)
