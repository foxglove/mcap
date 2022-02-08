import io
import time
import unittest

from mcap.mcap0.records import Attachment, Message
from mcap.mcap0.stream_writer import StreamWriter


class McapWriterTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = io.BytesIO()
        self.writer = StreamWriter(self.temp)

    def test_write(self):
        self.writer.start("test-profile", "test-library")
        channel_id = self.writer.register_channel(
            topic="test-topic",
            message_encoding="test-message-encoding",
            metadata={"first": "one", "second": "two"},
        )
        for seq in range(3):
            self.writer.add_record(
                Message(
                    channel_id=channel_id,
                    log_time=time.time_ns(),
                    publish_time=time.time_ns(),
                    sequence=seq,
                    data=f"test message data {seq}".encode(),
                )
            )
        for seq in range(3):
            self.writer.add_record(
                Attachment(
                    name=f"test_attachment_{seq}.txt",
                    created_at=time.time_ns(),
                    log_time=time.time_ns(),
                    content_type="text/plain",
                    data=f"Test attachment content {seq}".encode(),
                )
            )
        self.writer.finish()
        self.temp.seek(0)
        print(self.temp.read())
