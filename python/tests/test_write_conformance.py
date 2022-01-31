from email.header import Header
from io import BytesIO
import unittest
from pathlib import Path

from mcap0.StreamWriter import StreamWriter
from mcap0.Records import *


def destringify_record(key: str, fields: list[str]) -> McapRecord:
    kv = [f.split("=", 1) for f in fields]
    data = {k: v for k, v in kv}
    if key == "Header":
        return Header(**data)
    elif key == "Attachment":
        return Attachment(**data)
    elif key == "AttachmentIndex":
        return AttachmentIndex(**data)
    elif key == "ChannelInfo":
        return ChannelInfo(**data)
    elif key == "ChunkIndex":
        return ChunkIndex(**data)
    elif key == "Footer":
        return Footer(**data)
    elif key == "Message":
        return Message(**data)
    elif key == "MessageIndex":
        return MessageIndex(**data)
    elif key == "Schema":
        return Schema(**data)
    elif key == "Statistics":
        return Statistics(**data)
    elif key == "SummaryOffset":
        return SummaryOffset(**data)
    else:
        raise Exception("Unexpected record: ", key)


class McapStreamingWriterConformanceTestCase(unittest.TestCase):
    def setUp(self) -> None:
        path = Path("../tests/conformance/data/")
        self.files = list(path.rglob("*.expected.txt"))

    def test_conformances(self):
        for file in self.files:
            print(file.name)
            source_text = Path(file).read_text().splitlines()
            while file.suffix:
                file = file.with_suffix("")
            mcap_data = file.with_suffix(".mcap").read_bytes()
            output = self.write_file(source_text)
            if mcap_data != output:
                print(mcap_data, output)

    def write_file(self, lines: list[str]) -> bytes:
        output = BytesIO()
        writer = StreamWriter(output)
        for line in lines:
            key, *fields = line.split()
            print(key, fields)
            record = destringify_record(key, fields)
            if isinstance(record, Header):
                writer.start(record.library, record.profile)
            else:
                writer.add_record(record)
            print(record)
        output.seek(0)
        data = output.read()
        writer.finish()
        return data
