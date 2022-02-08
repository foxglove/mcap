import difflib
import json
import sys
import unittest
from io import BytesIO
from pathlib import Path
from typing import Any, Dict, List

# Need to ignore warnings here because we reference these dynamically by name.
from mcap.mcap0.records import (
    Attachment,  # type: ignore
    AttachmentIndex,  # type: ignore
    Channel,  # type: ignore
    ChunkIndex,  # type: ignore
    Footer,  # type: ignore
    Header,  # type: ignore
    McapRecord,  # type: ignore
    Message,  # type: ignore
    MessageIndex,  # type: ignore
    Schema,  # type: ignore
    Statistics,  # type: ignore
    SummaryOffset,  # type: ignore
)
from mcap.mcap0.stream_writer import StreamWriter
from mcap.mcap0.stream_writer import IndexType


def hexize_bytes(b: bytes) -> List[str]:
    return ["{:02x}".format(i) for i in b]


def deserialize_value(klass: McapRecord, field: str, value: Any) -> Any:
    field_type = klass.__dataclass_fields__[field].type
    if field_type == str:
        return value
    if field_type == int:
        return int(value)
    if field_type == bytes:
        return bytes([int(v) for v in value])
    if field_type == Dict[int, int]:
        return {int(k): int(v) for k, v in value.items()}
    return value


def deserialize_record(key: str, fields: List[Dict[str, Any]]) -> McapRecord:
    klass_name = "Channel" if key == "ChannelInfo" else key
    klass = getattr(sys.modules[__name__], klass_name)
    data = {k: deserialize_value(klass, k, v) for k, v in fields}
    return klass(**data)


def index_type_from_features(features: List[str]) -> IndexType:
    type = IndexType.NONE
    if "ax" in features:
        type |= IndexType.ATTACHMENT
    return type


class McapStreamingWriterConformanceTestCase(unittest.TestCase):
    def setUp(self) -> None:
        path = Path("../tests/conformance/data/")
        self.files = list(path.rglob("*.json"))

    def test_conformances(self):
        for file in self.files:
            expected_text = Path(file).read_text()
            expected_json = json.loads(expected_text)
            expected_meta = expected_json["meta"]
            features = expected_meta["variant"]["features"]
            if set(["ch", "pad", "rch", "rsh", "sum", "st"]).intersection(features):
                continue
            print(file.name)
            expected_records = expected_json["records"]
            while file.suffix:
                file = file.with_suffix("")
            mcap_data = file.with_suffix(".mcap").read_bytes()
            output = self.write_file(features, expected_records)
            if mcap_data != output:
                print(expected_text)
                diff = difflib.unified_diff(
                    hexize_bytes(mcap_data), hexize_bytes(output)
                )
                print("".join(hexize_bytes(mcap_data)))
                print("-" * 80)
                print("".join(hexize_bytes(output)))
                print("\n".join(list(diff)))
            print()

    def write_file(
        self, features: List[str], expected_records: List[Dict[str, Any]]
    ) -> bytes:
        output = BytesIO()
        writer = StreamWriter(
            output,
            profile="",
            index_types=index_type_from_features(features),
            use_chunking="ch" in features,
        )
        for line in expected_records:
            type, fields = line["type"], line["fields"]
            record = deserialize_record(type, fields)
            if isinstance(record, Header):
                writer.start(record.profile, record.library)
            if isinstance(record, Attachment):
                writer.add_attachment(record)
            if isinstance(record, Channel):
                writer.register_channel(
                    topic=record.topic,
                    message_encoding=record.message_encoding,
                    metadata=record.metadata,
                )
            if isinstance(record, Message):
                writer.add_message(record)
            if isinstance(record, Schema):
                writer.add_schema(record)
        writer.finish()
        output.seek(0)
        data = output.read()
        return data
