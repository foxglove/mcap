import json
import sys
from io import BytesIO
from pathlib import Path
from typing import Any, Dict, List

# Need to ignore warnings here because we reference these dynamically by name.
from mcap.mcap0.records import Attachment  # type: ignore
from mcap.mcap0.records import AttachmentIndex  # type: ignore
from mcap.mcap0.records import Channel  # type: ignore
from mcap.mcap0.records import ChunkIndex  # type: ignore
from mcap.mcap0.records import Footer  # type: ignore
from mcap.mcap0.records import Header  # type: ignore
from mcap.mcap0.records import McapRecord  # type: ignore
from mcap.mcap0.records import Message  # type: ignore
from mcap.mcap0.records import MessageIndex  # type: ignore
from mcap.mcap0.records import Schema  # type: ignore
from mcap.mcap0.records import Statistics  # type: ignore
from mcap.mcap0.records import SummaryOffset  # type: ignore
from mcap.mcap0.stream_writer import IndexType, StreamWriter


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
    klass = getattr(sys.modules[__name__], key)
    data = {k: deserialize_value(klass, k, v) for k, v in fields}
    return klass(**data)


def index_type_from_features(features: List[str]) -> IndexType:
    type = IndexType.NONE
    if "ax" in features:
        type |= IndexType.ATTACHMENT
    return type


def write_file(features: List[str], expected_records: List[Dict[str, Any]]) -> bytes:
    output = BytesIO()
    writer = StreamWriter(
        output,
        profile="",
        index_types=index_type_from_features(features),
        use_chunking="ch" in features,
        use_statistics="st" in features,
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
    return output.getvalue()


def main():
    input_text = Path(sys.argv[1]).read_text()
    input_json = json.loads(input_text)
    input_meta = input_json["meta"]
    features = input_meta["variant"]["features"]
    expected_records = input_json["records"]
    output = write_file(features, expected_records)
    sys.stdout.buffer.write(output)


if __name__ == "__main__":
    main()
