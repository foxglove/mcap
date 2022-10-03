import json
import sys
from io import BytesIO
from pathlib import Path
from typing import Any, Dict, List, Set, Type

from mcap.records import (
    Attachment,
    Channel,
    Header,
    McapRecord,
    Message,
    Metadata,
    Schema,
)
from mcap.writer import CompressionType, IndexType, Writer


def deserialize_value(klass: Type[McapRecord], field: str, value: Any) -> Any:
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
    klass = [c for c in McapRecord.__subclasses__() if c.__name__ == key][0]
    data = {k: deserialize_value(klass, k, v) for k, v in fields}
    return klass(**data)


def index_type_from_features(features: List[str]) -> IndexType:
    type = IndexType.NONE
    if "ax" in features:
        type |= IndexType.ATTACHMENT
    if "chx" in features:
        type |= IndexType.CHUNK
    if "mx" in features:
        type |= IndexType.MESSAGE
    if "mdx" in features:
        type |= IndexType.METADATA
    return type


def write_file(features: List[str], expected_records: List[Dict[str, Any]]) -> bytes:
    seen_channels: Set[str] = set()
    seen_schemas: Set[str] = set()
    output = BytesIO()
    writer = Writer(
        output=output,
        index_types=index_type_from_features(features),
        compression=CompressionType.NONE,
        repeat_channels="rch" in features,
        repeat_schemas="rsh" in features,
        use_chunking="ch" in features,
        use_statistics="st" in features,
        use_summary_offsets="sum" in features,
    )
    for line in expected_records:
        type, fields = line["type"], line["fields"]
        record = deserialize_record(type, fields)
        if isinstance(record, Header):
            writer.start(record.profile, record.library)
        if isinstance(record, Attachment):
            writer.add_attachment(
                create_time=record.create_time,
                log_time=record.log_time,
                name=record.name,
                media_type=record.media_type,
                data=record.data,
            )
        if isinstance(record, Channel):
            if record.topic not in seen_channels:
                writer.register_channel(
                    schema_id=record.schema_id,
                    topic=record.topic,
                    message_encoding=record.message_encoding,
                    metadata=record.metadata,
                )
            seen_channels.add(record.topic)
        if isinstance(record, Message):
            writer.add_message(
                channel_id=record.channel_id,
                log_time=record.log_time,
                data=record.data,
                publish_time=record.publish_time,
                sequence=record.sequence,
            )
        if isinstance(record, Schema):
            if record.name not in seen_schemas:
                writer.register_schema(
                    name=record.name, encoding=record.encoding, data=record.data
                )
            seen_schemas.add(record.name)
        if isinstance(record, Metadata):
            writer.add_metadata(record.name, record.metadata)
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
