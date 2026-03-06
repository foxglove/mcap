import json
import os
from tempfile import NamedTemporaryFile

import pytest

from mcap.records import (
    Attachment,
    AttachmentIndex,
    ChunkIndex,
    DataEnd,
    Metadata,
    MetadataIndex,
    Schema,
    Statistics,
)
from mcap.reader import SeekingReader
from mcap.stream_reader import StreamReader
from mcap.writer import IndexType, Writer


def test_append_mode():
    """Tests that append mode preserves original records and adds new ones correctly."""
    with NamedTemporaryFile(suffix=".mcap", delete=False) as tmp:
        tmp_path = tmp.name

    try:
        # Create initial MCAP file
        writer = Writer(tmp_path)
        writer.start(library="test")
        schema_id = writer.register_schema(
            name="schema1",
            encoding="json",
            data=json.dumps({"type": "object"}).encode(),
        )
        channel_id = writer.register_channel(
            schema_id=schema_id,
            topic="channel1",
            message_encoding="json",
        )
        writer.add_message(
            channel_id=channel_id,
            log_time=0,
            data=json.dumps({"msg": "initial"}).encode("utf-8"),
            publish_time=0,
        )
        writer.finish()

        # Read original records
        with open(tmp_path, "rb") as f:
            reader = StreamReader(f)
            original_records = list(reader.records)

        # Reopen in append mode
        append_writer = Writer.open_append(tmp_path)

        # Add new records
        append_writer.add_attachment(
            name="attachment1",
            log_time=0,
            create_time=0,
            media_type="text/plain",
            data=b"foo",
        )
        append_writer.add_metadata(
            name="metadata1",
            data={"test": "testValue"},
        )
        append_writer.add_message(
            channel_id=channel_id,
            log_time=1,
            data=json.dumps({"msg": "appended"}).encode("utf-8"),
            publish_time=1,
        )
        channel_id2 = append_writer.register_channel(
            schema_id=schema_id,
            topic="channel2",
            message_encoding="json",
        )
        append_writer.add_message(
            channel_id=channel_id2,
            log_time=2,
            data=json.dumps({"msg": "new channel"}).encode("utf-8"),
            publish_time=2,
        )
        append_writer.finish()

        # Read appended records
        with open(tmp_path, "rb") as f:
            reader = StreamReader(f)
            appended_records = list(reader.records)

        # Validate original records are preserved
        original_schemas = [r for r in original_records if isinstance(r, Schema)]
        appended_schemas = [r for r in appended_records if isinstance(r, Schema)]
        assert len(appended_schemas) == len(original_schemas)
        for orig, app in zip(original_schemas, appended_schemas):
            assert orig.id == app.id
            assert orig.name == app.name
            assert orig.encoding == app.encoding
            assert orig.data == app.data

        # Validate new records are present
        attachments = [r for r in appended_records if isinstance(r, Attachment)]
        assert len(attachments) == 1
        assert attachments[0].name == "attachment1"
        assert attachments[0].data == b"foo"

        metadata_list = [r for r in appended_records if isinstance(r, Metadata)]
        assert len(metadata_list) == 1
        assert metadata_list[0].name == "metadata1"
        assert metadata_list[0].metadata == {"test": "testValue"}

        # Validate new channel is present
        with open(tmp_path, "rb") as f:
            seeking_reader = SeekingReader(f)
            summary = seeking_reader.get_summary()
            assert summary is not None
            assert len(summary.channels) == 2
            channel_topics = {ch.topic for ch in summary.channels.values()}
            assert "channel1" in channel_topics
            assert "channel2" in channel_topics

        # Validate statistics updated
        statistics = next(r for r in appended_records if isinstance(r, Statistics))
        assert statistics.message_count == 3
        assert statistics.channel_count == 2
        assert statistics.schema_count == 1
        assert statistics.attachment_count == 1
        assert statistics.metadata_count == 1
        assert statistics.chunk_count == 2

        # Validate chunk indexes updated
        chunk_indexes = [r for r in appended_records if isinstance(r, ChunkIndex)]
        assert len(chunk_indexes) == 2

        # Validate attachment index is present
        attachment_indexes = [
            r for r in appended_records if isinstance(r, AttachmentIndex)
        ]
        assert len(attachment_indexes) == 1
        assert attachment_indexes[0].name == "attachment1"

        # Validate metadata index is present
        metadata_indexes = [r for r in appended_records if isinstance(r, MetadataIndex)]
        assert len(metadata_indexes) == 1
        assert metadata_indexes[0].name == "metadata1"

    finally:
        if os.path.exists(tmp_path):
            os.unlink(tmp_path)


def test_append_conflict():
    """Tests that conflict detection raises ValueError for mismatched schemas/channels."""
    with NamedTemporaryFile(suffix=".mcap", delete=False) as tmp:
        tmp_path = tmp.name

    try:
        # Create initial MCAP file
        writer = Writer(tmp_path)
        writer.start(library="test")
        schema_id = writer.register_schema(
            name="schema1",
            encoding="json",
            data=json.dumps({"type": "object"}).encode(),
        )
        writer.register_channel(
            schema_id=schema_id,
            topic="channel1",
            message_encoding="json",
        )
        writer.finish()

        # Reopen in append mode
        append_writer = Writer.open_append(tmp_path)

        # Test that re-registering existing schema with same data returns same ID
        same_schema_id = append_writer.register_schema(
            name="schema1",
            encoding="json",
            data=json.dumps({"type": "object"}).encode(),
        )
        assert same_schema_id == 1

        # Test that re-registering existing channel with same data returns same ID
        same_channel_id = append_writer.register_channel(
            schema_id=schema_id,
            topic="channel1",
            message_encoding="json",
        )
        assert same_channel_id == 1

        # Test conflict for channel
        with pytest.raises(ValueError) as exc_info:
            # Topic exists with message_encoding="json", try registering with "protobuf"
            append_writer.register_channel(
                schema_id=schema_id,
                topic="channel1",
                message_encoding="protobuf",
            )
        assert "differs from previous channel record" in str(exc_info.value)

    finally:
        if os.path.exists(tmp_path):
            os.unlink(tmp_path)


def test_append_non_indexed():
    """Tests that appending to a non-indexed MCAP raises ValueError."""
    with NamedTemporaryFile(suffix=".mcap", delete=False) as tmp:
        tmp_path = tmp.name

    try:
        # Create MCAP file without summary (non-indexed)
        writer = Writer(
            tmp_path,
            use_summary_offsets=False,
            use_statistics=False,
            repeat_schemas=False,
            repeat_channels=False,
            index_types=IndexType.NONE,
        )
        writer.start(library="test")
        schema_id = writer.register_schema(
            name="schema1",
            encoding="json",
            data=json.dumps({"type": "object"}).encode(),
        )
        channel_id = writer.register_channel(
            schema_id=schema_id, topic="channel1", message_encoding="json"
        )
        writer.add_message(
            channel_id=channel_id, log_time=0, data=b"{}", publish_time=0
        )
        writer.finish()

        # Try to open non-indexed MCAP for append - should raise ValueError
        with pytest.raises(ValueError) as exc_info:
            Writer.open_append(tmp_path)
        assert "cannot append to MCAP without summary" in str(exc_info.value)

    finally:
        if os.path.exists(tmp_path):
            os.unlink(tmp_path)


def test_append_crc_handling():
    """Tests that CRC handling works correctly when original file has CRC enabled/disabled."""
    with NamedTemporaryFile(suffix=".mcap", delete=False) as tmp:
        tmp_path = tmp.name

    try:
        # Test with data section CRC enabled
        writer = Writer(tmp_path, enable_data_crcs=True)
        writer.start(library="test")
        schema_id = writer.register_schema(
            name="schema1",
            encoding="json",
            data=json.dumps({"type": "object"}).encode(),
        )
        channel_id = writer.register_channel(
            schema_id=schema_id, topic="channel1", message_encoding="json"
        )
        writer.add_message(
            channel_id=channel_id, log_time=0, data=b"{}", publish_time=0
        )
        writer.finish()

        # Read original data end CRC
        with open(tmp_path, "rb") as f:
            reader = StreamReader(f)
            records = list(reader.records)
            original_data_end = next(r for r in records if isinstance(r, DataEnd))
            assert original_data_end.data_section_crc != 0

        # Reopen in append mode and add more data
        append_writer = Writer.open_append(tmp_path)
        append_writer.add_message(
            channel_id=channel_id, log_time=1, data=b"{}", publish_time=1
        )
        append_writer.finish()

        # Verify CRC is still present and updated
        with open(tmp_path, "rb") as f:
            reader = StreamReader(f)
            records = list(reader.records)
            new_data_end = next(r for r in records if isinstance(r, DataEnd))
            assert new_data_end.data_section_crc != 0
            assert new_data_end.data_section_crc != original_data_end.data_section_crc

    finally:
        if os.path.exists(tmp_path):
            os.unlink(tmp_path)


def test_append_statistics_disabled():
    """Tests that statistics are disabled when source file has no statistics."""
    with NamedTemporaryFile(suffix=".mcap", delete=False) as tmp:
        tmp_path = tmp.name

    try:
        # Create MCAP file without statistics
        writer = Writer(tmp_path, use_statistics=False)
        writer.start(library="test")
        schema_id = writer.register_schema(
            name="schema1",
            encoding="json",
            data=json.dumps({"type": "object"}).encode(),
        )
        channel_id = writer.register_channel(
            schema_id=schema_id, topic="channel1", message_encoding="json"
        )
        writer.add_message(
            channel_id=channel_id, log_time=0, data=b"{}", publish_time=0
        )
        writer.finish()

        # Reopen in append mode
        append_writer = Writer.open_append(tmp_path)
        append_writer.add_message(
            channel_id=channel_id, log_time=1, data=b"{}", publish_time=1
        )
        append_writer.finish()

        # Verify no Statistics record in appended file
        with open(tmp_path, "rb") as f:
            reader = StreamReader(f)
            records = list(reader.records)
            statistics = [r for r in records if isinstance(r, Statistics)]
            assert len(statistics) == 0

    finally:
        if os.path.exists(tmp_path):
            os.unlink(tmp_path)
