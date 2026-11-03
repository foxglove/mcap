"""Tests for SeekingReader queries that delegate to NonSeekingReader."""

from io import BytesIO
from pathlib import Path
from typing import Optional

import pytest

from mcap.exceptions import RecordLengthLimitExceeded
from mcap.reader import SeekingReader
from mcap.records import Attachment, Metadata
from mcap.stream_reader import CRCValidationError
from mcap.writer import CompressionType, IndexType, Writer

from .test_read_crc_validation import produce_corrupted_mcap

ATTACHMENTS = [
    Attachment(10, 10, "duplicate", "text/plain", b"first"),
    Attachment(20, 20, "duplicate", "text/plain", b"second"),
    Attachment(30, 30, "empty", "text/plain", b""),
]
METADATA = [
    Metadata("duplicate", {"key": "first"}),
    Metadata("duplicate", {"key": "second"}),
    Metadata("empty", {}),
]
METHODS = ["iter_messages", "iter_attachments", "iter_metadata"]


def _write_fallback_mcap(
    filepath: Path,
    has_summary: bool = False,
    use_chunking: bool = False,
    large_record: Optional[str] = None,
    index_types: IndexType = IndexType.NONE,
    statistics: bool = True,
    include_records: bool = True,
):
    with open(filepath, "wb") as stream:
        writer = Writer(
            stream,
            index_types=index_types,
            repeat_channels=has_summary,
            repeat_schemas=False,
            use_statistics=has_summary and statistics,
            use_summary_offsets=has_summary,
            use_chunking=use_chunking,
            compression=CompressionType.NONE,
            enable_data_crcs=True,
        )
        writer.start()
        foo = writer.register_channel("/foo", "json", 0)
        bar = writer.register_channel("/bar", "json", 0)
        for channel, time in [(foo, 30), (foo, 10), (bar, 20), (foo, 40)]:
            data = b"x" * 256 if large_record == "iter_messages" else str(time).encode()
            writer.add_message(channel, time, data, time)
        if include_records:
            for attachment in ATTACHMENTS:
                writer.add_attachment(
                    attachment.create_time,
                    attachment.log_time,
                    attachment.name,
                    attachment.media_type,
                    (
                        b"x" * 256
                        if large_record == "iter_attachments"
                        else attachment.data
                    ),
                )
            for metadata in METADATA:
                writer.add_metadata(
                    metadata.name,
                    (
                        {"key": "x" * 256}
                        if large_record == "iter_metadata"
                        else metadata.metadata
                    ),
                )
        writer.finish()


@pytest.mark.parametrize("has_summary", [False, True])
@pytest.mark.parametrize(
    "log_time_order, reverse, expected",
    [
        (True, False, [10, 20, 30, 40]),
        (True, True, [40, 30, 20, 10]),
        (False, False, [30, 10, 20, 40]),
        (False, True, [30, 10, 20, 40]),
    ],
)
def test_message_fallback_order_and_filters(
    tmp_path: Path,
    has_summary: bool,
    log_time_order: bool,
    reverse: bool,
    expected: list[int],
):
    filepath = tmp_path / "fallback.mcap"
    _write_fallback_mcap(filepath, has_summary=has_summary, use_chunking=True)
    reader = SeekingReader(BytesIO(filepath.read_bytes()))
    summary = reader.get_summary()
    assert (summary is not None) == has_summary
    if summary is not None:
        assert not summary.chunk_indexes
    messages = list(
        reader.iter_messages(log_time_order=log_time_order, reverse=reverse)
    )
    assert [message.log_time for _, _, message in messages] == expected
    messages = list(
        reader.iter_messages(
            topics="/foo",
            starting_at=10,
            ending_before=40,
            log_time_order=log_time_order,
            reverse=reverse,
        )
    )
    assert [message.data for _, _, message in messages] == [
        str(time).encode() for time in expected if time in (10, 30)
    ]
    assert all(channel.topic == "/foo" for _, channel, _ in messages)


@pytest.mark.parametrize("method", METHODS)
def test_fallback_crc_validation(tmp_path: Path, method: str):
    filepath = tmp_path / "fallback.mcap"
    _write_fallback_mcap(filepath, use_chunking=True)
    expected_count = 4 if method == "iter_messages" else 3
    reader = SeekingReader(BytesIO(filepath.read_bytes()), validate_crcs=True)
    assert len(list(getattr(reader, method)())) == expected_count
    content = produce_corrupted_mcap(filepath, "chunk")
    for options in [{}, {"validate_crcs": False}]:
        reader = SeekingReader(BytesIO(content), **options)
        assert len(list(getattr(reader, method)())) == expected_count
    reader = SeekingReader(BytesIO(content), validate_crcs=True)
    with pytest.raises(CRCValidationError):
        list(getattr(reader, method)())


@pytest.mark.parametrize(
    "method, record_type",
    [
        ("iter_messages", "MESSAGE"),
        ("iter_attachments", "ATTACHMENT"),
        ("iter_metadata", "METADATA"),
    ],
)
def test_fallback_record_size_limit(tmp_path: Path, method: str, record_type: str):
    filepath = tmp_path / "fallback.mcap"
    _write_fallback_mcap(filepath, large_record=method)
    content = filepath.read_bytes()
    expected = list(getattr(SeekingReader(BytesIO(content)), method)())
    assert len(expected) == (4 if method == "iter_messages" else 3)
    for limit in [1024, None]:
        reader = SeekingReader(BytesIO(content), record_size_limit=limit)
        assert list(getattr(reader, method)()) == expected
    reader = SeekingReader(BytesIO(content), record_size_limit=128)
    with pytest.raises(RecordLengthLimitExceeded, match=f"{record_type} record"):
        list(getattr(reader, method)())


@pytest.mark.parametrize(
    "method, expected",
    [("iter_attachments", ATTACHMENTS), ("iter_metadata", METADATA)],
)
@pytest.mark.parametrize(
    "has_summary, statistics, index_types",
    [
        (False, False, IndexType.NONE),
        (True, False, IndexType.NONE),
        (True, True, IndexType.NONE),
        (True, True, IndexType.ATTACHMENT | IndexType.METADATA),
    ],
)
def test_attachment_and_metadata_queries_with_optional_indexes(
    tmp_path: Path,
    method: str,
    expected: list,
    has_summary: bool,
    statistics: bool,
    index_types: IndexType,
):
    filepath = tmp_path / "fallback.mcap"
    _write_fallback_mcap(
        filepath,
        has_summary=has_summary,
        statistics=statistics,
        index_types=index_types,
    )
    reader = SeekingReader(BytesIO(filepath.read_bytes()), validate_crcs=True)
    summary = reader.get_summary()
    assert (summary is not None) == has_summary
    if summary is not None:
        assert (summary.statistics is not None) == statistics
        assert len(summary.attachment_indexes) == (
            3 if index_types & IndexType.ATTACHMENT else 0
        )
        assert len(summary.metadata_indexes) == (
            3 if index_types & IndexType.METADATA else 0
        )
    assert list(getattr(reader, method)()) == expected


@pytest.mark.parametrize("statistics", [False, True])
def test_empty_attachment_and_metadata_fallback(tmp_path: Path, statistics: bool):
    filepath = tmp_path / "fallback.mcap"
    _write_fallback_mcap(
        filepath, has_summary=True, statistics=statistics, include_records=False
    )
    reader = SeekingReader(BytesIO(filepath.read_bytes()))
    assert reader.get_summary() is not None
    assert list(reader.iter_attachments()) == []
    assert list(reader.iter_metadata()) == []


def test_indexed_attachment_and_metadata_queries_do_not_scan(tmp_path: Path):
    filepath = tmp_path / "indexed.mcap"
    _write_fallback_mcap(
        filepath,
        has_summary=True,
        index_types=IndexType.ATTACHMENT | IndexType.METADATA,
    )
    content = produce_corrupted_mcap(filepath, "data_end")
    reader = SeekingReader(BytesIO(content), validate_crcs=True)
    assert list(reader.iter_attachments()) == ATTACHMENTS
    assert list(reader.iter_metadata()) == METADATA
