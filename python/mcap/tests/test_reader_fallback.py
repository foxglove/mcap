"""Tests for SeekingReader queries that delegate to NonSeekingReader."""

import struct
from io import BytesIO
from pathlib import Path
from typing import Any, Optional

import pytest

from mcap.exceptions import EndOfFile, RecordLengthLimitExceeded
from mcap.reader import SeekingReader
from mcap.stream_reader import CRCValidationError
from mcap.writer import CompressionType, IndexType, Writer

from .test_read_crc_validation import produce_corrupted_mcap


def _write_fallback_mcap(
    filepath: Path,
    has_summary: bool = False,
    use_chunking: bool = False,
    large_record: Optional[str] = None,
):
    with open(filepath, "wb") as stream:
        writer = Writer(
            stream,
            index_types=IndexType.NONE,
            repeat_channels=has_summary,
            repeat_schemas=False,
            use_statistics=has_summary,
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
        writer.add_attachment(
            10,
            10,
            "attachment",
            "text/plain",
            b"x" * 256 if large_record == "iter_attachments" else b"attachment data",
        )
        writer.add_metadata(
            "metadata",
            {"key": "x" * 256 if large_record == "iter_metadata" else "value"},
        )
        writer.finish()


@pytest.mark.parametrize("has_summary", [False, True])
@pytest.mark.parametrize("log_time_order", [False, True])
@pytest.mark.parametrize("reverse", [False, True])
@pytest.mark.parametrize(
    "filters, file_times",
    [
        ({}, [30, 10, 20, 40]),
        ({"topics": "/foo"}, [30, 10, 40]),
        ({"start_time": 10, "end_time": 40}, [30, 10, 20]),
        ({"topics": ["/foo"], "start_time": 10, "end_time": 40}, [30, 10]),
    ],
)
def test_message_fallback_order_and_filters(
    tmp_path: Path,
    has_summary: bool,
    log_time_order: bool,
    reverse: bool,
    filters: dict[str, Any],
    file_times: list[int],
):
    filepath = tmp_path / "fallback.mcap"
    _write_fallback_mcap(filepath, has_summary=has_summary, use_chunking=True)
    with open(filepath, "rb") as stream:
        reader = SeekingReader(stream)
        summary = reader.get_summary()
        if has_summary:
            assert summary is not None
            assert not summary.chunk_indexes
        else:
            assert summary is None
        messages = list(
            reader.iter_messages(
                log_time_order=log_time_order, reverse=reverse, **filters
            )
        )
    expected = sorted(file_times, reverse=reverse) if log_time_order else file_times
    assert [message.log_time for _, _, message in messages] == expected
    assert [message.data for _, _, message in messages] == [
        str(time).encode() for time in expected
    ]


@pytest.mark.parametrize(
    "method, has_summary",
    [
        ("iter_messages", False),
        ("iter_messages", True),
        ("iter_attachments", False),
        ("iter_metadata", False),
    ],
)
@pytest.mark.parametrize("to_corrupt", [None, "chunk", "data_end"])
@pytest.mark.parametrize("validate_crcs", [False, True])
def test_fallback_crc_validation(
    tmp_path: Path,
    method: str,
    has_summary: bool,
    to_corrupt: Optional[str],
    validate_crcs: bool,
):
    filepath = tmp_path / "fallback.mcap"
    _write_fallback_mcap(filepath, has_summary=has_summary, use_chunking=True)
    content = (
        produce_corrupted_mcap(filepath, to_corrupt)
        if to_corrupt is not None
        else filepath.read_bytes()
    )
    reader = SeekingReader(BytesIO(content), validate_crcs=validate_crcs)
    if validate_crcs and to_corrupt is not None:
        with pytest.raises(CRCValidationError):
            list(getattr(reader, method)())
    else:
        assert len(list(getattr(reader, method)())) == (
            4 if method == "iter_messages" else 1
        )


@pytest.mark.parametrize(
    "method, has_summary",
    [
        ("iter_messages", False),
        ("iter_messages", True),
        ("iter_attachments", False),
        ("iter_metadata", False),
    ],
)
def test_fallback_custom_record_size_limit(
    tmp_path: Path, method: str, has_summary: bool
):
    filepath = tmp_path / "fallback.mcap"
    _write_fallback_mcap(filepath, has_summary=has_summary, large_record=method)
    with open(filepath, "rb") as stream:
        reader = SeekingReader(stream, record_size_limit=128)
        with pytest.raises(RecordLengthLimitExceeded, match="exceeds limit 128"):
            list(getattr(reader, method)())


@pytest.mark.parametrize(
    "method", ["iter_messages", "iter_attachments", "iter_metadata"]
)
@pytest.mark.parametrize("record_size_limit", ["default", 1024, None])
def test_fallback_record_size_limit_allows_small_records(
    tmp_path: Path, method: str, record_size_limit: Any
):
    filepath = tmp_path / "fallback.mcap"
    _write_fallback_mcap(filepath)
    with open(filepath, "rb") as stream:
        kwargs = (
            {}
            if record_size_limit == "default"
            else {"record_size_limit": record_size_limit}
        )
        reader = SeekingReader(stream, **kwargs)
        assert len(list(getattr(reader, method)())) == (
            4 if method == "iter_messages" else 1
        )


@pytest.mark.parametrize(
    "method", ["iter_messages", "iter_attachments", "iter_metadata"]
)
@pytest.mark.parametrize("record_size_limit", ["default", 4 * 2**30 + 1, None])
def test_fallback_record_size_limit_above_default(
    tmp_path: Path, method: str, record_size_limit: Any
):
    filepath = tmp_path / "fallback.mcap"
    _write_fallback_mcap(filepath)
    content = filepath.read_bytes()
    header_end = 8 + 9 + struct.unpack_from("<Q", content, 9)[0]
    # A truncated unknown record tests the limit without allocating a large payload.
    content = (
        content[:header_end]
        + struct.pack("<BQ", 0x80, 4 * 2**30 + 1)
        + content[header_end:]
    )
    kwargs = (
        {}
        if record_size_limit == "default"
        else {"record_size_limit": record_size_limit}
    )
    reader = SeekingReader(BytesIO(content), **kwargs)
    expected_error = (
        RecordLengthLimitExceeded if record_size_limit == "default" else EndOfFile
    )
    with pytest.raises(expected_error):
        list(getattr(reader, method)())
