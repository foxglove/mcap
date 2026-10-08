"""Metadata queries with optional summary indexes."""

from io import BytesIO
from typing import List, Optional

import pytest

from mcap.exceptions import RecordLengthLimitExceeded
from mcap.reader import NonSeekingReader, SeekingReader
from mcap.records import Metadata
from mcap.stream_reader import CRCValidationError
from mcap.writer import CompressionType, IndexType, Writer


def metadata_mcap(
    records: List[Metadata],
    indexed: bool = False,
    statistics: bool = True,
    summary: bool = True,
) -> bytes:
    stream = BytesIO()
    writer = Writer(
        stream,
        compression=CompressionType.NONE,
        index_types=IndexType.METADATA if indexed else IndexType.NONE,
        use_statistics=statistics if summary else False,
        repeat_channels=summary,
        repeat_schemas=False,
        use_summary_offsets=summary,
        enable_data_crcs=True,
    )
    writer.start()
    if summary:
        writer.register_channel("/topic", "json", 0)
    for record in records:
        writer.add_metadata(record.name, record.metadata)
    writer.finish()
    return stream.getvalue()


@pytest.mark.parametrize("statistics", [False, True])
@pytest.mark.parametrize("indexed", [False, True])
@pytest.mark.parametrize("empty", [False, True])
def test_metadata_with_optional_indexes(statistics: bool, indexed: bool, empty: bool):
    expected = (
        []
        if empty
        else [
            Metadata("duplicate", {"key": "first"}),
            Metadata("duplicate", {"key": "second"}),
            Metadata("other", {"key": "third"}),
        ]
    )
    content = metadata_mcap(expected, indexed=indexed, statistics=statistics)
    reader = SeekingReader(BytesIO(content), validate_crcs=True)
    summary = reader.get_summary()
    assert summary is not None
    assert (summary.statistics is not None) == statistics
    assert len(summary.metadata_indexes) == (len(expected) if indexed else 0)
    if summary.statistics is not None:
        assert summary.statistics.metadata_count == len(expected)
    assert list(reader.iter_metadata()) == expected
    assert (
        list(NonSeekingReader(BytesIO(content), validate_crcs=True).iter_metadata())
        == expected
    )


def test_metadata_without_summary_control():
    expected = [Metadata("name", {"key": "value"})]
    content = metadata_mcap(expected, summary=False)
    reader = SeekingReader(BytesIO(content))
    assert reader.get_summary() is None
    assert list(reader.iter_metadata()) == expected


@pytest.mark.parametrize("limit", [100, 1000, None])
def test_metadata_fallback_preserves_record_limit(limit: Optional[int]):
    expected = [Metadata("name", {"key": "x" * 128})]
    content = metadata_mcap(expected)
    reader = SeekingReader(BytesIO(content), record_size_limit=limit)
    assert reader.get_summary() is not None
    if limit == 100:
        with pytest.raises(RecordLengthLimitExceeded, match="METADATA"):
            list(reader.iter_metadata())
    else:
        assert list(reader.iter_metadata()) == expected


@pytest.mark.parametrize("validate_crcs", [False, True])
@pytest.mark.parametrize("indexed", [False, True])
def test_metadata_fallback_preserves_crc_validation(validate_crcs: bool, indexed: bool):
    expected = [Metadata("name", {"key": "value"})]
    content = metadata_mcap(expected, indexed=indexed)
    # Change only a metadata value, leaving the data-section CRC incorrect.
    content = content.replace(b"value", b"other", 1)
    reader = SeekingReader(BytesIO(content), validate_crcs=validate_crcs)
    assert reader.get_summary() is not None
    # Indexed reads do not scan or validate the entire data section.
    if validate_crcs and not indexed:
        with pytest.raises(CRCValidationError):
            list(reader.iter_metadata())
    else:
        assert list(reader.iter_metadata()) == [Metadata("name", {"key": "other"})]
