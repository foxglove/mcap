"""Tests for SeekingReader queries that fall back to a linear NonSeekingReader scan."""

from io import BytesIO
from typing import Any, List, Tuple

import pytest

from mcap.exceptions import RecordLengthLimitExceeded
from mcap.reader import SeekingReader
from mcap.stream_reader import CRCValidationError
from mcap.writer import CompressionType, IndexType, Writer

METHODS = ["iter_messages", "iter_attachments", "iter_metadata"]
MESSAGE_DATA = b"message-data"


def _write(
    index_types: IndexType = IndexType.NONE,
    statistics: bool = True,
    use_chunking: bool = True,
    records: int = 2,
    size: int = 1,
) -> bytes:
    out = BytesIO()
    writer = Writer(
        out,
        index_types=index_types,
        use_statistics=statistics,
        use_chunking=use_chunking,
        compression=CompressionType.NONE,
    )
    writer.start()
    schema = writer.register_schema("s", "jsonschema", b"{}")
    channel = writer.register_channel("/a", "json", schema)
    for sequence, log_time in enumerate([30, 10, 20, 20]):
        writer.add_message(channel, log_time, MESSAGE_DATA * size, log_time, sequence)
    for i in range(records):
        writer.add_attachment(i, i, f"att{i}", "text/plain", b"a" * size)
        writer.add_metadata(f"meta{i}", {"k": "v" * size})
    writer.finish()
    return out.getvalue()


def _order(reader: SeekingReader, **kwargs: Any) -> List[Tuple[int, int]]:
    return [(m.log_time, m.sequence) for _, _, m in reader.iter_messages(**kwargs)]


@pytest.mark.parametrize("use_chunking", [False, True])
def test_reverse_without_chunk_index(use_chunking: bool):
    reader = SeekingReader(BytesIO(_write(use_chunking=use_chunking)))
    summary = reader.get_summary()
    assert summary is not None and not summary.chunk_indexes
    forward = _order(reader)
    assert forward == [(10, 1), (20, 2), (20, 3), (30, 0)]
    assert _order(reader, reverse=True) == forward[::-1]
    window = {"starting_at": 15, "ending_before": 30}
    assert _order(reader, reverse=True, **window) == [(20, 3), (20, 2)]


@pytest.mark.parametrize("method", METHODS)
def test_validate_crcs_without_indexes(method: str):
    data = bytearray(_write())
    # Corrupt a message payload inside the (uncompressed) chunk.
    data[data.index(MESSAGE_DATA)] ^= 0xFF
    assert len(list(getattr(SeekingReader(BytesIO(data)), method)())) > 0
    with pytest.raises(CRCValidationError):
        list(getattr(SeekingReader(BytesIO(data), validate_crcs=True), method)())


@pytest.mark.parametrize("method", METHODS)
def test_record_size_limit_without_indexes(method: str):
    data = _write(size=256)
    assert len(list(getattr(SeekingReader(BytesIO(data)), method)())) > 0
    with pytest.raises(RecordLengthLimitExceeded):
        list(getattr(SeekingReader(BytesIO(data), record_size_limit=100), method)())


@pytest.mark.parametrize("statistics", [False, True])
def test_attachments_and_metadata_without_indexes(statistics: bool):
    data = _write(
        index_types=IndexType.CHUNK | IndexType.MESSAGE, statistics=statistics
    )
    reader = SeekingReader(BytesIO(data))
    summary = reader.get_summary()
    assert summary is not None
    assert not summary.attachment_indexes and not summary.metadata_indexes
    assert [a.name for a in reader.iter_attachments()] == ["att0", "att1"]
    assert [m.name for m in reader.iter_metadata()] == ["meta0", "meta1"]


def test_attachments_and_metadata_absent_in_statistics_skip_the_scan():
    stream = BytesIO(_write(index_types=IndexType.CHUNK | IndexType.MESSAGE, records=0))
    reader = SeekingReader(stream)
    assert reader.get_summary() is not None
    position = stream.tell()
    assert list(reader.iter_attachments()) == []
    assert list(reader.iter_metadata()) == []
    # Statistics report zero attachments and metadata, so the file is not scanned again.
    assert stream.tell() == position
