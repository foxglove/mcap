"""Tests for SeekingReader queries that fall back to a linear NonSeekingReader scan."""

from io import BytesIO
from typing import Any, List, Optional, Tuple

import pytest

from mcap.exceptions import EndOfFile, RecordLengthLimitExceeded
from mcap.reader import SeekingReader
from mcap.stream_reader import MAGIC_SIZE, CRCValidationError
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


class _CountingStream(BytesIO):
    """A BytesIO that records how many bytes were read from each region of the file."""

    def __init__(self, data: bytes):
        super().__init__(data)
        self.reads: List[Tuple[int, int]] = []

    def read(self, size: Optional[int] = -1) -> bytes:
        start = self.tell()
        result = super().read(size)
        self.reads.append((start, len(result)))
        return result

    def bytes_read_between(self, start: int, end: int) -> int:
        return sum(
            max(0, min(position + length, end) - max(position, start))
            for position, length in self.reads
        )


def _chunk_bytes_read(stream: _CountingStream, summary: Any) -> int:
    """Bytes read from chunk record bodies; the 9-byte opcode and length prefixes are not
    counted."""
    return sum(
        stream.bytes_read_between(
            index.chunk_start_offset + 9, index.chunk_start_offset + index.chunk_length
        )
        for index in summary.chunk_indexes
    )


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


@pytest.mark.parametrize("use_chunking", [False, True])
def test_reverse_requires_log_time_order_without_chunk_index(use_chunking: bool):
    reader = SeekingReader(BytesIO(_write(use_chunking=use_chunking)))
    summary = reader.get_summary()
    assert summary is not None and not summary.chunk_indexes
    with pytest.raises(
        ValueError, match="reverse is only valid with log_time_order=True"
    ):
        list(reader.iter_messages(log_time_order=False, reverse=True))
    # The indexed path raises the same error.
    indexed = SeekingReader(BytesIO(_write(index_types=IndexType.ALL)))
    with pytest.raises(
        ValueError, match="reverse is only valid with log_time_order=True"
    ):
        list(indexed.iter_messages(log_time_order=False, reverse=True))


def test_validate_crcs_without_chunk_index():
    data = bytearray(_write())
    # Corrupt a message payload inside the (uncompressed) chunk.
    data[data.index(MESSAGE_DATA)] ^= 0xFF
    assert len(list(SeekingReader(BytesIO(data)).iter_messages())) > 0
    with pytest.raises(CRCValidationError):
        list(SeekingReader(BytesIO(data), validate_crcs=True).iter_messages())


@pytest.mark.parametrize("method", ["iter_attachments", "iter_metadata"])
def test_scan_skips_chunks_so_their_crcs_are_not_checked(method: str):
    # Attachments and metadata are never inside chunks, so the scan does not read the
    # chunks, like the indexed path. A corrupt chunk does not affect the result.
    data = bytearray(_write())
    data[data.index(MESSAGE_DATA)] ^= 0xFF
    reader = SeekingReader(BytesIO(data), validate_crcs=True)
    assert len(list(getattr(reader, method)())) == 2


@pytest.mark.parametrize("method", METHODS)
def test_record_size_limit_without_indexes(method: str):
    data = _write(size=256)
    assert len(list(getattr(SeekingReader(BytesIO(data)), method)())) > 0
    with pytest.raises(RecordLengthLimitExceeded):
        list(getattr(SeekingReader(BytesIO(data), record_size_limit=100), method)())


def _wrong_statistics(extra: int) -> bytes:
    """A file with attachment and metadata indexes, whose Statistics count `extra` more."""
    out = BytesIO()
    writer = Writer(out, use_chunking=True, compression=CompressionType.NONE)
    writer.start()
    schema = writer.register_schema("s", "jsonschema", b"{}")
    channel = writer.register_channel("/a", "json", schema)
    for i in range(50):
        writer.add_message(channel, i, MESSAGE_DATA * 1000, i)
    writer.add_attachment(0, 0, "att0", "text/plain", b"a")
    writer.add_metadata("meta0", {"k": "v"})
    statistics = getattr(writer, "_Writer__statistics")
    statistics.attachment_count += extra
    statistics.metadata_count += extra
    writer.finish()
    return out.getvalue()


def _big_chunks(index_types: IndexType, statistics: bool = True) -> bytes:
    out = BytesIO()
    writer = Writer(
        out,
        index_types=index_types,
        use_statistics=statistics,
        use_chunking=True,
        chunk_size=50_000,
        compression=CompressionType.NONE,
    )
    writer.start()
    schema = writer.register_schema("s", "jsonschema", b"{}")
    channel = writer.register_channel("/a", "json", schema)
    for i in range(100):
        writer.add_message(channel, i, MESSAGE_DATA * 1000, i)
        if i % 25 == 0:
            writer.add_attachment(i, i, f"att{i}", "text/plain", b"a")
            writer.add_metadata(f"meta{i}", {"k": "v"})
    writer.finish()
    return out.getvalue()


@pytest.mark.parametrize("method", ["iter_attachments", "iter_metadata"])
@pytest.mark.parametrize("statistics", [False, True])
def test_scan_does_not_read_chunk_payloads_without_indexes(
    method: str, statistics: bool
):
    data = _big_chunks(IndexType.CHUNK | IndexType.MESSAGE, statistics=statistics)
    stream = _CountingStream(data)
    reader = SeekingReader(stream)
    summary = reader.get_summary()
    assert summary is not None and len(summary.chunk_indexes) > 1
    assert not summary.attachment_indexes and not summary.metadata_indexes
    stream.reads.clear()
    assert len(list(getattr(reader, method)())) == 4
    assert _chunk_bytes_read(stream, summary) == 0
    assert sum(length for _, length in stream.reads) < len(data) // 10


@pytest.mark.parametrize("method", ["iter_attachments", "iter_metadata"])
def test_scan_does_not_read_chunk_payloads_with_wrong_statistics(method: str):
    data = _wrong_statistics(extra=1)
    stream = _CountingStream(data)
    reader = SeekingReader(stream)
    summary = reader.get_summary()
    assert summary is not None and summary.chunk_indexes
    assert len(summary.attachment_indexes) == 1 and len(summary.metadata_indexes) == 1
    assert summary.statistics is not None and summary.statistics.attachment_count == 2
    stream.reads.clear()
    assert len(list(getattr(reader, method)())) == 1
    assert _chunk_bytes_read(stream, summary) == 0
    assert sum(length for _, length in stream.reads) < len(data) // 10


@pytest.mark.parametrize("method", ["iter_attachments", "iter_metadata"])
def test_scan_without_summary_does_not_read_chunk_payloads(method: str):
    data = _big_chunks(IndexType.NONE, statistics=False)
    stream = _CountingStream(data)
    assert len(list(getattr(SeekingReader(stream), method)())) == 4
    assert sum(length for _, length in stream.reads) < len(data) // 10


@pytest.mark.parametrize("method", ["iter_attachments", "iter_metadata"])
def test_scan_survives_the_caller_moving_the_stream(method: str):
    stream = BytesIO(_write(index_types=IndexType.CHUNK | IndexType.MESSAGE))
    reader = SeekingReader(stream)
    results = []
    for record in getattr(reader, method)():
        stream.seek(0)
        stream.read(3)
        results.append(record)
    assert len(results) == 2


@pytest.mark.parametrize("method", ["iter_attachments", "iter_metadata"])
def test_scan_on_truncated_data_section_raises_end_of_file(method: str):
    data = _write(index_types=IndexType.NONE, statistics=False, size=1000)
    # Remove bytes from inside the chunk, so that its length runs past the end of the file.
    start = data.index(MESSAGE_DATA) + 100
    data = data[:start] + data[start + 3000 :]
    # The summary offsets are stale now: clear them, as a file without a summary has.
    footer = len(data) - MAGIC_SIZE - 20
    data = data[:footer] + bytes(16) + data[footer + 16 :]
    reader = SeekingReader(BytesIO(data))
    assert reader.get_summary() is None
    with pytest.raises(EndOfFile):
        list(getattr(reader, method)())


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
