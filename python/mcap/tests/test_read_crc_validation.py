from io import BytesIO
from pathlib import Path

import pytest

from mcap.stream_reader import CRCValidationError, StreamReader
from mcap.writer import MCAP0_MAGIC
from mcap.records import Chunk, DataEnd
from mcap.reader import SeekingReader, NonSeekingReader

from mcap.data_stream import RecordBuilder

DEMO_MCAP = (
    Path(__file__).parent.parent.parent.parent / "testdata" / "mcap" / "demo.mcap"
)


def produce_corrupted_mcap(to_corrupt: str) -> bytes:
    builder = RecordBuilder()
    corrupted = False
    with open(DEMO_MCAP, "rb") as f:
        sr = StreamReader(f, emit_chunks=True)
        builder.write(MCAP0_MAGIC)
        for record in sr.records:
            if not corrupted and to_corrupt == "chunk" and isinstance(record, Chunk):
                record.uncompressed_crc += 1
                corrupted = True
            if (
                not corrupted
                and to_corrupt == "data_end"
                and isinstance(record, DataEnd)
            ):
                record.data_section_crc += 1
                corrupted = True
            record.write(builder)
        builder.write(MCAP0_MAGIC)
    return builder.end()


@pytest.mark.parametrize("reader_cls", [SeekingReader, NonSeekingReader])
def test_validation_passes(reader_cls):
    with open(DEMO_MCAP, "rb") as f:
        reader = reader_cls(f, validate_crcs=True)
        for _ in reader.iter_messages():
            pass


@pytest.mark.parametrize("reader_cls", [SeekingReader, NonSeekingReader])
def test_crc_chunk_validation(reader_cls):
    content = produce_corrupted_mcap("chunk")
    reader = reader_cls(BytesIO(content), validate_crcs=True)
    with pytest.raises(CRCValidationError):
        for _ in reader.iter_messages():
            pass


def test_crc_data_end_validation():
    content = produce_corrupted_mcap("data_end")
    # Note: the seeking reader has no opportunity to validate the DataEnd CRC.
    reader = NonSeekingReader(BytesIO(content), validate_crcs=True)
    with pytest.raises(CRCValidationError):
        for _ in reader.iter_messages():
            pass
