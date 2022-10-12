from io import BytesIO
from pathlib import Path
from typing import Type

import pytest

from mcap.stream_reader import CRCValidationError, StreamReader
from mcap.writer import MCAP0_MAGIC
from mcap.records import Chunk, DataEnd
from mcap.reader import SeekingReader, NonSeekingReader, McapReader

from mcap.data_stream import RecordBuilder

DEMO_MCAP = (
    Path(__file__).parent.parent.parent.parent / "testdata" / "mcap" / "demo.mcap"
)
ONE_MESSAGE_MCAP = (
    Path(__file__).parent.parent.parent.parent
    / "tests"
    / "conformance"
    / "data"
    / "OneMessage"
    / "OneMessage.mcap"
)


def produce_corrupted_mcap(filename: Path, to_corrupt: str) -> bytes:
    builder = RecordBuilder()
    corrupted = False
    with open(filename, "rb") as f:
        sr = StreamReader(f, emit_chunks=True)
        builder.write(MCAP0_MAGIC)
        for record in sr.records:
            if (
                not corrupted
                and to_corrupt == "chunk"
                and isinstance(record, Chunk)
                and record.uncompressed_crc != 0
            ):
                record.uncompressed_crc += 1
                corrupted = True
            if (
                not corrupted
                and to_corrupt == "data_end"
                and isinstance(record, DataEnd)
                and record.data_section_crc != 0
            ):
                record.data_section_crc += 1
                corrupted = True
            record.write(builder)
        if not corrupted:
            raise AssertionError(
                f"Could not find a {to_corrupt} to corrupt in {filename}"
            )
        builder.write(MCAP0_MAGIC)
    return builder.end()


@pytest.mark.parametrize("reader_cls", [SeekingReader, NonSeekingReader])
def test_validation_passes(reader_cls: Type[McapReader]):
    with open(DEMO_MCAP, "rb") as f:
        reader = reader_cls(f, validate_crcs=True)
        for _ in reader.iter_messages():
            pass
    with open(ONE_MESSAGE_MCAP, "rb") as f:
        reader = reader_cls(f, validate_crcs=True)
        for _ in reader.iter_messages():
            pass


@pytest.mark.parametrize("reader_cls", [SeekingReader, NonSeekingReader])
def test_crc_chunk_validation(reader_cls: Type[McapReader]):
    content = produce_corrupted_mcap(DEMO_MCAP, "chunk")
    reader = reader_cls(BytesIO(content), validate_crcs=True)
    with pytest.raises(CRCValidationError):
        for _ in reader.iter_messages():
            pass


def test_crc_data_end_validation():
    content = produce_corrupted_mcap(ONE_MESSAGE_MCAP, "data_end")
    # Note: the seeking reader has no opportunity to validate the DataEnd CRC.
    reader = NonSeekingReader(BytesIO(content), validate_crcs=True)
    with pytest.raises(CRCValidationError):
        for _ in reader.iter_messages():
            pass
