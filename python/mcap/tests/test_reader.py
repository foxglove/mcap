"""tests for the McapReader implementations."""
import json
import os
from pathlib import Path
from typing import IO, Any, Optional, Tuple, Type, Union

import pytest

from mcap.decoder import DecoderFactory
from mcap.exceptions import DecoderNotFoundError, InvalidMagic
from mcap.reader import McapReader, NonSeekingReader, SeekingReader, make_reader
from mcap.records import Channel, Message, Schema
from mcap.writer import IndexType, Writer

DEMO_MCAP = (
    Path(__file__).parent.parent.parent.parent / "testdata" / "mcap" / "demo.mcap"
)


@pytest.fixture
def pipe():
    r, w = os.pipe()
    try:
        yield os.fdopen(r, "rb"), os.fdopen(w, "wb")
    finally:
        os.close(r)
        os.close(w)


def test_make_seeking():
    """test that seekable streams get read with the seeking reader."""
    with open(DEMO_MCAP, "rb") as f:
        reader = make_reader(f)
        assert isinstance(reader, SeekingReader)


def test_make_not_seeking(pipe: Tuple[IO[bytes], IO[bytes]]):
    """test that non-seekable streams get read with the non-seeking reader."""
    r, _ = pipe
    reader: McapReader = make_reader(r)
    assert isinstance(reader, NonSeekingReader)


READER_SUBCLASSES = [SeekingReader, NonSeekingReader]
# We use this union rather than the base class McapReader so that the type-checker does not
# complain about instantiating the class with arguments that the base class constructor does not
# take.
AnyReaderSubclass = Union[Type[SeekingReader], Type[NonSeekingReader]]


@pytest.mark.parametrize("reader_cls", READER_SUBCLASSES)
def test_all_messages(reader_cls: AnyReaderSubclass):
    """test that we can find all messages correctly with all reader implementations."""
    with open(DEMO_MCAP, "rb") as f:
        reader: McapReader = reader_cls(f)
        count = 0
        for schema, channel, message in reader.iter_messages():
            assert isinstance(schema, Schema)
            assert isinstance(channel, Channel)
            assert isinstance(message, Message)
            count += 1

        assert count == 1606


@pytest.mark.parametrize("reader_cls", READER_SUBCLASSES)
def test_time_range(reader_cls: AnyReaderSubclass):
    """test that we can filter by time range with all reader implementations."""
    with open(DEMO_MCAP, "rb") as f:
        reader: McapReader = reader_cls(f)
        count = 0
        start = int(1490149582 * 1e9)
        end = int(1490149586 * 1e9)
        for schema, channel, message in reader.iter_messages(
            start_time=start, end_time=end
        ):
            assert isinstance(schema, Schema)
            assert isinstance(channel, Channel)
            assert isinstance(message, Message)
            assert message.log_time < end
            assert message.log_time >= start
            count += 1

        assert count == 825


@pytest.mark.parametrize("reader_cls", READER_SUBCLASSES)
def test_only_diagnostics(reader_cls: AnyReaderSubclass):
    """test that we can filter by topic with all reader implementations."""
    with open(DEMO_MCAP, "rb") as f:
        reader: McapReader = reader_cls(f)
        count = 0
        for schema, channel, message in reader.iter_messages(topics=["/diagnostics"]):
            assert isinstance(schema, Schema)
            assert isinstance(channel, Channel)
            assert channel.topic == "/diagnostics"
            assert isinstance(message, Message)
            count += 1

        assert count == 52


def write_json_mcap(filepath: Path):
    with open(filepath, "wb") as f:
        writer = Writer(f)
        writer.start()
        schemaless_channel = writer.register_channel("/a", "json", 0)
        writer.add_message(
            schemaless_channel, 10, json.dumps({"a": 0}).encode("utf8"), 10
        )
        schema = writer.register_schema("msg", "jsonschema", "true".encode("utf8"))
        channel_with_schema = writer.register_channel("/b", "json", schema)
        writer.add_message(
            channel_with_schema, 20, json.dumps({"a": 1}).encode("utf8"), 20
        )
        writer.finish()


@pytest.mark.parametrize("reader_cls", READER_SUBCLASSES)
def test_decode_schemaless(reader_cls: AnyReaderSubclass, tmpdir: Path):
    filepath = tmpdir / "json.mcap"
    write_json_mcap(filepath)

    with open(filepath, "rb") as f:
        reader = reader_cls(
            f, decoder_factories=[JsonDecoderFactory(require_schema=False)]
        )
        results = [
            (schema, value) for (schema, _, _, value) in reader.iter_decoded_messages()
        ]
        assert results[0] == (None, {"a": 0})
        assert results[1][0] is not None
        assert results[1][1] == {"a": 1}


class JsonDecoderFactory(DecoderFactory):
    def __init__(self, require_schema: bool = True):
        self._require_schema = require_schema

    def decoder_for(self, message_encoding: str, schema: Optional[Schema]):
        def decoder(message_data: bytes) -> Any:
            return json.loads(message_data)

        if message_encoding != "json":
            return None
        if self._require_schema and schema is None:
            return None
        return decoder


@pytest.mark.parametrize("reader_cls", READER_SUBCLASSES)
def test_decode_with_schema(reader_cls: AnyReaderSubclass, tmpdir: Path):
    filepath = tmpdir / "json.mcap"
    write_json_mcap(filepath)

    # should throw DecoderNotFound when it encounters the schemaless channel /a.
    with open(filepath, "rb") as f:
        reader = reader_cls(f, decoder_factories=[JsonDecoderFactory()])
        with pytest.raises(DecoderNotFoundError):
            for _ in reader.iter_decoded_messages():
                pass
    with open(filepath, "rb") as f:
        reader = reader_cls(f, decoder_factories=[JsonDecoderFactory()])
        results = [
            (schema, value)
            for (schema, _, _, value) in reader.iter_decoded_messages(topics=["/b"])
        ]
        assert len(results) == 1
        assert results[0][0] is not None
        assert results[0][1] == {"a": 1}


def test_non_seeking_used_once():
    """test that the non-seeking reader blocks users from trying to read more that once."""
    with open(DEMO_MCAP, "rb") as f:
        reader = NonSeekingReader(f)
        reader.get_summary()
        with pytest.raises(RuntimeError):
            reader.get_summary()

    with open(DEMO_MCAP, "rb") as f:
        reader = NonSeekingReader(f)
        _ = list(reader.iter_messages())
        with pytest.raises(RuntimeError):
            _ = list(reader.iter_messages())

    with open(DEMO_MCAP, "rb") as f:
        reader = NonSeekingReader(f)
        _ = list(reader.iter_attachments())
        with pytest.raises(RuntimeError):
            _ = list(reader.iter_attachments())

    with open(DEMO_MCAP, "rb") as f:
        reader = NonSeekingReader(f)
        _ = list(reader.iter_metadata())
        with pytest.raises(RuntimeError):
            _ = list(reader.iter_metadata())


def write_no_summary_mcap(filepath: Path):
    with open(filepath, "wb") as f:
        writer = Writer(
            f,
            index_types=IndexType.NONE,
            repeat_channels=False,
            repeat_schemas=False,
            use_chunking=False,
            use_statistics=False,
            use_summary_offsets=False,
        )
        writer.start()
        writer.add_attachment(10, 10, "my_attach", "text", b"some data")
        writer.add_metadata("my_meta", {"foo": "bar"})
        foo_channel = writer.register_channel("/foo", "json", 0)
        for i in range(200):
            writer.add_message(foo_channel, 10, json.dumps({"a": 0}).encode("utf8"), 10)
        writer.finish()


def test_no_summary_seeking(tmpdir: Path):
    filepath = tmpdir / "no_summary.mcap"
    write_no_summary_mcap(filepath)

    with open(filepath, "rb") as f:
        reader = SeekingReader(f)
        assert len(list(reader.iter_messages())) == 200
        assert len(list(reader.iter_attachments())) == 1
        assert len(list(reader.iter_metadata())) == 1


def test_no_summary_not_seeking(tmpdir: Path):
    filepath = tmpdir / "no_summary.mcap"
    write_no_summary_mcap(filepath)

    with open(filepath, "rb") as f:
        assert len(list(NonSeekingReader(f).iter_messages())) == 200
    with open(filepath, "rb") as f:
        assert len(list(NonSeekingReader(f).iter_attachments())) == 1
    with open(filepath, "rb") as f:
        assert len(list(NonSeekingReader(f).iter_metadata())) == 1


def test_detect_invalid_initial_magic(tmpdir: Path):
    filepath = tmpdir / "invalid_magic.mcap"
    with open(filepath, "w") as f:
        f.write("some bytes longer than the initial magic bytes")

    with open(filepath, "rb") as f:
        with pytest.raises(InvalidMagic):
            SeekingReader(f)

    with open(filepath, "rb") as f:
        with pytest.raises(InvalidMagic):
            NonSeekingReader(f).get_header()
