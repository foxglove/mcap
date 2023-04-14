"""tests for the McapReader implementations."""
import os
from pathlib import Path
import pytest
from typing import IO, Tuple, Type, Union
import json

from mcap.reader import make_reader, SeekingReader, NonSeekingReader, McapReader
from mcap.writer import Writer
from mcap.records import Schema, Channel, Message
from mcap.exceptions import DecoderNotFoundError

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


@pytest.mark.parametrize("reader_cls", [SeekingReader, NonSeekingReader])
def test_all_messages(reader_cls: Union[Type[SeekingReader], Type[NonSeekingReader]]):
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


@pytest.mark.parametrize("reader_cls", [SeekingReader, NonSeekingReader])
def test_time_range(reader_cls: Union[Type[SeekingReader], Type[NonSeekingReader]]):
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


@pytest.mark.parametrize("reader_cls", [SeekingReader, NonSeekingReader])
def test_only_diagnostics(
    reader_cls: Union[Type[SeekingReader], Type[NonSeekingReader]]
):
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


@pytest.mark.parametrize("reader_cls", [SeekingReader, NonSeekingReader])
def test_decode_schemaless(
    reader_cls: Union[Type[SeekingReader], Type[NonSeekingReader]], tmpdir: Path
):
    filepath = tmpdir / "json.mcap"
    write_json_mcap(filepath)

    with open(filepath, "rb") as f:

        def json_decoder(message: Message):
            return json.loads(message.data)

        reader = reader_cls(f, schemaless_decoders={"json": json_decoder})
        results = [
            (schema, value) for (schema, _, _, value) in reader.iter_decoded_messages()
        ]
        assert results[0] == (None, {"a": 0})
        assert results[1][0] is not None
        assert results[1][1] == {"a": 1}


@pytest.mark.parametrize("reader_cls", [SeekingReader, NonSeekingReader])
def test_decode_with_schema(
    reader_cls: Union[Type[SeekingReader], Type[NonSeekingReader]], tmpdir: Path
):
    filepath = tmpdir / "json.mcap"
    write_json_mcap(filepath)

    def json_decoder(schema: Schema, message: Message):
        return json.loads(message.data)

    # should throw DecoderNotFound when it encounters the schemaless channel /a.
    with open(filepath, "rb") as f:
        reader = reader_cls(f, decoders={"json": json_decoder})
        with pytest.raises(DecoderNotFoundError):
            for _ in reader.iter_decoded_messages():
                pass
    with open(filepath, "rb") as f:
        reader = reader_cls(f, decoders={"json": json_decoder})
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
