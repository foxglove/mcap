""" tests for the MCAPReader implementations. """
import os
from pathlib import Path

import pytest

from mcap.mcap0.reader import make_reader, SeekingReader, NonSeekingReader, MCAPReader
from mcap.mcap0.records import Schema, Channel, Message

DEMO_MCAP = Path(__file__).parent.parent.parent.parent / "testdata" / "mcap" / "demo.mcap"


@pytest.fixture
def pipe():
    r, w = os.pipe()
    try:
        yield os.fdopen(r, "rb"), os.fdopen(w, "wb")
    finally:
        os.close(r)
        os.close(w)


def test_make_seeking():
    """ test that seekable streams get read with the seeking reader. """
    with open(DEMO_MCAP, "rb") as f:
        reader = make_reader(f)
        assert isinstance(reader, SeekingReader)


def test_make_not_seeking(pipe):
    """ test that non-seekable streams get read with the non-seeking reader. """
    r, _ = pipe
    reader: MCAPReader = make_reader(r)
    assert isinstance(reader, NonSeekingReader)


@pytest.mark.parametrize("reader_cls", [SeekingReader, NonSeekingReader])
def test_all_messages(reader_cls):
    """ test that we can find all messages correctly with all reader implementations. """
    with open(DEMO_MCAP, "rb") as f:
        reader: MCAPReader = reader_cls(f)
        count = 0
        for schema, channel, message in reader.iter_messages():
            assert isinstance(schema, Schema)
            assert isinstance(channel, Channel)
            assert isinstance(message, Message)
            count += 1

        assert count == 1606


@pytest.mark.parametrize("reader_cls", [SeekingReader, NonSeekingReader])
def test_time_range(reader_cls):
    """ test that we can filter by time range with all reader implementations. """
    with open(DEMO_MCAP, "rb") as f:
        reader: MCAPReader = reader_cls(f)
        count = 0
        start = int(1490149582 * 1e9)
        end = int(1490149586 * 1e9)
        for schema, channel, message in reader.iter_messages(start_time=start, end_time=end):
            assert isinstance(schema, Schema)
            assert isinstance(channel, Channel)
            assert isinstance(message, Message)
            assert message.log_time < end
            assert message.log_time >= start
            count += 1

        assert count == 825


@pytest.mark.parametrize("reader_cls", [SeekingReader, NonSeekingReader])
def test_only_diagnostics(reader_cls):
    """ test that we can filter by topic with all reader implementations. """
    with open(DEMO_MCAP, "rb") as f:
        reader: MCAPReader = reader_cls(f)
        count = 0
        for schema, channel, message in reader.iter_messages(topics=["/diagnostics"]):
            assert isinstance(schema, Schema)
            assert isinstance(channel, Channel)
            assert channel.topic == "/diagnostics"
            assert isinstance(message, Message)
            count += 1

        assert count == 52
