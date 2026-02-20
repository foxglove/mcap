"""Tests for ULog to MCAP conversion, analogous to convertULogFileToMCAP.test.ts"""

import contextlib
import io
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from operator import attrgetter
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Generator, Iterator, Optional, cast

import numpy as np
import pytest
from convert import convert_ulog
from mcap_protobuf.decoder import DecoderFactory
from mcap_protobuf.writer import Writer
from pyulog import ULog

from mcap.reader import McapReader, make_reader


@dataclass
class ProtobufMessage:
    log_time_ns: int
    topic: str
    proto_msg: Any


def read_protobuf_messages(reader: McapReader) -> Iterator[ProtobufMessage]:
    for _, channel, message, proto_msg in reader.iter_decoded_messages():
        yield ProtobufMessage(
            log_time_ns=message.log_time,
            topic=channel.topic,
            proto_msg=proto_msg,
        )


def _make_message_format(
    name: str, fields: list[tuple[str, int, str]]
) -> SimpleNamespace:
    return SimpleNamespace(name=name, fields=fields)


def _make_data(
    name: str,
    multi_id: int,
    data: dict[str, np.ndarray[Any, Any]],
) -> SimpleNamespace:
    return SimpleNamespace(name=name, multi_id=multi_id, data=data)


def _make_log_message(
    timestamp: int,
    level: str,
    message: str,
) -> SimpleNamespace:
    def log_level_str() -> str:
        return level

    return SimpleNamespace(
        timestamp=timestamp, log_level_str=log_level_str, message=message
    )


def _make_log_message_tagged(
    timestamp: int,
    level: str,
    message: str,
    tag: int,
) -> SimpleNamespace:
    def log_level_str() -> str:
        return level

    return SimpleNamespace(
        timestamp=timestamp, log_level_str=log_level_str, message=message, tag=tag
    )


def _complex_data_message() -> dict[str, np.ndarray[Any, Any]]:
    return {
        "timestamp": np.array([2000], dtype=np.uint64),
        "items[0].enabled": np.array([1], dtype=np.int8),
        "items[0].matrix[0]": np.array([1.0], dtype=np.float32),
        "items[0].matrix[1]": np.array([0.0], dtype=np.float32),
        "items[0].matrix[2]": np.array([0.0], dtype=np.float32),
        "items[0].matrix[3]": np.array([0.0], dtype=np.float32),
        "items[1].enabled": np.array([0], dtype=np.int8),
        "items[1].matrix[0]": np.array([0.0], dtype=np.float32),
        "items[1].matrix[1]": np.array([1.0], dtype=np.float32),
        "items[1].matrix[2]": np.array([0.0], dtype=np.float32),
        "items[1].matrix[3]": np.array([0.0], dtype=np.float32),
    }


def create_ulog_mock(
    *,
    message_formats: dict[str, list[tuple[str, int, str]]],
    data_list: list[SimpleNamespace],
    start_timestamp: int = 0,
    logged_messages: Optional[list[SimpleNamespace]] = None,
    logged_messages_tagged: Optional[list[SimpleNamespace]] = None,
) -> SimpleNamespace:
    """Build a mock ULog with the given message formats and data lists."""
    formats = {
        name: _make_message_format(name, fields)
        for name, fields in message_formats.items()
    }
    messages_by_tag : defaultdict[int, list[SimpleNamespace]] = defaultdict(list)
    for msg in logged_messages_tagged or []:
        messages_by_tag[msg.tag].append(msg)
    return SimpleNamespace(
        message_formats=formats,
        data_list=data_list,
        logged_messages=logged_messages or [],
        logged_messages_tagged=messages_by_tag,
        start_timestamp=start_timestamp,
    )


@contextlib.contextmanager
def _write_mcap_yield_reader(
    ulog: SimpleNamespace,
    start_time: Optional[datetime] = None,
    metadata: Optional[list[tuple[str, dict[str, str]]]] = None,
) -> Generator[McapReader, Any, None]:
    buffer = io.BytesIO()
    with Writer(buffer) as mcap:
        convert_ulog(cast(ULog, ulog), mcap, start_time=start_time, metadata=metadata)
        mcap.finish()
    buffer.seek(0)
    yield make_reader(buffer, decoder_factories=[DecoderFactory()])


class TestMockedMcapWrites:
    """Tests using mock ULog and in-memory MCAP output."""

    @pytest.fixture
    def topic_fixture(self) -> dict[str, list[tuple[str, int, str]]]:
        """Message format definitions"""
        return {
            "sensor_data": [
                ("uint64_t", 0, "timestamp"),
                ("float", 0, "value"),
            ],
            "item_definition": [
                ("bool", 0, "enabled"),
                ("float", 4, "matrix"),
            ],
            "item_list": [
                ("item_definition", 2, "items"),
            ],
        }

    @pytest.fixture
    def message_fixture(self) -> list[SimpleNamespace]:
        """Standard message data to use in tests"""
        return [
            _make_data(
                "sensor_data",
                0,
                {
                    "timestamp": np.array([1000], dtype=np.uint64),
                    "value": np.array([42.0], dtype=np.float32),
                },
            ),
            _make_data(
                "item_list",
                0,
                _complex_data_message(),
            ),
            _make_data(
                "sensor_data",
                0,
                {
                    "timestamp": np.array([3000], dtype=np.uint64),
                    "value": np.array([84.0], dtype=np.float32),
                },
            ),
        ]

    def test_add_channels_for_all_subscriptions(
        self,
        topic_fixture: dict[str, list[tuple[str, int, str]]],
        message_fixture: list[SimpleNamespace],
    ) -> None:
        """Should add channels for all subscriptions (data topics)."""
        mock_ulog = create_ulog_mock(
            message_formats=topic_fixture,
            data_list=message_fixture,
        )
        with _write_mcap_yield_reader(mock_ulog) as reader:
            summary = reader.get_summary()
        assert summary is not None
        channel_topics = sorted(ch.topic for ch in summary.channels.values())
        assert channel_topics == ["/item_list", "/sensor_data"]

    def test_add_messages_with_same_content(
        self,
        topic_fixture: dict[str, list[tuple[str, int, str]]],
        message_fixture: list[SimpleNamespace],
    ) -> None:
        """Should add messages to MCAP with same content (timestamps, topics, data)."""
        mock_ulog = create_ulog_mock(
            message_formats=topic_fixture,
            data_list=message_fixture,
        )

        with _write_mcap_yield_reader(mock_ulog) as reader:
            messages = list(read_protobuf_messages(reader))
        assert len(messages) == len(message_fixture)
        assert list(map(attrgetter("log_time_ns"), messages)) == [
            1_000_000,
            2_000_000,
            3_000_000,
        ]  # ms -> ns in writer
        assert messages[0].proto_msg.value == 42.0
        assert messages[1].proto_msg.items[0].enabled
        assert messages[1].proto_msg.items[0].matrix == [1.0, 0.0, 0.0, 0.0]
        assert not messages[1].proto_msg.items[1].enabled
        assert messages[1].proto_msg.items[1].matrix == [0.0, 1.0, 0.0, 0.0]
        assert messages[2].proto_msg.value == 84.0

    def test_add_messages_with_correct_timestamps_when_start_time_provided(
        self,
        topic_fixture: dict[str, list[tuple[str, int, str]]],
        message_fixture: list[SimpleNamespace],
    ) -> None:
        """Should add messages with timestamps offset by start_time"""
        start_time = datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        mock_ulog = create_ulog_mock(
            message_formats=topic_fixture,
            data_list=message_fixture,
            start_timestamp=0,
        )
        with _write_mcap_yield_reader(mock_ulog, start_time=start_time) as reader:
            messages = list(read_protobuf_messages(reader))
        assert len(messages) == len(message_fixture)
        assert list(map(attrgetter("log_time_ns"), messages)) == [
            1704096000001000000,
            1704096000002000000,
            1704096000003000000,
        ]

    def test_handle_string_fields(self) -> None:
        """Should handle string (char array) fields."""
        message_formats = {
            "text_topic": [
                ("uint64_t", 0, "timestamp"),
                ("char", 10, "text"),
            ],
        }
        # "Message 1!" = 10 chars; "Message 2!" = 10 chars
        data_list = [
            _make_data(
                "text_topic",
                0,
                {
                    "timestamp": np.array([1000], dtype=np.uint64),
                    "text[0]": np.array([ord("M")], dtype=np.int8),
                    "text[1]": np.array([ord("e")], dtype=np.int8),
                    "text[2]": np.array([ord("s")], dtype=np.int8),
                    "text[3]": np.array([ord("s")], dtype=np.int8),
                    "text[4]": np.array([ord("a")], dtype=np.int8),
                    "text[5]": np.array([ord("g")], dtype=np.int8),
                    "text[6]": np.array([ord("e")], dtype=np.int8),
                    "text[7]": np.array([ord(" ")], dtype=np.int8),
                    "text[8]": np.array([ord("1")], dtype=np.int8),
                    "text[9]": np.array([ord("!")], dtype=np.int8),
                },
            ),
            _make_data(
                "text_topic",
                0,
                {
                    "timestamp": np.array([2000], dtype=np.uint64),
                    "text[0]": np.array([ord("M")], dtype=np.int8),
                    "text[1]": np.array([ord("e")], dtype=np.int8),
                    "text[2]": np.array([ord("s")], dtype=np.int8),
                    "text[3]": np.array([ord("s")], dtype=np.int8),
                    "text[4]": np.array([ord("a")], dtype=np.int8),
                    "text[5]": np.array([ord("g")], dtype=np.int8),
                    "text[6]": np.array([ord("e")], dtype=np.int8),
                    "text[7]": np.array([ord(" ")], dtype=np.int8),
                    "text[8]": np.array([ord("2")], dtype=np.int8),
                    "text[9]": np.array([ord("!")], dtype=np.int8),
                },
            ),
        ]
        mock_ulog = create_ulog_mock(
            message_formats=message_formats,
            data_list=data_list,
        )
        with _write_mcap_yield_reader(mock_ulog) as reader:
            messages = list(read_protobuf_messages(reader))
        assert len(messages) == len(data_list)
        assert list(map(attrgetter("log_time_ns"), messages)) == [1_000_000, 2_000_000]
        assert list(map(attrgetter("proto_msg.text"), messages)) == [
            "Message 1!",
            "Message 2!",
        ]

    def test_write_logs_to_separate_log_channel(self) -> None:
        """Should write ULog log messages to a separate /log_message channel"""
        # Mock with no data topics, only logged messages. Use empty message_formats and data_list.
        t = 1000
        logged_messages = [
            _make_log_message(t, "DEBUG", "one"),
            _make_log_message(t := t + 1000, "INFO", "two"),
            _make_log_message(t := t + 1000, "WARNING", "three"),
            _make_log_message(t + 1000, "ERROR", "four"),
        ]
        mock_ulog = create_ulog_mock(
            message_formats={},
            data_list=[],
            logged_messages=logged_messages,
        )
        with _write_mcap_yield_reader(mock_ulog) as reader:
            summary = reader.get_summary()
            messages = list(read_protobuf_messages(reader))
        assert summary is not None
        channel_topics = sorted(ch.topic for ch in summary.channels.values())
        assert channel_topics == ["/log_message"]

        assert len(messages) == 4
        assert list(map(attrgetter("log_time_ns"), messages)) == [
            1_000_000,
            2_000_000,
            3_000_000,
            4_000_000,
        ]
        assert list(map(attrgetter("topic"), messages)) == ["/log_message"] * 4
        expected = [
            ({"sec": 0, "nsec": 1_000_000}, 1, "one"),
            ({"sec": 0, "nsec": 2_000_000}, 2, "two"),
            ({"sec": 0, "nsec": 3_000_000}, 3, "three"),
            ({"sec": 0, "nsec": 4_000_000}, 4, "four"),
        ]
        for msg, (ts, level, text) in zip(messages, expected):
            assert msg.proto_msg.timestamp.sec == ts["sec"]
            assert msg.proto_msg.timestamp.nsec == ts["nsec"]
            assert msg.proto_msg.level == level
            assert msg.proto_msg.message == text

    def test_write_tagged_log_messages_to_log_channel(self) -> None:
        """Should write ULog tagged log messages to the same /log_message channel."""
        logged_messages = [
            _make_log_message(1000, "INFO", "untagged"),
        ]
        logged_messages_tagged = [
            _make_log_message_tagged(2000, "WARNING", "tagged", 42),
            _make_log_message_tagged(3000, "INFO", "other tagged", 43),
            _make_log_message_tagged(4000, "WARNING", "first tag again", 42)
        ]
        mock_ulog = create_ulog_mock(
            message_formats={},
            data_list=[],
            logged_messages=logged_messages,
            logged_messages_tagged=logged_messages_tagged,
        )
        with _write_mcap_yield_reader(mock_ulog) as reader:
            messages = list(read_protobuf_messages(reader))
        assert len(messages) == 2
        assert list(map(attrgetter("topic"), messages)) == ["/log_message"] * 2
        assert list(map(attrgetter("proto_msg.message"), messages)) == [
            "untagged",
            "tagged",
            "other tagged",
            "first tag again"
        ]
        assert list(map(attrgetter("proto_msg.level"), messages)) == [
            2,
            3,
            2,
            3,
        ]  # INFO, WARNING

    def test_separate_channels_for_distinct_multi_ids(
        self,
        topic_fixture: dict[str, list[tuple[str, int, str]]],
    ) -> None:
        """Should add separate channels for distinct multiIds (sensor_data, sensor_data/1)."""
        mock_ulog = create_ulog_mock(
            message_formats=topic_fixture,
            data_list=[
                _make_data(
                    "sensor_data",
                    0,
                    {
                        "timestamp": np.array([1000], dtype=np.uint64),
                        "value": np.array([42.0], dtype=np.float32),
                    },
                ),
                _make_data(
                    "sensor_data",
                    1,
                    {
                        "timestamp": np.array([1000], dtype=np.uint64),
                        "value": np.array([36.0], dtype=np.float32),
                    },
                ),
                _make_data(
                    "item_list",
                    0,
                    _complex_data_message(),
                ),
                _make_data(
                    "sensor_data",
                    0,
                    {
                        "timestamp": np.array([3000], dtype=np.uint64),
                        "value": np.array([84.0], dtype=np.float32),
                    },
                ),
                _make_data(
                    "sensor_data",
                    1,
                    {
                        "timestamp": np.array([3000], dtype=np.uint64),
                        "value": np.array([64.0], dtype=np.float32),
                    },
                ),
            ],
        )
        with _write_mcap_yield_reader(mock_ulog) as reader:
            messages = list(read_protobuf_messages(reader))
        assert len(messages) == 5
        assert list(map(attrgetter("log_time_ns"), messages)) == [
            1_000_000,
            1_000_000,
            2_000_000,
            3_000_000,
            3_000_000,
        ]
        assert list(map(attrgetter("topic"), messages)) == [
            "/sensor_data",
            "/sensor_data/1",
            "/item_list",
            "/sensor_data",
            "/sensor_data/1",
        ]
        assert len(messages) == 5

    def test_null_handling(self) -> None:
        """Should skip NaN (null) values and leave proto fields at default."""
        message_formats = {
            "sensor_data": [
                ("uint64_t", 0, "timestamp"),
                ("float", 0, "value"),
            ],
        }
        data_list = [
            _make_data(
                "sensor_data",
                0,
                {
                    "timestamp": np.array([1000], dtype=np.uint64),
                    "value": np.array([42.0], dtype=np.float32),
                },
            ),
            _make_data(
                "sensor_data",
                0,
                {
                    "timestamp": np.array([2000], dtype=np.uint64),
                    "value": np.array([np.nan], dtype=np.float32),
                },
            ),
            _make_data(
                "sensor_data",
                0,
                {
                    "timestamp": np.array([3000], dtype=np.uint64),
                    "value": np.array([1.5], dtype=np.float32),
                },
            ),
        ]
        mock_ulog = create_ulog_mock(
            message_formats=message_formats,
            data_list=data_list,
        )
        with _write_mcap_yield_reader(mock_ulog) as reader:
            messages = list(read_protobuf_messages(reader))
        assert len(messages) == 3
        assert messages[0].proto_msg.value == 42.0
        assert np.isnan(messages[1].proto_msg.value)
        assert messages[2].proto_msg.value == 1.5

    def test_uint64_integer_conversion(self) -> None:
        """Should handle uint64 / large integer conversion in JSON."""
        message_formats = {
            "sensor_data": [
                ("uint64_t", 0, "timestamp"),
                ("uint64_t", 0, "value"),
            ],
        }
        # max uint64
        data_list = [
            _make_data(
                "sensor_data",
                0,
                {
                    "timestamp": np.array([1000], dtype=np.uint64),
                    "value": np.array([18446744073709551615], dtype=np.uint64),
                },
            ),
        ]
        mock_ulog = create_ulog_mock(
            message_formats=message_formats,
            data_list=data_list,
        )
        with _write_mcap_yield_reader(mock_ulog) as reader:
            messages = list(read_protobuf_messages(reader))
        assert len(messages) == 1
        assert messages[0].proto_msg.value == 18446744073709551615


class TestFullConversion:
    """Integration test with real ULog fixture file."""

    def test_full_ulog_to_mcap_with_sample_file(self, tmp_path: Path) -> None:
        """Should perform full ULog to MCAP conversion with sample file."""
        src_dir = Path(__file__).resolve().parent  # .../python/examples/ulog2mcap
        ulog_path = src_dir / "fixtures" / "test_ulog.ulg"
        if not ulog_path.exists():
            raise FileNotFoundError(f"Fixture not found: {ulog_path}")

        output_path = tmp_path / "output.mcap"
        with open(output_path, "wb") as stream:
            mcap = Writer(stream)
            convert_ulog(ULog(ulog_path.as_posix()), mcap)
            mcap.finish()
        with open(output_path, "rb") as f:
            reader = make_reader(f, decoder_factories=[DecoderFactory()])
            summary = reader.get_summary()
            assert summary is not None
            channel_count = len(summary.channels)
            assert channel_count == 96, f"expected 96 channels, got {channel_count}"
