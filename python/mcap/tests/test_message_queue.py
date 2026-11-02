import random
from typing import List, Union

from mcap._message_queue import KeyedMessage, LogTimeOrderQueue, MessagePayload
from mcap.records import Channel, ChunkIndex, Message, Schema

QueueItem = Union[ChunkIndex, MessagePayload]


def dummy_chunk_index(start_time: int, end_time: int, chunk_offset: int) -> ChunkIndex:
    return ChunkIndex(
        chunk_length=0,
        chunk_start_offset=chunk_offset,
        compression="",
        compressed_size=0,
        message_end_time=end_time,
        message_index_length=0,
        message_index_offsets={},
        message_start_time=start_time,
        uncompressed_size=0,
    )


def keyed_message(
    log_time: int, chunk_offset: int, index: int, reverse: bool = False
) -> KeyedMessage:
    """A message under its queue key, with the record index kept as the sequence."""
    payload: MessagePayload = (
        Schema(id=0, data=b"", encoding="", name=""),
        Channel(id=0, topic="", message_encoding="", metadata={}, schema_id=0),
        Message(
            channel_id=0,
            log_time=log_time,
            data=b"",
            publish_time=log_time,
            sequence=index,
        ),
    )
    if reverse:
        return (-log_time, -chunk_offset, -index, payload)
    return (log_time, chunk_offset, index, payload)


def push_elements(mq: LogTimeOrderQueue, reverse: bool = False):
    mq.push(dummy_chunk_index(3, 6, 100))
    mq.push(dummy_chunk_index(1, 2, 400))
    mq.push(dummy_chunk_index(4, 5, 500))
    mq.push_chunk_messages(
        [
            keyed_message(3, 200, 10, reverse),
            keyed_message(3, 200, 20, reverse),
            keyed_message(5, 200, 30, reverse),
        ]
    )


def drain(mq: LogTimeOrderQueue) -> List[QueueItem]:
    results: List[QueueItem] = []
    while mq:
        results.append(mq.pop())
    return results


def sequences(items: List[QueueItem]) -> List[int]:
    return [item[2].sequence for item in items if not isinstance(item, ChunkIndex)]


def test_chunk_message_ordering():
    mq = LogTimeOrderQueue()
    push_elements(mq)
    results = drain(mq)

    assert isinstance(results[0], ChunkIndex)
    assert results[0].message_start_time == 1
    assert isinstance(results[1], ChunkIndex)
    assert results[1].message_start_time == 3
    assert isinstance(results[2], tuple)
    assert results[2][2].sequence == 10
    assert isinstance(results[3], tuple)
    assert results[3][2].sequence == 20
    assert isinstance(results[4], ChunkIndex)
    assert results[4].message_start_time == 4
    assert isinstance(results[5], tuple)
    assert results[5][2].sequence == 30


def test_reverse_ordering():
    mq = LogTimeOrderQueue(reverse=True)
    push_elements(mq, reverse=True)
    results = drain(mq)

    assert isinstance(results[0], ChunkIndex)
    assert results[0].message_end_time == 6
    assert isinstance(results[1], ChunkIndex)
    assert results[1].message_end_time == 5
    assert isinstance(results[2], tuple)
    assert results[2][2].sequence == 30
    assert isinstance(results[3], tuple)
    assert results[3][2].sequence == 20
    assert isinstance(results[4], tuple)
    assert results[4][2].sequence == 10
    assert isinstance(results[5], ChunkIndex)
    assert results[5].message_end_time == 2


def test_chunk_messages_out_of_log_time_order():
    """a chunk whose messages are not in log time order is still drained in order."""
    mq = LogTimeOrderQueue()
    mq.push_chunk_messages(
        [
            keyed_message(30, 100, 0),
            keyed_message(10, 100, 1),
            keyed_message(20, 100, 2),
        ]
    )
    assert sequences(drain(mq)) == [1, 2, 0]


def test_empty_chunk_messages():
    mq = LogTimeOrderQueue()
    mq.push_chunk_messages([])
    assert len(mq) == 0
    assert mq.take_leading_run() is None


def test_leading_run_is_taken_whole():
    """a run that sorts before everything else queued comes out in one piece."""
    mq = LogTimeOrderQueue()
    mq.push(dummy_chunk_index(5, 6, 400))
    mq.push_chunk_messages(
        [keyed_message(1, 100, 0), keyed_message(2, 100, 1), keyed_message(3, 100, 2)]
    )
    run = mq.take_leading_run()
    assert run is not None
    assert [keyed[3][2].sequence for keyed in run] == [0, 1, 2]
    assert len(mq) == 1
    assert isinstance(mq.pop(), ChunkIndex)
    assert len(mq) == 0


def test_leading_prefix_is_taken_up_to_a_chunk_index():
    """only the messages that sort before a queued chunk index are taken."""
    mq = LogTimeOrderQueue()
    mq.push(dummy_chunk_index(2, 6, 400))
    mq.push_chunk_messages(
        [keyed_message(1, 100, 0), keyed_message(2, 100, 1), keyed_message(3, 100, 2)]
    )
    run = mq.take_leading_run()
    assert run is not None
    # The message at log time 2 sorts before the chunk index at the same time because
    # its chunk comes first in the file.
    assert [keyed[3][2].sequence for keyed in run] == [0, 1]
    assert len(mq) == 2
    assert isinstance(mq.pop(), ChunkIndex)
    assert sequences(drain(mq)) == [2]


def test_interleaved_run_is_not_taken():
    """a run that leads by at most one message is left for pop() to merge."""
    mq = LogTimeOrderQueue()
    mq.push(dummy_chunk_index(2, 6, 400))
    mq.push_chunk_messages(
        [keyed_message(1, 100, 0), keyed_message(3, 100, 1), keyed_message(5, 100, 2)]
    )
    assert mq.take_leading_run() is None
    results = drain(mq)
    assert isinstance(results[1], ChunkIndex)
    assert sequences(results) == [0, 1, 2]


def test_overlapping_runs_merge_in_key_order():
    """runs from chunks that overlap in time are merged, in both directions."""
    rng = random.Random(7)
    for reverse in (False, True):
        mq = LogTimeOrderQueue(reverse)
        expected: List[KeyedMessage] = []
        for chunk in range(50):
            chunk_offset = 1000 * (chunk + 1)
            entries = [
                keyed_message(rng.randint(0, 30), chunk_offset, index, reverse)
                for index in range(rng.randint(1, 40))
            ]
            expected.extend(entries)
            mq.push_chunk_messages(entries)
            # Interleave draining with pushing, as the reader does.
            for _ in range(rng.randint(0, 5)):
                if mq:
                    mq.pop()
        mq2 = LogTimeOrderQueue(reverse)
        expected.sort()
        for keyed in expected:
            mq2.push_chunk_messages([keyed])
        assert sequences(drain(mq2)) == [keyed[3][2].sequence for keyed in expected]


def test_pop_on_empty_queue_keeps_length_at_zero():
    mq = LogTimeOrderQueue()
    try:
        mq.pop()
    except IndexError:
        pass
    assert len(mq) == 0
