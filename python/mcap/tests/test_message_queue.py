from typing import List

from mcap.records import ChunkIndex, Message, Schema, Channel
from mcap.message_queue import MessageQueue, QueueItem


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


def dummy_message_tuple(
    log_time: int, chunk_offset: int, message_offset: int
) -> QueueItem:
    return (
        (
            Schema(
                id=0,
                data=b"",
                encoding="",
                name="",
            ),
            Channel(
                id=0,
                topic="",
                message_encoding="",
                metadata={},
                schema_id=0,
            ),
            Message(
                channel_id=0,
                log_time=log_time,
                data=b"",
                publish_time=log_time,
                sequence=0,
            ),
        ),
        chunk_offset,
        message_offset,
    )


def push_elements(mq: MessageQueue):
    mq.push(dummy_chunk_index(3, 6, 100))
    mq.push(dummy_chunk_index(1, 2, 400))
    mq.push(dummy_chunk_index(4, 5, 500))
    mq.push(dummy_message_tuple(3, 200, 10))
    mq.push(dummy_message_tuple(3, 200, 20))
    mq.push(dummy_message_tuple(5, 200, 30))


def test_chunk_message_ordering():
    mq = MessageQueue(log_time_order=True)
    push_elements(mq)

    results: List[QueueItem] = []
    while mq:
        results.append(mq.pop())

    assert isinstance(results[0], ChunkIndex)
    assert results[0].message_start_time == 1
    assert isinstance(results[1], ChunkIndex)
    assert results[1].message_start_time == 3
    assert isinstance(results[2], tuple)
    assert results[2][2] == 10
    assert isinstance(results[3], tuple)
    assert results[3][2] == 20
    assert isinstance(results[4], ChunkIndex)
    assert results[4].message_start_time == 4
    assert isinstance(results[5], tuple)
    assert results[5][2] == 30


def test_reverse_ordering():
    mq = MessageQueue(log_time_order=True, reverse=True)
    push_elements(mq)

    results: List[QueueItem] = []
    while mq:
        results.append(mq.pop())

    assert isinstance(results[0], ChunkIndex)
    assert results[0].message_end_time == 6
    assert isinstance(results[1], ChunkIndex)
    assert results[1].message_end_time == 5
    assert isinstance(results[2], tuple)
    assert results[2][2] == 30
    assert isinstance(results[3], tuple)
    assert results[3][2] == 20
    assert isinstance(results[4], tuple)
    assert results[4][2] == 10
    assert isinstance(results[5], ChunkIndex)
    assert results[5].message_end_time == 2


def test_insert_ordering():
    mq = MessageQueue(log_time_order=False)
    push_elements(mq)

    results: List[QueueItem] = []
    while mq:
        results.append(mq.pop())

    assert isinstance(results[0], ChunkIndex)
    assert results[0].chunk_start_offset == 100
    assert isinstance(results[1], ChunkIndex)
    assert results[1].chunk_start_offset == 400
    assert isinstance(results[2], ChunkIndex)
    assert results[2].chunk_start_offset == 500
    assert isinstance(results[3], tuple)
    assert results[3][2] == 10
    assert isinstance(results[4], tuple)
    assert results[4][2] == 20
    assert isinstance(results[5], tuple)
    assert results[5][2] == 30
