from mcap.records import ChunkIndex, Message, Schema, Channel
from mcap.message_queue import MessageQueue, QueueItem


def dummy_chunk_index(start_time: int, end_time: int) -> ChunkIndex:
    return ChunkIndex(
        chunk_length=0,
        chunk_start_offset=0,
        compression="",
        compressed_size=0,
        message_end_time=end_time,
        message_index_length=0,
        message_index_offsets={},
        message_start_time=start_time,
        uncompressed_size=0,
    )


def dummy_message_tuple(log_time: int) -> QueueItem:
    return (
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
            publish_time=0,
            sequence=0,
        ),
    )


def push_elements(mq: MessageQueue):
    mq.push(dummy_chunk_index(3, 6))
    mq.push(dummy_chunk_index(1, 2))
    mq.push(dummy_chunk_index(4, 5))
    mq.push(dummy_message_tuple(7))
    mq.push(dummy_message_tuple(0))


def test_chunk_message_ordering():
    mq = MessageQueue(log_time_order=True)
    push_elements(mq)

    results = []
    while mq:
        results.append(mq.pop())

    assert results[0][2].log_time == 0
    assert results[1].message_start_time == 1
    assert results[2].message_start_time == 3
    assert results[3].message_start_time == 4
    assert results[4][2].log_time == 7


def test_reverse_ordering():
    mq = MessageQueue(log_time_order=True, reverse=True)
    push_elements(mq)

    results = []
    while mq:
        results.append(mq.pop())

    assert results[0][2].log_time == 7
    assert results[1].message_end_time == 6
    assert results[2].message_end_time == 5
    assert results[3].message_end_time == 2
    assert results[4][2].log_time == 0


def test_insert_ordering():
    mq = MessageQueue(log_time_order=False)
    push_elements(mq)

    results = []
    while mq:
        results.append(mq.pop())

    assert results[0].message_start_time == 3
    assert results[1].message_start_time == 1
    assert results[2].message_start_time == 4
    assert results[3][2].log_time == 7
    assert results[4][2].log_time == 0
