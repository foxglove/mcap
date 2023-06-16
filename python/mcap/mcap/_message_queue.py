import heapq
from typing import List, Optional, Tuple, Union

from .records import Channel, ChunkIndex, Message, Schema

QueueItem = Union[
    ChunkIndex, Tuple[Tuple[Optional[Schema], Channel, Message], int, int]
]


class _Orderable:
    def __init__(self, item: QueueItem, reverse: bool):
        self.item: QueueItem = item
        self.reverse = reverse

    def __lt__(self, other: "_Orderable") -> bool:
        if self.log_time() == other.log_time():
            return self._position_less_than(other)
        return self._compare(self.log_time(), other.log_time())

    def _compare(self, a: int, b: int) -> bool:
        if self.reverse:
            return a > b
        return a < b

    def _position_less_than(self, other: "_Orderable") -> bool:
        this_chunk_offset, this_message_offset = self.position()
        other_chunk_offset, other_message_offset = other.position()
        if this_message_offset is None or other_message_offset is None:
            return self._compare(this_chunk_offset, other_chunk_offset)
        if this_chunk_offset == other_chunk_offset:
            return self._compare(this_message_offset, other_message_offset)
        return self._compare(this_chunk_offset, other_chunk_offset)

    def log_time(self) -> int:
        raise NotImplementedError(
            "do not instantiate _Orderable directly, use a subclass"
        )

    def position(self) -> Tuple[int, Optional[int]]:
        raise NotImplementedError(
            "do not instantiate _Orderable directly, use a subclass"
        )


class _ChunkIndexWrapper(_Orderable):
    def log_time(self) -> int:
        self.item: ChunkIndex
        if self.reverse:
            return self.item.message_end_time
        return self.item.message_start_time

    def position(self) -> Tuple[int, Optional[int]]:
        if self.reverse:
            return (self.item.chunk_start_offset + self.item.chunk_length, None)
        return (self.item.chunk_start_offset, None)


class _MessageTupleWrapper(_Orderable):
    def log_time(self) -> int:
        self.item: Tuple[Tuple[Schema, Channel, Message], int, int]
        return self.item[0][2].log_time

    def position(self) -> Tuple[int, Optional[int]]:
        return (self.item[1], self.item[2])


class MessageQueue:
    """A queue of MCAP messages and chunk indices.

    :param log_time_order: if True, this queue acts as a priority queue, ordered by log time.
        if False, ``pop()`` returns elements in insert order.
    :param reverse: if True, order elements in descending log time order rather than ascending.
    """

    def __init__(self, log_time_order: bool, reverse: bool = False):
        self._q: List[_Orderable] = []
        self._log_time_order = log_time_order
        self._reverse = reverse

    def push(self, item: QueueItem):
        if isinstance(item, ChunkIndex):
            orderable = _ChunkIndexWrapper(item, self._reverse)
        else:
            orderable = _MessageTupleWrapper(item, self._reverse)
        if self._log_time_order:
            heapq.heappush(self._q, orderable)
        else:
            self._q.append(orderable)

    def pop(self) -> QueueItem:
        if self._log_time_order:
            return heapq.heappop(self._q).item
        else:
            return self._q.pop(0).item

    def __len__(self) -> int:
        return len(self._q)
