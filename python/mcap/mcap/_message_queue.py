import heapq
from abc import ABC, abstractmethod
from collections import deque
from typing import Deque, Generic, List, Optional, Tuple, TypeVar, Union

from .records import Channel, ChunkIndex, Message, Schema

MessageTuple = Tuple[Tuple[Optional[Schema], Channel, Message], int, int]
QueueItem = Union[ChunkIndex, MessageTuple]
QueueItemType = TypeVar(
    "QueueItemType",
    bound=Union[ChunkIndex, MessageTuple],
)


class _Orderable(Generic[QueueItemType]):
    def __init__(self, item: QueueItemType, reverse: bool):
        self.item: QueueItemType = item
        self.reverse = reverse

    def __lt__(self, other: "_Orderable[QueueItemType]") -> bool:
        if self.log_time() == other.log_time():
            return self._position_less_than(other)
        return self._compare(self.log_time(), other.log_time())

    def _compare(self, a: int, b: int) -> bool:
        if self.reverse:
            return a > b
        return a < b

    def _position_less_than(self, other: "_Orderable[QueueItemType]") -> bool:
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


class _ChunkIndexWrapper(_Orderable[ChunkIndex]):
    def log_time(self) -> int:
        if self.reverse:
            return self.item.message_end_time
        return self.item.message_start_time

    def position(self) -> Tuple[int, Optional[int]]:
        if self.reverse:
            return (self.item.chunk_start_offset + self.item.chunk_length, None)
        return (self.item.chunk_start_offset, None)


class _MessageTupleWrapper(_Orderable[MessageTuple]):
    def log_time(self) -> int:
        return self.item[0][2].log_time

    def position(self) -> Tuple[int, Optional[int]]:
        return (self.item[1], self.item[2])


def _make_orderable(
    item: QueueItem, reverse: bool
) -> Union[_ChunkIndexWrapper, _MessageTupleWrapper]:
    if isinstance(item, ChunkIndex):
        return _ChunkIndexWrapper(item, reverse)
    return _MessageTupleWrapper(item, reverse)


class _MessageQueue(ABC):
    @abstractmethod
    def push(self, item: QueueItem) -> None:
        raise NotImplementedError()

    @abstractmethod
    def pop(self) -> QueueItem:
        raise NotImplementedError()

    @abstractmethod
    def __len__(self) -> int:
        raise NotImplementedError()


class LogTimeOrderQueue(_MessageQueue):
    def __init__(self, reverse: bool = False):
        self._q: List[Union[_ChunkIndexWrapper, _MessageTupleWrapper]] = []
        self._reverse = reverse

    def push(self, item: QueueItem):
        orderable = _make_orderable(item, self._reverse)
        heapq.heappush(self._q, orderable)

    def pop(self) -> QueueItem:
        return heapq.heappop(self._q).item

    def __len__(self) -> int:
        return len(self._q)


class InsertOrderQueue(_MessageQueue):
    def __init__(self):
        self._q: Deque[QueueItem] = deque()

    def push(self, item: QueueItem):
        self._q.append(item)

    def pop(self) -> QueueItem:
        return self._q.popleft()  # cspell:disable-line

    def __len__(self) -> int:
        return len(self._q)


def make_message_queue(
    log_time_order: bool = True, reverse: bool = False
) -> _MessageQueue:
    """Create a queue of MCAP messages and chunk indices.

    :param log_time_order: if True, this queue acts as a priority queue, ordered by log time.
        if False, ``pop()`` returns elements in insert order.
    :param reverse: if True, order elements in descending log time order rather than ascending.
        only valid if ``log_time_order`` is True, otherwise throws a ValueError.
    """
    if log_time_order:
        return LogTimeOrderQueue(reverse)
    if reverse:
        raise ValueError("reverse is only valid with log_time_order=True")
    return InsertOrderQueue()
