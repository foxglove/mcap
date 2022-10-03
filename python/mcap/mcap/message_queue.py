import heapq
from typing import Union, Tuple, List

from .records import Schema, Channel, Message, ChunkIndex

QueueItem = Union[ChunkIndex, Tuple[Schema, Channel, Message]]


class _Orderable:
    def __init__(self, item: QueueItem, reverse: bool):
        self.item: QueueItem = item
        self.reverse = reverse

    def __lt__(self, other: "_Orderable") -> bool:
        if self.reverse:
            return self.log_time() > other.log_time()
        return self.log_time() < other.log_time()

    def log_time(self) -> int:
        raise NotImplementedError(
            "do not instantiate _Orderable directly, use a subclass"
        )


class _ChunkIndexWrapper(_Orderable):
    def log_time(self) -> int:
        self.item: ChunkIndex
        if self.reverse:
            return self.item.message_end_time
        return self.item.message_start_time


class _MessageTupleWrapper(_Orderable):
    def log_time(self) -> int:
        self.item: Tuple[Schema, Channel, Message]
        return self.item[2].log_time


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
