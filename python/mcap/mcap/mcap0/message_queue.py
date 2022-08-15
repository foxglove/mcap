import heapq
from typing import Union, Tuple, List

from .records import Schema, Channel, Message, ChunkIndex

QueueItem = Union[ChunkIndex, Tuple[Schema, Channel, Message]]


class _Orderable:
    def __init__(self, item: QueueItem):
        self.item: QueueItem = item

    def __lt__(self, other: "_Orderable") -> bool:
        return self.log_time() < other.log_time()

    def log_time(self) -> int:
        if isinstance(self.item, ChunkIndex):
            return self.item.message_start_time
        else:
            return self.item[2].log_time


class MessageQueue:
    def __init__(self, log_time_order: bool = False):
        self._q: List[_Orderable] = []
        self._log_time_order = log_time_order

    def push(self, item: QueueItem):
        if self._log_time_order:
            heapq.heappush(self._q, _Orderable(item))
        else:
            self._q.append(_Orderable(item))

    def pop(self) -> Union[ChunkIndex, Tuple[Schema, Channel, Message]]:
        if self._log_time_order:
            return heapq.heappop(self._q).item
        else:
            return self._q.pop(0).item

    def len(self) -> int:
        return len(self._q)
