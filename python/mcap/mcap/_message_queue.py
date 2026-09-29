import heapq
import itertools
from abc import ABC, abstractmethod
from bisect import bisect_left
from collections import deque
from typing import Any, Deque, List, Optional, Tuple, Union

from .records import Channel, ChunkIndex, Message, Schema

MessageTuple = Tuple[Tuple[Optional[Schema], Channel, Message], int, int]
QueueItem = Union[ChunkIndex, MessageTuple]

# Items are ordered by (log time, chunk offset, message offset). A ChunkIndex has no
# message offset, so these sentinels stand in for one and give it a definite place among
# messages it would otherwise only tie with. That matters in reverse, where a chunk index
# is ordered on the offset one past its own last byte: for chunks written back to back,
# with no message index between them, that is exactly the next chunk's start offset, so a
# chunk index and the messages of the following chunk can agree on both log time and
# offset. Leaving such a pair merely "equal" makes the comparison intransitive, because
# those messages are still ordered against each other, and a heap fed an intransitive
# comparison emits them out of order.
_CHUNK_TIEBREAK = -1
_CHUNK_TIEBREAK_REVERSE = 1

# (log time, chunk offset, message offset), negated component-wise when reversed so that
# a min-heap yields descending log time order.
SortKey = Tuple[int, int, int]

# (log time, message offset, item), negated when reversed. Sorting a chunk's messages on
# this orders them exactly as their SortKeys would, because every message in a chunk
# shares one chunk offset.
_RunEntry = Tuple[int, int, MessageTuple]


def _chunk_index_key(chunk_index: ChunkIndex, reverse: bool) -> SortKey:
    if reverse:
        return (
            -chunk_index.message_end_time,
            -(chunk_index.chunk_start_offset + chunk_index.chunk_length),
            _CHUNK_TIEBREAK_REVERSE,
        )
    return (
        chunk_index.message_start_time,
        chunk_index.chunk_start_offset,
        _CHUNK_TIEBREAK,
    )


def _message_key(item: MessageTuple, reverse: bool) -> SortKey:
    log_time = item[0][2].log_time
    if reverse:
        return (-log_time, -item[1], -item[2])
    return (log_time, item[1], item[2])


# How many times a run that failed to gain a lead is passed over before its lead is
# measured again. Measuring costs two binary searches, which is wasted on a file whose
# chunks interleave message for message, and that is the shape this backs off from.
_PROBE_BACKOFF = 32


class _MessageRun:
    """One chunk's messages, sorted into log time order and drained in place.

    Messages in a chunk are usually written in log time order already, so ``sort()``
    here is a single linear pass, since the built-in sort detects an ordered run.
    Keeping them as one run lets the queue heap hold one entry per chunk rather than
    one per message.

    The attributes are read and written by the owning :py:class:`LogTimeOrderQueue`.
    """

    __slots__ = ("entries", "pos", "chunk_offset", "tail_key", "skip_probes")

    def __init__(self, chunk_offset: int, entries: List[_RunEntry]):
        entries.sort()
        self.entries = entries
        self.pos = 0
        self.chunk_offset = chunk_offset
        tail = entries[-1]
        self.tail_key: SortKey = (tail[0], chunk_offset, tail[1])
        self.skip_probes = 0

    def __len__(self) -> int:
        return len(self.entries) - self.pos

    def head_key(self) -> SortKey:
        entry = self.entries[self.pos]
        return (entry[0], self.chunk_offset, entry[1])

    def count_below(self, limit: SortKey) -> int:
        """How many of the remaining messages sort before ``limit``.

        Every remaining message shares one chunk offset, so the answer comes from a
        binary search over log time and, where log times tie with ``limit``, over
        message offset.
        """
        entries = self.entries
        pos = self.pos
        # Messages with a strictly smaller log time are all below the limit.
        cut = bisect_left(entries, (limit[0],), pos)
        if self.chunk_offset < limit[1]:
            # Ties on log time are settled by chunk offset, in this run's favour.
            cut = bisect_left(entries, (limit[0] + 1,), cut)
        elif self.chunk_offset == limit[1]:
            cut = bisect_left(entries, (limit[0], limit[2]), cut)
        return cut - pos

    def next(self) -> MessageTuple:
        entry = self.entries[self.pos]
        self.pos += 1
        return entry[2]


class _MessageQueue(ABC):
    @abstractmethod
    def push(self, item: QueueItem) -> None:
        raise NotImplementedError()

    @abstractmethod
    def push_chunk_messages(
        self, chunk_start_offset: int, messages: List[MessageTuple]
    ) -> None:
        """Add every message read out of one chunk, in the order they appear in it."""
        raise NotImplementedError()

    @abstractmethod
    def pop(self) -> QueueItem:
        raise NotImplementedError()

    @abstractmethod
    def __len__(self) -> int:
        raise NotImplementedError()


class LogTimeOrderQueue(_MessageQueue):
    """A priority queue over chunk indices and messages, ordered by log time.

    Messages arrive one chunk at a time, so rather than heap every message the queue
    keeps each chunk's messages as a sorted run and heaps the runs. The heap then holds
    one entry per pending chunk index and per run not yet drained, so it is sized by
    the number of chunks in flight rather than by the number of messages. While a run
    holds the smallest keys in the queue its messages are handed out with no heap work
    at all, which covers the whole of a file whose chunks do not overlap in time.
    """

    def __init__(self, reverse: bool = False):
        self._q: List[Tuple[SortKey, int, Any]] = []
        self._reverse = reverse
        self._counter = itertools.count()
        self._length = 0
        # How many messages the run at the root of the heap may hand out with no
        # comparison, because they already hold the smallest keys in the queue.
        self._drain = 0

    def push(self, item: QueueItem) -> None:
        if isinstance(item, ChunkIndex):
            key = _chunk_index_key(item, self._reverse)
        else:
            key = _message_key(item, self._reverse)
        self._settle_root()
        heapq.heappush(self._q, (key, next(self._counter), item))
        self._length += 1

    def push_chunk_messages(
        self, chunk_start_offset: int, messages: List[MessageTuple]
    ) -> None:
        if not messages:
            return
        if self._reverse:
            chunk_offset = -chunk_start_offset
            entries = [(-m[0][2].log_time, -m[2], m) for m in messages]
        else:
            chunk_offset = chunk_start_offset
            entries = [(m[0][2].log_time, m[2], m) for m in messages]
        run = _MessageRun(chunk_offset, entries)
        self._settle_root()
        heapq.heappush(self._q, (run.head_key(), next(self._counter), run))
        self._length += len(entries)

    def _settle_root(self) -> None:
        """Bring the root's key up to date before a push can compare against it.

        While a run is draining, the root's stored key describes a message that has
        already been handed out. ``heappop`` and ``heapreplace`` never read the root's
        own key, but ``heappush`` does, so it has to be corrected first.
        """
        if self._drain:
            self._drain = 0
            run = self._q[0][2]
            heapq.heapreplace(self._q, (run.head_key(), next(self._counter), run))

    def pop(self) -> QueueItem:
        q = self._q
        self._length -= 1
        drain = self._drain
        if drain:
            self._drain = drain - 1
            run = q[0][2]
            item = run.next()
            if drain == 1:
                # The run's lead is spent. Either it is empty, or it has to compete
                # for the root again with an up to date key.
                if run:
                    heapq.heapreplace(q, (run.head_key(), next(self._counter), run))
                else:
                    heapq.heappop(q)
            return item

        entry = q[0][2]
        if type(entry) is not _MessageRun:
            heapq.heappop(q)
            return entry

        item = entry.next()
        if not entry:
            heapq.heappop(q)
            return item

        # The root's two children each bound their own subtree, so the smaller of them
        # is the smallest key the rest of the queue can offer.
        size = len(q)
        if size == 1:
            limit = None
        elif size == 2 or q[1][0] < q[2][0]:
            limit = q[1][0]
        else:
            limit = q[2][0]

        if limit is None or entry.tail_key < limit:
            # The rest of the run wins outright; hand it out without touching the heap.
            self._drain = len(entry)
        elif entry.skip_probes:
            entry.skip_probes -= 1
            heapq.heapreplace(q, (entry.head_key(), next(self._counter), entry))
        else:
            ahead = entry.count_below(limit)
            if ahead > 1:
                entry.skip_probes = 0
                self._drain = ahead
            else:
                # This run and the rest of the queue are trading message for message,
                # so stop measuring for a while and just merge.
                entry.skip_probes = _PROBE_BACKOFF
                heapq.heapreplace(q, (entry.head_key(), next(self._counter), entry))
        return item

    def __len__(self) -> int:
        return self._length


class InsertOrderQueue(_MessageQueue):
    def __init__(self):
        self._q: Deque[QueueItem] = deque()

    def push(self, item: QueueItem) -> None:
        self._q.append(item)

    def push_chunk_messages(
        self, chunk_start_offset: int, messages: List[MessageTuple]
    ) -> None:
        self._q.extend(messages)

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
