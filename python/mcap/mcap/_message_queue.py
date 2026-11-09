import heapq
import itertools
from bisect import bisect_left
from typing import Any, List, Optional, Tuple, Union

from .records import Channel, ChunkIndex, Message, Schema

# Items are ordered by (log time, chunk offset, record index). A ChunkIndex has no
# record index, so these sentinels stand in for one and give it a definite place among
# messages it would otherwise only tie with. That matters in reverse, where a chunk index
# is ordered on the offset one past its own last byte: for chunks written back to back,
# with no message index between them, that is exactly the next chunk's start offset, so a
# chunk index and the messages of the following chunk can agree on both log time and
# offset. Leaving such a pair merely "equal" makes the comparison intransitive, because
# those messages are still ordered against each other, and a heap fed an intransitive
# comparison emits them out of order.
_CHUNK_TIEBREAK = -1
_CHUNK_TIEBREAK_REVERSE = 1

# (log time, chunk offset, record index), negated component-wise when reversed so that
# a min-heap yields descending log time order.
SortKey = Tuple[int, int, int]

MessagePayload = Tuple[Optional[Schema], Channel, Message]

# A message under its sort key. Every message of one chunk shares a chunk offset and
# has its own record index, so a plain tuple sort orders a chunk's messages exactly as
# their keys do and never reaches the payload.
KeyedMessage = Tuple[int, int, int, MessagePayload]


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


# How many times a run that failed to gain a lead is passed over before its lead is
# measured again. Measuring costs a binary search, which is wasted on a file whose
# chunks interleave message for message, and that is the shape this backs off from.
_PROBE_BACKOFF = 32


class _MessageRun:
    """One chunk's messages, sorted into log time order and drained in place.

    Messages in a chunk are usually written in log time order already, so ``sort()``
    here is a single linear pass, since the built-in sort detects an ordered run.
    Keeping them as one run lets the queue heap hold one entry per chunk.

    The attributes are read and written by the owning :py:class:`LogTimeOrderQueue`.
    """

    __slots__ = ("entries", "pos", "tail_key", "skip_probes")

    def __init__(self, entries: List[KeyedMessage]):
        entries.sort()
        self.entries = entries
        self.pos = 0
        self.tail_key: SortKey = entries[-1][:3]
        self.skip_probes = 0

    def __len__(self) -> int:
        return len(self.entries) - self.pos

    def head_key(self) -> SortKey:
        return self.entries[self.pos][:3]

    def count_below(self, limit: SortKey) -> int:
        """How many of the remaining messages sort before ``limit``.

        The entries carry their full keys, and a key sorts before every entry that
        shares it, so this is one binary search.
        """
        return bisect_left(self.entries, limit, self.pos) - self.pos

    def next(self) -> MessagePayload:
        entry = self.entries[self.pos]
        self.pos += 1
        return entry[3]


class LogTimeOrderQueue:
    """A priority queue over chunk indices and messages, ordered by log time.

    Messages arrive one chunk at a time, and the queue keeps each chunk's messages as
    a sorted run and heaps the runs. The heap holds one entry per pending chunk index
    and per run not yet drained, so it is sized by the number of chunks in flight.
    :py:meth:`pop` merges one item at a time; :py:meth:`take_leading_run` hands out
    in one piece the messages at the root that sort before everything else queued,
    which is a whole run at a time for a file whose chunks do not overlap in time.
    """

    def __init__(self, reverse: bool = False):
        self._q: List[Tuple[SortKey, int, Any]] = []
        self._reverse = reverse
        self._counter = itertools.count()
        self._length = 0

    def push(self, chunk_index: ChunkIndex) -> None:
        key = _chunk_index_key(chunk_index, self._reverse)
        heapq.heappush(self._q, (key, next(self._counter), chunk_index))
        self._length += 1

    def push_chunk_messages(self, entries: List[KeyedMessage]) -> None:
        """Add every message read out of one chunk, keyed as the queue is: negated
        component-wise when the queue is reversed. The list is sorted in place."""
        if not entries:
            return
        run = _MessageRun(entries)
        heapq.heappush(self._q, (run.head_key(), next(self._counter), run))
        self._length += len(entries)

    def _limit(self) -> Optional[SortKey]:
        """The smallest key the queue holds outside the root, or None if there is none.

        The root's two children each bound their own subtree, so the smaller of them
        is the smallest key the rest of the queue can offer.
        """
        q = self._q
        size = len(q)
        if size == 1:
            return None
        if size == 2 or q[1][0] < q[2][0]:
            return q[1][0]
        return q[2][0]

    def take_leading_run(self) -> Optional[List[KeyedMessage]]:
        """Remove and return the messages at the root of the queue that sort before
        everything else in it, as one list.

        Returns None when the root is a chunk index, and when the root run leads by at
        most one message, which is the interleaved shape that :py:meth:`pop` merges one
        message at a time.
        """
        q = self._q
        if not q:
            return None
        run = q[0][2]
        if type(run) is not _MessageRun:
            return None
        limit = self._limit()
        if limit is None or run.tail_key < limit:
            # The whole run wins outright.
            heapq.heappop(q)
            taken = run.entries[run.pos :]
        else:
            if run.skip_probes:
                run.skip_probes -= 1
                return None
            ahead = run.count_below(limit)
            if ahead <= 1:
                run.skip_probes = _PROBE_BACKOFF
                return None
            taken = run.entries[run.pos : run.pos + ahead]
            run.pos += ahead
            heapq.heapreplace(q, (run.head_key(), next(self._counter), run))
        self._length -= len(taken)
        return taken

    def pop(self) -> Union[ChunkIndex, MessagePayload]:
        """Remove and return the chunk index or message with the smallest key."""
        q = self._q
        entry = q[0][2]
        self._length -= 1
        if type(entry) is not _MessageRun:
            heapq.heappop(q)
            return entry
        item = entry.next()
        if entry:
            heapq.heapreplace(q, (entry.head_key(), next(self._counter), entry))
        else:
            heapq.heappop(q)
        return item

    def __len__(self) -> int:
        return self._length
