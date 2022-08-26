package mcap

import (
	"container/heap"
	"fmt"
)

// rangeIndex refers to either a chunk (via the ChunkIndex, with other fields nil)
// or to a message in a chunk, in which case all fields are set.
type rangeIndex struct {
	chunkIndex        *ChunkIndex
	messageIndexEntry *MessageIndexEntry
	buf               []uint8 // if messageIndexEntry is not nil, `buf` should point to the underlying chunk.
}

// heap of rangeIndex entries, where the entries are sorted by their log time.
type rangeIndexHeap struct {
	indices []rangeIndex
	order   ReadOrder
	lastErr error
}

// key returns the comparison key used for elements in this heap.
func (h rangeIndexHeap) timestamp(i int) uint64 {
	ri := h.indices[i]
	if ri.messageIndexEntry == nil {
		if h.order == ReadOrderReverseLogTime {
			return ri.chunkIndex.MessageEndTime
		}
		return ri.chunkIndex.MessageStartTime
	}
	return ri.messageIndexEntry.Timestamp
}

func (h *rangeIndexHeap) filePositionLess(i, j int) bool {
	a := h.indices[i]
	b := h.indices[j]

	// if comparing two chunks, whichever chunk comes earlier wins.
	// if comparing messages in two different chunks, the message in the earlier chunk wins.
	// if comparing a message in one chunk to another chunk, whichever chunk is earlier wins.
	if a.chunkIndex.ChunkStartOffset != b.chunkIndex.ChunkStartOffset {
		return a.chunkIndex.ChunkStartOffset < b.chunkIndex.ChunkStartOffset
	}
	// If comparing two messages in the same chunk, the earlier message in the chunk wins.
	if a.messageIndexEntry != nil && b.messageIndexEntry != nil {
		return a.messageIndexEntry.Offset < b.messageIndexEntry.Offset
	}
	// If we came this far, we're comparing a message in a chunk against the same chunk!
	// this is a problem, because when the chunk reaches the top of the heap it will be expanded,
	// and the same message will be pushed into the heap twice.
	h.lastErr = fmt.Errorf("detected duplicate data: a: %v, b: %v", a, b)
	return false
}

// Required for sort.Interface.
func (h rangeIndexHeap) Len() int      { return len(h.indices) }
func (h rangeIndexHeap) Swap(i, j int) { h.indices[i], h.indices[j] = h.indices[j], h.indices[i] }

// Push is required by `heap.Interface`. Note that this is not the same as `heap.Push`!
// expected behavior by `heap` is: "add x as element len()".
func (h *rangeIndexHeap) Push(x interface{}) {
	h.indices = append(h.indices, x.(rangeIndex))
}

// Pop is required by `heap.Interface`. Note that this is not the same as `heap.Pop`!
// expected behavior by `heap` is: "remove and return element Len() - 1".
func (h *rangeIndexHeap) Pop() interface{} {
	old := h.indices
	n := len(old)
	x := old[n-1]
	h.indices = old[0 : n-1]
	return x
}

// Less is required by `heap.Interface`.
func (h *rangeIndexHeap) Less(i, j int) bool {
	switch h.order {
	case ReadOrderFile:
		return h.filePositionLess(i, j)
	case ReadOrderLogTime:
		return h.timestamp(i) < h.timestamp(j)
	case ReadOrderReverseLogTime:
		return h.timestamp(i) > h.timestamp(j)
	}
	h.lastErr = fmt.Errorf("ReadOrder case not handled: %v", h.order)
	return false
}

func (h *rangeIndexHeap) HeapPush(ri rangeIndex) error {
	heap.Push(h, ri)
	return h.lastErr
}

func (h *rangeIndexHeap) HeapPop() (*rangeIndex, error) {
	result := heap.Pop(h).(rangeIndex)
	if h.lastErr != nil {
		return nil, h.lastErr
	}
	return &result, nil
}
