package mcap

// rangeIndex contains either a ChunkIndex or a MessageIndexEntry to be sorted on LogTime.
type rangeIndex struct {
	chunkIndex        *ChunkIndex
	messageIndexEntry *MessageIndexEntry
	buf               []uint8 // if messageIndexEntry is not nil, `buf` should point to the underlying chunk.
}

// heap of rangeIndex entries, where the entries are sorted by their log time.
type rangeIndexHeap struct {
	indices []rangeIndex
	reverse bool
}

// key returns the comparison key used for elements in this heap.
func (h rangeIndexHeap) key(i int) uint64 {
	ri := h.indices[i]
	if ri.chunkIndex != nil {
		if h.reverse {
			return ri.chunkIndex.MessageEndTime
		}
		return ri.chunkIndex.MessageStartTime
	}
	return ri.messageIndexEntry.Timestamp
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
func (h rangeIndexHeap) Less(i, j int) bool {
	if h.reverse {
		return h.key(i) > h.key(j)
	}
	return h.key(i) < h.key(j)
}
