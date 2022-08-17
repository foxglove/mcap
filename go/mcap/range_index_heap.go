package mcap

type rangeIndex struct {
	chunkIndex        *ChunkIndex
	messageIndexEntry *MessageIndexEntry
	reverse           bool
	buf               []uint8
}

func (ri *rangeIndex) logTime() uint64 {
	if ri.chunkIndex != nil {
		if ri.reverse {
			return ri.chunkIndex.MessageEndTime
		}
		return ri.chunkIndex.MessageStartTime
	}
	return ri.messageIndexEntry.Timestamp
}

type rangeIndexHeap []rangeIndex

func (h rangeIndexHeap) Len() int      { return len(h) }
func (h rangeIndexHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *rangeIndexHeap) Push(x interface{}) {
	*h = append(*h, x.(rangeIndex))
}

func (h *rangeIndexHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

func (h rangeIndexHeap) Less(i, j int) bool {
	if h[i].reverse {
		return h[i].logTime() > h[j].logTime()
	}
	return h[i].logTime() < h[j].logTime()
}
