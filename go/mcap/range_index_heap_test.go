package mcap

import (
	"container/heap"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestForwardOrdering(t *testing.T) {
	h := &rangeIndexHeap{}
	heap.Push(h, rangeIndex{chunkIndex: &ChunkIndex{MessageStartTime: 0}})
	heap.Push(h, rangeIndex{messageIndexEntry: &MessageIndexEntry{Timestamp: 1}})
	heap.Push(h, rangeIndex{chunkIndex: &ChunkIndex{MessageStartTime: 3}})
	heap.Push(h, rangeIndex{messageIndexEntry: &MessageIndexEntry{Timestamp: 2}})

	assert.Equal(t, h.Len(), 4)

	assert.Equal(t, rangeIndex{chunkIndex: &ChunkIndex{MessageStartTime: 0}}, heap.Pop(h))
	assert.Equal(t, rangeIndex{messageIndexEntry: &MessageIndexEntry{Timestamp: 1}}, heap.Pop(h))
	assert.Equal(t, rangeIndex{messageIndexEntry: &MessageIndexEntry{Timestamp: 2}}, heap.Pop(h))
	assert.Equal(t, rangeIndex{chunkIndex: &ChunkIndex{MessageStartTime: 3}}, heap.Pop(h))

	assert.Equal(t, h.Len(), 0)
}

func TestReverseOrdering(t *testing.T) {
	h := &rangeIndexHeap{reverse: true}
	heap.Push(h, rangeIndex{chunkIndex: &ChunkIndex{MessageEndTime: 0}})
	heap.Push(h, rangeIndex{messageIndexEntry: &MessageIndexEntry{Timestamp: 1}})
	heap.Push(h, rangeIndex{chunkIndex: &ChunkIndex{MessageEndTime: 3}})
	heap.Push(h, rangeIndex{messageIndexEntry: &MessageIndexEntry{Timestamp: 2}})

	assert.Equal(t, h.Len(), 4)

	assert.Equal(t, rangeIndex{chunkIndex: &ChunkIndex{MessageEndTime: 3}}, heap.Pop(h))
	assert.Equal(t, rangeIndex{messageIndexEntry: &MessageIndexEntry{Timestamp: 2}}, heap.Pop(h))
	assert.Equal(t, rangeIndex{messageIndexEntry: &MessageIndexEntry{Timestamp: 1}}, heap.Pop(h))
	assert.Equal(t, rangeIndex{chunkIndex: &ChunkIndex{MessageEndTime: 0}}, heap.Pop(h))

	assert.Equal(t, h.Len(), 0)
}
