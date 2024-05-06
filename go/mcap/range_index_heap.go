package mcap

import (
	"fmt"
)

// rangeIndex refers to either a chunk (via the ChunkIndex, with other fields nil)
// or to a message in a chunk, in which case all fields are set.
type rangeIndex struct {
	chunkIndex           *ChunkIndex
	MessageTimestamp     uint64
	MessageOffsetInChunk uint64 // offset in chunk
	ChunkSlotIndex       int    // -1 if this index refers to a chunk, otherwise refers to a message
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
	if ri.ChunkSlotIndex == -1 {
		if h.order == ReverseLogTimeOrder {
			return ri.chunkIndex.MessageEndTime
		}
		return ri.chunkIndex.MessageStartTime
	}
	return ri.MessageTimestamp
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
	if a.ChunkSlotIndex != -1 && b.ChunkSlotIndex != -1 {
		return a.MessageOffsetInChunk < b.MessageOffsetInChunk
	}
	// If we came this far, we're comparing a message in a chunk against the same chunk!
	// this is a problem, because when the chunk reaches the top of the heap it will be expanded,
	// and the same message will be pushed into the heap twice.
	h.lastErr = fmt.Errorf("detected duplicate data: a: %v, b: %v", a, b)
	return false
}

func (h rangeIndexHeap) len() int      { return len(h.indices) }
func (h rangeIndexHeap) swap(i, j int) { h.indices[i], h.indices[j] = h.indices[j], h.indices[i] }

func (h *rangeIndexHeap) PushMessage(ci *ChunkIndex, chunkSlotIndex int, timestamp uint64, offset uint64) error {
	return h.heapPush(rangeIndex{
		chunkIndex:           ci,
		MessageTimestamp:     timestamp,
		MessageOffsetInChunk: offset,
		ChunkSlotIndex:       chunkSlotIndex,
	})
}

func (h *rangeIndexHeap) PushChunkIndex(ci *ChunkIndex) error {
	return h.heapPush(rangeIndex{
		chunkIndex:     ci,
		ChunkSlotIndex: -1,
	})
}

// compares range indexes at indices i and j.
func (h *rangeIndexHeap) less(i, j int) bool {
	switch h.order {
	case FileOrder:
		return h.filePositionLess(i, j)
	case LogTimeOrder:
		if h.timestamp(i) == h.timestamp(j) {
			return h.filePositionLess(i, j)
		}
		return h.timestamp(i) < h.timestamp(j)
	case ReverseLogTimeOrder:
		if h.timestamp(i) == h.timestamp(j) {
			return h.filePositionLess(j, i)
		}
		return h.timestamp(i) > h.timestamp(j)
	}
	h.lastErr = fmt.Errorf("ReadOrder case not handled: %v", h.order)
	return false
}

func (h *rangeIndexHeap) heapPush(ri rangeIndex) error {
	h.indices = append(h.indices, ri)
	heapUp(h, h.len()-1)
	return h.lastErr
}

func (h *rangeIndexHeap) Pop() (rangeIndex, error) {
	n := h.len() - 1
	h.swap(0, n)
	heapDown(h, 0, n)
	old := h.indices
	newn := len(old)
	result := old[newn-1]
	h.indices = old[0 : newn-1]
	if h.lastErr != nil {
		return rangeIndex{}, h.lastErr
	}
	return result, nil
}

// The following functions are adapted from the source for the `container/heap` module,
// but specialized to remove the cost of converting indexes to and from interface{}.
// Copyright (c) 2009 The Go Authors. All rights reserved.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are
// met:
//
//    * Redistributions of source code must retain the above copyright
// notice, this list of conditions and the following disclaimer.
//    * Redistributions in binary form must reproduce the above
// copyright notice, this list of conditions and the following disclaimer
// in the documentation and/or other materials provided with the
// distribution.
//    * Neither the name of Google Inc. nor the names of its
// contributors may be used to endorse or promote products derived from
// this software without specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
// "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
// LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
// A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
// OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
// SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
// LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
// DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
// THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
// (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
// OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

func heapUp(h *rangeIndexHeap, j int) {
	for {
		i := (j - 1) / 2 // parent
		if i == j || !h.less(j, i) {
			break
		}
		h.swap(i, j)
		j = i
	}
}

func heapDown(h *rangeIndexHeap, i0, n int) bool {
	i := i0
	for {
		j1 := 2*i + 1
		if j1 >= n || j1 < 0 { // j1 < 0 after int overflow
			break
		}
		j := j1 // left child
		if j2 := j1 + 1; j2 < n && h.less(j2, j1) {
			j = j2 // = 2*i + 2  // right child
		}
		if !h.less(j, i) {
			break
		}
		h.swap(i, j)
		i = j
	}
	return i > i0
}
