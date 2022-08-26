package mcap

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
)

var rangeIndexHeapTestItems = []rangeIndex{
	{
		chunkIndex: &ChunkIndex{
			ChunkStartOffset: 1,
			MessageStartTime: 100,
			MessageEndTime:   300,
		},
	},
	{
		chunkIndex: &ChunkIndex{
			ChunkStartOffset: 2,
			MessageStartTime: 200,
			MessageEndTime:   400,
		},
		messageIndexEntry: &MessageIndexEntry{Offset: 3, Timestamp: 200},
	},
	{
		chunkIndex: &ChunkIndex{
			ChunkStartOffset: 2,
			MessageStartTime: 200,
			MessageEndTime:   400,
		},
		messageIndexEntry: &MessageIndexEntry{Offset: 2, Timestamp: 250},
	},
	{
		chunkIndex: &ChunkIndex{
			ChunkStartOffset: 3,
			MessageStartTime: 300,
			MessageEndTime:   400,
		},
	},
}

func TestMessageOrdering(t *testing.T) {
	cases := []struct {
		assertion          string
		order              ReadOrder
		expectedIndexOrder []int
	}{
		{
			assertion:          "read time order forwards",
			order:              ReadOrderLogTime,
			expectedIndexOrder: []int{0, 1, 2, 3},
		},
		{
			assertion:          "read time order backwards",
			order:              ReadOrderReverseLogTime,
			expectedIndexOrder: []int{3, 0, 2, 1},
		},
		{
			assertion:          "read file order",
			order:              ReadOrderFile,
			expectedIndexOrder: []int{0, 2, 1, 3},
		},
	}
	for _, c := range cases {
		t.Run(c.assertion, func(t *testing.T) {
			h := &rangeIndexHeap{order: c.order}
			for _, item := range rangeIndexHeapTestItems {
				assert.Nil(t, h.HeapPush(item))
			}
			assert.Equal(t, h.Len(), len(rangeIndexHeapTestItems))
			i := 0
			for h.Len() > 0 {
				popped_item, err := h.HeapPop()
				assert.Nil(t, err)
				found := false
				for index, item := range rangeIndexHeapTestItems {
					if reflect.DeepEqual(item, *popped_item) {
						assert.Equal(t, c.expectedIndexOrder[i], index)
						found = true
					}
				}
				assert.True(t, found)
				i++
			}
		})
	}
}
