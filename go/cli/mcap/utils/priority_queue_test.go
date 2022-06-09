package utils

import (
	"container/heap"
	"testing"

	"github.com/foxglove/mcap/go/mcap"
	"github.com/stretchr/testify/assert"
)

func TestPriorityQueue(t *testing.T) {
	a := NewTaggedMessage(1, &mcap.Message{LogTime: 1})
	b := NewTaggedMessage(2, &mcap.Message{LogTime: 2})
	c := NewTaggedMessage(3, &mcap.Message{LogTime: 3})
	t.Run("initialized with messages", func(t *testing.T) {
		pq := NewPriorityQueue([]TaggedMessage{c, b, a})
		for _, expectedTime := range []uint64{1, 2, 3} {
			msg, ok := heap.Pop(pq).(TaggedMessage)
			assert.True(t, ok)
			assert.Equal(t, expectedTime, msg.Message.LogTime)
		}
		assert.Panics(t, func() { heap.Pop(pq) }, "expected Pop on empty heap to panic")
	})
}
