package utils

import (
	"container/heap"

	"github.com/foxglove/mcap/go/mcap"
)

type PriorityQueue []TaggedMessage

func (pq PriorityQueue) Len() int {
	return len(pq)
}

func (pq PriorityQueue) Less(i, j int) bool {
	if pq[i].Message.LogTime != pq[j].Message.LogTime {
		return pq[i].Message.LogTime < pq[j].Message.LogTime
	}
	if pq[i].InputID != pq[j].InputID {
		return pq[i].InputID < pq[j].InputID
	}
	return pq[i].Message.ChannelID < pq[j].Message.ChannelID
}

func (pq PriorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
}

func (pq *PriorityQueue) Push(x any) {
	msg := x.(TaggedMessage)
	*pq = append(*pq, msg)
}

func (pq *PriorityQueue) Pop() any {
	old := *pq
	n := len(old)
	if n == 0 {
		return nil
	}
	msg := old[n-1]
	*pq = old[0 : n-1]
	return msg
}

// TaggedMessage is an mcap message, tagged with an identifier for the input it
// came from.
type TaggedMessage struct {
	Message *mcap.Message
	InputID int
}

func NewTaggedMessage(inputID int, msg *mcap.Message) TaggedMessage {
	return TaggedMessage{msg, inputID}
}

func NewPriorityQueue(msgs []TaggedMessage) *PriorityQueue {
	pq := &PriorityQueue{}
	heap.Init(pq)
	for _, msg := range msgs {
		heap.Push(pq, msg)
	}
	return pq
}
