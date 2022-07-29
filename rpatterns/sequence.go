package rpatterns

import (
	"container/heap"
)

type minHeap []int64

func (h minHeap) Len() int           { return len(h) }
func (h minHeap) Less(i, j int) bool { return h[i] < h[j] }
func (h minHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *minHeap) Push(x interface{}) {
	*h = append(*h, x.(int64))
}

func (h *minHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

func (h minHeap) Peek() int64 {
	if len(h) == 0 {
		return 0
	}
	return h[0]
}

// GapSequence can keep track of a maximum "done" identifier
// in a sequence that is consumed in ascending sequence
// but completed in a random sequence.
type GapSequence struct {
	max         int64
	doing, done *minHeap
}

func NewGapSequence() *GapSequence {
	return &GapSequence{
		doing: new(minHeap),
		done:  new(minHeap),
	}
}

// Doing marks `val` as in progress
// CurrentMax will not be allowed to go >= `val` until it is Done
func (s *GapSequence) Doing(val int64) {
	heap.Push(s.doing, val)
}

// Done marks `val` as done and sets CurrentMax to
// the lowest done value so far
func (s *GapSequence) Done(val int64) {
	heap.Push(s.done, val)
	for s.doing.Len() > 0 && s.done.Len() > 0 && s.doing.Peek() == s.done.Peek() {
		heap.Pop(s.doing)
		s.max = heap.Pop(s.done).(int64)
	}
}

func (s GapSequence) CurrentMax() int64 {
	return s.max
}
