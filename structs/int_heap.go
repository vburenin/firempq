package structs

import (
	"container/heap"
)

// An IntHeap is a min-heap of integers.
type IntHeap []int64

func NewIntHeap() *IntHeap {
	return &IntHeap{}
}

func (h IntHeap) Len() int           { return len(h) }
func (h IntHeap) Less(i, j int) bool { return h[i] < h[j] }
func (h IntHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h IntHeap) Empty() bool        { return len(h) == 0 }

func (h *IntHeap) PushItem(item int64) {
	heap.Push(h, item)
}

func (h *IntHeap) PopItem() int64 {
	return heap.Pop(h).(int64)
}

func (h IntHeap) MinItem() int64 {
	return h[0]
}

func (h *IntHeap) Push(x interface{}) {
	// Push and Pop use pointer receivers because they modify the slice's length,
	// not just its contents.
	*h = append(*h, x.(int64))
}

func (h *IntHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}
