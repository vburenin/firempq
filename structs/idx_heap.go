package structs

import (
	"container/heap"
)

// Heap items are very light, so you them directly with no pointers
// it should save tons of time on GC.
type HeapItem struct {
	Id       string
	Priority int64
}

var EMPTY_HEAP_ITEM = HeapItem{"", -1}

type IndexHeap struct {
	// Used as a heap container.
	iHeap []HeapItem
	// Used as a heap item to remove elements by id.
	iMap map[string]int
}

func NewIndexHeap() *IndexHeap {
	return &IndexHeap{
		iHeap: []HeapItem{},
		iMap:  make(map[string]int),
	}
}

func (h *IndexHeap) Len() int {
	return len(h.iHeap)
}

func (h *IndexHeap) Empty() bool {
	return len(h.iHeap) == 0
}

func (h *IndexHeap) Less(i, j int) bool {
	return h.iHeap[i].Priority < h.iHeap[j].Priority
}

func (h *IndexHeap) Swap(i, j int) {
	hp := h.iHeap

	iItem := hp[i]
	jItem := hp[j]

	h.iMap[iItem.Id] = j
	h.iMap[jItem.Id] = i

	hp[i] = jItem
	hp[j] = iItem
}

func (h *IndexHeap) Push(x interface{}) {
	msgRef := x.(*HeapItem)
	h.iHeap = append(h.iHeap, *msgRef)
	h.iMap[msgRef.Id] = h.Len() - 1
}

func (h *IndexHeap) Pop() interface{} {
	last := h.Len() - 1
	retVal := h.iHeap[last]
	h.iHeap = h.iHeap[0:last]
	delete(h.iMap, retVal.Id)
	return retVal
}

func (h *IndexHeap) MinElement() int64 {
	return h.iHeap[0].Priority
}

func (h *IndexHeap) PushHeapItem(item *HeapItem) bool {
	_, ok := h.iMap[item.Id]
	if !ok {
		heap.Push(h, item)
		return true
	}
	return false
}

func (h *IndexHeap) PushItem(itemId string, priority int64) bool {
	return h.PushHeapItem(&HeapItem{itemId, priority})
}

func (h *IndexHeap) PopItem() HeapItem {
	return heap.Pop(h).(HeapItem)
}

// Pop item by ID. As as result message reference is removed
// from the queue based by its id, not by its priority.
func (h *IndexHeap) PopById(id string) HeapItem {
	idx, ok := h.iMap[id]

	if !ok {
		return EMPTY_HEAP_ITEM
	}

	msgRef := h.iHeap[idx]
	heap.Remove(h, idx)

	return msgRef
}

func (h *IndexHeap) ContainsId(id string) bool {
	_, ok := h.iMap[id]
	return ok
}
