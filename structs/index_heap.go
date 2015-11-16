package structs

import "github.com/golangplus/container/heap"

type ItemHeap []HeapItem

type IndexedPriorityQueue struct {
	// Used as a heap container.
	iHeap ItemHeap
	// Used as a heap item to remove elements by id.
	iMap map[string]int
}

func NewIndexedPriorityQueue() *IndexedPriorityQueue {
	return &IndexedPriorityQueue{
		iHeap: ItemHeap{},
		iMap:  make(map[string]int),
	}
}

func (h *IndexedPriorityQueue) Len() int {
	return len(h.iHeap)
}

func (h *IndexedPriorityQueue) Empty() bool {
	return len(h.iHeap) == 0
}

func (h *IndexedPriorityQueue) Less(i, j int) bool {
	return h.iHeap[i].Priority < h.iHeap[j].Priority
}

func (h *IndexedPriorityQueue) Swap(i, j int) {
	hp := h.iHeap

	iItem := hp[i]
	jItem := hp[j]

	h.iMap[iItem.Id] = j
	h.iMap[jItem.Id] = i

	hp[i] = jItem
	hp[j] = iItem
}

func (h *IndexedPriorityQueue) MinElement() int64 {
	return h.iHeap[0].Priority
}

func (h *IndexedPriorityQueue) PushItem(itemId string, priority int64) bool {
	_, ok := h.iMap[itemId]
	if ok {
		return false
	}
	h.iMap[itemId] = len(h.iHeap)
	h.iHeap = append(h.iHeap, HeapItem{itemId, priority})
	heap.PushLast(h)
	return true
}

func (h *IndexedPriorityQueue) popLast() HeapItem {
	l := len(h.iHeap) - 1
	item := h.iHeap[l]
	h.iHeap = h.iHeap[:l]
	delete(h.iMap, item.Id)
	return item
}

func (h *IndexedPriorityQueue) PopItem() HeapItem {
	heap.PopToLast(h)
	return h.popLast()
}

func (h *IndexedPriorityQueue) PopById(id string) HeapItem {
	idx, ok := h.iMap[id]
	if !ok {
		return EmptyHeapItem
	}
	heap.RemoveToLast(h, idx)
	return h.popLast()
}

func (h *IndexedPriorityQueue) ContainsId(id string) bool {
	_, ok := h.iMap[id]
	return ok
}
