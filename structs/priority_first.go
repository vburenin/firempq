package structs

// Items with higher priority always go first.

import "firempq/common"

type PriorityFirstQueue struct {
	// The highest priority queue used for the returned items.
	frontQueue *IndexList
	// The slice of all available sub queues.
	queues []*IndexList

	// Heap of indexes of not empty ListQueues.
	withItems   *IntHeap
	maxPriority int64
}

func NewActiveQueues(size int64) *PriorityFirstQueue {
	queues := make([]*IndexList, size, size)
	maxPriority := size
	for size > 0 {
		size--
		queues[size] = NewListQueue()
	}
	frontQueue := NewListQueue()
	return &PriorityFirstQueue{queues: queues,
		withItems:   NewIntHeap(),
		maxPriority: maxPriority,
		frontQueue:  frontQueue}
}

// Sometimes queue can be available in the item heaps event though it doesn't
// contain any items. It can happen if item was removed because of it has been expired.
// To reduce unnecessary CPU load to walk through of all priority lists. It is just
// better to keep in the heap and clean it when we walk through all queue items.
func (aq *PriorityFirstQueue) getFirstAvailable() int64 {
	for !aq.withItems.Empty() {
		minIdx := aq.withItems.MinItem()
		if aq.queues[minIdx].Empty() {
			aq.withItems.PopItem()
		} else {
			return minIdx
		}
	}
	return -1
}

func (aq *PriorityFirstQueue) RemoveItem(itemId string, priority int64) bool {
	if !aq.frontQueue.RemoveById(itemId) {
		return aq.queues[priority].RemoveById(itemId)
	}
	return false
}

func (aq *PriorityFirstQueue) Empty() bool {
	return aq.getFirstAvailable() == -1 && aq.frontQueue.Empty()
}

func (aq *PriorityFirstQueue) Pop() string {
	// Pop from the highest priority queue if there are any items.
	if !aq.frontQueue.Empty() {
		return aq.frontQueue.PopFront()
	}
	// Check if there are any queues with the items.
	minIdx := aq.getFirstAvailable()
	if minIdx >= 0 {
		return aq.queues[minIdx].PopFront()
	}
	return ""
}

func (aq *PriorityFirstQueue) Push(id string, priority int64) error {
	if priority >= aq.maxPriority || priority < 0 {
		return common.ERR_UNEXPECTED_PRIORITY
	}
	if aq.queues[priority].Empty() {
		aq.withItems.PushItem(priority)
	}
	aq.queues[priority].PushBack(id)
	return nil
}

func (aq *PriorityFirstQueue) PushFront(id string) {
	aq.frontQueue.PushBack(id)
}

func (aq *PriorityFirstQueue) Len() int {
	size := 0
	for _, v := range aq.queues {
		size += v.Len()
	}
	return size + aq.frontQueue.Len()
}
