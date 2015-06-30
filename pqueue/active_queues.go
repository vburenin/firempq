package pqueue

import "firempq/idx_heap"
import "firempq/qerrors"

type ActiveQueues struct {
	// The slice of all available sub queues.
	queues []*ListQueue

	// Heap of indexes of not empty ListQueues.
	withItems   *idx_heap.IntHeap
	maxPriority int64
}

func NewActiveQueues(size int64) *ActiveQueues {
	queues := make([]*ListQueue, size, size)
	maxPriority := size
	for size > 0 {
		size--
		queues[size] = NewListQueue()
	}
	return &ActiveQueues{queues: queues,
		withItems:   idx_heap.NewIntHeap(),
		maxPriority: maxPriority}
}

// Sometimes queue can be available in the item heaps event though it doesn't
// contain any items. It can happen if item was removed because of it has been expired.
// To reduce unnecessary CPU load to walk through of all priority lists. It is just
// better to keep in the heap and clean it when we walk through all queue items.
func (aq *ActiveQueues) getFirstAvailable() int64 {
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

func (aq *ActiveQueues) RemoveItem(itemId string, priority int64) bool {
	return aq.queues[priority].RemoveById(itemId)
}

func (aq *ActiveQueues) PrioritiesCount() int64 {
	return aq.maxPriority
}

func (aq *ActiveQueues) Empty() bool {
	return aq.getFirstAvailable() == -1
}

func (aq *ActiveQueues) Pop() string {
	minIdx := aq.getFirstAvailable()
	if minIdx >= 0 {
		return aq.queues[minIdx].PopFront()
	}
	return ""
}

func (aq *ActiveQueues) Push(id string, priority int64) error {
	if priority >= aq.PrioritiesCount() || priority < 0 {
		return qerrors.ERR_UNEXPECTED_PRIORITY
	}
	if aq.queues[priority].Empty() {
		aq.withItems.PushItem(priority)
	}
	aq.queues[priority].PushBack(id)
	return nil
}

func (aq *ActiveQueues) PushFront(id string) {
	aq.queues[0].PushFront(id)
}
