package priority_first

import "firempq/structs"
import "firempq/qerrors"

type PriorityFirstQueue struct {
	// The slice of all available sub queues.
	queues []*structs.IndexList

	// Heap of indexes of not empty ListQueues.
	withItems   *structs.IntHeap
	maxPriority int64
}

func NewActiveQueues(size int64) *PriorityFirstQueue {
	queues := make([]*structs.IndexList, size, size)
	maxPriority := size
	for size > 0 {
		size--
		queues[size] = structs.NewListQueue()
	}
	return &PriorityFirstQueue{queues: queues,
		withItems:   structs.NewIntHeap(),
		maxPriority: maxPriority}
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
	return aq.queues[priority].RemoveById(itemId)
}

func (aq *PriorityFirstQueue) PrioritiesCount() int64 {
	return aq.maxPriority
}

func (aq *PriorityFirstQueue) Empty() bool {
	return aq.getFirstAvailable() == -1
}

func (aq *PriorityFirstQueue) Pop() string {
	minIdx := aq.getFirstAvailable()
	if minIdx >= 0 {
		return aq.queues[minIdx].PopFront()
	}
	return ""
}

func (aq *PriorityFirstQueue) Push(id string, priority int64) error {
	if priority >= aq.PrioritiesCount() || priority < 0 {
		return qerrors.ERR_UNEXPECTED_PRIORITY
	}
	if aq.queues[priority].Empty() {
		aq.withItems.PushItem(priority)
	}
	aq.queues[priority].PushBack(id)
	return nil
}

func (aq *PriorityFirstQueue) PushFront(id string) {
	aq.queues[0].PushFront(id)
}
