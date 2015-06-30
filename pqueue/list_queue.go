package pqueue

import "container/list"

type ListQueue struct {
	itemList *list.List
	idxMap   map[string]*list.Element
}

func NewListQueue() *ListQueue {
	return &ListQueue{
		itemList: list.New(),
		idxMap:   make(map[string]*list.Element),
	}
}

func (lq *ListQueue) PushBack(id string) bool {
	_, ok := lq.idxMap[id]
	if !ok {
		el := lq.itemList.PushBack(id)
		lq.idxMap[id] = el
		return true
	} else {
		return false
	}
}

func (lq *ListQueue) PushFront(id string) bool {
	_, ok := lq.idxMap[id]
	if !ok {
		el := lq.itemList.PushFront(id)
		lq.idxMap[id] = el
		return true
	} else {
		return false
	}
}

func (lq *ListQueue) unlink(el *list.Element) string {
	retVal := lq.itemList.Remove(el).(string)
	delete(lq.idxMap, retVal)
	return retVal
}

func (lq *ListQueue) PopFront() string {
	el := lq.itemList.Front()
	return lq.unlink(el)
}

func (lq *ListQueue) PopBack() string {
	el := lq.itemList.Back()
	return lq.unlink(el)
}

func (lq *ListQueue) RemoveById(id string) bool {
	idx, ok := lq.idxMap[id]
	if !ok {
		return false
	}
	lq.unlink(idx)
	return true
}

func (lq *ListQueue) Len() int {
	return len(lq.idxMap)
}

func (lq *ListQueue) Empty() bool {
	return len(lq.idxMap) == 0
}
