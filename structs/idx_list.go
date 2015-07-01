package structs

import "container/list"

type IndexList struct {
	itemList *list.List
	idxMap   map[string]*list.Element
}

func NewListQueue() *IndexList {
	return &IndexList{
		itemList: list.New(),
		idxMap:   make(map[string]*list.Element),
	}
}

func (lq *IndexList) PushBack(id string) bool {
	_, ok := lq.idxMap[id]
	if !ok {
		el := lq.itemList.PushBack(id)
		lq.idxMap[id] = el
		return true
	} else {
		return false
	}
}

func (lq *IndexList) PushFront(id string) bool {
	_, ok := lq.idxMap[id]
	if !ok {
		el := lq.itemList.PushFront(id)
		lq.idxMap[id] = el
		return true
	} else {
		return false
	}
}

func (lq *IndexList) unlink(el *list.Element) string {
	retVal := lq.itemList.Remove(el).(string)
	delete(lq.idxMap, retVal)
	return retVal
}

func (lq *IndexList) PopFront() string {
	el := lq.itemList.Front()
	return lq.unlink(el)
}

func (lq *IndexList) PopBack() string {
	el := lq.itemList.Back()
	return lq.unlink(el)
}

func (lq *IndexList) RemoveById(id string) bool {
	idx, ok := lq.idxMap[id]
	if !ok {
		return false
	}
	lq.unlink(idx)
	return true
}

func (lq *IndexList) Len() int {
	return len(lq.idxMap)
}

func (lq *IndexList) Empty() bool {
	return len(lq.idxMap) == 0
}
