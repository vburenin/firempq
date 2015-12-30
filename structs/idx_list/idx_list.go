package structs

import "container/list"

type StrIndexList struct {
	itemList *list.List
	idxMap   map[string]*list.Element
}

func NewStrIndexList() *StrIndexList {
	return &StrIndexList{
		itemList: list.New(),
		idxMap:   make(map[string]*list.Element),
	}
}

func (lq *StrIndexList) PushBack(id string) bool {
	_, ok := lq.idxMap[id]
	if !ok {
		el := lq.itemList.PushBack(id)
		lq.idxMap[id] = el
		return true
	} else {
		return false
	}
}

func (lq *StrIndexList) PushFront(id string) bool {
	_, ok := lq.idxMap[id]
	if !ok {
		el := lq.itemList.PushFront(id)
		lq.idxMap[id] = el
		return true
	} else {
		return false
	}
}

func (lq *StrIndexList) unlink(el *list.Element) string {
	retVal := lq.itemList.Remove(el).(string)
	delete(lq.idxMap, retVal)
	return retVal
}

func (lq *StrIndexList) PopFront() string {
	el := lq.itemList.Front()
	return lq.unlink(el)
}

func (lq *StrIndexList) PopBack() string {
	el := lq.itemList.Back()
	return lq.unlink(el)
}

func (lq *StrIndexList) RemoveById(id string) bool {
	idx, ok := lq.idxMap[id]
	if !ok {
		return false
	}
	lq.unlink(idx)
	return true
}

func (lq *StrIndexList) Len() int {
	return len(lq.idxMap)
}

func (lq *StrIndexList) Empty() bool {
	return len(lq.idxMap) == 0
}
