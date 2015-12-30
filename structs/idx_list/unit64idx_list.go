package structs

import "container/list"

type Uint64IndexList struct {
	itemList list.List
	idxMap   map[uint64]*list.Element
}

func NewUint64IndexList() *Uint64IndexList {
	v := &Uint64IndexList{
		itemList: list.List{},
		idxMap:   make(map[uint64]*list.Element),
	}
	v.itemList.Init()
	return v
}

func (lq *Uint64IndexList) PushBack(id uint64) bool {
	_, ok := lq.idxMap[id]
	if !ok {
		el := lq.itemList.PushBack(id)
		lq.idxMap[id] = el
		return true
	} else {
		return false
	}
}

func (lq *Uint64IndexList) PushFront(id uint64) bool {
	_, ok := lq.idxMap[id]
	if !ok {
		el := lq.itemList.PushFront(id)
		lq.idxMap[id] = el
		return true
	} else {
		return false
	}
}

func (lq *Uint64IndexList) unlink(el *list.Element) uint64 {
	retVal := lq.itemList.Remove(el).(uint64)
	delete(lq.idxMap, retVal)
	return retVal
}

func (lq *Uint64IndexList) PopFront() uint64 {
	el := lq.itemList.Front()
	return lq.unlink(el)
}

func (lq *Uint64IndexList) PopBack() uint64 {
	el := lq.itemList.Back()
	return lq.unlink(el)
}

func (lq *Uint64IndexList) RemoveById(id uint64) bool {
	idx, ok := lq.idxMap[id]
	if !ok {
		return false
	}
	lq.unlink(idx)
	return true
}

func (lq *Uint64IndexList) Len() int {
	return len(lq.idxMap)
}

func (lq *Uint64IndexList) Empty() bool {
	return len(lq.idxMap) == 0
}
