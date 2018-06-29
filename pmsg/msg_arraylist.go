package pmsg

type MessageArrayList struct {
	lists   [][]*MsgMeta
	curList []*MsgMeta
	limit   int
	pos     int
	size    uint64
}

func NewMessageArrayList(rowLimit int) *MessageArrayList {
	return &MessageArrayList{
		curList: make([]*MsgMeta, 0, rowLimit),
		limit:   rowLimit,
		pos:     0,
		size:    0,
	}
}

func (l *MessageArrayList) Add(msg *MsgMeta) {
	l.size++
	l.curList = append(l.curList, msg)
	if len(l.curList) == cap(l.curList) {
		l.lists = append(l.lists, l.curList)
		l.curList = make([]*MsgMeta, 0, l.limit)
	}
}

func (l *MessageArrayList) Size() uint64 {
	return l.size
}

func (l *MessageArrayList) Pop() *MsgMeta {
	if l.size == 0 {
		return nil
	}

	l.size--
	if len(l.lists) > 0 {
		m := l.lists[0][l.pos]
		l.pos++
		if l.pos >= l.limit {
			l.pos = 0
			l.lists = l.lists[1:]
		}
		return m
	}

	m := l.curList[l.pos]
	l.pos++

	return m

}
