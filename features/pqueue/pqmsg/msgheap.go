package pqmsg

func NewSnHeap() *MsgHeap {
	h := NewMsgHeap()
	h.geq = func(l *PQMsgMetaData, r *PQMsgMetaData) bool {
		v := l.Priority - r.Priority
		if v == 0 {
			return l.SerialNumber >= r.SerialNumber
		}
		return v > 0
	}
	return h
}

func NewTsHeap() *MsgHeap {
	h := NewMsgHeap()
	h.geq = func(l *PQMsgMetaData, r *PQMsgMetaData) bool {
		if l.UnlockTs == 0 {
			if r.UnlockTs == 0 {
				return l.ExpireTs >= r.ExpireTs
			} else {
				return l.ExpireTs >= r.UnlockTs
			}
		} else {
			if r.UnlockTs == 0 {
				return l.UnlockTs >= r.ExpireTs
			} else {
				return l.UnlockTs >= r.UnlockTs
			}
		}
	}
	return h
}

type MsgHeap struct {
	geq   func(*PQMsgMetaData, *PQMsgMetaData) bool
	data  []*PQMsgMetaData
	index map[uint64]int
}

func NewMsgHeap() *MsgHeap {
	return &MsgHeap{
		data:  make([]*PQMsgMetaData, 0, 128),
		index: make(map[uint64]int, 128),
	}
}

func (self *MsgHeap) Init() {
	n := len(self.data)
	for i := n/2 - 1; i >= 0; i-- {
		self.down(i, n)
	}
}

func (self *MsgHeap) Push(msg *PQMsgMetaData) {
	sn := msg.SerialNumber
	if _, ok := self.index[sn]; ok {
		self.Remove(sn)
	}
	l := len(self.data)
	self.data = append(self.data, msg)
	self.index[sn] = l
	self.up(l)
}

func (self *MsgHeap) Pop() *PQMsgMetaData {
	v := self.data[0]
	n := len(self.data) - 1
	self.swap(0, n)
	self.down(0, n)
	self.data = self.data[:n]
	delete(self.index, v.SerialNumber)
	return v
}

func (self *MsgHeap) Remove(sn uint64) *PQMsgMetaData {
	if i, ok := self.index[sn]; ok {
		v := self.data[i]
		n := len(self.data) - 1
		if n != i {
			self.swap(i, n)
			self.down(i, n)
			self.up(i)
		}
		self.data = self.data[:n]
		delete(self.index, v.SerialNumber)
		return v
	}
	return nil
}

func (self *MsgHeap) MinMsg() *PQMsgMetaData {
	return self.data[0]
}

func (self *MsgHeap) GetMsg(sn uint64) *PQMsgMetaData {
	if pos, ok := self.index[sn]; ok {
		return self.data[pos]
	}
	return nil
}

func (self *MsgHeap) Len() int {
	return len(self.data)
}

func (self *MsgHeap) NotEmpty() bool {
	return len(self.data) > 0
}

func (self *MsgHeap) Empty() bool {
	return len(self.data) == 0
}

func (self *MsgHeap) ContainsSn(sn uint64) bool {
	_, ok := self.index[sn]
	return ok
}

// Swap and reindex data in heap.
func (self *MsgHeap) swap(i, j int) {
	d := self.data
	d[i], d[j] = d[j], d[i]
	self.index[d[i].SerialNumber], self.index[d[j].SerialNumber] = i, j
}

func (self *MsgHeap) up(j int) {
	for {
		i := (j - 1) / 2 // parent
		if i == j || self.geq(self.data[j], self.data[i]) {
			break
		}
		self.swap(i, j)
		j = i
	}
}

func (self MsgHeap) down(i, n int) {
	for {
		j1 := 2*i + 1
		if j1 >= n || j1 < 0 {
			break
		}
		j := j1
		if j2 := j1 + 1; j2 < n && self.geq(self.data[j1], self.data[j2]) {
			j = j2
		}
		if self.geq(self.data[j], self.data[i]) {
			break
		}
		self.swap(i, j)
		i = j
	}
}
