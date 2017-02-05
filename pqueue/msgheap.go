package pqueue

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

func (s *MsgHeap) Init() {
	n := len(s.data)
	for i := n/2 - 1; i >= 0; i-- {
		s.down(i, n)
	}
}

func (s *MsgHeap) Push(msg *PQMsgMetaData) {
	sn := msg.SerialNumber
	if _, ok := s.index[sn]; ok {
		s.Remove(sn)
	}
	l := len(s.data)
	s.data = append(s.data, msg)
	s.index[sn] = l
	s.up(l)
}

func (s *MsgHeap) Pop() *PQMsgMetaData {
	v := s.data[0]
	n := len(s.data) - 1
	s.swap(0, n)
	s.down(0, n)
	s.data = s.data[:n]
	delete(s.index, v.SerialNumber)
	return v
}

func (s *MsgHeap) Remove(sn uint64) *PQMsgMetaData {
	if i, ok := s.index[sn]; ok {
		v := s.data[i]
		n := len(s.data) - 1
		if n != i {
			s.swap(i, n)
			s.down(i, n)
			s.up(i)
		}
		s.data = s.data[:n]
		delete(s.index, v.SerialNumber)
		return v
	}
	return nil
}

func (s *MsgHeap) MinMsg() *PQMsgMetaData {
	return s.data[0]
}

func (s *MsgHeap) GetMsg(sn uint64) *PQMsgMetaData {
	if pos, ok := s.index[sn]; ok {
		return s.data[pos]
	}
	return nil
}

func (s *MsgHeap) Len() int {
	return len(s.data)
}

func (s *MsgHeap) NotEmpty() bool {
	return len(s.data) > 0
}

func (s *MsgHeap) Empty() bool {
	return len(s.data) == 0
}

func (s *MsgHeap) ContainsSn(sn uint64) bool {
	_, ok := s.index[sn]
	return ok
}

// Swap and reindex data in heap.
func (s *MsgHeap) swap(i, j int) {
	d := s.data
	d[i], d[j] = d[j], d[i]
	s.index[d[i].SerialNumber], s.index[d[j].SerialNumber] = i, j
}

func (s *MsgHeap) up(j int) {
	for {
		i := (j - 1) / 2 // parent
		if i == j || s.geq(s.data[j], s.data[i]) {
			break
		}
		s.swap(i, j)
		j = i
	}
}

func (s MsgHeap) down(i, n int) {
	for {
		j1 := 2*i + 1
		if j1 >= n || j1 < 0 {
			break
		}
		j := j1
		if j2 := j1 + 1; j2 < n && s.geq(s.data[j1], s.data[j2]) {
			j = j2
		}
		if s.geq(s.data[j], s.data[i]) {
			break
		}
		s.swap(i, j)
		i = j
	}
}
