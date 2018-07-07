package pqueue

import "github.com/vburenin/firempq/pmsg"

func geq(l *pmsg.MsgMeta, r *pmsg.MsgMeta) bool {
	lm, rm := l.UnlockTs, r.UnlockTs
	if lm == 0 {
		lm = l.ExpireTs
	}
	if rm == 0 {
		rm = r.ExpireTs
	}

	if lm == rm {
		return l.Serial >= r.Serial
	}

	return lm > rm
}

type TimeoutHeap struct {
	data  []*pmsg.MsgMeta
	index map[uint64]int
}

func SizedTimeoutHeap(size int) *TimeoutHeap {
	return &TimeoutHeap{
		data:  make([]*pmsg.MsgMeta, 0, size),
		index: make(map[uint64]int, size),
	}
}

func NewTimeoutHeap() *TimeoutHeap {
	return &TimeoutHeap{
		data:  make([]*pmsg.MsgMeta, 0, 4096),
		index: make(map[uint64]int, 4096),
	}
}

func (s *TimeoutHeap) Init() {
	n := len(s.data)
	for i := n/2 - 1; i >= 0; i-- {
		s.down(i, n)
	}
}

func (s *TimeoutHeap) Push(msg *pmsg.MsgMeta) {
	sn := msg.Serial
	if _, ok := s.index[sn]; ok {
		s.Remove(sn)
	}
	l := len(s.data)
	s.data = append(s.data, msg)
	s.index[sn] = l
	s.up(l)
}

func (s *TimeoutHeap) Pop() *pmsg.MsgMeta {
	v := s.data[0]
	n := len(s.data) - 1
	s.swap(0, n)
	s.down(0, n)
	s.data = s.data[:n]
	delete(s.index, v.Serial)
	return v
}

func (s *TimeoutHeap) Remove(sn uint64) *pmsg.MsgMeta {
	if i, ok := s.index[sn]; ok {
		v := s.data[i]
		n := len(s.data) - 1
		if n != i {
			s.swap(i, n)
			s.down(i, n)
			s.up(i)
		}
		s.data = s.data[:n]
		delete(s.index, v.Serial)
		return v
	}
	return nil
}

func (s *TimeoutHeap) MinMsg() *pmsg.MsgMeta {
	return s.data[0]
}

func (s *TimeoutHeap) GetMsg(sn uint64) *pmsg.MsgMeta {
	if pos, ok := s.index[sn]; ok {
		return s.data[pos]
	}
	return nil
}

func (s *TimeoutHeap) Len() int {
	return len(s.data)
}

func (s *TimeoutHeap) NotEmpty() bool {
	return len(s.data) > 0
}

func (s *TimeoutHeap) Empty() bool {
	return len(s.data) == 0
}

func (s *TimeoutHeap) ContainsSn(sn uint64) bool {
	_, ok := s.index[sn]
	return ok
}

// Swap and reindex data in heap.
func (s *TimeoutHeap) swap(i, j int) {
	d := s.data
	d[i], d[j] = d[j], d[i]
	s.index[d[i].Serial], s.index[d[j].Serial] = i, j
}

func (s *TimeoutHeap) up(j int) {
	for {
		i := (j - 1) / 2 // parent
		if i == j || geq(s.data[j], s.data[i]) {
			break
		}
		s.swap(i, j)
		j = i
	}
}

func (s TimeoutHeap) down(i, n int) {
	for {
		j1 := 2*i + 1
		if j1 >= n || j1 < 0 {
			break
		}
		j := j1
		if j2 := j1 + 1; j2 < n && geq(s.data[j1], s.data[j2]) {
			j = j2
		}
		if geq(s.data[j], s.data[i]) {
			break
		}
		s.swap(i, j)
		i = j
	}
}
