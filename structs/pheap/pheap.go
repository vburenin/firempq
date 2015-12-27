package pheap

type PHeapItem struct {
	Priority int64
	IntId    uint64
}

func (pi PHeapItem) geq(o PHeapItem) bool {
	v := pi.Priority - o.Priority
	if v == 0 {
		return pi.IntId >= o.IntId
	}
	return v > 0
}

type PHeap struct {
	data  []PHeapItem
	index map[uint64]int
}

func NewPHeap() *PHeap {
	return &PHeap{
		data:  make([]PHeapItem, 0, 128),
		index: make(map[uint64]int, 128),
	}
}

func (self *PHeap) Init() {
	n := len(self.data)
	for i := n/2 - 1; i >= 0; i-- {
		self.down(i, n)
	}
}

func (self *PHeap) Push(priority int64, intId uint64) {
	if _, ok := self.index[intId]; ok {
		self.Remove(intId)
	}
	l := len(self.data)
	self.data = append(self.data, PHeapItem{Priority: priority, IntId: intId})
	self.index[intId] = l
	self.up(l)
}

func (self *PHeap) Pop() PHeapItem {
	v := self.data[0]
	n := len(self.data) - 1
	self.swap(0, n)
	self.down(0, n)
	self.data = self.data[:n]
	delete(self.index, v.IntId)
	return v
}

func (self *PHeap) Remove(intId uint64) bool {
	if i, ok := self.index[intId]; ok {
		n := len(self.data) - 1
		if n != i {
			self.swap(i, n)
			self.down(i, n)
			self.up(i)
		}
		v := self.data[n]
		self.data = self.data[:n]
		delete(self.index, v.IntId)
		return true
	}
	return false
}

func (self *PHeap) MinItem() int64 {
	return self.data[0].Priority
}

func (self *PHeap) Len() int {
	return len(self.data)
}

func (self *PHeap) NotEmpty() bool {
	return len(self.data) > 0
}

func (self *PHeap) Empty() bool {
	return len(self.data) == 0
}

func (self *PHeap) ContainsIntId(intId uint64) bool {
	_, ok := self.index[intId]
	return ok
}

// Swap and reindex data in heap.
func (self *PHeap) swap(i, j int) {
	d := self.data
	d[i], d[j] = d[j], d[i]
	self.index[d[i].IntId], self.index[d[j].IntId] = i, j
}

func (self *PHeap) up(j int) {
	for {
		i := (j - 1) / 2 // parent
		if i == j || self.data[j].geq(self.data[i]) {
			break
		}
		self.swap(i, j)
		j = i
	}
}

func (self PHeap) down(i, n int) {
	for {
		j1 := 2*i + 1
		if j1 >= n || j1 < 0 {
			break
		}
		j := j1
		if j2 := j1 + 1; j2 < n && self.data[j1].geq(self.data[j2]) {
			j = j2
		}
		if self.data[j].geq(self.data[i]) {
			break
		}
		self.swap(i, j)
		i = j
	}
}
