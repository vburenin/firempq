package utils

import "sync"

type BoolFlag struct {
	sync.Mutex
	flag bool
}

func (bf *BoolFlag) SetFalse() {
	bf.Lock()
	bf.flag = false
	bf.Unlock()
}

func (bf *BoolFlag) SetTrue() {
	bf.Lock()
	bf.flag = true
	bf.Unlock()
}

func (bf *BoolFlag) IsTrue() bool {
	bf.Lock()
	r := bf.flag
	bf.Unlock()
	return r
}

func (bf *BoolFlag) IsFalse() bool {
	return !bf.IsTrue()
}
