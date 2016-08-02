package utils

import (
	"sync"
	"sync/atomic"
)

type BoolFlag struct {
	sync.Mutex
	flag int32
}

func (bf *BoolFlag) SetFalse() {
	bf.Lock()
	bf.flag = 0
	bf.Unlock()
}

func (bf *BoolFlag) SetTrue() {
	bf.Lock()
	bf.flag = 1
	bf.Unlock()
}

func (bf *BoolFlag) IsTrue() bool {
	return atomic.LoadInt32(&bf.flag) > 0
}

func (bf *BoolFlag) IsFalse() bool {
	return atomic.LoadInt32(&bf.flag) == 0
}
